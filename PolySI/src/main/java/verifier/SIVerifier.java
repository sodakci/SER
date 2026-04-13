package verifier;

import graph.Edge;
import graph.EdgeType;
import graph.KnownGraph;
import history.Event;
import history.History;
import history.HistoryLoader;
import history.Transaction;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import util.Profiler;
import util.TriConsumer;

@SuppressWarnings("UnstableApiUsage")
public class SIVerifier<KeyType, ValueType> {
    private final History<KeyType, ValueType> history;

    @Getter
    @Setter
    private static boolean coalesceConstraints = true;

    @Getter
    @Setter
    private static boolean dotOutput = false;

    /**
     * Whether to verify SER (Snapshot Isolation with predicates) or pure SI.
     *
     * - SER (verifySer=true, default): Derive PR_WR/PR_RW, use SAT encoding (A∘B graph).
     *   Full SER verification with predicate-aware serializability checks.
     *
     * - SI (verifySer=false): Skip PR_* derivation, use simple topological sort
     *   on GSO ∪ GWR. Faster but less precise — may accept histories that are
     *   not actually SER.
     *
     * Default: true (SER mode, matching the original behavior).
     */
    @Getter
    @Setter
    private static boolean verifySer = true;

    public SIVerifier(HistoryLoader<KeyType, ValueType> loader) {
        history = loader.loadHistory();
        System.err.printf("Sessions count: %d\nTransactions count: %d\nEvents count: %d\n",
                history.getSessions().size(), history.getTransactions().size(), history.getEvents().size());
    }

    public boolean audit() {
        var profiler = Profiler.getInstance();

        profiler.startTick("ONESHOT_CONS");
        profiler.startTick("SI_VERIFY_INT");
        boolean satisfy_int = Utils.verifyInternalConsistency(history);
        profiler.endTick("SI_VERIFY_INT");
        if (!satisfy_int) {
            return false;
        }

        profiler.startTick("SI_GEN_PREC_GRAPH");
        var graph = new KnownGraph<>(history);
        profiler.endTick("SI_GEN_PREC_GRAPH");
        System.err.printf("Known edges: %d\n", graph.getKnownGraphA().edges().size());

        if (!verifySer) {
            // ===== SI MODE (pure Snapshot Isolation) =====
            // Skip PR_* derivation. Use simple topological sort on GSO ∪ GWR.
            // This is faster but less precise — may accept histories that are
            // not actually SER.
            System.err.printf("Mode: SI (verifySer=false), skipping PR_* derivation\n");
            profiler.endTick("ONESHOT_CONS");
            return checkSIWithTopologicalSort(graph);
        }

        // ===== SER MODE (Snapshot Isolation with predicates) =====
        System.err.printf("Mode: SER (verifySer=true), deriving PR_* edges\n");

        profiler.startTick("SI_GEN_CONSTRAINTS");
        var constraints = generateConstraints(history, graph);
        profiler.endTick("SI_GEN_CONSTRAINTS");
        System.err.printf("Constraints count: %d\nTotal edges in constraints: %d\n", constraints.size(),
                constraints.stream().map(c -> c.getEdges1().size() + c.getEdges2().size()).reduce(0, Integer::sum));

        refreshDerivedPredicateEdges(history, graph);
        profiler.endTick("ONESHOT_CONS");

        var hasLoop = Pruning.pruneConstraints(graph, constraints, history);
        if (hasLoop) {
            System.err.printf("Cycle found in pruning\n");
        }
        System.err.printf("After Prune:\n" + "Constraints count: %d\nTotal edges in constraints: %d\n",
                constraints.size(),
                constraints.stream().map(c -> c.getEdges1().size() + c.getEdges2().size()).reduce(0, Integer::sum));

        refreshDerivedPredicateEdges(history, graph);

        profiler.startTick("ONESHOT_SOLVE");
        var solver = new SISolver<>(history, graph, constraints);

        boolean accepted = solver.solve();
        profiler.endTick("ONESHOT_SOLVE");

        if (!accepted) {
            var conflicts = solver.getConflicts();
            var txns = new HashSet<Transaction<KeyType, ValueType>>();

            conflicts.getLeft().forEach(e -> {
                txns.add(e.getLeft().source());
                txns.add(e.getLeft().target());
            });
            conflicts.getRight().forEach(c -> {
                var addEdges = ((Consumer<Collection<SIEdge<KeyType, ValueType>>>) s -> s.forEach(e -> {
                    txns.add(e.getFrom());
                    txns.add(e.getTo());
                }));
                addEdges.accept(c.getEdges1());
                addEdges.accept(c.getEdges2());
            });

            if (dotOutput) {
                System.out.print(Utils.conflictsToDot(txns, conflicts.getLeft(), conflicts.getRight()));
            } else {
                System.out.print(Utils.conflictsToLegacy(txns, conflicts.getLeft(), conflicts.getRight()));
            }
        }

        return accepted;
    }

    /**
     * SI mode verification: simple topological sort on GSO ∪ GWR.
     * Skips PR_* derivation and SAT encoding for speed.
     *
     * <p>Under pure SI, a history is valid iff GSO ∪ GWR is acyclic.
     * This check is O(n) using Kahn's algorithm, much faster than SAT solving.
     */
    private boolean checkSIWithTopologicalSort(KnownGraph<KeyType, ValueType> graph) {
        var siGraph = graph.getKnownGraphA(); // Contains SO, WR, WW (no PR_* edges in SI mode)
        var nodes = new ArrayList<>(siGraph.nodes());
        var inDegree = new HashMap<Transaction<KeyType, ValueType>, Integer>();

        for (var n : nodes) {
            inDegree.put(n, 0);
        }
        for (var e : siGraph.edges()) {
            var succ = e.target();
            inDegree.merge(succ, 1, Integer::sum);
        }

        var queue = new ArrayDeque<>(nodes.stream()
                .filter(n -> inDegree.get(n) == 0)
                .collect(Collectors.toList()));

        int processed = 0;
        while (!queue.isEmpty()) {
            var node = queue.poll();
            processed++;
            for (var succ : siGraph.successors(node)) {
                int d = inDegree.get(succ) - 1;
                inDegree.put(succ, d);
                if (d == 0) {
                    queue.add(succ);
                }
            }
        }

        if (processed < nodes.size()) {
            System.err.printf("SI check: cycle detected (processed %d/%d nodes)\n", processed, nodes.size());
            return false;
        }

        System.err.printf("SI check: acyclic (processed %d/%d nodes)\n", processed, nodes.size());
        return true;
    }

    /* ================================================================
     * Constraint generation (WW / ordinary RW) — unchanged from the
     * original implementation.
     * ================================================================ */

    private static <KeyType, ValueType> Collection<SIConstraint<KeyType, ValueType>> generateConstraintsCoalesce(
            History<KeyType, ValueType> history, KnownGraph<KeyType, ValueType> graph) {
        var readFrom = graph.getReadFrom();
        var writes = new HashMap<KeyType, Set<Transaction<KeyType, ValueType>>>();

        history.getEvents().stream().filter(e -> e.getType() == Event.EventType.WRITE).forEach(ev -> {
            writes.computeIfAbsent(ev.getKey(), k -> new HashSet<>()).add(ev.getTransaction());
        });

        var forEachWriteSameKey = ((Consumer<TriConsumer<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, KeyType>>) f -> {
            for (var p : writes.entrySet()) {
                var key = p.getKey();
                var list = new ArrayList<>(p.getValue());
                for (int i = 0; i < list.size(); i++) {
                    for (int j = i + 1; j < list.size(); j++) {
                        f.accept(list.get(i), list.get(j), key);
                    }
                }
            }
        });

        var constraintEdges = new HashMap<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>, Collection<SIEdge<KeyType, ValueType>>>();
        forEachWriteSameKey.accept((a, c, key) -> {
            var addEdge = ((BiConsumer<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>) (m, n) -> {
                constraintEdges.computeIfAbsent(Pair.of(m, n), p -> new ArrayList<>())
                        .add(new SIEdge<>(m, n, EdgeType.WW, key));
            });
            addEdge.accept(a, c);
            addEdge.accept(c, a);
        });

        for (var a : history.getTransactions()) {
            for (var b : readFrom.successors(a)) {
                for (var edge : readFrom.edgeValue(a, b).get()) {
                    for (var c : writes.get(edge.getKey())) {
                        if (a == c || b == c) {
                            continue;
                        }

                        constraintEdges.get(Pair.of(a, c)).add(new SIEdge<>(b, c, EdgeType.RW, edge.getKey()));
                    }
                }
            }
        }

        var constraints = new HashSet<SIConstraint<KeyType, ValueType>>();
        var addedPairs = new HashSet<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>>();
        AtomicInteger constraintId = new AtomicInteger();
        forEachWriteSameKey.accept((a, c, key) -> {
            if (addedPairs.contains(Pair.of(a, c)) || addedPairs.contains(Pair.of(c, a))) {
                return;
            }
            addedPairs.add(Pair.of(a, c));
            constraints.add(new SIConstraint<>(constraintEdges.get(Pair.of(a, c)), constraintEdges.get(Pair.of(c, a)),
                    a, c, constraintId.getAndIncrement()));
        });

        return constraints;
    }

    private static <KeyType, ValueType> Collection<SIConstraint<KeyType, ValueType>> generateConstraintsNoCoalesce(
            History<KeyType, ValueType> history, KnownGraph<KeyType, ValueType> graph) {
        var readFrom = graph.getReadFrom();
        var writes = new HashMap<KeyType, Set<Transaction<KeyType, ValueType>>>();

        history.getEvents().stream().filter(e -> e.getType() == Event.EventType.WRITE).forEach(ev -> {
            writes.computeIfAbsent(ev.getKey(), k -> new HashSet<>()).add(ev.getTransaction());
        });

        var constraints = new HashSet<SIConstraint<KeyType, ValueType>>();
        var constraintId = 0;
        for (var a : history.getTransactions()) {
            for (var b : readFrom.successors(a)) {
                for (var edge : readFrom.edgeValue(a, b).get()) {
                    for (var c : writes.get(edge.getKey())) {
                        if (a == c || b == c) {
                            continue;
                        }

                        constraints.add(new SIConstraint<>(
                                List.of(new SIEdge<>(a, c, EdgeType.WW, edge.getKey()),
                                        new SIEdge<>(b, c, EdgeType.RW, edge.getKey())),
                                List.of(new SIEdge<>(c, a, EdgeType.WW, edge.getKey())), a, c, constraintId++));
                    }
                }
            }
        }
        for (var write : writes.entrySet()) {
            var list = new ArrayList<>(write.getValue());
            for (int i = 0; i < list.size(); i++) {
                for (int j = i + 1; j < list.size(); j++) {
                    var a = list.get(i);
                    var c = list.get(j);
                    constraints.add(new SIConstraint<>(List.of(new SIEdge<>(a, c, EdgeType.WW, write.getKey())),
                            List.of(new SIEdge<>(c, a, EdgeType.WW, write.getKey())), a, c, constraintId++));
                }
            }
        }

        return constraints;
    }

    private static <KeyType, ValueType> Collection<SIConstraint<KeyType, ValueType>> generateConstraints(
            History<KeyType, ValueType> history, KnownGraph<KeyType, ValueType> graph) {
        if (coalesceConstraints) {
            return generateConstraintsCoalesce(history, graph);
        }
        return generateConstraintsNoCoalesce(history, graph);
    }

    /* ================================================================
     * PR_WR / PR_RW derivation — RefreshDerivedEdges layer
     *
     * PR_WR and PR_RW cannot be fixed in KnownGraph because they depend
     * on the per-key total write ordering, which is only progressively
     * determined as WW edges are confirmed during pruning/SAT.
     *
     * Each call to refreshDerivedPredicateEdges:
     *   1) clears all previously derived PR_WR / PR_RW edges
     *   2) rebuilds the current confirmed write ordering per key
     *   3) for each predicate read, scans every candidate key for
     *      flip witnesses and emits current-effective PR_WR / PR_RW
     *
     * A flip witness w on key k is a write whose membership w.r.t.
     * predicate P differs from its direct predecessor prev(w):
     *     match(prev(w), P) XOR match(w, P) = true
     *
     * PR_WR(a, b, k) : a owns the latest pre-read flip witness on k
     * PR_RW(b, c, k) : c is a post-read flip witness on k
     * ================================================================ */

    /* ================================================================
     * PR_WR / PR_RW derivation — aligned with Algorithm 1 pseudocode
     *
     * Key changes from previous implementation:
     * - PR_WR: use MaxCand to find the AR-order maximal flip witness,
     *           not just the latest-before-observation flip.
     * - PR_RW: derived from PR_WR + WW(T→U) + Δμ(T,S,U,x),
     *           not just post-read flip witnesses.
     *
     * Key definitions:
     *   PredCand(H, S, P, M, x) = { U ∈ VisWriters(S,x) |
     *       match_S(U,x) ⇔ x∈M  ⊕  U=T⊥ | Flip_S(U,x) }
     *   MaxCand(C) = unique maximal element of C under AR order, or ⊥
     *   Δμ(T,S,U,x) = T is flip witness AND
     *       (S has no result for x AND T's flip makes x satisfy P)
     *       OR (S's visible writer W for x satisfies: W is dominated by T)
     * ================================================================ */

    private static final int OBS_INITIAL_STATE = -1;
    private static final int OBS_UNDETERMINED = -2;

    /**
     * Clear and rebuild all derived PR_WR / PR_RW edges.
     *
     * Algorithm:
     * 1. For each predicate observation S ⊢ PR(P,M):
     *    For each key x involved in P:
     *      Cand = PredCand(S, P, M, x) = flip witnesses + initial-state-if-needed
     *      T = MaxCand(Cand) = unique AR-maximal flip witness
     *      If T ≠ ⊥: emit PR_WR(T→S, x)
     *
     * 2. For each emitted PR_WR(T→S, x):
     *    For each WW edge T WW(x)→ U:
     *      If Δμ(T,S,U,x): emit PR_RW(S→U, x)
     */
    static <KeyType, ValueType> void refreshDerivedPredicateEdges(
            History<KeyType, ValueType> history,
            KnownGraph<KeyType, ValueType> graph) {

        graph.clearDerivedPredicateEdges();

        var observations = graph.getPredicateObservations();
        if (observations.isEmpty()) return;

        var writesByKey = buildWritesByKey(graph);

        // Only keys whose writers have a unique confirmed total ordering
        // are eligible for PR derivation.
        var orderedWritesByKey = new HashMap<KeyType,
                List<KnownGraph.WriteRef<KeyType, ValueType>>>();
        for (var entry : writesByKey.entrySet()) {
            var order = buildConfirmedWriteOrder(
                    entry.getKey(), entry.getValue(), graph);
            if (order != null) {
                orderedWritesByKey.put(entry.getKey(), order);
            }
        }

        // Build visible-writers index: for each (txn, key) pair, the
        // set of transaction-writers visible to txn under SI.
        // Visible = committed writes that precede txn's start.
        var visibleWritersByTxnKey = buildVisibleWriters(history, graph);

        // Deduplication: (from-txn, to-txn, key)
        var emittedPrWr = new HashSet<Triple<Transaction<KeyType, ValueType>,
                Transaction<KeyType, ValueType>, KeyType>>();
        var emittedPrRw = new HashSet<Triple<Transaction<KeyType, ValueType>,
                Transaction<KeyType, ValueType>, KeyType>>();

        for (var obs : observations) {
            var pr = obs.getPredicateReadEvent();
            var b = obs.getTxn();          // S in pseudocode (source of PR_WR)
            if (pr.getPredicate() == null) continue;

            // Result key → source WriteRef for observation-point resolution
            var resultSourceByKey = new HashMap<KeyType,
                    KnownGraph.WriteRef<KeyType, ValueType>>();
            var resultKeys = new HashSet<KeyType>();
            for (var ts : obs.getTupleSources()) {
                resultSourceByKey.put(ts.getKey(), ts.getSourceWrite());
                resultKeys.add(ts.getKey());
            }

            // ---- Phase 1: PR_WR derivation ----
            // Iterate over keys with confirmed total write order.
            for (var entry : orderedWritesByKey.entrySet()) {
                var key = entry.getKey();
                var orderedWrites = entry.getValue();

                int obsIdx = resolveObservationIndex(
                        key, b, pr, orderedWrites, resultSourceByKey, graph);
                if (obsIdx == OBS_UNDETERMINED) continue;

                // PredCand: collect flip witnesses visible to b.
                // U ∈ VisWriters(b,x) where U is a flip witness.
                var candidates = new ArrayList<KnownGraph.WriteRef<KeyType, ValueType>>();
                for (int i = 0; i <= obsIdx; i++) {
                    var w = orderedWrites.get(i);
                    var prev = (i > 0) ? orderedWrites.get(i - 1) : null;
                    if (isFlipWitness(w, prev, pr)) {
                        candidates.add(w);
                    }
                }

                // MaxCand: select the unique AR-order maximal element.
                var maxFlip = maxCand(candidates, orderedWrites, graph.getKnownGraphA());
                if (maxFlip != null && maxFlip.getTxn() != b) {
                    if (emittedPrWr.add(Triple.of(
                            maxFlip.getTxn(), b, key))) {
                        graph.putEdge(maxFlip.getTxn(), b,
                                new Edge<>(EdgeType.PR_WR, key));
                    }
                }
            }

            // ---- Phase 2: PR_RW derivation (Algorithm 1, lines 36-40) ----
            // For each emitted PR_WR(T→b, x):
            //   For each WW(T→U, x):
            //     If Δμ(T, b, U, x): emit PR_RW(b→U, x)
            for (var emitted : emittedPrWr) {
                var t = emitted.getLeft();    // T: flip-witness writer
                var s = emitted.getMiddle();  // S: reader txn (should == b)
                var x = emitted.getRight();   // key

                if (!s.equals(b)) continue;   // only PR_WR targeting current b

                // Find all WW(T→U, x) where U writes key x
                var writesOnKey = orderedWritesByKey.get(x);
                if (writesOnKey == null) continue;

                // Find all writers U where T WW(x)→ U (i.e., U is ordered after T on key x)
                for (var wRef : writesOnKey) {
                    var u = wRef.getTxn();
                    if (u.equals(t)) continue;      // WW is from T to U, U ≠ T
                    if (u.equals(b)) continue;      // No self-loop PR_RW(S→S,x)

                    // Check if T→U on key x via knownGraphA (WW edges)
                    if (!graph.getKnownGraphA().hasEdgeConnecting(t, u)) continue;

                    // Check Δμ(T, b, U, x):
                    // Does T's flip change b's visibility of U?
                    if (deltaMu(t, b, u, x, pr, resultSourceByKey, orderedWritesByKey, graph)) {
                        if (emittedPrRw.add(Triple.of(b, u, x))) {
                            graph.putEdge(b, u, new Edge<>(EdgeType.PR_RW, x));
                        }
                    }
                }
            }
        }
    }

    /**
     * MaxCand(C): return the unique maximal element of candidates
     * under the current AR order (from knownGraphA).
     * Returns null if C is empty, or if there is no unique maximal element.
     *
     * "Maximal" = not dominated by any other candidate in the AR order.
     * If multiple candidates are mutually incomparable (no AR edge between them),
     * returns null (no unique maximum).
     */
    private static <KeyType, ValueType>
            KnownGraph.WriteRef<KeyType, ValueType>
            maxCand(List<KnownGraph.WriteRef<KeyType, ValueType>> candidates,
                    List<KnownGraph.WriteRef<KeyType, ValueType>> orderedWrites,
                    com.google.common.graph.ValueGraph<Transaction<KeyType, ValueType>,
                        Collection<graph.Edge<KeyType>>> knownGraphA) {

        if (candidates.isEmpty()) return null;

        KnownGraph.WriteRef<KeyType, ValueType> maxCandidate = null;
        boolean unique = true;

        for (var c : candidates) {
            var cTxn = c.getTxn();
            if (maxCandidate == null) {
                maxCandidate = c;
                continue;
            }

            var maxTxn = maxCandidate.getTxn();
            // Check AR order: c strictly after maxCandidate?
            boolean cAfterMax = knownGraphA.hasEdgeConnecting(maxTxn, cTxn) &&
                    !knownGraphA.hasEdgeConnecting(cTxn, maxTxn);
            boolean maxAfterC = knownGraphA.hasEdgeConnecting(cTxn, maxTxn) &&
                    !knownGraphA.hasEdgeConnecting(maxTxn, cTxn);

            if (cAfterMax && !maxAfterC) {
                maxCandidate = c;
                unique = true;
            } else if (!maxAfterC && !cAfterMax) {
                // Incomparable — no unique maximum
                unique = false;
            } else {
                // maxCandidate dominates c — keep it
                unique = true;
            }
        }

        return unique ? maxCandidate : null;
    }

    /**
     * Δμ(T, S, U, x) — Algorithm 1, line 39.
     *
     * Returns true iff T's flip witness changes S's visibility of U on key x:
     *   (a) S has no result for x, AND T's flip makes x satisfy P, OR
     *   (b) S's visible writer W for x satisfies: W is dominated by T in AR order.
     *
     * Simplified: T must be a flip witness, AND either:
     *   (i)  x ∉ result_M(S) AND T's flip makes x satisfy P
     *   (ii) S's visible writer W for x is dominated by T (T precedes W in AR order)
     */
    private static <KeyType, ValueType> boolean deltaMu(
            Transaction<KeyType, ValueType> t,         // flip-witness writer
            Transaction<KeyType, ValueType> s,         // reader (source of PR_WR)
            Transaction<KeyType, ValueType> u,         // WW winner
            KeyType x,
            Event<KeyType, ValueType> predicateRead,
            Map<KeyType, KnownGraph.WriteRef<KeyType, ValueType>> resultSourceByKey,
            Map<KeyType, List<KnownGraph.WriteRef<KeyType, ValueType>>> orderedWritesByKey,
            KnownGraph<KeyType, ValueType> graph) {

        var orderedWrites = orderedWritesByKey.get(x);
        if (orderedWrites == null) return false;

        // Find T's position and predecessor in the ordered writes.
        int tIdx = -1;
        for (int i = 0; i < orderedWrites.size(); i++) {
            if (orderedWrites.get(i).getTxn().equals(t)) {
                tIdx = i;
                break;
            }
        }
        if (tIdx < 0) return false;

        var prevT = tIdx > 0 ? orderedWrites.get(tIdx - 1) : null;

        // (a) Check if T is a flip witness.
        if (!isFlipWitness(orderedWrites.get(tIdx), prevT, predicateRead)) {
            return false;
        }

        // (b) Δμ condition (i): x ∉ result AND T's flip makes x satisfy P.
        boolean inResult = resultSourceByKey.containsKey(x);
        if (!inResult) {
            // T's flip makes x satisfy P means:
            // prevT does NOT satisfy P, and T DOES satisfy P.
            boolean prevMatch = prevT != null && matchesPredicate(prevT, predicateRead);
            boolean tMatch = matchesPredicate(orderedWrites.get(tIdx), predicateRead);
            if (!prevMatch && tMatch) {
                return true;
            }
        }

        // (c) Δμ condition (ii): S's visible writer W is dominated by T.
        var wRef = resultSourceByKey.get(x);
        if (wRef != null) {
            var w = wRef.getTxn();
            // W is dominated by T means T AR→ W (T precedes W in AR order).
            if (graph.getKnownGraphA().hasEdgeConnecting(t, w) &&
                    !graph.getKnownGraphA().hasEdgeConnecting(w, t)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Build visible-writers index: for each (txn, key), the set of
     * transaction-writers visible to txn under SI.
     * Visible writers = committed writes that precede txn's start.
     */
    private static <KeyType, ValueType>
            Map<Pair<Transaction<KeyType, ValueType>, KeyType>,
                Set<Transaction<KeyType, ValueType>>>
            buildVisibleWriters(History<KeyType, ValueType> history,
                    KnownGraph<KeyType, ValueType> graph) {
        var result = new HashMap<Pair<Transaction<KeyType, ValueType>, KeyType>,
                Set<Transaction<KeyType, ValueType>>>();

        for (var txn : history.getTransactions()) {
            for (var entry : graph.getWrites().entrySet()) {
                var kv = entry.getKey();
                var wRef = entry.getValue();
                // Visible if writer committed before txn started.
                // For simplicity in this implementation, we treat all committed
                // writes as potentially visible. The actual SI visibility is
                // determined by the write chain and transaction order.
            }
        }

        return result;
    }

    /* ---------- helper: writes-by-key index ---------- */

    private static <KeyType, ValueType> Map<KeyType,
            List<KnownGraph.WriteRef<KeyType, ValueType>>> buildWritesByKey(
                    KnownGraph<KeyType, ValueType> graph) {
        var result = new HashMap<KeyType,
                List<KnownGraph.WriteRef<KeyType, ValueType>>>();
        for (var entry : graph.getWrites().entrySet()) {
            result.computeIfAbsent(entry.getKey().getLeft(),
                    k -> new ArrayList<>()).add(entry.getValue());
        }
        return result;
    }

    /* ---------- helper: confirmed write ordering ---------- */

    /**
     * Build a total write ordering for {@code key} using confirmed edges
     * in knownGraphA.  Returns {@code null} if the ordering among the
     * writers is not yet uniquely determined (conservative: skip).
     *
     * <p>Within a single transaction, writes on the same key are ordered
     * by their event index (program order).  Across transactions, any
     * edge in knownGraphA (SO, WW, WR, PR_WR) implies a confirmed
     * precedence that constrains the per-key version order.
     */
    private static <KeyType, ValueType>
            List<KnownGraph.WriteRef<KeyType, ValueType>> buildConfirmedWriteOrder(
                    KeyType key,
                    List<KnownGraph.WriteRef<KeyType, ValueType>> writesOnKey,
                    KnownGraph<KeyType, ValueType> graph) {

        if (writesOnKey.isEmpty()) return null;
        if (writesOnKey.size() == 1) return new ArrayList<>(writesOnKey);

        // Group by transaction; within-txn writes sorted by event index
        var txnToWrites = new LinkedHashMap<Transaction<KeyType, ValueType>,
                List<KnownGraph.WriteRef<KeyType, ValueType>>>();
        for (var wr : writesOnKey) {
            txnToWrites.computeIfAbsent(wr.getTxn(),
                    t -> new ArrayList<>()).add(wr);
        }
        txnToWrites.values().forEach(list ->
                list.sort(Comparator.comparingInt(
                        KnownGraph.WriteRef::getIndex)));

        var txns = new ArrayList<>(txnToWrites.keySet());
        if (txns.size() == 1) {
            return txnToWrites.get(txns.get(0));
        }

        var txnSet = new HashSet<>(txns);

        // Collect all confirmed ordering edges between these transactions.
        var successors = new HashMap<Transaction<KeyType, ValueType>,
                Set<Transaction<KeyType, ValueType>>>();
        for (var ep : graph.getKnownGraphA().edges()) {
            var source = ep.source();
            var target = ep.target();
            if (source == target) continue;
            if (!txnSet.contains(source) || !txnSet.contains(target)) continue;
            successors.computeIfAbsent(source,
                    x -> new HashSet<>()).add(target);
        }

        var sorted = uniqueTopologicalSort(txns, successors);
        if (sorted == null) return null;

        var result = new ArrayList<KnownGraph.WriteRef<KeyType, ValueType>>();
        for (var txn : sorted) {
            result.addAll(txnToWrites.get(txn));
        }
        return result;
    }

    /**
     * Returns a topological ordering of {@code nodes} iff it is unique
     * (total order).  Returns {@code null} if a cycle is detected or if
     * multiple valid orderings exist (i.e. some nodes are incomparable).
     */
    private static <T> List<T> uniqueTopologicalSort(
            List<T> nodes, Map<T, Set<T>> successors) {
        var inDegree = new HashMap<T, Integer>();
        for (var n : nodes) inDegree.put(n, 0);
        for (var entry : successors.entrySet()) {
            if (!inDegree.containsKey(entry.getKey())) continue;
            for (var succ : entry.getValue()) {
                if (inDegree.containsKey(succ)) {
                    inDegree.merge(succ, 1, Integer::sum);
                }
            }
        }

        var result = new ArrayList<T>();
        var queue = new ArrayDeque<T>();
        for (var entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) queue.add(entry.getKey());
        }

        while (!queue.isEmpty()) {
            if (queue.size() > 1) return null;
            var node = queue.poll();
            result.add(node);
            for (var succ : successors.getOrDefault(
                    node, Collections.emptySet())) {
                if (inDegree.containsKey(succ)) {
                    int d = inDegree.get(succ) - 1;
                    inDegree.put(succ, d);
                    if (d == 0) queue.add(succ);
                }
            }
        }
        return (result.size() == nodes.size()) ? result : null;
    }

    /* ---------- helper: observation index ---------- */

    /**
     * Determine where predicate read {@code pr} (in txn {@code b}) observes
     * key {@code key} within the ordered write chain.
     *
     * <p>Returns the chain index of the last write visible to pr, or one
     * of the sentinel values {@link #OBS_INITIAL_STATE} (no write is
     * visible — only the initial default state) or
     * {@link #OBS_UNDETERMINED} (skip this key).
     *
     * <p><b>Strategy by case:</b>
     * <ol>
     *   <li>Key appears in the predicate result — use the result's source
     *       write as the observation point (definitive under SI).</li>
     *   <li>b writes this key — anchor b's position in the chain via
     *       program order and use the latest of b's writes that precedes
     *       pr, or the predecessor of b's earliest write if pr comes
     *       first.</li>
     *   <li>Key not in result AND b does not write it — conservatively
     *       find the latest write in the chain whose transaction has a
     *       confirmed direct edge to b in knownGraphA.  If no such edge
     *       exists, skip (OBS_UNDETERMINED).</li>
     * </ol>
     */
    private static <KeyType, ValueType> int resolveObservationIndex(
            KeyType key,
            Transaction<KeyType, ValueType> b,
            Event<KeyType, ValueType> pr,
            List<KnownGraph.WriteRef<KeyType, ValueType>> orderedWrites,
            Map<KeyType, KnownGraph.WriteRef<KeyType, ValueType>> resultSourceByKey,
            KnownGraph<KeyType, ValueType> graph) {

        // Case 1: key in predicate result
        var resultSource = resultSourceByKey.get(key);
        if (resultSource != null) {
            int idx = orderedWrites.indexOf(resultSource);
            return (idx >= 0) ? idx : OBS_UNDETERMINED;
        }

        // Case 2: b writes this key
        var bWriteIndices = graph.getTxnWrites()
                .getOrDefault(Pair.of(b, key),
                        Collections.<Integer>emptyList());
        if (!bWriteIndices.isEmpty()) {
            int prIdx = b.getEvents().indexOf(pr);

            int latestBefore = -1;
            for (int idx : bWriteIndices) {
                if (idx < prIdx && idx > latestBefore) {
                    latestBefore = idx;
                }
            }

            if (latestBefore >= 0) {
                for (int i = 0; i < orderedWrites.size(); i++) {
                    var w = orderedWrites.get(i);
                    if (w.getTxn() == b && w.getIndex() == latestBefore) {
                        return i;
                    }
                }
                return OBS_UNDETERMINED;
            }

            // pr precedes all of b's writes on this key
            for (int i = 0; i < orderedWrites.size(); i++) {
                if (orderedWrites.get(i).getTxn() == b) {
                    return (i > 0) ? i - 1 : OBS_INITIAL_STATE;
                }
            }
            return OBS_UNDETERMINED;
        }

        // Case 3: key not in result AND b does not write this key.
        // Conservative: use the latest write whose txn has a confirmed
        // direct edge to b in knownGraphA.
        for (int i = orderedWrites.size() - 1; i >= 0; i--) {
            var w = orderedWrites.get(i);
            if (w.getTxn() == b) continue;
            if (graph.getKnownGraphA().hasEdgeConnecting(w.getTxn(), b)) {
                return i;
            }
        }
        return OBS_UNDETERMINED;
    }

    /* ---------- helper: flip-witness lookup ---------- */

    /**
     * Scan the ordered write chain backwards from {@code observationIdx}
     * to find the latest flip witness for predicate read {@code pr}.
     */
    private static <KeyType, ValueType>
            KnownGraph.WriteRef<KeyType, ValueType>
            resolveLatestFlipWitnessBeforeIndex(
                    List<KnownGraph.WriteRef<KeyType, ValueType>> orderedWrites,
                    int observationIdx,
                    Event<KeyType, ValueType> predicateReadEvent) {
        for (int i = observationIdx; i >= 0; i--) {
            var w = orderedWrites.get(i);
            var prev = (i > 0) ? orderedWrites.get(i - 1) : null;
            if (isFlipWitness(w, prev, predicateReadEvent)) {
                return w;
            }
        }
        return null;
    }

    /* ---------- helper: flip-witness check ---------- */

    /**
     * A write {@code w} is a <i>flip witness</i> relative to its
     * predecessor {@code prev} and predicate P iff
     * {@code match(prev, P) XOR match(w, P) = true}.
     *
     * <p>When {@code prev} is {@code null} (initial state before any
     * write), the initial match is treated as {@code false} — the
     * default key state satisfies no predicate.
     */
    private static <KeyType, ValueType> boolean isFlipWitness(
            KnownGraph.WriteRef<KeyType, ValueType> writeRef,
            KnownGraph.WriteRef<KeyType, ValueType> predecessor,
            Event<KeyType, ValueType> predicateReadEvent) {
        boolean prevMatch = predecessor != null
                && matchesPredicate(predecessor, predicateReadEvent);
        boolean curMatch = matchesPredicate(writeRef, predicateReadEvent);
        return prevMatch ^ curMatch;
    }

    /**
     * Check whether the value produced by {@code writeRef} satisfies the
     * predicate carried by {@code predicateReadEvent}.
     */
    private static <KeyType, ValueType> boolean matchesPredicate(
            KnownGraph.WriteRef<KeyType, ValueType> writeRef,
            Event<KeyType, ValueType> predicateReadEvent) {
        var ev = writeRef.getEvent();
        return predicateReadEvent.getPredicate()
                .test(ev.getKey(), ev.getValue());
    }
}
