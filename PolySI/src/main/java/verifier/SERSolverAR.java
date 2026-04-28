package verifier;

import graph.Edge;
import graph.EdgeType;
import graph.KnownGraph;
import history.Event;
import history.History;
import history.Transaction;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;
import com.google.common.graph.EndpointPair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Encodes serializability as a SAT problem over an arbitration order (AR).
 *
 * <p>Each non-reflexive pair of transactions gets one Boolean literal
 * {@code ar[i][j]}, meaning transaction {@code i} is ordered before transaction
 * {@code j}.  The solver constrains these literals to form a strict total
 * order, then adds the known precedence edges, unresolved WW choices, derived
 * RW edges, and predicate-read visibility constraints on top of that order.</p>
 */
class SERSolverAR<KeyType, ValueType> {
    private final History<KeyType, ValueType> history;
    private final KnownGraph<KeyType, ValueType> graph;
    private final Collection<SERConstraint<KeyType, ValueType>> constraints;
    private final Solver solver = new Solver();
    private final boolean collectConflicts;

    private final List<Transaction<KeyType, ValueType>> txns;
    private final Map<Transaction<KeyType, ValueType>, Integer> txnIndex;
    // ar[i][j] is true iff txns[i] precedes txns[j] in the candidate serial order.
    private final Lit[][] ar;
    // Per-key write lists provide the local write order candidates used by WW/RW
    // and predicate-read encodings.
    private final Map<KeyType, List<KnownGraph.WriteRef<KeyType, ValueType>>> writesByKey;
    private Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> conflictEdges =
            Collections.emptyList();
    private Collection<SERConstraint<KeyType, ValueType>> conflictConstraints = Collections.emptyList();

    SERSolverAR(History<KeyType, ValueType> history,
                KnownGraph<KeyType, ValueType> graph,
                Collection<SERConstraint<KeyType, ValueType>> constraints) {
        this(history, graph, constraints, true);
    }

    private SERSolverAR(History<KeyType, ValueType> history,
                        KnownGraph<KeyType, ValueType> graph,
                        Collection<SERConstraint<KeyType, ValueType>> constraints,
                        boolean collectConflicts) {
        this.history = history;
        this.graph = graph;
        this.constraints = constraints;
        this.collectConflicts = collectConflicts;
        this.txns = history.getTransactions().stream()
                .filter(txn -> !isBottomTxn(txn))
                .collect(Collectors.toList());
        this.txnIndex = new HashMap<>();
        for (int i = 0; i < txns.size(); i++) {
            txnIndex.put(txns.get(i), i);
        }
        this.ar = createArMatrix();
        this.writesByKey = buildWritesByKey(graph);
        encodeStrictTotalOrder();
        encodeKnownEdges();
        encodeRemainingWwChoices();
        encodeRwFromWrAndWw();
        encodePredicateConstraints();
    }

    /**
     * Solves the AR encoding.  On UNSAT, the outer solver instance can collect
     * a reduced explanation; recursive satisfiability checks disable that work.
     */
    boolean solve() {
        boolean sat = solver.solve();
        if (sat || !collectConflicts) {
            conflictEdges = Collections.emptyList();
            conflictConstraints = Collections.emptyList();
            return sat;
        }

        extractConflicts();
        return false;
    }

    Pair<Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>, Collection<SERConstraint<KeyType, ValueType>>> getConflicts() {
        return Pair.of(conflictEdges, conflictConstraints);
    }

    int getArVariableCount() {
        int count = 0;
        for (int i = 0; i < txns.size(); i++) {
            for (int j = 0; j < txns.size(); j++) {
                if (i != j && ar[i][j] != Lit.False) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Allocates one SAT variable for each directed transaction pair.  Reflexive
     * entries are permanently false because a transaction cannot precede itself.
     */
    private Lit[][] createArMatrix() {
        var result = new Lit[txns.size()][txns.size()];
        for (int i = 0; i < txns.size(); i++) {
            for (int j = 0; j < txns.size(); j++) {
                if (i == j) {
                    result[i][j] = Lit.False;
                } else {
                    result[i][j] = new Lit(solver);
                    solver.setDecisionLiteral(result[i][j], true);
                }
            }
        }
        return result;
    }

    /**
     * Enforces AR as a strict total order: every pair has exactly one direction,
     * and the chosen directed graph is acyclic.
     */
    private void encodeStrictTotalOrder() {
        for (int i = 0; i < txns.size(); i++) {
            for (int j = i + 1; j < txns.size(); j++) {
                solver.assertTrue(Logic.xor(ar[i][j], ar[j][i]));
            }
        }

        var monoGraph = new monosat.Graph(solver);
        var nodes = new int[txns.size()];
        for (int i = 0; i < txns.size(); i++) {
            nodes[i] = monoGraph.addNode();
        }
        for (int i = 0; i < txns.size(); i++) {
            for (int j = 0; j < txns.size(); j++) {
                if (i == j) {
                    continue;
                }
                var edge = monoGraph.addEdge(nodes[i], nodes[j]);
                solver.assertEqual(edge, ar[i][j]);
            }
        }
        solver.assertTrue(monoGraph.acyclic());
    }

    /**
     * Existing precedence edges are mandatory AR edges.  The graph keeps two
     * known-edge partitions, but both must be respected by the same order.
     */
    private void encodeKnownEdges() {
        encodeKnownGraphRespectAr(graph.getKnownGraphA());
        encodeKnownGraphRespectAr(graph.getKnownGraphB());
    }

    private void encodeKnownGraphRespectAr(com.google.common.graph.ValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> known) {
        for (var ep : known.edges()) {
            var edges = known.edgeValue(ep).orElse(Collections.emptyList());
            if (edges.stream().anyMatch(edge -> isEncodedKnownEdge(edge.getType()))) {
                solver.assertTrue(ar(ep.source(), ep.target()));
            }
        }
    }

    /**
     * Encodes each unresolved WW pair as a binary choice.  Choosing one write
     * direction also activates the dependent edges generated for that branch.
     */
    private void encodeRemainingWwChoices() {
        for (var c : constraints) {
            var forward = ar(c.getWriteTransaction1(), c.getWriteTransaction2());
            var backward = ar(c.getWriteTransaction2(), c.getWriteTransaction1());
            solver.assertTrue(Logic.xor(forward, backward));

            for (var edge : c.getEdges1()) {
                solver.assertTrue(Logic.implies(forward, ar(edge.getFrom(), edge.getTo())));
            }
            for (var edge : c.getEdges2()) {
                solver.assertTrue(Logic.implies(backward, ar(edge.getFrom(), edge.getTo())));
            }
        }
    }

    /**
     * Derives ordinary RW edges directly in SAT from WR and WW order:
     * if T' writes a value read by T, and T' is ordered before another writer U
     * of the same key, then T must be ordered before U.
     */
    private void encodeRwFromWrAndWw() {
        for (var ep : graph.getReadFrom().edges()) {
            var readers = graph.getReadFrom().edgeValue(ep.source(), ep.target()).orElse(Collections.emptyList());
            for (var wrEdge : readers) {
                var key = wrEdge.getKey();
                for (var writer : this.writesByKey.getOrDefault(key, Collections.emptyList())) {
                    var u = writer.getTxn();
                    if (u.equals(ep.source()) || u.equals(ep.target())) {
                        continue;
                    }
                    // Algorithm 1, lines 28-30:
                    // if T' --WR(x)--> T and T' --WW(x)--> U then T --RW(x)--> U.
                    solver.assertTrue(Logic.implies(ar(ep.source(), u), ar(ep.target(), u)));
                }
            }
        }
    }

    /**
     * Encodes predicate-read consistency without pre-materializing PR_* edges.
     *
     * <p>For each observed predicate read and each key, the encoding identifies
     * which writes are visible to the read, the latest visible write frontier,
     * and the writes whose value flips predicate membership.  Maximal candidate
     * witnesses then imply the same AR edges that the predicate derivation would
     * otherwise add to the known graph.</p>
     */
    private void encodePredicateConstraints() {
        for (var observation : graph.getPredicateObservations()) {
            var predicateRead = observation.getPredicateReadEvent();
            if (predicateRead.getPredicate() == null) {
                continue;
            }

            var resultSourcesByKey = observation.getTupleSources().stream()
                    .collect(Collectors.toMap(KnownGraph.PredicateTupleSource::getKey, KnownGraph.PredicateTupleSource::getSourceWrite));

            for (var entry : writesByKey.entrySet()) {
                var key = entry.getKey();
                var writes = entry.getValue();
                if (writes.isEmpty()) {
                    continue;
                }

                boolean keyInResult = resultSourcesByKey.containsKey(key);
                var visibleWriters = createVisibleWriterLits(observation, predicateRead, key, writes, resultSourcesByKey);
                var observationFrontier = createObservationFrontierLits(visibleWriters, writes);
                assertObservationFrontierMatchesResult(keyInResult, observationFrontier, writes, predicateRead);
                var flips = createFlipLits(writes, predicateRead);
                var candidates = new ArrayList<Lit>(writes.size());
                for (int i = 0; i < writes.size(); i++) {
                    boolean writeMatchesMembership = matchesPredicate(writes.get(i), predicateRead) == keyInResult;
                    candidates.add(Logic.and(
                            visibleUnderFrontier(i, observationFrontier, writes),
                            flips.get(i),
                            writeMatchesMembership ? Lit.True : Lit.False));
                }

                var maxCandidates = new ArrayList<Lit>(writes.size());
                for (int i = 0; i < writes.size(); i++) {
                    Lit noLaterCandidate = Lit.True;
                    for (int j = 0; j < writes.size(); j++) {
                        if (i == j) {
                            continue;
                        }
                        noLaterCandidate = Logic.and(noLaterCandidate,
                                Logic.not(Logic.and(candidates.get(j), beforeWrite(writes.get(i), writes.get(j)))));
                    }
                    maxCandidates.add(Logic.and(candidates.get(i), noLaterCandidate));
                }
                var initialStateCandidate = createInitialStateCandidate(keyInResult, visibleWriters, candidates);
                for (int i = 0; i < writes.size(); i++) {
                    for (int j = i + 1; j < writes.size(); j++) {
                        solver.assertTrue(Logic.not(Logic.and(maxCandidates.get(i), maxCandidates.get(j))));
                    }
                    solver.assertTrue(Logic.not(Logic.and(initialStateCandidate, maxCandidates.get(i))));
                }

                if (!keyInResult) {
                    for (int j = 0; j < writes.size(); j++) {
                        var u = writes.get(j).getTxn();
                        if (u.equals(observation.getTxn())) {
                            continue;
                        }

                        var predRw = Logic.and(
                                initialStateCandidate,
                                deltaMu(predicateRead, key, writes, j, observationFrontier, resultSourcesByKey));
                        solver.assertTrue(Logic.implies(predRw, ar(observation.getTxn(), u)));
                    }
                }

                for (int i = 0; i < writes.size(); i++) {
                    var t = writes.get(i).getTxn();
                    if (!t.equals(observation.getTxn())) {
                        solver.assertTrue(Logic.implies(maxCandidates.get(i), ar(t, observation.getTxn())));
                    }

                    for (int j = 0; j < writes.size(); j++) {
                        if (i == j) {
                            continue;
                        }
                        var u = writes.get(j).getTxn();
                        if (u.equals(observation.getTxn()) || u.equals(t)) {
                            continue;
                        }

                        var predRw = Logic.and(
                                maxCandidates.get(i),
                                beforeWrite(writes.get(i), writes.get(j)),
                                deltaMu(predicateRead, key, writes, j, observationFrontier, resultSourcesByKey));
                        solver.assertTrue(Logic.implies(predRw, ar(observation.getTxn(), u)));
                    }
                }
            }
        }
    }

    /**
     * Represents the initial database state as the maximal witness when a key is
     * absent from the predicate result and no concrete visible write can explain
     * the absence.
     */
    private Lit createInitialStateCandidate(
            boolean keyInResult,
            List<Lit> visibleWriters,
            List<Lit> candidates) {
        if (keyInResult) {
            return Lit.False;
        }

        Lit noVisibleWriter = Lit.True;
        for (var visible : visibleWriters) {
            noVisibleWriter = Logic.and(noVisibleWriter, Logic.not(visible));
        }

        Lit noConcreteCandidate = Lit.True;
        for (var candidate : candidates) {
            noConcreteCandidate = Logic.and(noConcreteCandidate, Logic.not(candidate));
        }

        // Explicit T_bottom candidate from Algorithm 1 / PredCand:
        // if x is absent from M and no visible writer contributes a concrete flip witness,
        // the maximal candidate is the initial state rather than a transaction writer.
        return Logic.and(noVisibleWriter, noConcreteCandidate);
    }

    /**
     * For one predicate observation and key, computes whether each write can be
     * visible to the predicate read under the current AR assignment.
     */
    private List<Lit> createVisibleWriterLits(KnownGraph.PredicateObservation<KeyType, ValueType> observation,
                                              Event<KeyType, ValueType> predicateRead,
                                              KeyType key,
                                              List<KnownGraph.WriteRef<KeyType, ValueType>> writes,
                                              Map<KeyType, KnownGraph.WriteRef<KeyType, ValueType>> resultSourcesByKey) {
        var resultSource = resultSourcesByKey.get(key);
        var result = new ArrayList<Lit>(writes.size());
        int predicateIndex = observation.getTxn().getEvents().indexOf(predicateRead);
        if (resultSource != null) {
            if (resultSource.getTxn().equals(observation.getTxn())) {
                solver.assertTrue(resultSource.getIndex() < predicateIndex ? Lit.True : Lit.False);
            } else {
                solver.assertTrue(ar(resultSource.getTxn(), observation.getTxn()));
            }
        }
        for (var write : writes) {
            if (resultSource != null) {
                solver.assertTrue(Logic.implies(
                        beforeWrite(resultSource, write),
                        Logic.not(visibleToPredicateRead(write, observation, predicateRead))));
                result.add(beforeOrEqualWrite(write, resultSource));
                continue;
            }
            if (write.getTxn().equals(observation.getTxn())) {
                result.add(write.getIndex() < predicateIndex ? Lit.True : Lit.False);
                continue;
            }
            result.add(ar(write.getTxn(), observation.getTxn()));
        }
        return result;
    }

    private Lit visibleToPredicateRead(KnownGraph.WriteRef<KeyType, ValueType> write,
                                       KnownGraph.PredicateObservation<KeyType, ValueType> observation,
                                       Event<KeyType, ValueType> predicateRead) {
        if (write.getTxn().equals(observation.getTxn())) {
            return write.getIndex() < observation.getTxn().getEvents().indexOf(predicateRead) ? Lit.True : Lit.False;
        }
        return ar(write.getTxn(), observation.getTxn());
    }

    /**
     * The selected latest visible write must agree with the observed
     * predicate-result membership for this key.
     */
    private void assertObservationFrontierMatchesResult(
            boolean keyInResult,
            List<Lit> observationFrontier,
            List<KnownGraph.WriteRef<KeyType, ValueType>> writes,
            Event<KeyType, ValueType> predicateRead) {
        for (int i = 0; i < writes.size(); i++) {
            if (matchesPredicate(writes.get(i), predicateRead) != keyInResult) {
                solver.assertTrue(Logic.not(observationFrontier.get(i)));
            }
        }
    }

    /**
     * Marks writes that can change predicate membership relative to the
     * immediately preceding write of the same key.
     */
    private List<Lit> createFlipLits(List<KnownGraph.WriteRef<KeyType, ValueType>> writes,
                                     Event<KeyType, ValueType> predicateRead) {
        var result = new ArrayList<Lit>(writes.size());
        for (int i = 0; i < writes.size(); i++) {
            var write = writes.get(i);
            Lit flip = matchesPredicate(write, predicateRead) ? noPredecessor(write, writes) : Lit.False;
            for (int j = 0; j < writes.size(); j++) {
                if (i == j) {
                    continue;
                }
                var pred = writes.get(j);
                if (matchesPredicate(write, predicateRead) ^ matchesPredicate(pred, predicateRead)) {
                    flip = Logic.or(flip, immediatePredecessor(pred, write, writes));
                }
            }
            result.add(flip);
        }
        return result;
    }

    /**
     * Selects the latest visible write for the observed key.  At most one write
     * can be on this frontier because AR is a total order.
     */
    private List<Lit> createObservationFrontierLits(
            List<Lit> visibleWriters,
            List<KnownGraph.WriteRef<KeyType, ValueType>> writes) {
        var result = new ArrayList<Lit>(writes.size());
        for (int i = 0; i < writes.size(); i++) {
            Lit noLaterVisibleWriter = Lit.True;
            for (int j = 0; j < writes.size(); j++) {
                if (i == j) {
                    continue;
                }
                noLaterVisibleWriter = Logic.and(
                        noLaterVisibleWriter,
                        Logic.not(Logic.and(visibleWriters.get(j), beforeWrite(writes.get(i), writes.get(j)))));
            }
            result.add(Logic.and(visibleWriters.get(i), noLaterVisibleWriter));
        }
        for (int i = 0; i < writes.size(); i++) {
            for (int j = i + 1; j < writes.size(); j++) {
                solver.assertTrue(Logic.not(Logic.and(result.get(i), result.get(j))));
            }
        }
        return result;
    }

    /**
     * A write is visible under a frontier if it is at or before the selected
     * latest visible write.
     */
    private Lit visibleUnderFrontier(
            int writeIndex,
            List<Lit> observationFrontier,
            List<KnownGraph.WriteRef<KeyType, ValueType>> writes) {
        Lit visible = Lit.False;
        for (int j = 0; j < writes.size(); j++) {
            visible = Logic.or(
                    visible,
                    Logic.and(observationFrontier.get(j), beforeOrEqualWrite(writes.get(writeIndex), writes.get(j))));
        }
        return visible;
    }

    /**
     * Predicate RW guard for the concrete later writer U.  The caller is
     * responsible for proving that the chosen T is a valid MaxCand witness;
     * this method only checks whether U is the first write that changes the
     * observed boundary for this key.
     */
    private Lit deltaMu(Event<KeyType, ValueType> predicateRead,
                        KeyType key,
                        List<KnownGraph.WriteRef<KeyType, ValueType>> writes,
                        int laterWriterIndex,
                        List<Lit> observationFrontier,
                        Map<KeyType, KnownGraph.WriteRef<KeyType, ValueType>> resultSourcesByKey) {
        var laterWriter = writes.get(laterWriterIndex);
        var resultSource = resultSourcesByKey.get(key);

        if (resultSource != null) {
            return immediateSuccessor(resultSource, laterWriter, writes);
        }

        return Logic.or(
                firstMatchingWriteAfterObservationFrontier(laterWriterIndex, observationFrontier, writes, predicateRead),
                Logic.and(
                        noObservationFrontier(observationFrontier),
                        firstMatchingWriteAfterInitialState(laterWriterIndex, writes, predicateRead)));
    }

    private Lit noObservationFrontier(List<Lit> observationFrontier) {
        Lit result = Lit.True;
        for (var frontier : observationFrontier) {
            result = Logic.and(result, Logic.not(frontier));
        }
        return result;
    }

    /**
     * Returns true iff {@code laterWriter} is the first concrete write after the
     * visible frontier that turns an absent key into a matching predicate tuple.
     */
    private Lit firstMatchingWriteAfterObservationFrontier(
            int laterWriterIndex,
            List<Lit> observationFrontier,
            List<KnownGraph.WriteRef<KeyType, ValueType>> writes,
            Event<KeyType, ValueType> predicateRead) {
        var laterWriter = writes.get(laterWriterIndex);
        if (!matchesPredicate(laterWriter, predicateRead)) {
            return Lit.False;
        }

        Lit result = Lit.False;
        for (int i = 0; i < writes.size(); i++) {
            var frontier = writes.get(i);
            if (frontier == laterWriter) {
                continue;
            }

            Lit noEarlierMatchingWriter = Lit.True;
            for (int j = 0; j < writes.size(); j++) {
                var candidate = writes.get(j);
                if (candidate == frontier || candidate == laterWriter
                        || !matchesPredicate(candidate, predicateRead)) {
                    continue;
                }
                noEarlierMatchingWriter = Logic.and(noEarlierMatchingWriter,
                        Logic.not(Logic.and(beforeWrite(frontier, candidate), beforeWrite(candidate, laterWriter))));
            }

            result = Logic.or(result, Logic.and(
                    observationFrontier.get(i),
                    beforeWrite(frontier, laterWriter),
                    noEarlierMatchingWriter));
        }
        return result;
    }

    /**
     * Handles the initial-state boundary for absent predicate results: with no
     * visible writer, only the first matching write can change the observation.
     */
    private Lit firstMatchingWriteAfterInitialState(
            int laterWriterIndex,
            List<KnownGraph.WriteRef<KeyType, ValueType>> writes,
            Event<KeyType, ValueType> predicateRead) {
        var laterWriter = writes.get(laterWriterIndex);
        if (!matchesPredicate(laterWriter, predicateRead)) {
            return Lit.False;
        }

        Lit noEarlierMatchingWriter = Lit.True;
        for (int i = 0; i < writes.size(); i++) {
            var candidate = writes.get(i);
            if (candidate == laterWriter || !matchesPredicate(candidate, predicateRead)) {
                continue;
            }
            noEarlierMatchingWriter = Logic.and(noEarlierMatchingWriter, Logic.not(beforeWrite(candidate, laterWriter)));
        }
        return noEarlierMatchingWriter;
    }

    /** Returns true iff {@code right} is the immediate same-key successor of {@code left}. */
    private Lit immediateSuccessor(KnownGraph.WriteRef<KeyType, ValueType> left,
                                   KnownGraph.WriteRef<KeyType, ValueType> right,
                                   List<KnownGraph.WriteRef<KeyType, ValueType>> writes) {
        Lit result = beforeWrite(left, right);
        for (var candidate : writes) {
            if (candidate == left || candidate == right) {
                continue;
            }
            result = Logic.and(result,
                    Logic.not(Logic.and(beforeWrite(left, candidate), beforeWrite(candidate, right))));
        }
        return result;
    }

    /** Returns true when no same-key write precedes {@code write}. */
    private Lit noPredecessor(KnownGraph.WriteRef<KeyType, ValueType> write,
                              List<KnownGraph.WriteRef<KeyType, ValueType>> writes) {
        Lit result = Lit.True;
        for (var candidate : writes) {
            if (candidate == write) {
                continue;
            }
            result = Logic.and(result, Logic.not(beforeWrite(candidate, write)));
        }
        return result;
    }

    /** Returns true when {@code predecessor} is the immediate same-key predecessor of {@code write}. */
    private Lit immediatePredecessor(KnownGraph.WriteRef<KeyType, ValueType> predecessor,
                                     KnownGraph.WriteRef<KeyType, ValueType> write,
                                     List<KnownGraph.WriteRef<KeyType, ValueType>> writes) {
        Lit result = beforeWrite(predecessor, write);
        for (var candidate : writes) {
            if (candidate == predecessor || candidate == write) {
                continue;
            }
            result = Logic.and(result,
                    Logic.not(Logic.and(beforeWrite(predecessor, candidate), beforeWrite(candidate, write))));
        }
        return result;
    }

    /** Compares two writes in the local write order, allowing equality. */
    private Lit beforeOrEqualWrite(KnownGraph.WriteRef<KeyType, ValueType> left,
                                   KnownGraph.WriteRef<KeyType, ValueType> right) {
        if (left == right) {
            return Lit.True;
        }
        return beforeWrite(left, right);
    }

    /**
     * Compares two writes by program order inside one transaction, or by AR when
     * they come from different transactions.
     */
    private Lit beforeWrite(KnownGraph.WriteRef<KeyType, ValueType> left,
                            KnownGraph.WriteRef<KeyType, ValueType> right) {
        if (left == right) {
            return Lit.False;
        }
        if (left.getTxn().equals(right.getTxn())) {
            return left.getIndex() < right.getIndex() ? Lit.True : Lit.False;
        }
        return ar(left.getTxn(), right.getTxn());
    }

    /** Evaluates the predicate read against a concrete write value. */
    private boolean matchesPredicate(KnownGraph.WriteRef<KeyType, ValueType> write,
                                     Event<KeyType, ValueType> predicateRead) {
        return predicateRead.getPredicate().test(write.getEvent().getKey(), write.getEvent().getValue());
    }

    /** Groups writes by key and gives each key a deterministic iteration order. */
    private Map<KeyType, List<KnownGraph.WriteRef<KeyType, ValueType>>> buildWritesByKey(KnownGraph<KeyType, ValueType> graph) {
        var result = new HashMap<KeyType, List<KnownGraph.WriteRef<KeyType, ValueType>>>();
        for (var entry : graph.getWrites().entrySet()) {
            result.computeIfAbsent(entry.getKey().getLeft(), ignored -> new ArrayList<>()).add(entry.getValue());
        }
        for (var writes : result.values()) {
            writes.sort(Comparator
                    .comparing((KnownGraph.WriteRef<KeyType, ValueType> w) -> w.getTxn().getId())
                    .thenComparingInt(KnownGraph.WriteRef::getIndex));
        }
        return result;
    }

    private Lit ar(Transaction<KeyType, ValueType> from, Transaction<KeyType, ValueType> to) {
        boolean fromBottom = isBottomTxn(from);
        boolean toBottom = isBottomTxn(to);

        if (fromBottom && toBottom) {
            return Lit.False;
        }
        if (fromBottom) {
            return Lit.True;
        }
        if (toBottom) {
            return Lit.False;
        }

        return ar[txnIndex.get(from)][txnIndex.get(to)];
    }

    private static boolean isBottomTxn(Transaction<?, ?> txn) {
        return txn.getId() == -1L
                && txn.getSession() != null
                && txn.getSession().getId() == -1L;
    }

    /**
     * Extracts a compact UNSAT explanation.  If the known graph alone is
     * inconsistent, report a known-edge cycle; otherwise greedily shrink the
     * unresolved WW constraint set while preserving UNSAT.
     */
    private void extractConflicts() {
        if (!isSatisfiable(List.of())) {
            conflictEdges = extractKnownEdgeCycle();
            conflictConstraints = Collections.emptyList();
            return;
        }

        var coreConstraints = new ArrayList<>(constraints);
        for (int i = 0; i < coreConstraints.size(); ) {
            var candidate = new ArrayList<>(coreConstraints);
            candidate.remove(i);
            if (!isSatisfiable(candidate)) {
                coreConstraints = candidate;
            } else {
                i++;
            }
        }

        conflictConstraints = coreConstraints;
        conflictEdges = supportingKnownEdges(coreConstraints);
    }

    private boolean isSatisfiable(Collection<SERConstraint<KeyType, ValueType>> activeConstraints) {
        return new SERSolverAR<>(history, graph, activeConstraints, false).solve();
    }

    /**
     * Reports known edges that touch the transactions participating in the
     * minimized unresolved constraint core.
     */
    private Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> supportingKnownEdges(
            Collection<SERConstraint<KeyType, ValueType>> coreConstraints) {
        if (coreConstraints.isEmpty()) {
            return Collections.emptyList();
        }

        var txnsInCore = new HashSet<Transaction<KeyType, ValueType>>();
        for (var constraint : coreConstraints) {
            txnsInCore.add(constraint.getWriteTransaction1());
            txnsInCore.add(constraint.getWriteTransaction2());
            for (var edge : constraint.getEdges1()) {
                txnsInCore.add(edge.getFrom());
                txnsInCore.add(edge.getTo());
            }
            for (var edge : constraint.getEdges2()) {
                txnsInCore.add(edge.getFrom());
                txnsInCore.add(edge.getTo());
            }
        }

        var result = new ArrayList<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>();
        collectKnownEdgesAmong(graph.getKnownGraphA(), txnsInCore, result);
        collectKnownEdgesAmong(graph.getKnownGraphB(), txnsInCore, result);
        return result;
    }

    private void collectKnownEdgesAmong(
            com.google.common.graph.ValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> known,
            Set<Transaction<KeyType, ValueType>> txnsInCore,
            List<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> out) {
        for (var ep : known.edges()) {
            if (!txnsInCore.contains(ep.source()) || !txnsInCore.contains(ep.target())) {
                continue;
            }
            var edges = known.edgeValue(ep).orElse(List.of()).stream()
                    .filter(edge -> isEncodedKnownEdge(edge.getType()))
                    .collect(Collectors.toList());
            if (!edges.isEmpty()) {
                out.add(Pair.of(EndpointPair.ordered(ep.source(), ep.target()), edges));
            }
        }
    }

    /** Finds a concrete directed cycle formed only by mandatory known edges. */
    private Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> extractKnownEdgeCycle() {
        var adjacency = buildKnownEdgeAdjacency();
        var color = new HashMap<Transaction<KeyType, ValueType>, Integer>();
        var stack = new ArrayList<Transaction<KeyType, ValueType>>();
        var stackIndex = new HashMap<Transaction<KeyType, ValueType>, Integer>();

        for (var txn : txns) {
            if (color.getOrDefault(txn, 0) != 0) {
                continue;
            }
            var cycle = dfsKnownEdgeCycle(txn, adjacency, color, stack, stackIndex);
            if (!cycle.isEmpty()) {
                return cycle;
            }
        }
        return Collections.emptyList();
    }

    /** Builds adjacency for the known-edge subgraph used by cycle extraction. */
    private Map<Transaction<KeyType, ValueType>, Set<Transaction<KeyType, ValueType>>> buildKnownEdgeAdjacency() {
        var adjacency = new HashMap<Transaction<KeyType, ValueType>, Set<Transaction<KeyType, ValueType>>>();
        addAdjacency(graph.getKnownGraphA(), adjacency);
        addAdjacency(graph.getKnownGraphB(), adjacency);
        return adjacency;
    }

    private void addAdjacency(
            com.google.common.graph.ValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> known,
            Map<Transaction<KeyType, ValueType>, Set<Transaction<KeyType, ValueType>>> adjacency) {
        for (var ep : known.edges()) {
            var edges = known.edgeValue(ep).orElse(Collections.emptyList());
            if (edges.stream().anyMatch(edge -> isEncodedKnownEdge(edge.getType()))) {
                adjacency.computeIfAbsent(ep.source(), ignored -> new LinkedHashSet<>()).add(ep.target());
            }
        }
    }

    private Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> dfsKnownEdgeCycle(
            Transaction<KeyType, ValueType> node,
            Map<Transaction<KeyType, ValueType>, Set<Transaction<KeyType, ValueType>>> adjacency,
            Map<Transaction<KeyType, ValueType>, Integer> color,
            List<Transaction<KeyType, ValueType>> stack,
            Map<Transaction<KeyType, ValueType>, Integer> stackIndex) {
        color.put(node, 1);
        stackIndex.put(node, stack.size());
        stack.add(node);

        for (var succ : adjacency.getOrDefault(node, Collections.emptySet())) {
            int succColor = color.getOrDefault(succ, 0);
            if (succColor == 0) {
                var cycle = dfsKnownEdgeCycle(succ, adjacency, color, stack, stackIndex);
                if (!cycle.isEmpty()) {
                    return cycle;
                }
            } else if (succColor == 1) {
                var cycleNodes = new ArrayList<>(stack.subList(stackIndex.get(succ), stack.size()));
                cycleNodes.add(succ);
                return cycleEdgesFromNodes(cycleNodes);
            }
        }

        stack.remove(stack.size() - 1);
        stackIndex.remove(node);
        color.put(node, 2);
        return Collections.emptyList();
    }

    private Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> cycleEdgesFromNodes(
            List<Transaction<KeyType, ValueType>> cycleNodes) {
        var result = new ArrayList<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>();
        for (int i = 0; i + 1 < cycleNodes.size(); i++) {
            var from = cycleNodes.get(i);
            var to = cycleNodes.get(i + 1);
            var edges = new ArrayList<Edge<KeyType>>();
            graph.getKnownGraphA().edgeValue(from, to).orElse(List.of()).stream()
                    .filter(edge -> isEncodedKnownEdge(edge.getType()))
                    .forEach(edges::add);
            graph.getKnownGraphB().edgeValue(from, to).orElse(List.of()).stream()
                    .filter(edge -> isEncodedKnownEdge(edge.getType()))
                    .forEach(edges::add);
            result.add(Pair.of(EndpointPair.ordered(from, to), edges));
        }
        return result;
    }

    private static boolean isEncodedKnownEdge(EdgeType type) {
        return type != EdgeType.PR_WR && type != EdgeType.PR_RW;
    }
}
