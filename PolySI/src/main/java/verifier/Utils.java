package verifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraph;
import com.google.common.graph.ValueGraphBuilder;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import graph.Edge;
import graph.EdgeType;
import graph.MatrixGraph;
import history.Event;
import history.History;
import history.Transaction;
import history.Event.EventType;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;

class Utils {
    @lombok.Data
    private static class WriteRef<KeyType, ValueType> {
        private final Transaction<KeyType, ValueType> transaction;
        private final Event<KeyType, ValueType> event;
        private final int index;
    }

    static <KeyType, ValueType> boolean verifyInternalConsistency(History<KeyType, ValueType> history) {
        var writes = new HashMap<Pair<KeyType, ValueType>, WriteRef<KeyType, ValueType>>();
        var txnWrites = new HashMap<Pair<Transaction<KeyType, ValueType>, KeyType>, ArrayList<Integer>>();
        var getEvents = ((Function<Event.EventType, Stream<Pair<Integer, Event<KeyType, ValueType>>>>) type -> history
                .getTransactions().stream().flatMap(txn -> {
                    var events = txn.getEvents();
                    return IntStream.range(0, events.size()).mapToObj(i -> Pair.of(i, events.get(i)))
                            .filter(p -> p.getRight().getType() == type);
                }));

        getEvents.apply(Event.EventType.WRITE).forEach(p -> {
            var i = p.getLeft();
            var ev = p.getRight();
            writes.put(Pair.of(ev.getKey(), ev.getValue()), new WriteRef<>(ev.getTransaction(), ev, i));
            txnWrites.computeIfAbsent(Pair.of(ev.getTransaction(), ev.getKey()), k -> new ArrayList()).add(i);
        });

        for (var p : getEvents.apply(Event.EventType.READ).collect(Collectors.toList())) {
            var i = p.getLeft();
            var ev = p.getRight();
            if (!checkItemRead(ev, i, writes, txnWrites)) {
                return false;
            }
        }

        for (var p : getEvents.apply(Event.EventType.PREDICATE_READ).collect(Collectors.toList())) {
            var i = p.getLeft();
            var ev = p.getRight();
            if (!checkPredicateRead(ev, i, writes, txnWrites)) {
                return false;
            }
        }
        return true;
    }

    private static <KeyType, ValueType> boolean checkItemRead(Event<KeyType, ValueType> ev, int i,
            Map<Pair<KeyType, ValueType>, WriteRef<KeyType, ValueType>> writes,
            Map<Pair<Transaction<KeyType, ValueType>, KeyType>, ArrayList<Integer>> txnWrites) {
        var writeEv = writes.get(Pair.of(ev.getKey(), ev.getValue()));
        if (writeEv == null) {
            System.err.printf("%s has no corresponding write\n", ev);
            return false;
        }

        var myWriteIndices = txnWrites.getOrDefault(Pair.of(ev.getTransaction(), ev.getKey()), new ArrayList<>());
        var writeIndices = txnWrites.get(Pair.of(writeEv.getTransaction(), writeEv.getEvent().getKey()));
        var j = Collections.binarySearch(writeIndices, writeEv.getIndex());

        if (writeEv.getTransaction() == ev.getTransaction()) {
            if (j != writeIndices.size() - 1 && writeIndices.get(j + 1) < i) {
                System.err.printf("%s not reading from latest write: %s\n", ev, writeEv.getEvent());
                return false;
            } else if (writeEv.getIndex() > i) {
                System.err.printf("%s reads from a write after it: %s\n", ev, writeEv.getEvent());
                return false;
            }
        } else if (j != writeIndices.size() - 1 || (!myWriteIndices.isEmpty() && myWriteIndices.get(0) < i)) {
            System.err.printf("%s not reading from latest write: %s\n", ev, writeEv.getEvent());
            return false;
        }
        return true;
    }

    static int latestWriteBefore(List<Integer> writeIndices, int pos) {
        if (writeIndices == null || writeIndices.isEmpty()) {
            return -1;
        }
        var k = Collections.binarySearch(writeIndices, pos);
        if (k >= 0) {
            k--;
        } else {
            k = -k - 2;
        }
        return k >= 0 ? writeIndices.get(k) : -1;
    }

    private static <KeyType, ValueType> boolean isCommitted(Transaction<KeyType, ValueType> transaction) {
        // Transactions without COMMIT status are treated as aborted/non-visible.
        return transaction.getStatus() == Transaction.TransactionStatus.COMMIT;
    }

    private static <KeyType, ValueType> boolean checkPredicateRead(Event<KeyType, ValueType> ev, int pos,
            Map<Pair<KeyType, ValueType>, WriteRef<KeyType, ValueType>> writes,
            Map<Pair<Transaction<KeyType, ValueType>, KeyType>, ArrayList<Integer>> txnWrites) {
        var predicate = ev.getPredicate();
        var results = ev.getPredResults();
        if (predicate == null || results == null) {
            System.err.printf("%s has null predicate or results\n", ev);
            return false;
        }

        var resultByKey = new HashMap<KeyType, ValueType>();
        for (var result : results) {
            var key = result.getKey();
            var value = result.getValue();

            if (resultByKey.containsKey(key) && !Objects.equals(resultByKey.get(key), value)) {
                System.err.printf("%s has multiple values for key %s in predicate result\n", ev, key);
                return false;
            }
            resultByKey.put(key, value);

            var ref = writes.get(Pair.of(key, value));
            if (ref == null) {
                System.err.printf("%s result (%s,%s) has no corresponding write\n", ev, key, value);
                return false;
            }
            if (!isCommitted(ref.getTransaction())) {
                System.err.printf("%s result (%s,%s) comes from non-committed transaction %s\n", ev, key, value,
                        ref.getTransaction());
                return false;
            }
            if (!predicate.test(key, value)) {
                System.err.printf("%s result (%s,%s) does not satisfy predicate\n", ev, key, value);
                return false;
            }

            var selfWrites = txnWrites.getOrDefault(Pair.of(ev.getTransaction(), key), new ArrayList<>());
            var latestSelf = latestWriteBefore(selfWrites, pos);
            if (latestSelf >= 0) {
                var selfEvent = ev.getTransaction().getEvents().get(latestSelf);
                if (!Objects.equals(selfEvent.getValue(), value)) {
                    System.err.printf("%s should return own latest write for key %s\n", ev, key);
                    return false;
                }
                continue;
            }

            if (ref.getTransaction() != ev.getTransaction()) {
                var writerIndices = txnWrites.get(Pair.of(ref.getTransaction(), key));
                if (writerIndices == null) {
                    System.err.printf("%s writer indices missing for (%s,%s)\n", ev, key, value);
                    return false;
                }
                var j = Collections.binarySearch(writerIndices, ref.getIndex());
                if (j != writerIndices.size() - 1) {
                    System.err.printf("%s result (%s,%s) reads from intermediate write\n", ev, key, value);
                    return false;
                }
            } else if (ref.getIndex() >= pos) {
                System.err.printf("%s result (%s,%s) reads from future self write\n", ev, key, value);
                return false;
            }
        }

        for (var p : txnWrites.entrySet()) {
            var txnAndKey = p.getKey();
            if (txnAndKey.getLeft() != ev.getTransaction()) {
                continue;
            }
            var key = txnAndKey.getRight();
            var latestSelf = latestWriteBefore(p.getValue(), pos);
            if (latestSelf < 0) {
                continue;
            }
            var selfValue = ev.getTransaction().getEvents().get(latestSelf).getValue();
            var inResult = resultByKey.containsKey(key);
            var predTrue = predicate.test(key, selfValue);
            if (predTrue && !inResult) {
                System.err.printf("%s misses own visible tuple (%s,%s)\n", ev, key, selfValue);
                return false;
            }
            if (!predTrue && inResult) {
                System.err.printf("%s should not include key %s because own latest write fails predicate\n", ev, key);
                return false;
            }
            if (predTrue && inResult && !Objects.equals(resultByKey.get(key), selfValue)) {
                System.err.printf("%s returns wrong own value for key %s\n", ev, key);
                return false;
            }
        }

        return true;
    }

    /**
     * Collect unknown edges.
     * A∘B composition should only use RW edges, NOT PR_RW edges (which are known).
     *
     * @param graphA       graph A containing known and unknown edges
     * @param graphB       graph B containing RW and PR_RW edges
     * @param reachability known reachable node pairs
     * @param solver       SAT solver
     * @param knownPrRwPairs Set of (from,to) pairs that are PR_RW known edges (to exclude from A∘B)
     */
    static <KeyType, ValueType> List<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>> getUnknownEdges(
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphA,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphB,
            MatrixGraph<Transaction<KeyType, ValueType>> reachability, Solver solver,
            Set<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>> knownPrRwPairs) {
        var edges = new ArrayList<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>>();

        for (var p : graphA.nodes()) {
            for (var n : graphA.successors(p)) {
                var predEdges = graphA.edgeValue(p, n).get();

                if (p == n || !reachability.hasEdgeConnecting(p, n)) {
                    predEdges.forEach(e -> edges.add(Triple.of(p, n, e)));
                }

                // Only use RW edges (NOT PR_RW) for A∘B composition.
                // PR_RW edges are "known" — they must appear in the final graph.
                var txns = graphB.successors(n).stream()
                        .filter(t -> p == t || !reachability.hasEdgeConnecting(p, t))
                        // Exclude PR_RW known edges from composition
                        .filter(t -> !knownPrRwPairs.contains(Pair.of(n, t)))
                        .collect(Collectors.toList());

                for (var s : txns) {
                    var succEdges = graphB.edgeValue(n, s).get();
                    predEdges.forEach(e1 -> succEdges.forEach(e2 -> {
                        var lit = Logic.and(e1, e2);
                        solver.setDecisionLiteral(lit, false);
                        edges.add(Triple.of(p, s, lit));
                    }));
                }
            }
        }

        return edges;
    }

    /**
     * Collect unknown edges (backward-compatible overload, no PR_RW exclusion).
     */
    static <KeyType, ValueType> List<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>> getUnknownEdges(
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphA,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphB,
            MatrixGraph<Transaction<KeyType, ValueType>> reachability, Solver solver) {
        return getUnknownEdges(graphA, graphB, reachability, solver, Collections.emptySet());
    }

    /**
     * Collect known edges in A union C.
     *
     * AC = A union (A ∘ B). For each edge n→m in AC:
     * - If n→m exists directly in graphA, use its literal.
     * - Otherwise n→m = n→x (from A) ∘ x→m (from B), use A(n→x) AND B(x→m).
     *
     * PR_RW edges (from graphB) that are NOT in AC are handled via
     * the constraint system in addConstraints, not here.
     *
     * @param graphA known graph A
     * @param graphB known graph B
     * @param AC the graph containing the edges to collect
     */
    static <KeyType, ValueType> List<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>> getKnownEdges(
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphA,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphB,
            MatrixGraph<Transaction<KeyType, ValueType>> AC) {
        var result = new ArrayList<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>>();

        // 1. AC 中的直达边（来自 graphA 或 A∘B 合成路径）
        for (var e : AC.edges()) {
            var n = e.source();
            var m = e.target();
            var firstEdge = ((Function<Optional<Collection<Lit>>, Lit>) c -> c.get().iterator().next());

            if (graphA.hasEdgeConnecting(n, m)) {
                result.add(Triple.of(n, m, firstEdge.apply(graphA.edgeValue(n, m))));
            } else {
                var middle = Sets.intersection(graphA.successors(n), graphB.predecessors(m)).iterator().next();
                result.add(Triple.of(n, m, Logic.and(
                        firstEdge.apply(graphA.edgeValue(n, middle)),
                        firstEdge.apply(graphB.edgeValue(middle, m)))));
            }
        }

        // PR_RW edges from graphB that are NOT part of the A∘B composition
        // are handled via the constraint system in addConstraints, not here.
        return result;
    }

    static <KeyType, ValueType> Map<Transaction<KeyType, ValueType>, Integer> getOrderInSession(
            History<KeyType, ValueType> history) {
        // @formatter:off
        return history.getSessions().stream()
                .flatMap(s -> Streams.zip(
                    s.getTransactions().stream(),
                    IntStream.range(0, s.getTransactions().size()).boxed(),
                    Pair::of))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        // @formatter:on
    }

    static <KeyType, ValueType> MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> createEmptyGraph(
            History<KeyType, ValueType> history) {
        MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> g = ValueGraphBuilder.directed()
                .allowsSelfLoops(true).build();

        history.getTransactions().forEach(g::addNode);
        return g;
    }

    static <KeyType, ValueType> void addEdge(MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> g,
            Transaction<KeyType, ValueType> src, Transaction<KeyType, ValueType> dst, Lit lit) {
        if (!g.hasEdgeConnecting(src, dst)) {
            g.putEdgeValue(src, dst, new ArrayList<>());
        }
        g.edgeValue(src, dst).get().add(lit);
    }

    /*
     * Delete edges in a way that preserves reachability
     */
    static <KeyType, ValueType> MatrixGraph<Transaction<KeyType, ValueType>> reduceEdges(
            MatrixGraph<Transaction<KeyType, ValueType>> graph,
            Map<Transaction<KeyType, ValueType>, Integer> orderInSession) {
        System.err.printf("Before: %d edges\n", graph.edges().size());
        var newGraph = MatrixGraph.ofNodes(graph);

        for (var n : graph.nodes()) {
            var succ = graph.successors(n);
            // @formatter:off
            var firstInSession = succ.stream()
                .collect(Collectors.toMap(
                    m -> m.getSession(),
                    Function.identity(),
                    (p, q) -> orderInSession.get(p)
                        < orderInSession.get(q) ? p : q));

            firstInSession.values().forEach(m -> newGraph.putEdge(n, m));

            succ.stream()
                .filter(m -> m.getSession() == n.getSession()
                        && orderInSession.get(m) == orderInSession.get(n) + 1)
                .forEach(m -> newGraph.putEdge(n, m));
            // @formatter:on
        }

        System.err.printf("After: %d edges\n", newGraph.edges().size());
        return newGraph;
    }

    static <KeyType, ValueType> String conflictsToDot(Collection<Transaction<KeyType, ValueType>> transactions,
            Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> edges,
            Collection<SIConstraint<KeyType, ValueType>> constraints) {
        var builder = new StringBuilder();
        builder.append("digraph {\n");

        for (var txn : transactions) {
            builder.append(String.format("\"%s\";\n", txn));
        }

        for (var e : edges) {
            var pair = e.getLeft();
            var keys = e.getRight();
            var label = new StringBuilder();

            for (var k : keys) {
                if (k.getType() != EdgeType.SO) {
                    label.append(String.format("%s %s\\n", k.getType(), k.getKey()));
                } else {
                    label.append(String.format("%s\\n", k.getType()));
                }
            }

            builder.append(
                    String.format("\"%s\" -> \"%s\" [label=\"%s\"];\n", pair.source(), pair.target(), label));
        }

        int colorStep = 0x1000000 / (constraints.size() + 1);
        int color = 0;
        for (var c : constraints) {
            color += colorStep;
            for (var e : c.getEdges1()) {
                builder.append(String.format("\"%s\" -> \"%s\" [style=dotted,color=\"#%06x\"];\n", e.getFrom(), e.getTo(), color));
            }

            for (var e : c.getEdges2()) {
                builder.append(String.format("\"%s\" -> \"%s\" [style=dashed,color=\"#%06x\"];\n", e.getFrom(), e.getTo(), color));
            }
        }

        builder.append("}\n");
        return builder.toString();
    }

    static <KeyType, ValueType> String conflictsToLegacy(Collection<Transaction<KeyType, ValueType>> transactions,
            Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> edges,
            Collection<SIConstraint<KeyType, ValueType>> constraints) {
        var builder = new StringBuilder();

        edges.forEach(p -> builder.append(String.format("Edge: %s\n", p)));
        constraints.forEach(c -> builder.append(String.format("Constraint: %s\n", c)));
        builder.append(String.format("Related transactions:\n"));
        transactions.forEach(t -> {
            builder.append(String.format("sessionid: %d, id: %d\nops:\n", t.getSession().getId(), t.getId()));
            t.getEvents()
                    .forEach(e -> builder.append(String.format("%s %s = %s\n", e.getType(), e.getKey(), e.getValue())));
        });

        return builder.toString();
    }
}
