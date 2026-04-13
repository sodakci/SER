package graph;

import static history.Event.EventType.PREDICATE_READ;
import static history.Event.EventType.READ;
import static history.Event.EventType.WRITE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraph;
import com.google.common.graph.ValueGraphBuilder;

import org.apache.commons.lang3.tuple.Pair;

import history.Event;
import history.History;
import history.Transaction;
import lombok.Data;
import lombok.Getter;

@SuppressWarnings("UnstableApiUsage")
@Getter
public class KnownGraph<KeyType, ValueType> {
    @Data
    public static class WriteRef<KeyType, ValueType> {
        private final Transaction<KeyType, ValueType> txn;
        private final Event<KeyType, ValueType> event;
        private final int index;
    }

    @Data
    public static class PredicateTupleSource<KeyType, ValueType> {
        private final KeyType key;
        private final ValueType value;
        private final WriteRef<KeyType, ValueType> sourceWrite;
    }

    @Data
    public static class PredicateObservation<KeyType, ValueType> {
        private final Transaction<KeyType, ValueType> txn;
        private final Event<KeyType, ValueType> predicateReadEvent;
        private final List<PredicateTupleSource<KeyType, ValueType>> tupleSources;
    }

    private final MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> readFrom = ValueGraphBuilder
            .directed().build();
    private final MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> knownGraphA = ValueGraphBuilder
            .directed().build();
    private final MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> knownGraphB = ValueGraphBuilder
            .directed().build();
    // Assumption: (key, value) uniquely identifies a write version in current history model.
    private final Map<Pair<KeyType, ValueType>, WriteRef<KeyType, ValueType>> writes = new HashMap<>();
    private final Map<Pair<Transaction<KeyType, ValueType>, KeyType>, List<Integer>> txnWrites = new HashMap<>();
    private final List<PredicateObservation<KeyType, ValueType>> predicateObservations = new ArrayList<>();

    /**
     * Build a graph from a history
     *
     * The built graph contains SO and WR edges
     */
    public KnownGraph(History<KeyType, ValueType> history) {
        history.getTransactions().forEach(txn -> {
            knownGraphA.addNode(txn);
            knownGraphB.addNode(txn);
            readFrom.addNode(txn);
        });

        // add SO edges
        history.getSessions().forEach(session -> {
            Transaction<KeyType, ValueType> prevTxn = null;
            for (var txn : session.getTransactions()) {
                if (prevTxn != null) {
                    addEdge(knownGraphA, prevTxn, txn,
                            new Edge<>(EdgeType.SO, null));
                }
                prevTxn = txn;
            }
        });

        // build write indexes
        history.getTransactions().forEach(txn -> {
            var events = txn.getEvents();
            for (int i = 0; i < events.size(); i++) {
                var ev = events.get(i);
                if (ev.getType() != WRITE) {
                    continue;
                }
                writes.put(Pair.of(ev.getKey(), ev.getValue()), new WriteRef<>(txn, ev, i));
                txnWrites.computeIfAbsent(Pair.of(txn, ev.getKey()), k -> new ArrayList<>()).add(i);
            }
        });

        // add WR edges from point reads
        var events = history.getEvents();
        events.stream().filter(e -> e.getType() == READ).forEach(ev -> {
            var writeRef = writes.get(Pair.of(ev.getKey(), ev.getValue()));
            var writeTxn = writeRef == null ? null : writeRef.getTxn();
            var txn = ev.getTransaction();

            if (writeTxn == txn) {
                return;
            }

            putEdge(writeTxn, txn, new Edge<KeyType>(EdgeType.WR, ev.getKey()));
        });

        // collect predicate-read observations only, no PR_* edge generation here
        events.stream().filter(e -> e.getType() == PREDICATE_READ).forEach(ev -> {
            var tupleSources = new ArrayList<PredicateTupleSource<KeyType, ValueType>>();
            for (var result : ev.getPredResults()) {
                var sourceWrite = writes.get(Pair.of(result.getKey(), result.getValue()));
                if (sourceWrite == null) {
                    throw new IllegalStateException(String.format(
                            "No source write for predicate-read tuple (%s,%s)", result.getKey(), result.getValue()));
                }
                tupleSources.add(new PredicateTupleSource<>(result.getKey(), result.getValue(), sourceWrite));
            }
            predicateObservations.add(new PredicateObservation<>(ev.getTransaction(), ev, tupleSources));
        });
    }

    public void putEdge(Transaction<KeyType, ValueType> u,
            Transaction<KeyType, ValueType> v, Edge<KeyType> edge) {
        switch (edge.getType()) {
        case WR:
            addEdge(readFrom, u, v, edge);
            addEdge(knownGraphA, u, v, edge);
            break;
        case WW:
        case SO:
        case PR_WR:
            addEdge(knownGraphA, u, v, edge);
            break;
        case RW:
        case PR_RW:
            addEdge(knownGraphB, u, v, edge);
            break;
        }
    }

    /**
     * Remove all derived PR_WR edges from knownGraphA and all PR_RW edges
     * from knownGraphB.  Called at the start of each refresh cycle so that
     * stale derived edges are not accumulated across rounds.
     */
    public void clearDerivedPredicateEdges() {
        clearEdgesOfType(knownGraphA, EdgeType.PR_WR);
        clearEdgesOfType(knownGraphB, EdgeType.PR_RW);
    }

    private void clearEdgesOfType(
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> graph,
            EdgeType type) {
        var snapshot = new ArrayList<>(graph.edges());
        for (var ep : snapshot) {
            var edgeOpt = graph.edgeValue(ep.source(), ep.target());
            if (edgeOpt.isEmpty()) continue;
            var edges = edgeOpt.get();
            edges.removeIf(e -> e.getType() == type);
            if (edges.isEmpty()) {
                graph.removeEdge(ep.source(), ep.target());
            }
        }
    }

    private void addEdge(
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> graph,
            Transaction<KeyType, ValueType> u,
            Transaction<KeyType, ValueType> v, Edge<KeyType> edge) {
        if (!graph.hasEdgeConnecting(u, v)) {
            graph.putEdgeValue(u, v, new ArrayList<>());
        }
        graph.edgeValue(u, v).get().add(edge);
    }
}
