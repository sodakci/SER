package verifier;

import graph.KnownGraph;
import history.History;
import history.Transaction;
import util.Profiler;
import graph.Edge;
import graph.EdgeType;
import graph.MatrixGraph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.ValueGraph;

import java.util.*;

import org.apache.commons.lang3.tuple.Pair;

import lombok.Getter;
import lombok.Setter;

public class Pruning {
    @Getter
    @Setter
    private static boolean enablePruning = true;

    @Getter
    @Setter
    private static double stopThreshold = 0.01;

    private static Pair<?, ?> lastConflicts = emptyConflicts();

    static <KeyType, ValueType> boolean pruneConstraints(KnownGraph<KeyType, ValueType> knownGraph,
            Collection<SERConstraint<KeyType, ValueType>> constraints, History<KeyType, ValueType> history) {
        if (!enablePruning) {
            return false;
        }

        lastConflicts = emptyConflicts();

        var profiler = Profiler.getInstance();
        profiler.startTick("SER_PRUNE");

        int rounds = 1, solvedConstraints = 0, totalConstraints = constraints.size();
        boolean hasCycle = false;
        while (!hasCycle) {
            System.err.printf("Pruning round %d\n", rounds);
            var result = pruneConstraintsWithPostChecking(knownGraph, constraints, history);

            hasCycle = result.getRight();
            solvedConstraints += result.getLeft();

            if (result.getLeft() <= stopThreshold * totalConstraints
                    || totalConstraints - solvedConstraints <= stopThreshold * totalConstraints) {
                break;
            }
            rounds++;
        }

        profiler.endTick("SER_PRUNE");
        System.err.printf("Pruned %d rounds, solved %d constraints\n" + "After prune: graphA: %d, graphB: %d\n", rounds,
                solvedConstraints, knownGraph.getKnownGraphA().edges().size(),
                knownGraph.getKnownGraphB().edges().size());
        return hasCycle;
    }

    private static <KeyType, ValueType> Pair<Integer, Boolean> pruneConstraintsWithPostChecking(
            KnownGraph<KeyType, ValueType> knownGraph, Collection<SERConstraint<KeyType, ValueType>> constraints,
            History<KeyType, ValueType> history) {
        var profiler = Profiler.getInstance();

        var solvedConstraints = new ArrayList<SERConstraint<KeyType, ValueType>>();

        profiler.startTick("SER_PRUNE_POST_CHECK");
        int checked = 0;
        int total = constraints.size();
        int progressStep = Math.max(1, Math.min(100, Math.max(1, total / 100)));
        long progressStartNanos = System.nanoTime();
        long lastHeartbeatNanos = progressStartNanos;
        printProgress(checked, total, solvedConstraints.size(), progressStartNanos, false);
        System.err.flush();
        for (var c : constraints) {
            checked++;
            long now = System.nanoTime();
            if (checked == 1 || now - lastHeartbeatNanos >= 1_000_000_000L) {
                printHeartbeat(checked, total, solvedConstraints.size(), progressStartNanos);
                lastHeartbeatNanos = now;
            }
            var eitherAttempt = buildAttempt(history, knownGraph, c.getEdges1());
            var orAttempt = buildAttempt(history, knownGraph, c.getEdges2());
            boolean okEither = !dependencyGraph(eitherAttempt).hasLoops();
            boolean okOr = !dependencyGraph(orAttempt).hasLoops();

            if (!okEither && !okOr) {
                lastConflicts = Pair.of(extractCycleEdges(eitherAttempt), List.of(c));
                printProgress(checked, total, solvedConstraints.size(), progressStartNanos, true);
                profiler.endTick("SER_PRUNE_POST_CHECK");
                return Pair.of(0, true);
            }

            if (!okEither) {
                addToKnownGraph(knownGraph, c.getEdges2());
                solvedConstraints.add(c);
                continue;
            }

            if (!okOr) {
                addToKnownGraph(knownGraph, c.getEdges1());
                solvedConstraints.add(c);
            }

            if (checked % progressStep == 0 || checked == total) {
                printProgress(checked, total, solvedConstraints.size(), progressStartNanos, checked == total);
                System.err.flush();
            }
        }
        profiler.endTick("SER_PRUNE_POST_CHECK");

        System.err.printf("solved %d constraints\n", solvedConstraints.size());
        // constraints.removeAll(solvedConstraints);
        // java removeAll has performance bugs; do it manually
        solvedConstraints.forEach(constraints::remove);
        return Pair.of(solvedConstraints.size(), false);
    }

    private static void printProgress(int checked, int total, int solved, long startNanos, boolean done) {
        int percent = total == 0 ? 100 : (int) Math.floor(checked * 100.0 / total);
        int width = 30;
        int filled = Math.min(width, Math.max(0, percent * width / 100));
        var bar = new StringBuilder(width);
        for (int i = 0; i < width; i++) {
            bar.append(i < filled ? '#' : '-');
        }
        double elapsedSeconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        System.err.printf("\rPruning post-check [%s] %3d%% checked %d/%d, solved %d, elapsed %.1fs%s",
                bar, percent, checked, total, solved, elapsedSeconds, done ? "\n" : "");
    }

    private static void printHeartbeat(int checked, int total, int solved, long startNanos) {
        double elapsedSeconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        System.err.printf("\nPruning still running: starting constraint %d/%d, solved %d, elapsed %.1fs\n",
                checked, total, solved, elapsedSeconds);
        System.err.flush();
    }

    private static <KeyType, ValueType> KnownGraph<KeyType, ValueType> buildAttempt(
            History<KeyType, ValueType> history,
            KnownGraph<KeyType, ValueType> knownGraph,
            Collection<SEREdge<KeyType, ValueType>> side) {
        var temp = new KnownGraph<>(history);
        copyConfirmedPruningEdges(knownGraph, temp);
        addTemporaryPruningEdges(temp, side);
        return temp;
    }

    private static <KeyType, ValueType> void copyConfirmedPruningEdges(
            KnownGraph<KeyType, ValueType> from,
            KnownGraph<KeyType, ValueType> to) {
        copyConfirmedPruningEdges(from.getKnownGraphA(), to);
        copyConfirmedPruningEdges(from.getKnownGraphB(), to);
    }

    private static <KeyType, ValueType> void copyConfirmedPruningEdges(
            ValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> from,
            KnownGraph<KeyType, ValueType> to) {
        for (var ep : from.edges()) {
            var edges = from.edgeValue(ep).orElse(Collections.emptyList());
            for (var edge : edges) {
                if (isPruningEdge(edge.getType())) {
                    to.putEdge(ep.source(), ep.target(), new Edge<>(edge.getType(), edge.getKey()));
                }
            }
        }
    }

    private static <KeyType, ValueType> void addTemporaryPruningEdges(
            KnownGraph<KeyType, ValueType> graph,
            Collection<SEREdge<KeyType, ValueType>> side) {
        for (var edge : side) {
            if (isPruningEdge(edge.getType())) {
                graph.putEdge(edge.getFrom(), edge.getTo(), new Edge<>(edge.getType(), edge.getKey()));
            }
        }
    }

    private static <KeyType, ValueType> MatrixGraph<Transaction<KeyType, ValueType>> dependencyGraph(
            KnownGraph<KeyType, ValueType> graph) {
        var graphA = nonPredicateGraph(graph.getKnownGraphA(), graph.getKnownGraphB());
        var graphB = nonPredicateGraph(graph.getKnownGraphB(), graph.getKnownGraphA());
        var dep = new MatrixGraph<>(graphA);
        var depB = new MatrixGraph<>(graphB, dep.getNodeMap());
        return dep.union(depB);
    }

    private static <KeyType, ValueType> MutableGraph<Transaction<KeyType, ValueType>> nonPredicateGraph(
            ValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> source,
            ValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> other) {
        MutableGraph<Transaction<KeyType, ValueType>> result = GraphBuilder.directed().build();
        source.nodes().forEach(result::addNode);
        other.nodes().forEach(result::addNode);
        for (var ep : source.edges()) {
            var edges = source.edgeValue(ep).orElse(Collections.emptyList());
            if (edges.stream().anyMatch(edge -> !isPredicateEdge(edge.getType()))) {
                result.putEdge(ep.source(), ep.target());
            }
        }
        return result;
    }

    private static <KeyType, ValueType> void addToKnownGraph(KnownGraph<KeyType, ValueType> knownGraph,
            Collection<SEREdge<KeyType, ValueType>> edges) {
        for (var e : edges) {
            switch (e.getType()) {
            case WW:
                knownGraph.putEdge(e.getFrom(), e.getTo(), new Edge<KeyType>(EdgeType.WW, e.getKey()));
                break;
            case RW:
                knownGraph.putEdge(e.getFrom(), e.getTo(), new Edge<KeyType>(e.getType(), e.getKey()));
                break;
            case PR_RW:
                break;
            default:
                throw new Error("only WW, RW and PR_RW edges should appear in constraints");
            }
        }
    }

    private static boolean isPruningEdge(EdgeType type) {
        return type == EdgeType.WW || type == EdgeType.RW;
    }

    private static boolean isPredicateEdge(EdgeType type) {
        return type == EdgeType.PR_WR || type == EdgeType.PR_RW;
    }

    private static Pair<?, ?> emptyConflicts() {
        return Pair.of(Collections.emptyList(), Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    static <KeyType, ValueType> Pair<Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>,
            Collection<SERConstraint<KeyType, ValueType>>> getLastConflicts() {
        return (Pair<Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>,
                Collection<SERConstraint<KeyType, ValueType>>>) lastConflicts;
    }

    private static <KeyType, ValueType> Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>
    extractCycleEdges(KnownGraph<KeyType, ValueType> graph) {
        var adjacency = new HashMap<Transaction<KeyType, ValueType>, Set<Transaction<KeyType, ValueType>>>();
        addAdjacency(graph.getKnownGraphA(), adjacency);
        addAdjacency(graph.getKnownGraphB(), adjacency);

        var color = new HashMap<Transaction<KeyType, ValueType>, Integer>();
        var stack = new ArrayList<Transaction<KeyType, ValueType>>();
        var stackIndex = new HashMap<Transaction<KeyType, ValueType>, Integer>();
        for (var txn : graph.getKnownGraphA().nodes()) {
            if (color.getOrDefault(txn, 0) != 0) {
                continue;
            }
            var cycle = dfsCycle(txn, graph, adjacency, color, stack, stackIndex);
            if (!cycle.isEmpty()) {
                return cycle;
            }
        }
        return Collections.emptyList();
    }

    private static <KeyType, ValueType> void addAdjacency(
            com.google.common.graph.ValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> known,
            Map<Transaction<KeyType, ValueType>, Set<Transaction<KeyType, ValueType>>> adjacency) {
        for (var ep : known.edges()) {
            adjacency.computeIfAbsent(ep.source(), ignored -> new LinkedHashSet<>()).add(ep.target());
        }
    }

    private static <KeyType, ValueType> Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>
    dfsCycle(Transaction<KeyType, ValueType> node,
             KnownGraph<KeyType, ValueType> graph,
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
                var cycle = dfsCycle(succ, graph, adjacency, color, stack, stackIndex);
                if (!cycle.isEmpty()) {
                    return cycle;
                }
            } else if (succColor == 1) {
                var cycleNodes = new ArrayList<>(stack.subList(stackIndex.get(succ), stack.size()));
                cycleNodes.add(succ);
                return cycleEdgesFromNodes(graph, cycleNodes);
            }
        }

        stack.remove(stack.size() - 1);
        stackIndex.remove(node);
        color.put(node, 2);
        return Collections.emptyList();
    }

    private static <KeyType, ValueType> Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>
    cycleEdgesFromNodes(KnownGraph<KeyType, ValueType> graph,
                        List<Transaction<KeyType, ValueType>> cycleNodes) {
        var result = new ArrayList<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>();
        for (int i = 0; i + 1 < cycleNodes.size(); i++) {
            var from = cycleNodes.get(i);
            var to = cycleNodes.get(i + 1);
            var edges = new ArrayList<Edge<KeyType>>();
            edges.addAll(graph.getKnownGraphA().edgeValue(from, to).orElse(List.of()));
            edges.addAll(graph.getKnownGraphB().edgeValue(from, to).orElse(List.of()));
            result.add(Pair.of(EndpointPair.ordered(from, to), edges));
        }
        return result;
    }

}
