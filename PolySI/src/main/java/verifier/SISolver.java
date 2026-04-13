package verifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraph;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import graph.Edge;
import graph.EdgeType;
import graph.MatrixGraph;
import graph.KnownGraph;
import history.History;
import history.Transaction;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;
import util.Profiler;

@SuppressWarnings("UnstableApiUsage")
class SISolver<KeyType, ValueType> {
    private final Solver solver = new Solver();

    // The literals of the known graph
    private final Map<Lit, Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> knownLiterals = new HashMap<>();

    // The literals asserting that exactly one set of edges exists in the graph
    // for each constraint
    private final Map<Lit, SIConstraint<KeyType, ValueType>> constraintLiterals = new HashMap<>();

    boolean solve() {
        var profiler = Profiler.getInstance();
        var lits = Stream
                .concat(knownLiterals.keySet().stream(),
                        constraintLiterals.keySet().stream())
                .collect(Collectors.toList());

        profiler.startTick("SI_SOLVER_SOLVE");
        var result = solver.solve(lits);
        profiler.endTick("SI_SOLVER_SOLVE");

        return result;
    }

    Pair<Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>, Collection<SIConstraint<KeyType, ValueType>>> getConflicts() {
        var edges = new ArrayList<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>();
        var constraints = new ArrayList<SIConstraint<KeyType, ValueType>>();

        solver.getConflictClause().stream().map(Logic::not).forEach(lit -> {
            if (knownLiterals.containsKey(lit)) {
                edges.add(knownLiterals.get(lit));
            } else {
                constraints.add(constraintLiterals.get(lit));
            }
        });
        return Pair.of(edges, constraints);
    }

    /*
     * Construct SISolver from constraints
     *
     * First construct two graphs: 1. Graph A contains WR, WW and SO edges. 2.
     * Graph B contains RW edges.
     *
     * For each edge in A and B, create a literal for it. The edge exists in the
     * final graph iff. the literal is true.
     *
     * Then, construct a third graph C using A and B: If P -> Q in A and Q -> R
     * in B, then P -> R in C The literal of P -> R is ((P -> Q) and (Q -> R)).
     *
     * Lastly, we add graph A and C to monosat, resulting in the final graph.
     *
     * Literals that are passed as assumptions to monograph: 1. The literals of
     * WR, SO edges, because those edges always exist. 2. For each constraint, a
     * literal that asserts exactly one set of edges exist in the graph.
     */
    SISolver(History<KeyType, ValueType> history,
            KnownGraph<KeyType, ValueType> precedenceGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints) {
        var profiler = Profiler.getInstance();

        // CRITICAL: Re-derive predicate edges in SISolver constructor.
        // Pruning clears them, so they must be refreshed here before SAT encoding.
        SIVerifier.refreshDerivedPredicateEdges(history, precedenceGraph);

        int prWrCount = 0, prRwCount = 0;
        for (var ep : precedenceGraph.getKnownGraphA().edges()) {
            var edgeVals = precedenceGraph.getKnownGraphA().edgeValue(ep).orElse(null);
            if (edgeVals == null) continue;
            for (var edge : edgeVals) {
                if (edge.getType() == EdgeType.PR_WR) prWrCount++;
            }
        }
        for (var ep : precedenceGraph.getKnownGraphB().edges()) {
            var edgeVals = precedenceGraph.getKnownGraphB().edgeValue(ep).orElse(null);
            if (edgeVals == null) continue;
            for (var edge : edgeVals) {
                if (edge.getType() == EdgeType.PR_RW) prRwCount++;
            }
        }
        System.err.printf("[SER-Inject] PR_WR edges in A: %d, PR_RW edges in B: %d\n",
                prWrCount, prRwCount);

        profiler.startTick("SI_SOLVER_GEN");
        profiler.startTick("SI_SOLVER_GEN_GRAPH_A_B");
        var graphA = createKnownGraph(history,
                precedenceGraph.getKnownGraphA());
        var graphB = createKnownGraph(history,
                precedenceGraph.getKnownGraphB());
        profiler.endTick("SI_SOLVER_GEN_GRAPH_A_B");

        profiler.startTick("SI_SOLVER_GEN_REACHABILITY");
        var matA = new MatrixGraph<>(graphA.asGraph());
        var orderInSession = Utils.getOrderInSession(history);
        var matAC = Utils.reduceEdges(
                matA.union(
                        matA.composition(new MatrixGraph<>(graphB.asGraph(), matA.getNodeMap()))),
                orderInSession);
        var reachability = matAC.reachability();
        profiler.endTick("SI_SOLVER_GEN_REACHABILITY");

        profiler.startTick("SI_SOLVER_GEN_GRAPH_A_UNION_C");
        var knownEdges = Utils.getKnownEdges(graphA, graphB, matAC);
        addConstraints(constraints, graphA, graphB);

        // Collect PR_RW pairs from graphB (they are "known" and must not participate in A∘B composition)
        var knownPrRwPairs = new HashSet<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>>();
        for (var ep : graphB.edges()) {
            var edgeVals = graphB.edgeValue(ep).orElse(null);
            if (edgeVals == null) continue;
            for (var lit : edgeVals) {
                // Check if this literal corresponds to a PR_RW edge via knownLiterals
                if (knownLiterals.containsKey(lit)) {
                    var meta = knownLiterals.get(lit);
                    var edges = meta.getRight();
                    for (var edge : edges) {
                        if (edge.getType() == EdgeType.PR_RW) {
                            knownPrRwPairs.add(Pair.of(ep.source(), ep.target()));
                            break;
                        }
                    }
                }
            }
        }
        var unknownEdges = Utils.getUnknownEdges(graphA, graphB, reachability,
                solver, knownPrRwPairs);
        profiler.endTick("SI_SOLVER_GEN_GRAPH_A_UNION_C");

        int knownA = 0, knownB = 0;
        for (var p : List.of(Pair.of('A', graphA), Pair.of('B', graphB))) {
            var g = p.getRight();
            var edgesSize = g.edges().stream()
                    .map(e -> g.edgeValue(e).get().size()).reduce(Integer::sum)
                    .orElse(0);
            System.err.printf("Graph %s edges count: %d\n", p.getLeft(),
                    edgesSize);
            if (p.getLeft() == 'A') knownA = edgesSize;
            else knownB = edgesSize;
        }

        int prWrInMono = 0, prRwInMono = 0;
        for (var e : knownEdges) {
            // Count PR edges in the merged graph for debugging
        }
        System.err.printf("Graph A union C edges count: %d\n",
                knownEdges.size() + unknownEdges.size());
        System.err.printf("Merged SER graph (A+B): %d known edges, %d unknown edges\n",
                knownEdges.size(), unknownEdges.size());

        profiler.startTick("SI_SOLVER_GEN_MONO_GRAPH");
        var monoGraph = new monosat.Graph(solver);
        var nodeMap = new HashMap<Transaction<KeyType, ValueType>, Integer>();

        history.getTransactions().forEach(n -> {
            nodeMap.put(n, monoGraph.addNode());
        });

        var addToMonoSAT = ((Consumer<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>>) e -> {
            var n = e.getLeft();
            var s = e.getMiddle();
            solver.assertEqual(e.getRight(),
                    monoGraph.addEdge(nodeMap.get(n), nodeMap.get(s)));
        });

        // Count predicate edges being added to MonoSAT
        int prWrInKnown = 0, prRwInKnown = 0;
        for (var e : knownEdges) {
            addToMonoSAT.accept(e);
        }
        // PR_RW edges are part of knownEdges (via the A∘B synthesis), not duplicated here.
        // (No separate PR_RW handling needed: all PR_RW edges that are part of A∘B
        // are already captured in knownEdges; PR_RW edges not in A∘B would need separate
        // handling, but in this architecture they are handled via constraints.)
        for (var e : unknownEdges) {
            addToMonoSAT.accept(e);
        }
        solver.assertTrue(monoGraph.acyclic());

        profiler.endTick("SI_SOLVER_GEN_MONO_GRAPH");
        profiler.endTick("SI_SOLVER_GEN");
    }

    private MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> createKnownGraph(
            History<KeyType, ValueType> history,
            ValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> knownGraph) {
        var g = Utils.createEmptyGraph(history);
        for (var e : knownGraph.edges()) {
            var lit = new Lit(solver);
            knownLiterals.put(lit, Pair.of(e, knownGraph.edgeValue(e).get()));
            Utils.addEdge(g, e.source(), e.target(), lit);
        }

        return g;
    }

    private void addConstraints(
            Collection<SIConstraint<KeyType, ValueType>> constraints,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphA,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphB) {
        var addEdges = ((Function<Collection<SIEdge<KeyType, ValueType>>, Pair<Lit, Lit>>) edges -> {
            // all means all edges exists in the graph.
            // Similar for none.
            Lit all = Lit.True, none = Lit.True;
            for (var e : edges) {
                var lit = new Lit(solver);
                var not = Logic.not(lit);
                all = Logic.and(all, lit);
                none = Logic.and(none, not);
                solver.setDecisionLiteral(lit, false);
                solver.setDecisionLiteral(not, false);
                solver.setDecisionLiteral(all, false);
                solver.setDecisionLiteral(none, false);


                if (e.getType().equals(EdgeType.WW)) {
                    Utils.addEdge(graphA, e.getFrom(), e.getTo(), lit);
                } else if (e.getType().equals(EdgeType.RW) || e.getType().equals(EdgeType.PR_RW)) {
                    Utils.addEdge(graphB, e.getFrom(), e.getTo(), lit);
                } else {
                    Utils.addEdge(graphA, e.getFrom(), e.getTo(), lit);
                }
            }
            return Pair.of(all, none);
        });

        for (var c : constraints) {
            var p1 = addEdges.apply(c.getEdges1());
            var p2 = addEdges.apply(c.getEdges2());

            constraintLiterals
                    .put(Logic.or(Logic.and(p1.getLeft(), p2.getRight()),
                            Logic.and(p2.getLeft(), p1.getRight())), c);
        }
    }
}
