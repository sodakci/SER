package verifier;

import graph.KnownGraph;
import history.Event;
import history.History;
import history.Session;
import history.Transaction;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 纯单元测试：验证 SERVerifier 中的 predicate 边推导辅助函数。
 * 所有方法均为 private static，通过反射调用。
 *
 * 测试目标：
 * 1. matchesPredicate — 写值是否满足谓词
 * 2. isFlipWitness    — 是否为 flip witness
 * 3. resolveLatestFlipWitnessBeforeIndex — 找最近 flip
 * 4. uniqueTopologicalSort — 拓扑排序唯一性
 * 5. buildWritesByKey — 按 key 分组 writes
 */
public class SERVerifierPredicateTest {

    // ================================================================
    // 辅助：最小 fixture 构造
    // ================================================================

    private static History<String, Integer> makeHistory(
            List<Triple<String, Event.EventType, Integer>> events) {
        return makeHistory(events, Map.of());
    }

    private static History<String, Integer> makeHistory(
            List<Triple<String, Event.EventType, Integer>> events,
            Map<Long, List<Long>> txnMap) {
        Map<Long, List<Triple<Event.EventType, String, Integer>>> txnEvents = new HashMap<>();
        for (int i = 0; i < events.size(); i++) {
            int txnId = i; // 每个事件一个独立事务
            txnEvents.computeIfAbsent((long) txnId, k -> new ArrayList<>())
                    .add(Triple.of(events.get(i).getMiddle(), events.get(i).getLeft(), events.get(i).getRight()));
        }
        // 合并外部传入的 txnMap
        for (var e : txnMap.entrySet()) {
            txnEvents.put(e.getKey(), txnEvents.getOrDefault(e.getKey(), new ArrayList<>()));
        }

        Set<Long> sessions = new HashSet<>();
        Map<Long, List<Long>> finalTxnMap = new HashMap<>();
        for (Long txnId : txnEvents.keySet()) {
            long sessId = txnId; // 1:1 mapping for simplicity
            sessions.add(sessId);
            finalTxnMap.computeIfAbsent(sessId, k -> new ArrayList<>()).add(txnId);
        }

        return new History<>(sessions, finalTxnMap, txnEvents);
    }

    private static History<String, Integer> makeSingleSessionHistory(
            List<Triple<String, Event.EventType, Integer>> events) {
        Map<Long, List<Triple<Event.EventType, String, Integer>>> txnEvents = new HashMap<>();
        txnEvents.put(0L, new ArrayList<>());
        for (var e : events) {
            txnEvents.get(0L).add(Triple.of(e.getMiddle(), e.getLeft(), e.getRight()));
        }
        return new History<>(Set.of(0L), Map.of(0L, List.of(0L)), txnEvents);
    }

    private static KnownGraph<String, Integer> makeGraph(History<String, Integer> h) {
        return new KnownGraph<>(h);
    }

    private static KnownGraph.WriteRef<String, Integer> makeWriteRef(
            KnownGraph<String, Integer> graph, String key, Integer value) {
        var entry = graph.getWrites().entrySet().stream()
                .filter(e -> e.getKey().getLeft().equals(key) && e.getKey().getRight().equals(value))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Write not found: " + key + "=" + value));
        return entry.getValue();
    }

    private static Event<String, Integer> makePredicateReadEvent(
            Transaction<String, Integer> txn,
            Event.PredEval<String, Integer> predicate,
            List<Event.PredResult<String, Integer>> results) {
        return new Event<>(txn, Event.EventType.PREDICATE_READ, null, null, predicate, results);
    }

    // ================================================================
    // 反射调用工具
    // ================================================================

    private static Method findMethod(Class<?> cls, String name, Class<?>... paramTypes) {
        try {
            var m = cls.getDeclaredMethod(name, paramTypes);
            m.setAccessible(true);
            return m;
        } catch (NoSuchMethodException e) {
            throw new AssertionError("Method not found: " + name, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <R> R invokeStatic(Method m, Object... args) {
        try {
            return (R) m.invoke(null, args);
        } catch (Exception e) {
            throw new AssertionError("Reflection invoke failed", e);
        }
    }

    // ================================================================
    // Test 1: matchesPredicate — 写值是否满足谓词
    // ================================================================

    @Test
    void matchesPredicate_writeValueSatisfiesPredicate() throws Exception {
        // 构造历史: T0: W(x, 10)
        var h = makeSingleSessionHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 10)
        ));
        var graph = makeGraph(h);

        // 谓词: value > 5
        Event.PredEval<String, Integer> pred = (k, v) -> v > 5;
        var writeRef = makeWriteRef(graph, "x", 10);

        // 构造一个最小 predicate read event（用于传参）
        var prEvent = makePredicateReadEvent(
                h.getTransaction(0L), pred, List.of());

        Method m = findMethod(SERVerifier.class, "matchesPredicate",
                KnownGraph.WriteRef.class, Event.class);
        boolean result = invokeStatic(m, writeRef, prEvent);

        assertTrue(result, "value=10 should satisfy predicate v > 5");
    }

    @Test
    void matchesPredicate_writeValueDoesNotSatisfyPredicate() throws Exception {
        // 构造历史: T0: W(x, 3)
        var h = makeSingleSessionHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 3)
        ));
        var graph = makeGraph(h);

        // 谓词: value > 5
        Event.PredEval<String, Integer> pred = (k, v) -> v > 5;
        var writeRef = makeWriteRef(graph, "x", 3);

        var prEvent = makePredicateReadEvent(
                h.getTransaction(0L), pred, List.of());

        Method m = findMethod(SERVerifier.class, "matchesPredicate",
                KnownGraph.WriteRef.class, Event.class);
        boolean result = invokeStatic(m, writeRef, prEvent);

        assertFalse(result, "value=3 should NOT satisfy predicate v > 5");
    }

    @Test
    void matchesPredicate_predicateOnKey() throws Exception {
        // 构造历史: T0: W(x, 100)
        var h = makeSingleSessionHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 100)
        ));
        var graph = makeGraph(h);

        // 谓词: key == "x"
        Event.PredEval<String, Integer> pred = (k, v) -> "x".equals(k);
        var writeRef = makeWriteRef(graph, "x", 100);

        var prEvent = makePredicateReadEvent(
                h.getTransaction(0L), pred, List.of());

        Method m = findMethod(SERVerifier.class, "matchesPredicate",
                KnownGraph.WriteRef.class, Event.class);
        boolean result = invokeStatic(m, writeRef, prEvent);

        assertTrue(result, "key=x should satisfy predicate key == 'x'");
    }

    // ================================================================
    // Test 2: isFlipWitness — 是否为 flip witness
    // ================================================================

    @Test
    void isFlipWitness_prevNotMatch_currMatches() throws Exception {
        // 构造: T0: W(x, 1) -> T1: W(x, 10)
        // predicate: v > 5
        // prev(T0): 1 不满足 | curr(T1): 10 满足  => flip = true
        var h = makeHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 1),
                Triple.of("x", Event.EventType.WRITE, 10)
        ), Map.of(0L, List.of(0L), 1L, List.of(1L)));
        var graph = makeGraph(h);

        var pred = (Event.PredEval<String, Integer>) (k, v) -> v > 5;
        var prevRef = makeWriteRef(graph, "x", 1);
        var currRef = makeWriteRef(graph, "x", 10);

        var prEvent = makePredicateReadEvent(h.getTransaction(1L), pred, List.of());

        Method m = findMethod(SERVerifier.class, "isFlipWitness",
                KnownGraph.WriteRef.class, KnownGraph.WriteRef.class, Event.class);
        boolean result = invokeStatic(m, currRef, prevRef, prEvent);

        assertTrue(result, "prev=1(!match) -> curr=10(match) should be flip witness");
    }

    @Test
    void isFlipWitness_prevMatches_currNotMatch() throws Exception {
        // 构造: T0: W(x, 10) -> T1: W(x, 1)
        // predicate: v > 5
        // prev(T0): 10 满足 | curr(T1): 1 不满足  => flip = true
        var h = makeHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 10),
                Triple.of("x", Event.EventType.WRITE, 1)
        ), Map.of(0L, List.of(0L), 1L, List.of(1L)));
        var graph = makeGraph(h);

        var pred = (Event.PredEval<String, Integer>) (k, v) -> v > 5;
        var prevRef = makeWriteRef(graph, "x", 10);
        var currRef = makeWriteRef(graph, "x", 1);

        var prEvent = makePredicateReadEvent(h.getTransaction(1L), pred, List.of());

        Method m = findMethod(SERVerifier.class, "isFlipWitness",
                KnownGraph.WriteRef.class, KnownGraph.WriteRef.class, Event.class);
        boolean result = invokeStatic(m, currRef, prevRef, prEvent);

        assertTrue(result, "prev=10(match) -> curr=1(!match) should be flip witness");
    }

    @Test
    void isFlipWitness_bothMatch() throws Exception {
        // prev(T0): 10 满足 | curr(T1): 20 满足  => flip = false
        var h = makeHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 10),
                Triple.of("x", Event.EventType.WRITE, 20)
        ), Map.of(0L, List.of(0L), 1L, List.of(1L)));
        var graph = makeGraph(h);

        var pred = (Event.PredEval<String, Integer>) (k, v) -> v > 5;
        var prevRef = makeWriteRef(graph, "x", 10);
        var currRef = makeWriteRef(graph, "x", 20);

        var prEvent = makePredicateReadEvent(h.getTransaction(1L), pred, List.of());

        Method m = findMethod(SERVerifier.class, "isFlipWitness",
                KnownGraph.WriteRef.class, KnownGraph.WriteRef.class, Event.class);
        boolean result = invokeStatic(m, currRef, prevRef, prEvent);

        assertFalse(result, "prev=10(match) -> curr=20(match) should NOT be flip witness");
    }

    @Test
    void isFlipWitness_neitherMatch() throws Exception {
        // prev(T0): 1 不满足 | curr(T1): 2 不满足  => flip = false
        var h = makeHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 1),
                Triple.of("x", Event.EventType.WRITE, 2)
        ), Map.of(0L, List.of(0L), 1L, List.of(1L)));
        var graph = makeGraph(h);

        var pred = (Event.PredEval<String, Integer>) (k, v) -> v > 5;
        var prevRef = makeWriteRef(graph, "x", 1);
        var currRef = makeWriteRef(graph, "x", 2);

        var prEvent = makePredicateReadEvent(h.getTransaction(1L), pred, List.of());

        Method m = findMethod(SERVerifier.class, "isFlipWitness",
                KnownGraph.WriteRef.class, KnownGraph.WriteRef.class, Event.class);
        boolean result = invokeStatic(m, currRef, prevRef, prEvent);

        assertFalse(result, "prev=1(!match) -> curr=2(!match) should NOT be flip witness");
    }

    @Test
    void isFlipWitness_nullPredecessor_treatedAsInitialState() throws Exception {
        // prev=null, curr=3 不满足 predicate v>5 => false ^ false = false
        // prev=null, curr=10 满足 predicate v>5 => false ^ true = true
        var h = makeSingleSessionHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 10)
        ));
        var graph = makeGraph(h);

        var pred = (Event.PredEval<String, Integer>) (k, v) -> v > 5;
        var currRef = makeWriteRef(graph, "x", 10);

        var prEvent = makePredicateReadEvent(h.getTransaction(0L), pred, List.of());

        Method m = findMethod(SERVerifier.class, "isFlipWitness",
                KnownGraph.WriteRef.class, KnownGraph.WriteRef.class, Event.class);

        // null predecessor -> initial state treated as NOT matching
        boolean result = invokeStatic(m, currRef, null, prEvent);
        assertTrue(result, "null predecessor + curr=10(match) should be flip (initial state = false)");
    }

    // ================================================================
    // Test 3: resolveLatestFlipWitnessBeforeIndex
    // ================================================================

    @Test
    void resolveLatestFlipWitnessBeforeIndex_returnsClosestFlip() throws Exception {
        // W(x,1) [T0]  不满足  -> W(x,10) [T1] 满足  -> W(x,20) [T2] 满足  -> W(x,3) [T3]  不满足
        // 谓词: v > 5
        // flip 在 T1 (1->10) 和 T3 (20->3)
        // 从 idx=3 向前找: T3 是 flip → 返回 T3
        var h = makeHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 1),
                Triple.of("x", Event.EventType.WRITE, 10),
                Triple.of("x", Event.EventType.WRITE, 20),
                Triple.of("x", Event.EventType.WRITE, 3)
        ), Map.of(0L, List.of(0L), 1L, List.of(1L), 2L, List.of(2L), 3L, List.of(3L)));
        var graph = makeGraph(h);

        var pred = (Event.PredEval<String, Integer>) (k, v) -> v > 5;
        var prEvent = makePredicateReadEvent(h.getTransaction(0L), pred, List.of());

        List<KnownGraph.WriteRef<String, Integer>> orderedWrites = List.of(
                makeWriteRef(graph, "x", 1),
                makeWriteRef(graph, "x", 10),
                makeWriteRef(graph, "x", 20),
                makeWriteRef(graph, "x", 3)
        );

        Method m = findMethod(SERVerifier.class, "resolveLatestFlipWitnessBeforeIndex",
                List.class, int.class, Event.class);
        KnownGraph.WriteRef<String, Integer> result =
                invokeStatic(m, orderedWrites, 3, prEvent);

        assertNotNull(result, "Should find a flip witness before index 3");
        assertEquals(3L, result.getTxn().getId(), "Latest flip before idx=3 should be T3 (x=3)");
    }

    @Test
    void resolveLatestFlipWitnessBeforeIndex_noFlip_returnsNull() throws Exception {
        // 所有写都不满足 predicate v > 5: 1, 2, 3
        // prev=null (初始状态不满足), curr=1 不满足 => false^false = false (不是 flip)
        // prev=1(curr=2): false^false = false; prev=2(curr=3): false^false = false
        // 没有任何 flip -> 返回 null
        var h = makeHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 1),
                Triple.of("x", Event.EventType.WRITE, 2),
                Triple.of("x", Event.EventType.WRITE, 3)
        ), Map.of(0L, List.of(0L), 1L, List.of(1L), 2L, List.of(2L)));
        var graph = makeGraph(h);

        var pred = (Event.PredEval<String, Integer>) (k, v) -> v > 5;
        var prEvent = makePredicateReadEvent(h.getTransaction(0L), pred, List.of());

        List<KnownGraph.WriteRef<String, Integer>> orderedWrites = List.of(
                makeWriteRef(graph, "x", 1),
                makeWriteRef(graph, "x", 2),
                makeWriteRef(graph, "x", 3)
        );

        Method m = findMethod(SERVerifier.class, "resolveLatestFlipWitnessBeforeIndex",
                List.class, int.class, Event.class);
        KnownGraph.WriteRef<String, Integer> result =
                invokeStatic(m, orderedWrites, 2, prEvent);

        assertNull(result, "All writes non-matching predicate: no flip witness should return null");
    }

    @Test
    void resolveLatestFlipWitnessBeforeIndex_firstMatchingIsFlip() throws Exception {
        // 验证: 初始状态=null(不满足), 第一个满足谓词的写就是 flip
        // W(x,10) 满足 predicate v>5, 前驱=null -> false^true = true -> 是 flip
        var h = makeSingleSessionHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 10)
        ));
        var graph = makeGraph(h);

        var pred = (Event.PredEval<String, Integer>) (k, v) -> v > 5;
        var prEvent = makePredicateReadEvent(h.getTransaction(0L), pred, List.of());

        List<KnownGraph.WriteRef<String, Integer>> orderedWrites = List.of(
                makeWriteRef(graph, "x", 10)
        );

        Method m = findMethod(SERVerifier.class, "resolveLatestFlipWitnessBeforeIndex",
                List.class, int.class, Event.class);
        KnownGraph.WriteRef<String, Integer> result =
                invokeStatic(m, orderedWrites, 0, prEvent);

        assertNotNull(result, "First matching write should be flip (prev=null treated as non-match)");
        assertEquals(10, result.getEvent().getValue());
    }

    @Test
    void resolveLatestFlipWitnessBeforeIndex_obsIdxIsFlip() throws Exception {
        // obsIdx 本身正好是 flip (x=3)
        var h = makeHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 10),
                Triple.of("x", Event.EventType.WRITE, 3)
        ), Map.of(0L, List.of(0L), 1L, List.of(1L)));
        var graph = makeGraph(h);

        var pred = (Event.PredEval<String, Integer>) (k, v) -> v > 5;
        var prEvent = makePredicateReadEvent(h.getTransaction(0L), pred, List.of());

        List<KnownGraph.WriteRef<String, Integer>> orderedWrites = List.of(
                makeWriteRef(graph, "x", 10),
                makeWriteRef(graph, "x", 3)
        );

        Method m = findMethod(SERVerifier.class, "resolveLatestFlipWitnessBeforeIndex",
                List.class, int.class, Event.class);
        KnownGraph.WriteRef<String, Integer> result =
                invokeStatic(m, orderedWrites, 1, prEvent);

        assertNotNull(result);
        assertEquals(1L, result.getTxn().getId(), "Index 1 is the flip witness (x=3)");
    }

    // ================================================================
    // Test 4: uniqueTopologicalSort
    // ================================================================

    @Test
    void uniqueTopologicalSort_linearOrder() throws Exception {
        // A -> B -> C 只有唯一拓扑序
        List<String> nodes = List.of("A", "B", "C");
        Map<String, Set<String>> succs = new HashMap<>();
        succs.put("A", Set.of("B"));
        succs.put("B", Set.of("C"));

        Method m = findMethod(SERVerifier.class, "uniqueTopologicalSort",
                List.class, Map.class);
        List<String> result = invokeStatic(m, nodes, succs);

        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("A", result.get(0));
        assertEquals("B", result.get(1));
        assertEquals("C", result.get(2));
    }

    @Test
    void uniqueTopologicalSort_incomparable_returnsNull() throws Exception {
        // A, B 无边，不可比 -> 不止一种拓扑序
        List<String> nodes = List.of("A", "B");
        Map<String, Set<String>> succs = new HashMap<>();

        Method m = findMethod(SERVerifier.class, "uniqueTopologicalSort",
                List.class, Map.class);
        var result = invokeStatic(m, nodes, succs);

        assertNull(result, "Incomparable nodes should return null (not a total order)");
    }

    @Test
    void uniqueTopologicalSort_parallelEdges_returnsNull() throws Exception {
        // A -> B, A -> C, B -> D, C -> D
        // 拓扑序: A 必须第一，但 B 和 C 是平行的（都依赖 A，都指向 D）
        // 入度: A=0, B=1, C=1, D=2
        // 第一步: 队列=[A] ✓; 处理 A 后: B入度=0, C入度=0 -> 队列=[B,C] -> size>1 -> 返回 null
        List<String> nodes = List.of("A", "B", "C", "D");
        Map<String, Set<String>> succs = new HashMap<>();
        succs.put("A", Set.of("B", "C"));
        succs.put("B", Set.of("D"));
        succs.put("C", Set.of("D"));

        Method m = findMethod(SERVerifier.class, "uniqueTopologicalSort",
                List.class, Map.class);
        List<String> result = invokeStatic(m, nodes, succs);

        assertNull(result, "B and C are parallel (queue.size > 1 at step 2) — not a unique total order");
    }

    @Test
    void uniqueTopologicalSort_cycle_returnsNull() throws Exception {
        // A -> B -> C -> A 循环
        List<String> nodes = List.of("A", "B", "C");
        Map<String, Set<String>> succs = new HashMap<>();
        succs.put("A", Set.of("B"));
        succs.put("B", Set.of("C"));
        succs.put("C", Set.of("A"));

        Method m = findMethod(SERVerifier.class, "uniqueTopologicalSort",
                List.class, Map.class);
        var result = invokeStatic(m, nodes, succs);

        assertNull(result, "Cycle should return null");
    }

    // ================================================================
    // Test 5: buildWritesByKey
    // ================================================================

    @Test
    void buildWritesByKey_groupsWritesByKey() throws Exception {
        // W(x,1), W(y,2), W(x,3)
        var h = makeHistory(List.of(
                Triple.of("x", Event.EventType.WRITE, 1),
                Triple.of("y", Event.EventType.WRITE, 2),
                Triple.of("x", Event.EventType.WRITE, 3)
        ), Map.of(0L, List.of(0L), 1L, List.of(1L), 2L, List.of(2L)));
        var graph = makeGraph(h);

        Method m = findMethod(SERVerifier.class, "buildWritesByKey",
                KnownGraph.class);
        @SuppressWarnings("unchecked")
        Map<String, List<KnownGraph.WriteRef<String, Integer>>> result =
                invokeStatic(m, graph);

        assertEquals(2, result.size(), "Should have 2 keys: x and y");
        assertEquals(2, result.get("x").size(), "Key x should have 2 writes");
        assertEquals(1, result.get("y").size(), "Key y should have 1 write");
    }

    @Test
    void buildWritesByKey_emptyGraph() throws Exception {
        var h = makeSingleSessionHistory(List.of()); // 空历史
        var graph = makeGraph(h);

        Method m = findMethod(SERVerifier.class, "buildWritesByKey",
                KnownGraph.class);
        @SuppressWarnings("unchecked")
        Map<String, List<KnownGraph.WriteRef<String, Integer>>> result =
                invokeStatic(m, graph);

        assertTrue(result.isEmpty(), "Empty graph should produce empty map");
    }
}
