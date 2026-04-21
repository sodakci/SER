import static history.Event.EventType.PREDICATE_READ;
import static history.Event.EventType.READ;
import static history.Event.EventType.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import history.History;
import history.Transaction;
import history.loaders.PredicateHistoryLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class TestPredicateHistoryLoader {
    @Test
    void loadsPredicateJsonlHistoryAndBuildsPredicateEvents() throws Exception {
        var historyDir = Files.createTempDirectory("prhist-loader");
        Files.writeString(historyDir.resolve("history.prhist.jsonl"), String.join("\n",
                "{\"session\":0,\"txn\":0,\"kind\":\"inventory.reserve_predicate\",\"status\":\"commit\",\"ops\":["
                        + "{\"type\":\"pr\",\"predicate\":{\"kind\":\"inventory_threshold\",\"key_prefix\":\"inventory_onhand_\",\"comparator\":\"ge\",\"threshold\":80,\"limit\":8},\"results\":["
                        + "{\"key\":\"inventory_onhand_0001\",\"value\":81000001,\"semantic\":81,\"source_txn\":-1}]},"
                        + "{\"type\":\"r\",\"key\":\"inventory_reserved_0001\",\"value\":10000002,\"semantic\":1,\"source_txn\":-1},"
                        + "{\"type\":\"w\",\"key\":\"inventory_reserved_0001\",\"value\":30000003,\"semantic\":3}]}",
                "{\"session\":1,\"txn\":1,\"kind\":\"order.dispatch_predicate\",\"status\":\"commit\",\"ops\":["
                        + "{\"type\":\"pr\",\"predicate\":{\"kind\":\"order_filter\",\"key_prefix\":\"order_status_\",\"comparator\":\"ge\",\"threshold\":0,\"allowed_semantics\":[1,2],\"limit\":5},\"results\":["
                        + "{\"key\":\"order_status_0002\",\"value\":20000004,\"semantic\":2,\"source_txn\":-1}]},"
                        + "{\"type\":\"w\",\"key\":\"warehouse_lane_0002\",\"value\":50000005,\"semantic\":5}]}",
                "{\"session\":2,\"txn\":2,\"kind\":\"search.rank_predicate\",\"status\":\"commit\",\"ops\":["
                        + "{\"type\":\"pr\",\"predicate\":{\"kind\":\"search_ranked_docs\",\"key_prefix\":\"search_score_\",\"min_score\":90,\"require_visible\":true,\"freshness_floor\":12,\"limit\":5},\"results\":["
                        + "{\"key\":\"search_score_0003\",\"value\":95000006,\"semantic\":95,\"source_txn\":-1}]},"
                        + "{\"type\":\"r\",\"key\":\"search_score_0003\",\"value\":95000006,\"semantic\":95,\"source_txn\":-1},"
                        + "{\"type\":\"w\",\"key\":\"search_cache_0003\",\"value\":70000007,\"semantic\":7}]}"));
        Files.writeString(historyDir.resolve("manifest.json"), "{}");

        History<String, PredicateHistoryLoader.PredicateValue> history =
                new PredicateHistoryLoader(historyDir).loadHistory();

        assertEquals(4, history.getSessions().size());
        assertEquals(4, history.getTransactions().size());

        var initTxn = history.getTransaction(-1);
        assertNotNull(initTxn);
        assertEquals(Transaction.TransactionStatus.COMMIT, initTxn.getStatus());
        assertEquals(4, initTxn.getEvents().size());

        var txn0 = history.getTransaction(0);
        assertEquals(Transaction.TransactionStatus.COMMIT, txn0.getStatus());
        assertEquals(PREDICATE_READ, txn0.getEvents().get(0).getType());
        assertEquals(READ, txn0.getEvents().get(1).getType());
        assertEquals(WRITE, txn0.getEvents().get(2).getType());
        assertTrue(txn0.getEvents().get(0).getPredicate()
                .test("inventory_onhand_9999", new PredicateHistoryLoader.PredicateValue(123L, 90)));
        assertTrue(!txn0.getEvents().get(0).getPredicate()
                .test("inventory_onhand_9999", new PredicateHistoryLoader.PredicateValue(123L, 70)));

        var txn1Predicate = history.getTransaction(1).getEvents().get(0);
        assertTrue(txn1Predicate.getPredicate()
                .test("order_status_1234", new PredicateHistoryLoader.PredicateValue(124L, 2)));
        assertTrue(!txn1Predicate.getPredicate()
                .test("order_status_1234", new PredicateHistoryLoader.PredicateValue(124L, 3)));

        var txn2Predicate = history.getTransaction(2).getEvents().get(0);
        assertTrue(txn2Predicate.getPredicate()
                .test("search_score_1234", new PredicateHistoryLoader.PredicateValue(125L, 95)));
        assertTrue(!txn2Predicate.getPredicate()
                .test("search_score_1234", new PredicateHistoryLoader.PredicateValue(125L, 85)));

        var initKeys = initTxn.getEvents().stream()
                .map(ev -> ev.getKey())
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());
        assertEquals(
                java.util.List.of("inventory_onhand_0001", "inventory_reserved_0001",
                        "order_status_0002", "search_score_0003"),
                initKeys);
    }

    @Test
    void acceptsDirectHistoryFilePath() throws Exception {
        Path file = Files.createTempFile("single-prhist", ".jsonl");
        Files.writeString(file,
                "{\"session\":0,\"txn\":0,\"kind\":\"inventory.ship\",\"status\":\"commit\",\"ops\":["
                        + "{\"type\":\"r\",\"key\":\"inventory_reserved_0001\",\"value\":10000001,\"semantic\":1,\"source_txn\":-1},"
                        + "{\"type\":\"w\",\"key\":\"inventory_onhand_0001\",\"value\":90000002,\"semantic\":90}]}");

        var history = new PredicateHistoryLoader(file).loadHistory();
        assertEquals(2, history.getTransactions().size());
        assertEquals(Transaction.TransactionStatus.COMMIT, history.getTransaction(0).getStatus());
        assertNotNull(history.getTransaction(-1));
    }
}
