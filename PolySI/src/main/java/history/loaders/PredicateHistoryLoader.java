package history.loaders;

import static history.Event.EventType.READ;
import static history.Event.EventType.WRITE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import history.Event;
import history.History;
import history.InvalidHistoryError;
import history.Session;
import history.Transaction;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;

public class PredicateHistoryLoader implements history.HistoryLoader<String, PredicateHistoryLoader.PredicateValue> {
    private static final String HISTORY_FILE = "history.prhist.jsonl";
    private static final long INIT_SESSION_ID = -1L;
    private static final long INIT_TXN_ID = -1L;

    private final File historyFile;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PredicateHistoryLoader(Path path) {
        var file = path.toFile();
        historyFile = file.isDirectory() ? path.resolve(HISTORY_FILE).toFile() : file;

        if (!historyFile.isFile()) {
            throw new Error(String.format("%s is not a predicate history file", historyFile));
        }
    }

    @Override
    @SneakyThrows
    public History<String, PredicateValue> loadHistory() {
        var history = new History<String, PredicateValue>();
        var initWrites = new LinkedHashMap<String, PredicateValue>();

        try (var in = new BufferedReader(new FileReader(historyFile))) {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                parseTransaction(history, initWrites, objectMapper.readTree(line));
            }
        }

        if (!initWrites.isEmpty()) {
            var initSession = history.getSession(INIT_SESSION_ID);
            if (initSession == null) {
                initSession = history.addSession(INIT_SESSION_ID);
            }
            var initTxn = history.getTransaction(INIT_TXN_ID);
            if (initTxn == null) {
                initTxn = history.addTransaction(initSession, INIT_TXN_ID);
            }
            for (var entry : initWrites.entrySet()) {
                history.addEvent(initTxn, WRITE, entry.getKey(), entry.getValue());
            }
            initTxn.setStatus(Transaction.TransactionStatus.COMMIT);
        }

        return history;
    }

    private void parseTransaction(History<String, PredicateValue> history,
            LinkedHashMap<String, PredicateValue> initWrites, JsonNode txnNode) {
        var status = requiredText(txnNode, "status");
        if (!"commit".equalsIgnoreCase(status)) {
            throw new InvalidHistoryError();
        }

        var sessionId = requiredLong(txnNode, "session");
        var txnId = requiredLong(txnNode, "txn");
        var session = history.getSession(sessionId);
        if (session == null) {
            session = history.addSession(sessionId);
        }

        if (history.getTransaction(txnId) != null) {
            throw new InvalidHistoryError();
        }
        var transaction = history.addTransaction(session, txnId);

        for (var opNode : requiredArray(txnNode, "ops")) {
            parseOperation(history, initWrites, transaction, opNode);
        }
        transaction.setStatus(Transaction.TransactionStatus.COMMIT);
    }

    private void parseOperation(History<String, PredicateValue> history,
            LinkedHashMap<String, PredicateValue> initWrites,
            Transaction<String, PredicateValue> transaction, JsonNode opNode) {
        var type = requiredText(opNode, "type");
        switch (type) {
        case "w":
            history.addEvent(transaction, WRITE, requiredText(opNode, "key"), parseValue(opNode));
            break;
        case "r": {
            var key = requiredText(opNode, "key");
            var value = parseValue(opNode);
            history.addEvent(transaction, READ, key, value);
            recordInitWrite(initWrites, key, value, requiredSourceTxn(opNode));
            break;
        }
        case "pr":
            history.addPredicateReadEvent(transaction,
                    parsePredicate(requiredObject(opNode, "predicate")),
                    parsePredicateResults(initWrites, requiredArray(opNode, "results")));
            break;
        default:
            throw new InvalidHistoryError();
        }
    }

    private List<Event.PredResult<String, PredicateValue>> parsePredicateResults(
            LinkedHashMap<String, PredicateValue> initWrites, JsonNode resultsNode) {
        var results = new ArrayList<Event.PredResult<String, PredicateValue>>();
        for (var resultNode : resultsNode) {
            var key = requiredText(resultNode, "key");
            var value = parseValue(resultNode);
            results.add(new Event.PredResult<>(key, value));
            recordInitWrite(initWrites, key, value, requiredSourceTxn(resultNode));
        }
        return results;
    }

    private void recordInitWrite(LinkedHashMap<String, PredicateValue> initWrites, String key,
            PredicateValue value, long sourceTxn) {
        if (sourceTxn != INIT_TXN_ID) {
            return;
        }
        var existing = initWrites.putIfAbsent(key, value);
        if (existing != null && !existing.equals(value)) {
            throw new InvalidHistoryError();
        }
    }

    private Event.PredEval<String, PredicateValue> parsePredicate(JsonNode predicateNode) {
        var kind = requiredText(predicateNode, "kind");
        switch (kind) {
        case "inventory_threshold":
            return parseInventoryThreshold(predicateNode);
        case "order_filter":
            return parseOrderFilter(predicateNode);
        case "search_ranked_docs":
            return parseSearchRankedDocs(predicateNode);
        default:
            throw new InvalidHistoryError();
        }
    }

    private Event.PredEval<String, PredicateValue> parseInventoryThreshold(JsonNode predicateNode) {
        var keyPrefix = requiredText(predicateNode, "key_prefix");
        var comparator = requiredText(predicateNode, "comparator");
        var threshold = requiredInt(predicateNode, "threshold");
        return (key, value) -> key != null
                && value != null
                && key.startsWith(keyPrefix)
                && compareSemantics(value.getSemantic(), comparator, threshold);
    }

    private Event.PredEval<String, PredicateValue> parseOrderFilter(JsonNode predicateNode) {
        var keyPrefix = requiredText(predicateNode, "key_prefix");
        var comparator = requiredText(predicateNode, "comparator");
        var threshold = requiredInt(predicateNode, "threshold");
        var allowed = new HashSet<Integer>();
        for (var value : requiredArray(predicateNode, "allowed_semantics")) {
            allowed.add(value.asInt());
        }
        return (key, value) -> key != null
                && value != null
                && key.startsWith(keyPrefix)
                && compareSemantics(value.getSemantic(), comparator, threshold)
                && (allowed.isEmpty() || allowed.contains(value.getSemantic()));
    }

    private Event.PredEval<String, PredicateValue> parseSearchRankedDocs(JsonNode predicateNode) {
        var keyPrefix = requiredText(predicateNode, "key_prefix");
        var minScore = requiredInt(predicateNode, "min_score");
        return (key, value) -> key != null
                && value != null
                && key.startsWith(keyPrefix)
                && value.getSemantic() >= minScore;
    }

    private boolean compareSemantics(int semantic, String comparator, int threshold) {
        switch (comparator) {
        case "ge":
            return semantic >= threshold;
        case "gt":
            return semantic > threshold;
        case "le":
            return semantic <= threshold;
        case "lt":
            return semantic < threshold;
        case "eq":
            return semantic == threshold;
        default:
            throw new InvalidHistoryError();
        }
    }

    private PredicateValue parseValue(JsonNode node) {
        return new PredicateValue(requiredLong(node, "value"), requiredInt(node, "semantic"));
    }

    private long requiredSourceTxn(JsonNode node) {
        var sourceTxn = node.get("source_txn");
        return sourceTxn == null || sourceTxn.isNull() ? Long.MIN_VALUE : sourceTxn.asLong();
    }

    private JsonNode requiredObject(JsonNode node, String fieldName) {
        var child = node.get(fieldName);
        if (child == null || !child.isObject()) {
            throw new InvalidHistoryError();
        }
        return child;
    }

    private JsonNode requiredArray(JsonNode node, String fieldName) {
        var child = node.get(fieldName);
        if (child == null || !child.isArray()) {
            throw new InvalidHistoryError();
        }
        return child;
    }

    private String requiredText(JsonNode node, String fieldName) {
        var child = node.get(fieldName);
        if (child == null || !child.isTextual()) {
            throw new InvalidHistoryError();
        }
        return child.asText();
    }

    private long requiredLong(JsonNode node, String fieldName) {
        var child = node.get(fieldName);
        if (child == null || !child.canConvertToLong()) {
            throw new InvalidHistoryError();
        }
        return child.asLong();
    }

    private int requiredInt(JsonNode node, String fieldName) {
        var child = node.get(fieldName);
        if (child == null || !child.canConvertToInt()) {
            throw new InvalidHistoryError();
        }
        return child.asInt();
    }

    @Data
    @AllArgsConstructor
    @EqualsAndHashCode(of = "encoded")
    public static class PredicateValue {
        private final long encoded;
        private final int semantic;

        @Override
        public String toString() {
            return Long.toString(encoded);
        }
    }
}
