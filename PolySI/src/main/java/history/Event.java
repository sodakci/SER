package history;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Event<KeyType, ValueType> {
	public enum EventType {
    	READ, WRITE, PREDICATE_READ
    }

    @FunctionalInterface
    public interface PredEval<KeyType, ValueType> {
        boolean test(KeyType key, ValueType value);
    }

    @Data
    public static class PredResult<KeyType, ValueType> {
        private final KeyType key;
        private final ValueType value;
    }

	@EqualsAndHashCode.Include
	private final Transaction<KeyType, ValueType> transaction;

	@EqualsAndHashCode.Include
	private final Event.EventType type;

	@EqualsAndHashCode.Include
	private final KeyType key;

	@EqualsAndHashCode.Include
	private final ValueType value;

    // For PREDICATE_READ only.
    private final PredEval<KeyType, ValueType> predicate;

    // For PREDICATE_READ only.
    private final Collection<PredResult<KeyType, ValueType>> predResults;

    public Event(Transaction<KeyType, ValueType> transaction, Event.EventType type, KeyType key, ValueType value) {
        this(transaction, type, key, value, null, Collections.emptyList());
    }

    public Event(Transaction<KeyType, ValueType> transaction, Event.EventType type, KeyType key, ValueType value,
            PredEval<KeyType, ValueType> predicate, Collection<PredResult<KeyType, ValueType>> predResults) {
        this.transaction = transaction;
        this.type = type;
        this.key = key;
        this.value = value;
        this.predicate = predicate;
        this.predResults = predResults == null ? Collections.emptyList() : new ArrayList<>(predResults);
    }

	@Override
	public String toString() {
        if (type == EventType.PREDICATE_READ) {
            return String.format("%s(%s)", type, predResults);
        }
		return String.format("%s(%s, %s)", type, key, value);
	}
}
