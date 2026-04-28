# Grouped Predicate Test Suite

This suite groups predicate-capable `prhist` histories into two categories:

- `ACCEPT`: serializable by construction
- `REJECT`: intentionally non-serializable

The ACCEPT group uses a frozen predicate domain and materializes complete
predicate results. The REJECT group keeps the same PRHIST shape but injects
repeated write-skew motifs that create deterministic RW cycles.
