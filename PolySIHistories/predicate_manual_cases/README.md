This directory contains hand-crafted predicate histories.

- `ACCEPT`: serializable by construction, with overlapping predicate domains and
  evolving result sets.
- `REJECT`: non-serializable due to a dense predicate-boundary cycle hidden
  inside a larger history. The same cycle keys appear in preparer, cycle,
  cleanup, and observer transactions.

All files keep the existing `history.prhist.jsonl` structure that
`PredicateHistoryLoader` already accepts.
