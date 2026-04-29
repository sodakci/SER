# Complex Predicate SER Suite

This suite contains predicate-aware histories grouped by expected verdict.

- ACCEPT histories are serializable by construction. The witness is ascending
  transaction id order.
- REJECT histories contain repeated pure predicate-boundary cycles. Each motif
  has three predicate reads and three writes, with no point read used to close
  the cycle.

Each history directory contains:

- `history.prhist.jsonl`
- `manifest.json`
- `conflicts.json` for REJECT cases
