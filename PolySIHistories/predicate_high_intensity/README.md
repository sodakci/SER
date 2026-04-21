# Predicate High-Intensity Histories

This dataset extends the style of `fig7` / `fig8_9_10` with predicate-heavy workloads.

## Directory layout

- One profile directory per parameter setting:
  - `<workload>_<sessions>_<entities>_<result_limit>_<txns>_<predicate_ratio>_<distribution>`
- Three replicas are generated under each profile:
  - `hist-00000`
  - `hist-00001`
  - `hist-00002`

Each `hist-xxxxx` directory contains:

- `history.prhist.jsonl`: one committed transaction per line
- `manifest.json`: scale and count summary

## File format

Each line in `history.prhist.jsonl` is a JSON object:

```json
{
  "session": 0,
  "txn": 17,
  "kind": "inventory.reserve_predicate",
  "status": "commit",
  "ops": [
    {"type": "pr", "predicate": {...}, "results": [...]},
    {"type": "r", "key": "inventory_onhand_0007", "value": 81000077, "semantic": 81, "source_txn": 9},
    {"type": "w", "key": "inventory_reserved_0007", "value": 12000123, "semantic": 12}
  ]
}
```

## Notes

- `value` is globally unique for every write so a future loader can identify the source write unambiguously.
- `semantic` stores the business-level quantity / status / score encoded into `value`.
- Predicate results are materialized explicitly to match PolySI's `addPredicateReadEvent(...)` model.
- These files are intentionally high-intensity samples; the current repository does not yet include a file loader for `history.prhist.jsonl`.
