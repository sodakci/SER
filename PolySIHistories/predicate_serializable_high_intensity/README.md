# Serializable Predicate High-Intensity History

This directory contains a high-intensity predicate workload that is serializable
by construction.

The concrete serial witness is ascending transaction id order. Each transaction
is generated against the latest state produced by all earlier transactions, and
all predicate reads materialize a complete result set for their predicate at
that serial point.

Each history directory contains:

- `history.prhist.jsonl`
- `manifest.json`

The expected verifier result is `ACCEPT`.
