# PolySI

This repo contains the predicate-aware SER verifier based on PolySI, together
with predicate history datasets used for regression and evaluation.

## Requirements

The verifier was tested on Ubuntu 22.04. Build and usage instructions are in
`PolySI/README.md`.

## Reproducing results

1. Build PolySI

   Please follow the instructions in `PolySI/README.md`.

2. Modify the paths in `repro/reproduce.sh` to point to the directories of
   histories and verifiers

   The legacy Cobra/Elle verifier integrations are no longer part of this
   predicate-focused project.

3. Run `repro/reproduce.sh`

   The results are stored in `/tmp/csv` in csv format. For figure 7, the first
   column is the parameters used to generate the history, and is formatted as
   `${#sessions}_${#txns/session}_${#ops/txn}_${#keys}_${read_probability}_${key_distribution}`.

   Running the entire experiment takes a few hours.
# SER
