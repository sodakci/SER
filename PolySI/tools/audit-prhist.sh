#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
POLYSI_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
JAR_PATH=${POLYSI_JAR:-"$POLYSI_DIR/build/libs/PolySI-1.0.0-SNAPSHOT.jar"}
HEAP_SIZE=${POLYSI_HEAP:-8g}
OUTPUT_ROOT=${POLYSI_OUTPUT_DIR:-"/tmp/polysi-prhist-audit"}

usage() {
  cat <<'EOF'
Usage:
  tools/audit-prhist.sh <history-or-root-dir>

Environment variables:
  POLYSI_JAR         Override the PolySI jar path.
  POLYSI_HEAP        JVM heap size passed via -Xmx. Default: 8g
  POLYSI_JAVA_OPTS   Extra JVM options, split on spaces.
  POLYSI_OUTPUT_DIR  Directory for audit logs. Default: /tmp/polysi-prhist-audit

Examples:
  tools/audit-prhist.sh ../PolySIHistories/predicate_high_intensity
  POLYSI_HEAP=12g tools/audit-prhist.sh ../PolySIHistories/predicate_high_intensity/search_32_420_18_10000_0.5_hotspot/hist-00000
EOF
}

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

if [[ ! -f "$JAR_PATH" ]]; then
  echo "PolySI jar not found: $JAR_PATH" >&2
  exit 2
fi

INPUT_PATH=$(realpath "$1")
if [[ ! -e "$INPUT_PATH" ]]; then
  echo "Input path not found: $INPUT_PATH" >&2
  exit 2
fi

declare -a TARGETS=()
if [[ -f "$INPUT_PATH/history.prhist.jsonl" ]]; then
  TARGETS=("$INPUT_PATH")
elif [[ -f "$INPUT_PATH" && "$(basename "$INPUT_PATH")" == "history.prhist.jsonl" ]]; then
  TARGETS=("$(dirname "$INPUT_PATH")")
else
  while IFS= read -r history_file; do
    TARGETS+=("$(dirname "$history_file")")
  done < <(find "$INPUT_PATH" -type f -name history.prhist.jsonl | sort)
fi

if [[ ${#TARGETS[@]} -eq 0 ]]; then
  echo "No predicate histories found under: $INPUT_PATH" >&2
  exit 2
fi

mkdir -p "$OUTPUT_ROOT"

declare -a EXTRA_JAVA_OPTS=()
if [[ -n "${POLYSI_JAVA_OPTS:-}" ]]; then
  # Intentional word splitting for caller-provided JVM flags.
  read -r -a EXTRA_JAVA_OPTS <<<"${POLYSI_JAVA_OPTS}"
fi

accept_count=0
reject_count=0
error_count=0

for target in "${TARGETS[@]}"; do
  rel_target=${target#/}
  safe_name=${rel_target//\//__}
  log_path="$OUTPUT_ROOT/${safe_name}.log"

  echo "=== Auditing $target"
  set +e
  java "-Xmx$HEAP_SIZE" "${EXTRA_JAVA_OPTS[@]}" -jar "$JAR_PATH" audit -t prhist "$target" >"$log_path" 2>&1
  status=$?
  set -e

  if grep -q '\[\[\[\[ ACCEPT \]\]\]\]' "$log_path"; then
    result="ACCEPT"
    accept_count=$((accept_count + 1))
  elif grep -q '\[\[\[\[ REJECT \]\]\]\]' "$log_path"; then
    result="REJECT"
    reject_count=$((reject_count + 1))
  else
    result="RUNTIME_ERROR"
    error_count=$((error_count + 1))
  fi

  echo "Result: $result (exit=$status, log=$log_path)"
done

echo
echo "Summary: ACCEPT=$accept_count REJECT=$reject_count RUNTIME_ERROR=$error_count"

if [[ $error_count -gt 0 ]]; then
  exit 1
fi
