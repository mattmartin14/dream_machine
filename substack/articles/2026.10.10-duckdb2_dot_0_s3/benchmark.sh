#!/usr/bin/env bash
# Usage: ./benchmark.sh [runs=5]
# Runs query.sql on DuckDB 1.5 then 2.0 and compares the .timer "real" times.
set -euo pipefail

cd "$(dirname "$0")"
RUNS="${1:-5}"

# Prints per-run times to stderr and the average to stdout.
bench() {
  local version="$1" times=() out t
  ./toggle_version.sh "$version" >/dev/null 2>&1
  echo "== DuckDB $version ($(duckdb --version)) ==" >&2
  # Warm-up so extension install/secret setup is not measured; only .timer output is parsed.
  duckdb -c ".read query.sql" >/dev/null 2>&1
  for i in $(seq 1 "$RUNS"); do
    out="$(duckdb -c ".read query.sql" 2>&1)"
    t="$(grep -Eo 'real [0-9.]+' <<<"$out" | tail -1 | awk '{print $2}')"
    [[ -n "$t" ]] || { echo "Could not parse timer output:" >&2; echo "$out" >&2; exit 1; }
    echo "  run $i: ${t}s" >&2
    times+=("$t")
  done
  printf '%s\n' "${times[@]}" | awk '{s+=$1} END {printf "%.4f", s/NR}'
}

avg_15="$(bench 1.5)"
echo "  average: ${avg_15}s" >&2
avg_20="$(bench 2.0)"
echo "  average: ${avg_20}s" >&2

echo
echo "DuckDB 1.5 average: ${avg_15}s"
echo "DuckDB 2.0 average: ${avg_20}s"
awk -v a="$avg_15" -v b="$avg_20" 'BEGIN {printf "Run time change: %.1f%% decrease\n", (a-b)/a*100}'
