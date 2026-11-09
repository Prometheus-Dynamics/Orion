#!/usr/bin/env bash
# Runs every fuzz target for a short budget: scripts/fuzz-short.sh [seconds-each]
# Needs a nightly toolchain and cargo-fuzz. The gate runs this when they are installed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
seconds="${1:-3}"
log="$(mktemp "${TMPDIR:-/tmp}/orion-fuzz.XXXXXX")"
trap 'rm -f "$log"' EXIT
for path in testing/fuzz/fuzz_targets/*.rs; do
  target="$(basename "$path" .rs)"
  if ! cargo +nightly fuzz run --fuzz-dir testing/fuzz "$target" -- "-max_total_time=$seconds" -rss_limit_mb=2048 >"$log" 2>&1; then
    tail -40 "$log"
    echo "fuzz target $target failed"
    exit 1
  fi
  echo "$target: clean ($(grep -oE '^Done [0-9]+ runs' "$log" || echo 'ran'))"
done
