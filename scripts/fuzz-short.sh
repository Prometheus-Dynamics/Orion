#!/usr/bin/env bash
# Runs every fuzz target for a short budget: scripts/fuzz-short.sh [seconds-each]
# Needs a nightly toolchain and cargo-fuzz. The gate runs this when they are installed.
#
# The targets are built once, then all run at the same time, each for the whole budget: one
# libFuzzer process is one thread.
set -euo pipefail
# Never incremental, whatever the caller set: the targets are built as cargo-fuzz builds them.
export CARGO_INCREMENTAL=0
cd "$(dirname "${BASH_SOURCE[0]}")/.."
seconds="${1:-3}"
logs="$(mktemp -d "${TMPDIR:-/tmp}/orion-fuzz.XXXXXX")"
trap 'rm -rf "$logs"' EXIT
if ! cargo +nightly fuzz build --fuzz-dir testing/fuzz >"$logs/build" 2>&1; then
  tail -40 "$logs/build"
  echo "the fuzz targets do not build"
  exit 1
fi
# In the gate: the targets as built, keyed by what they run (scripts/lane-reuse.py
# binary-plan-item, the allocated sections of each, and every file under testing/fuzz that is not
# Rust source, the corpora among them). If those passed before for this budget, the runs are
# known and not made again; when they run, each has its whole budget.
state=""
if [ -n "${GATE_LANE_STATE_DIR:-}" ] && [ -n "${CARGO_TARGET_DIR:-}" ]; then
  state="$GATE_LANE_STATE_DIR/fuzz.bin"
  binaries=()
  for path in testing/fuzz/fuzz_targets/*.rs; do
    binaries+=("$CARGO_TARGET_DIR/x86_64-unknown-linux-gnu/release/$(basename "$path" .rs)")
  done
  scripts/lane-reuse.py binary-plan-item fuzz "$state" "fuzz-short $seconds" -- "${binaries[@]}"
  if awk -F'\t' '$3 == "reused" {f = 1} END {exit !f}' "$state"; then
    exit 0
  fi
fi
targets=()
pids=()
for path in testing/fuzz/fuzz_targets/*.rs; do
  target="$(basename "$path" .rs)"
  cargo +nightly fuzz run --fuzz-dir testing/fuzz "$target" -- "-max_total_time=$seconds" -rss_limit_mb=2048 >"$logs/$target" 2>&1 &
  targets+=("$target")
  pids+=("$!")
done
failed=0
for i in "${!targets[@]}"; do
  target="${targets[$i]}"
  if wait "${pids[$i]}"; then
    echo "$target: clean ($(grep -oE '^Done [0-9]+ runs' "$logs/$target" || echo 'ran'))"
  else
    tail -40 "$logs/$target"
    echo "fuzz target $target failed"
    failed=1
  fi
done
if [ "$failed" -eq 0 ] && [ -n "$state" ]; then
  scripts/lane-reuse.py record "$state" "fuzz-short $seconds"
fi
exit "$failed"
