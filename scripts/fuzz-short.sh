#!/usr/bin/env bash
# Runs every fuzz target for a short budget: scripts/fuzz-short.sh [seconds-each]
# Needs a nightly toolchain and cargo-fuzz. The gate runs this when they are installed.
#
# The targets are built once, then all run at the same time, each for the whole budget: one
# libFuzzer process is one thread.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
seconds="${1:-3}"
logs="$(mktemp -d "${TMPDIR:-/tmp}/orion-fuzz.XXXXXX")"
trap 'rm -rf "$logs"' EXIT
if ! cargo +nightly fuzz build --fuzz-dir testing/fuzz >"$logs/build" 2>&1; then
  tail -40 "$logs/build"
  echo "the fuzz targets do not build"
  exit 1
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
exit "$failed"
