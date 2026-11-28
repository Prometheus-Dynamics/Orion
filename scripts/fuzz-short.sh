#!/usr/bin/env bash
# Runs every fuzz target for its short budget: scripts/fuzz-short.sh [cap-seconds]
# Needs a nightly toolchain and cargo-fuzz. The gate runs this when they are installed.
#
# The targets are built once, then all run at the same time: one libFuzzer process is one
# thread. Each makes the runs testing/fuzz/short-runs.tsv gives it, from the seed it gives, from
# an empty corpus: a budget of runs, not of seconds, so a loaded machine does not make it fewer,
# from the same start every time. The coverage each reached is printed, and is no verdict: it
# varies by a few percent from run to run of one seed (libFuzzer steers by what it sees of the
# machine). A crash is what is kept exactly: its input, and the command that runs it again.
# A target that has not made its runs when the cap (45 s) ends it fails the step, with what
# every target made and the load; so does one that made more than its runs.
set -euo pipefail
# Never incremental, whatever the caller set: the targets are built as cargo-fuzz builds them.
export CARGO_INCREMENTAL=0
cd "$(dirname "${BASH_SOURCE[0]}")/.."
cap="${1:-45}"
table=testing/fuzz/short-runs.tsv
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
  scripts/lane-reuse.py binary-plan-item fuzz "$state" "fuzz-short runs cap-$cap" -- "${binaries[@]}"
  if awk -F'\t' '$3 == "reused" {f = 1} END {exit !f}' "$state"; then
    exit 0
  fi
fi
# Every target has its row, and every row its target.
declare -A runs=() seed=()
while IFS=$'\t' read -r target n s; do
  [[ "$target" == "#"* || -z "$target" ]] && continue
  [[ "$n" =~ ^[1-9][0-9]*$ && "$s" =~ ^[0-9]+$ ]] || { echo "$table: '$target' needs a number of runs above 0 and a seed"; exit 1; }
  runs[$target]="$n"
  seed[$target]="$s"
done < "$table"
for path in testing/fuzz/fuzz_targets/*.rs; do
  target="$(basename "$path" .rs)"
  [ -n "${runs[$target]:-}" ] || { echo "$table: no row for the target $target"; exit 1; }
done
for target in "${!runs[@]}"; do
  [ -f "testing/fuzz/fuzz_targets/$target.rs" ] || { echo "$table: a row for $target, which is no target"; exit 1; }
done
# A crash's input, kept after the run, outside the tree: under the target directory, in a
# directory of this run's own. The red names it and the command that runs it again.
artifacts="${CARGO_TARGET_DIR:-$PWD/testing/fuzz/target}/fuzz-artifacts/$(date +%Y%m%dT%H%M%S)-$$"
targets=()
pids=()
for path in testing/fuzz/fuzz_targets/*.rs; do
  target="$(basename "$path" .rs)"
  mkdir "$logs/corpus-$target"
  mkdir -p "$artifacts/$target"
  cargo +nightly fuzz run --fuzz-dir testing/fuzz "$target" "$logs/corpus-$target" -- \
    "-runs=${runs[$target]}" "-seed=${seed[$target]}" "-max_total_time=$cap" \
    "-artifact_prefix=$artifacts/$target/" -rss_limit_mb=2048 >"$logs/$target" 2>&1 &
  targets+=("$target")
  pids+=("$!")
done
failed=0
short=0
lines=()
for i in "${!targets[@]}"; do
  target="${targets[$i]}"
  if wait "${pids[$i]}"; then
    made="$(sed -n 's/^Done \([0-9]*\) runs in \([0-9]*\) second.*/\1 \2/p' "$logs/$target")"
    read -r count took <<< "${made:-0 ?}"
    covered="$(grep -oE 'DONE +cov: [0-9]+ ft: [0-9]+' "$logs/$target" | sed 's/DONE *//')"
    line="$target: $count of ${runs[$target]} runs in $took s (seed ${seed[$target]}, ${covered:-no coverage line})"
    # libFuzzer counts the empty input it starts from as a run: exactly the table's runs, or one
    # more.
    if [ "$count" -lt "${runs[$target]}" ]; then
      lines+=("$line: SHORT")
      short=1
    elif [ "$count" -gt "$((runs[$target] + 1))" ]; then
      lines+=("$line: OVER (not the table's runs)")
      failed=1
    else
      lines+=("$line: clean")
    fi
  else
    tail -40 "$logs/$target"
    lines+=("$target: FAILED (seed ${seed[$target]})")
    echo "fuzz target $target failed"
    crash="$(sed -n 's/.*Test unit written to \(.*\)$/\1/p' "$logs/$target" | tail -1)"
    if [ -n "$crash" ]; then
      echo "fuzz: its input is $crash; run it alone with:"
      echo "  cd $PWD && ${CARGO_HOME:+CARGO_HOME=$CARGO_HOME }${CARGO_TARGET_DIR:+CARGO_TARGET_DIR=$CARGO_TARGET_DIR }cargo +nightly fuzz run --fuzz-dir testing/fuzz $target $crash"
    fi
    failed=1
  fi
done
printf '%s\n' "${lines[@]}"
# This run's directories that no crash wrote to, and its own directory if none did.
find "$artifacts" -mindepth 1 -maxdepth 1 -type d -empty -delete
rmdir "$artifacts" 2> /dev/null || true
if [ "$short" -eq 1 ]; then
  echo "fuzz: a target did not make its runs inside the cap of $cap s, so this run fuzzed less than its"
  echo "budget: load $(cut -d' ' -f1-3 /proc/loadavg), $(nproc) processors. Not rerun: the red goes to whoever owns the gate."
  failed=1
fi
if [ "$failed" -eq 0 ] && [ -n "$state" ]; then
  scripts/lane-reuse.py record "$state" "fuzz-short runs cap-$cap"
fi
exit "$failed"
