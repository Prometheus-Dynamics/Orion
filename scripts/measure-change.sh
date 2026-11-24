#!/usr/bin/env bash
# What a gate costs after a change: a comment appended to FILE, the whole gate run as a worker's
# machine runs it (scripts/run-gate.sh, at the one nice level), the file put back; RUNS times.
# Then each run's wall time, CPU time and load, and the median and the slowest.
#
#   scripts/measure-change.sh FILE RUNS TARGET_DIR LOG_DIR
#
# A different comment each run, so each is a change. GATE_CPUS and GATE_JOBS go to run-gate.sh.
# Run it from a committed tree: an uncommitted file is a change too, and the gates would measure it.
# The one-minute rule: every gate ends in under 60 s, the slowest of twenty included.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
. scripts/work-env.sh
at_gate_nice
[ "$#" -eq 4 ] || { echo "usage: scripts/measure-change.sh FILE RUNS TARGET_DIR LOG_DIR" >&2; exit 2; }
file="$1" runs="$2" target="$3" logs="$4"
git diff --quiet || { echo "measure-change: the worktree has changes; commit or set them aside" >&2; exit 1; }
saved="$(mktemp)"
cp -f "$file" "$saved"
# Put back with a fresh mtime (cp -f, not -p): an old one could make cargo reuse a build of
# the change.
trap 'cp -f "$saved" "$file"; rm -f "$saved"' EXIT
mkdir -p "$logs"
stamp="$(date +%Y%m%dT%H%M%S)"
walls=()
for run in $(seq 1 "$runs"); do
  printf '\n// measure-change %s run %s\n' "$stamp" "$run" >> "$file"
  log="$logs/measure-$stamp-$run.log"
  status=0
  scripts/run-gate.sh "$target" "$log" > /dev/null 2>&1 || status=$?
  cp -f "$saved" "$file"
  line="$(tail -1 "$log")"
  wall="$(sed -n 's/.* time \([0-9.]*\) s wall.*/\1/p' <<< "$line")"
  cpu="$(sed -n 's/.* s wall \([0-9.]*\) user \([0-9.]*\) sys.*/\1 \2/p' <<< "$line" | awk '{print $1 + $2}')"
  loads="$(sed -n 's/.*load-before \([0-9.]*\) .*load-after \([0-9.]*\) .*/\1 to \2/p' <<< "$line")"
  echo "run $run: exit $status, ${wall:-?} s wall, ${cpu:-?} CPU-s, nice ${GATE_NICE:-10}, load $loads ($log)"
  [ "$status" -eq 0 ] || { echo "run $run failed: see $log" >&2; exit 1; }
  walls+=("$wall")
done
printf '%s\n' "${walls[@]}" | sort -n | awk '{w[NR] = $1} END {
  median = (NR % 2) ? w[(NR + 1) / 2] : (w[NR / 2] + w[NR / 2 + 1]) / 2
  printf "median %.1f s, slowest %.1f s, of %d runs\n", median, w[NR], NR
}'
