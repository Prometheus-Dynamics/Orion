#!/usr/bin/env bash
# A/B of the feature matrix's shard counts, on the change that costs it most: a comment
# appended to orion-types, then scripts/feature-matrix.sh run whole with each count
# (FEATURE_MATRIX_SHARDS), each in a target directory of its own. Each round runs every count
# on the same source, the order rotating, so a load that rises or falls favours none.
#
#   scripts/matrix-ab.sh ROUNDS N...    shard counts; N's lanes in TARGET/feature-matrix-abN
#                                       (TARGET: CARGO_TARGET_DIR, else the work area's)
#
# One line per run: wall, CPU-s, nice and load before. Every count's directories must be warm:
# run a round first, not counted. Measurement only: it never runs a test, and leaves the file
# as it found it.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
. scripts/work-env.sh
at_gate_nice
rounds="${1:?usage: scripts/matrix-ab.sh ROUNDS N...}"
shift
[ "$#" -ge 2 ] || { echo "matrix-ab: give two or more shard counts" >&2; exit 2; }
counts=("$@")
target="${CARGO_TARGET_DIR:-/mnt/nvme-wd/orion/target}"
file=crates/core/types/src/lib.rs
git diff --quiet -- "$file" || { echo "matrix-ab: $file has changes" >&2; exit 1; }
saved="$(mktemp)"
times="$(mktemp)"
cp -f "$file" "$saved"
# Restored with a fresh mtime (cp -f, not -p): an old one could make cargo reuse the edit's build.
trap 'cp -f "$saved" "$file"; rm -f "$saved" "$times"' EXIT

run() { # N
  local n="$1" load wall user sys
  load="$(cut -d' ' -f1 /proc/loadavg)"
  # GATE_LANE_STATE_DIR unset: nothing is reused or recorded, every build runs.
  env -u GATE_LANE_STATE_DIR FEATURE_MATRIX_SHARDS="$n" CARGO_TARGET_DIR="$target/feature-matrix-ab$n" \
    /usr/bin/time -o "$times" -f '%e %U %S' taskset -c "${AB_CPUS:-6-31}" scripts/feature-matrix.sh > /dev/null
  read -r wall user sys < "$times"
  printf '%s shards: wall %6.1f s  CPU %6.1f s  nice %s load %s\n' \
    "$n" "$wall" "$(echo "$user + $sys" | bc -l)" "$GATE_NICE" "$load"
}

count="${#counts[@]}"
for round in $(seq 1 "$rounds"); do
  printf '\n// matrix-ab round %s\n' "$round" >> "$file"
  echo "round $round"
  for k in $(seq 0 $((count - 1))); do run "${counts[$(((k + round - 1) % count))]}"; done
  cp -f "$saved" "$file"
done
