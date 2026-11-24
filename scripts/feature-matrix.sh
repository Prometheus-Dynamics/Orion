#!/usr/bin/env bash
# Every crate with optional features builds with none of them, with each one alone, and with
# all of them. A use of something a feature brings, outside that feature's cfg, fails here.
#
# With crate names as arguments, only those crates are checked.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# One line for each build: the crate, then the flags.
listing="$(scripts/workspace.py matrix "$@")"
mapfile -t builds <<< "$listing"

# A selection that matches nothing is a failure, never a pass: a listing that failed, or a crate
# name that matches no crate with features, would otherwise check nothing and say so as passed.
# scripts/workspace.py fails on both itself; this says so again, here, if it ever did not.
if [ "${#builds[@]}" -eq 0 ]; then
  echo "feature matrix: no build selected${*:+ for $*}: nothing was checked" >&2
  exit 1
fi

# In the gate (GATE_LANE_STATE_DIR set), a build that passed before on exactly these inputs is
# not made again (scripts/lane-reuse.py), and each that passes is recorded. Asked for crates by
# name, every build runs.
state=""
if [ -n "${GATE_LANE_STATE_DIR:-}" ] && [ "$#" -eq 0 ]; then
  state="$GATE_LANE_STATE_DIR/matrix"
  scripts/lane-reuse.py plan matrix "$state" "${builds[@]}"
fi

# The builds go to SHARDS lanes, each with a target directory of its own under this one, all
# at once: cargo holds a target directory for one build at a time. A build is always in the
# same lane, so each lane keeps what it built last time.
shards="${FEATURE_MATRIX_SHARDS:-6}"
lanes="${CARGO_TARGET_DIR:-target}/feature-matrix"
logs=$(mktemp -d)
trap 'rm -rf "$logs"' EXIT

lane() {
  local shard="$1" i crate flags failed=0
  for i in "${!builds[@]}"; do
    [ $((i % shards)) -eq "$shard" ] || continue
    read -r crate flags <<< "${builds[$i]}"
    if [ -n "$state" ] && awk -F'\t' -v b="${builds[$i]}" '$1 == b && $3 == "reused" {f = 1} END {exit !f}' "$state"; then
      continue
    fi
    # shellcheck disable=SC2086
    if ! CARGO_TARGET_DIR="$lanes/$shard" cargo check -q --locked -p "$crate" $flags 2> "$logs/$shard.err"; then
      echo "FAILED: cargo check -p $crate $flags"
      grep -E '^error' -A6 "$logs/$shard.err" | head -40
      failed=$((failed + 1))
    elif [ -n "$state" ]; then
      scripts/lane-reuse.py record "$state" "${builds[$i]}"
    fi
  done
  echo "$failed" > "$logs/$shard.failed"
}

pids=()
for ((shard = 0; shard < shards; shard++)); do
  lane "$shard" > "$logs/$shard.out" &
  pids+=("$!")
done
failed=0
for ((shard = 0; shard < shards; shard++)); do
  wait "${pids[$shard]}" || true
  cat "$logs/$shard.out"
  failed=$((failed + $(cat "$logs/$shard.failed" 2> /dev/null || echo 1)))
done
echo "feature matrix: ${#builds[@]} builds, $failed failed"
[ "$failed" -eq 0 ]
if [ -n "$state" ]; then
  scripts/lane-reuse.py status matrix "$state" > "$state.status"
fi
