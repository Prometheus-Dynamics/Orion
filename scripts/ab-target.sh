#!/usr/bin/env bash
# A/B of build targets, on the change that costs most: a comment appended to orion-types (87
# crates build on it), every test the gate's tests step builds rebuilt, then the comment taken
# out and all rebuilt again. Each round builds every target on the same source, so all see
# the same machine; the order rotates each round, so a load that rises or falls favours none.
#
#   scripts/ab-target.sh ROUNDS TARGET...      two or more; labelled A, B, C... in that order
#
# One line per build: wall, CPU-s, load before, and the machine's and this cgroup's I/O stall
# (PSI) over it; then where its threads waited (scripts/wait-states.py). Every target must be
# warm: built once on this source, with the same CARGO_INCREMENTAL. Measurement only: it
# builds, never runs a test, and leaves the file as it found it.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
. scripts/work-env.sh
at_gate_nice
rounds="${1:?usage: scripts/ab-target.sh ROUNDS TARGET...}"
shift
[ "$#" -ge 2 ] || { echo "ab-target: give two or more targets" >&2; exit 2; }
targets=("$@")
labels=(A B C D E F)
file=crates/core/types/src/lib.rs
git diff --quiet -- "$file" || { echo "ab-target: $file has changes" >&2; exit 1; }
saved="$(mktemp)"
cp -f "$file" "$saved"
# A restored file gets a fresh mtime (cp -f, not -p): an old one could make cargo reuse a
# build of the edit.
trap 'cp -f "$saved" "$file"; rm -f "$saved"' EXIT
cgroup="/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)"
stall() { # microseconds stalled on I/O: some full
  awk '/^some/ {split($5, s, "=")} /^full/ {split($5, f, "=")} END {print s[2], f[2]}' "$1"
}

build() {
  local label="$1" target="$2" what="$3" times log load m1 m2 c1 c2 wall user sys
  times="$(mktemp)"
  log="$(mktemp)"
  load="$(cut -d' ' -f1 /proc/loadavg)"
  read -r m1 _ <<< "$(stall /proc/pressure/io)"
  read -r c1 _ <<< "$(stall "$cgroup/io.pressure")"
  CARGO_TARGET_DIR="$target" /usr/bin/time -o "$times" -f '%e %U %S' \
    taskset -c "${AB_CPUS:-6-31}" scripts/wait-states.py --every 1 -- \
    cargo build --workspace --all-features --tests -j "${AB_JOBS:-13}" > "$log" 2>&1 ||
    { cat "$log" >&2; exit 1; }
  read -r m2 _ <<< "$(stall /proc/pressure/io)"
  read -r c2 _ <<< "$(stall "$cgroup/io.pressure")"
  read -r wall user sys < "$times"
  printf '%s %-6s wall %6.1f s  CPU %6.1f s  nice %s load %6s  io-stall machine %5.1f s  cgroup %5.1f s  %s\n' \
    "$label" "$what" "$wall" "$(echo "$user + $sys" | bc -l)" "$GATE_NICE" "$load" \
    "$(echo "($m2 - $m1) / 1000000" | bc -l)" "$(echo "($c2 - $c1) / 1000000" | bc -l)" "$target"
  sed -n '/^wait-states:/,$p' "$log" | sed 's/^/    /'
  rm -f "$times" "$log"
}

for i in "${!targets[@]}"; do
  echo "${labels[$i]} = ${targets[$i]}"
done
count="${#targets[@]}"
for round in $(seq 1 "$rounds"); do
  order=()
  for k in $(seq 0 $((count - 1))); do
    order+=($(((k + round - 1) % count)))
  done
  printf '\n// ab-target round %s\n' "$round" >> "$file"
  for i in "${order[@]}"; do build "${labels[$i]}" "${targets[$i]}" edit; done
  cp -f "$saved" "$file"
  for i in "${order[@]}"; do build "${labels[$i]}" "${targets[$i]}" revert; done
done
