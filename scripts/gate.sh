#!/usr/bin/env bash
# The check every PR into next must pass. CI runs exactly this, in parts.
#
#   scripts/gate.sh                 everything; stamps the tree for scripts/push.sh if it passes
#   scripts/gate.sh --part <part>   one part, and no stamp: `main` (every step not in a lane), or
#                                   a lane: `matrix`, `other-systems`, `size`, `devices`
#   scripts/gate.sh --drop LANE:WHY a lane not run, recorded so (`not-run(WHY)`); a lane behind
#                                   (scripts/lane-reuse.py behind) cannot be dropped
#
# The parts together are the whole gate. CI runs each on a machine of its own, at once.
#
# In the build lanes, an item that passed before on exactly these inputs is not run again
# (scripts/lane-reuse.py: the feature matrix's builds, the no_std and other-system checks, the
# device sizes); a lane behind runs in full. The tree's stamp holds what each lane did, the
# `Gate-Lanes:` line the commit carries (scripts/push.sh checks it).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
part=""
declare -A dropped=()
usage="usage: scripts/gate.sh [--part main|matrix|other-systems|size|devices] [--drop LANE:WHY]..."
while [ "$#" -gt 0 ]; do
  case "$1 ${2:-}" in
    "--part main" | "--part matrix" | "--part other-systems" | "--part size" | "--part devices")
      part="$2"
      ;;
    --drop\ matrix:?* | --drop\ other-systems:?* | --drop\ size:?* | --drop\ devices:?*)
      dropped[${2%%:*}]="${2#*:}"
      ;;
    *)
      echo "$usage" >&2
      exit 2
      ;;
  esac
  shift 2
done
for name in "${!dropped[@]}"; do
  if [ "$name" != size ] && why="$(scripts/lane-reuse.py behind "$name")" && [[ "$why" != *"may reuse" ]]; then
    echo "--drop $name: it cannot be dropped: $why" >&2
    exit 2
  fi
done
# Debug and test builds are incremental (cargo's default): a change rebuilds only what it
# touched. The steps whose output is measured pin it off themselves.

# Each step has a bound: three times the slowest it has taken in a gate that passed, here or in
# CI, and never under a minute. That slowest time is the number after each step's label. A step
# runs under scripts/bounded.py, in a session of its own, its output through a file: past its
# bound, it and everything it started are ended (TERM, then KILL) and the gate fails, naming
# it. A step that ends with a process of its own still running fails too, so no process it
# leaves can hold the gate's output open. Each step has a TMPDIR of its own, and fails if it
# leaves anything there.
#
# A hang is found apart from that bound: a step whose processes use no CPU for a minute
# (STALL_SECONDS) has stalled, and is ended then, named so, however long its bound. Each step
# says the CPU seconds it used.
#
# A step written without its number does not run (exit 2): it would be bounded by nothing
# anyone chose. On a machine slower than any these numbers came from, GATE_STEP_BOUND_SCALE (a
# whole number, 1 if unset) multiplies every bound, the stall's among them.
#
# bounded <label> <slowest seconds> <command...>: a command, or a function of this script.
STALL_SECONDS=60
bounded() {
  local label="$1" scale="${GATE_STEP_BOUND_SCALE:-1}" bound stall
  if ! [[ "${2:-}" =~ ^[1-9][0-9]*$ ]]; then
    echo "$label: no bound: '${2:-}' is not its slowest time in seconds" >&2
    exit 2
  fi
  if ! [[ "$scale" =~ ^[1-9][0-9]*$ ]]; then
    echo "$label: GATE_STEP_BOUND_SCALE must be a whole number, not '$scale'" >&2
    exit 2
  fi
  bound=$(($2 * 3))
  shift 2
  [ "$bound" -ge 60 ] || bound=60
  bound=$((bound * scale))
  stall=$((STALL_SECONDS * scale))
  if declare -F "$1" > /dev/null; then
    scripts/bounded.py "$bound" "$label" --stall "$stall" -- \
      bash -c 'set -euo pipefail; "$@"' "$label" "$@"
  else
    scripts/bounded.py "$bound" "$label" --stall "$stall" -- "$@"
  fi
}

# step <label> <slowest seconds> <command...>
step() {
  local label="$1" start
  echo "==> $label"
  start=$SECONDS
  bounded "$@"
  echo "    $label: $((SECONDS - start)) s"
}

# A part of a bounded step, timed: its bound is the step's.
timed() {
  local label="$1" start
  shift
  echo "==> $label"
  start=$SECONDS
  "$@"
  echo "    $label: $((SECONDS - start)) s"
}

# A step of the main part: run unless the gate was asked for a lane alone.
main() {
  if [ -z "$part" ] || [ "$part" = main ]; then
    step "$@"
  fi
}

# Steps that build in target directories of their own run beside the rest, each in a lane: a
# process group of its own, its output kept in a file and printed, with its time, once every
# lane has ended. A lane runs its steps one after another and stops at the first that fails.
# If the gate stops first, its lanes are ended with it.
target_dir="${CARGO_TARGET_DIR:-$PWD/target}"
export target_dir
lane_logs="$(mktemp -d)"
# What each build lane reused (scripts/lane-reuse.py), outside every step's own TMPDIR.
GATE_LANE_STATE_DIR="$lane_logs/state"
mkdir -p "$GATE_LANE_STATE_DIR"
export GATE_LANE_STATE_DIR
lane_pids=()
lane_names=()
set -m
end_lanes() {
  local pid waited
  for pid in "${lane_pids[@]}"; do
    kill -- "-$pid" 2> /dev/null || true
  done
  # Each lane's running step ends what it started (TERM, then KILL): the gate ends after them.
  for pid in "${lane_pids[@]}"; do
    waited=0
    while kill -0 -- "-$pid" 2> /dev/null && [ "$waited" -lt 150 ]; do
      sleep 0.1
      waited=$((waited + 1))
    done
  done
  rm -rf "$lane_logs"
}
trap end_lanes EXIT

# lane <name> <label> <slowest seconds> <command...> [-- <label> <slowest seconds> <command...>]...
# The whole gate runs each beside the rest. Asked for that lane alone, it runs it, in front.
lane() {
  local name="$1"
  shift
  if [ -n "${dropped[$name]:-}" ]; then
    echo "==> lane $name: not run: ${dropped[$name]}"
    return
  fi
  if [ "$part" = "$name" ]; then
    lane_steps "$@"
  elif [ -z "$part" ]; then
    lane_steps "$@" > "$lane_logs/$name" 2>&1 &
    lane_pids+=("$!")
    lane_names+=("$name")
  fi
}
lane_steps() {
  (
    set +m
    # Each command runs plainly under `set -e`: one run as `command || ...` would have `-e`
    # ignored inside it, and a function's failing step would not stop it.
    label=""
    trap 'status=$?; [ "$status" -eq 0 ] || echo "    FAILED: $label"' EXIT
    while [ "$#" -gt 0 ]; do
      label="$1"
      local slowest="$2"
      shift 2
      local command=()
      while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
        command+=("$1")
        shift
      done
      [ "$#" -gt 0 ] && shift
      echo "==> $label (beside the rest)"
      local start=$SECONDS
      bounded "$label" "$slowest" "${command[@]}"
      echo "    $label: $((SECONDS - start)) s"
    done
  )
}

# Waits for every lane, prints what each said, and fails if one did.
join_lanes() {
  local failed=0 i
  for i in "${!lane_pids[@]}"; do
    wait "${lane_pids[$i]}" || failed=1
    cat "$lane_logs/${lane_names[$i]}"
  done
  lane_pids=()
  return "$failed"
}

# Board images are built `--locked --offline` from Cargo.lock alone: a lockfile that no longer
# matches the manifests fails here, first, before anything else has had the chance to rewrite
# it.
lockfile() {
  cargo metadata --locked --format-version 1 > /dev/null
}

# Every test, as `cargo test --workspace --all-features` runs them. With cargo-nextest
# installed, each test binary's cases run in processes of their own, all binaries at once, and
# the doctests (which nextest does not run) beside them, their output after; without it, cargo
# test runs them all. The same cases run either way, ignored ones skipped by both. The doctests
# are mostly rustdoc passes over crates that have none, about 15 s warm, which the slowest test
# outlasts; one after the other, they were on the gate's critical path.
tests() {
  if cargo nextest --version > /dev/null 2>&1; then
    local doc_log doc_pid doc_status=0 status=0
    doc_log="$(mktemp)"
    cargo test --workspace --all-features --doc > "$doc_log" 2>&1 &
    doc_pid=$!
    cargo nextest run --workspace --all-features --no-tests=fail || status=$?
    wait "$doc_pid" || doc_status=$?
    cat "$doc_log"
    rm -f "$doc_log"
    [ "$doc_status" -eq 0 ] || echo "    the doctests failed" >&2
    [ "$status" -eq 0 ] && [ "$doc_status" -eq 0 ]
  else
    cargo test --workspace --all-features
  fi
}

# The comm core's tests again, built as each shipped binary builds it, and those binaries' own
# tests alone, so with only what they ship. `--all-features` above is no build that ships: a
# test that holds only with every feature on, or a part that needs one the binary leaves out,
# shows only here. scripts/shipped-features.py reads the sets from the binaries' manifests.
shipped_tests() {
  local binary line args run=(cargo test)
  if cargo nextest --version > /dev/null 2>&1; then
    run=(cargo nextest run --no-tests=fail)
  fi
  for binary in orion-bin orion-recovery-bin; do
    line="$(scripts/shipped-features.py "$binary" orion-comm-app)" || return 1
    read -ra args <<< "$line"
    echo "    orion-comm-app as $binary builds it: ${args[*]}"
    "${run[@]}" -p orion-comm-app "${args[@]}" || return 1
    echo "    $binary alone"
    "${run[@]}" -p "$binary" || return 1
  done
}

# Every filter in .config/nextest.toml (a priority, a test group) matches a test: a test renamed
# or moved into a shared binary would otherwise lose it without a word. Against the list nextest
# gives for the binaries the tests just ran; nothing to check without nextest.
test_filters() {
  if cargo nextest --version > /dev/null 2>&1; then
    local listing
    listing="$(mktemp)"
    cargo nextest list --workspace --all-features --message-format json > "$listing"
    scripts/check-test-filters.py "$listing"
    rm -f "$listing"
  fi
}

# The tree this gate checks, as `git add -A` would commit it: stamped at the end if it passed and
# nothing changed while it ran, so scripts/push.sh pushes only what passed.
tree_now() {
  local index
  index="$(mktemp)"
  cp "$(git rev-parse --git-dir)/index" "$index" 2> /dev/null || true
  GIT_INDEX_FILE="$index" git add -A > /dev/null 2>&1
  GIT_INDEX_FILE="$index" git write-tree
  rm -f "$index"
}
checked_tree=""
if git rev-parse --git-dir > /dev/null 2>&1; then
  checked_tree="$(tree_now)"
fi

main "push guard" 3 scripts/push.sh --self-test
main "step bounds self-test" 4 scripts/bounded.py --self-test
# A step with no number, or a scale that is not one, refuses to run.
bounds_refuse() {
  local number scale status
  for number in x "" 0; do
    status=0
    (bounded probe "$number" true) > /dev/null 2>&1 || status=$?
    [ "$status" -eq 2 ] || { echo "a bound of '$number': exit $status, not 2" >&2; return 1; }
  done
  for scale in 1.5 0 2; do
    status=0
    (GATE_STEP_BOUND_SCALE="$scale" bounded probe 1 true) > /dev/null 2>&1 || status=$?
    [ "$status" -eq "$([ "$scale" = 2 ] && echo 0 || echo 2)" ] ||
      { echo "a scale of '$scale': exit $status" >&2; return 1; }
  done
}
if [ -z "$part" ] || [ "$part" = main ]; then
  timed "steps without a bound refuse" bounds_refuse
fi

# Checks that build nothing come first: they fail fastest.
if [ -x scripts/check-boundaries.sh ]; then
  main boundaries 6 scripts/check-boundaries.sh
  main boundaries-self-test 12 scripts/check-boundaries.sh --self-test
fi
main "test filters self-test" 1 scripts/check-test-filters.py --self-test
main "test files" 1 scripts/check-test-filters.py --files

if [ -x scripts/check-links.sh ]; then
  main links 2 scripts/check-links.sh
fi

# The client builds on Windows and macOS as well as Linux. Clippy for each, tests included:
# what needs a comm core is Linux's and drops out. CI's hosted runners build and run them on
# those systems (`.github/workflows/next.yml`). And a crate that must build without std says
# so in its manifest:
# [package.metadata.orion]
# no_std = true
other_systems() {
  renice -n "$behind" -p "$BASHPID" > /dev/null
  export CARGO_TARGET_DIR="$target_dir/other-systems"
  local target crate group client=() packages=() targets=(thumbv7em-none-eabihf riscv32imac-unknown-none-elf)
  mapfile -t no_std_crates < <(grep -l '^no_std = true' crates/*/*/Cargo.toml | xargs -r sed -n 's/^name = "\(.*\)"$/\1/p')
  # Checked together, one cargo call each: each of these crates builds exactly as it does
  # alone, every package with the same features (scripts/same-features.py, which fails if that
  # stops being so). A crate in no group is checked alone. One call for twelve crates, not
  # twelve: after a change to orion-types, median of three A B A B, 33 s and 18 CPU-s alone,
  # 14 s and 5 CPU-s grouped (I39).
  local groups=(
    orion-types,orion-check,orion-supervise
    orion-values,orion-device-topics,orion-msg-workloads,orion-msg-topics,orion-msg-session,orion-msg-actions,orion-msg-files,orion-msg-peers,orion-msg-records,orion-wire,orion-device-core,orion-device-firmware
  )
  local grouped=",$(IFS=,; echo "${groups[*]}"),"
  for crate in $(echo "${groups[@]}" | tr ', ' '\n\n'); do
    [[ " ${no_std_crates[*]} " == *" $crate "* ]] || { echo "$crate is in a no_std group, but is not no_std" >&2; return 1; }
  done
  for crate in "${no_std_crates[@]}"; do
    [[ "$grouped" == *",$crate,"* ]] || groups+=("$crate")
  done
  timed "no_std groups build as their crates alone" \
    scripts/same-features.py "$CARGO_TARGET_DIR/same-features" --no-default-features "${targets[@]}" -- "${groups[@]}"
  local clients=orion-connect,orion-topics,orion-records,orion-actions,orion-files,orion-executor,orion-provider,orion-ctl,orion-capi,orion
  local items=() others=(x86_64-pc-windows-msvc x86_64-pc-windows-gnu x86_64-apple-darwin aarch64-apple-darwin)
  for target in "${targets[@]}"; do
    for group in "${groups[@]}"; do
      items+=("check $target $group")
    done
  done
  for target in "${others[@]}"; do
    items+=("clippy $target $clients")
  done
  local state="$GATE_LANE_STATE_DIR/other-systems"
  scripts/lane-reuse.py plan other-systems "$state" "${items[@]}"
  for target in "${targets[@]}"; do
    for group in "${groups[@]}"; do
      packages=()
      for crate in ${group//,/ }; do
        packages+=(-p "$crate")
      done
      lane_item "$state" "check $target $group" "$group on $target" \
        cargo check -q "${packages[@]}" --no-default-features --target "$target"
    done
  done
  for crate in ${clients//,/ }; do
    client+=(-p "$crate")
  done
  for target in "${others[@]}"; do
    lane_item "$state" "clippy $target $clients" "client on $target" \
      cargo clippy -q "${client[@]}" --all-features --all-targets --target "$target" -- -D warnings
  done
  scripts/lane-reuse.py status other-systems "$state" > "$state.status"
}

# lane_item STATE ITEM LABEL COMMAND...: COMMAND, timed, unless ITEM passed before on these inputs
# (STATE, from `scripts/lane-reuse.py plan`); recorded once it passes.
lane_item() {
  local state="$1" item="$2" label="$3"
  shift 3
  if awk -F'\t' -v item="$item" '$1 == item && $3 == "reused" {f = 1} END {exit !f}' "$state"; then
    echo "    $label: reused"
    return 0
  fi
  timed "$label" "$@"
  scripts/lane-reuse.py record "$state" "$item"
}

# The device sizes, unless they passed before on these inputs.
device_size() {
  local state="$GATE_LANE_STATE_DIR/devices"
  scripts/lane-reuse.py plan devices "$state" device-size
  lane_item "$state" device-size "device size" scripts/device-size.sh
  scripts/lane-reuse.py status devices "$state" > "$state.status"
}

# The functions a step runs in a shell of its own (`bounded`), and what they call.
export -f lockfile tests shipped_tests test_filters other_systems timed lane_item device_size

# The workspace glob matches nothing until the first crate lands.
if compgen -G "crates/*/*/Cargo.toml" > /dev/null; then
  main lockfile 8 lockfile
  main format 12 cargo fmt --all --check

  # Beside the rest, each in target directories of its own. The size gate goes last of all
  # when a crate the shipped files hold changes: each of its builds ends in a fat-LTO link of one
  # thread. The other lanes and the main steps finish well before it, so the other lanes run at a
  # lower priority (`nice`) and leave those links the processors first. Mean of six interleaved
  # gates after a change to the comm core, at load 29-83: 125 s, 91 s with this (I21).
  behind=10
  export behind
  # Every crate with optional features: none, each alone, all.
  lane matrix "feature matrix" 605 nice -n "$behind" scripts/feature-matrix.sh
  lane other-systems "no_std and the client on other systems" 416 other_systems
  if [ -x scripts/size-gate.sh ] && [ -d crates/bins/orion-recovery ]; then
    lane size "size gate" 610 scripts/size-gate.sh
  fi
  devices=()
  if [ -x scripts/device-size.sh ] && [ -d crates/device/probe ]; then
    devices+=("device size" 422 nice -n "$behind" env CARGO_TARGET_DIR="$target_dir/device-lane" bash -c 'set -euo pipefail; device_size' --)
  fi
  if [ -x scripts/mcu-template.sh ] && [ -d examples/mcu-template ]; then
    devices+=("mcu template" 140 env CARGO_TARGET_DIR="$target_dir" scripts/mcu-template.sh --)
  fi
  if [ -x scripts/packaging-check.sh ] && [ -d packaging ]; then
    devices+=(packaging 6 scripts/packaging-check.sh --)
  fi
  [ "${#devices[@]}" -gt 0 ] && lane devices "${devices[@]}"

  main clippy 163 cargo clippy --workspace --all-targets --all-features -- -D warnings
  main tests 630 tests
  main "the shipped builds' tests" 300 shipped_tests
  main "test filters" 8 test_filters
  main "nothing built reads the gate's own variables" 5 scripts/lane-reuse.py own-vars
  main "the check compiled out" 17 cargo test -q -p orion-check --no-default-features --lib
  # In this shell, where the lanes are its children: each step in them has its own bound.
  if [ -z "$part" ] || [ "$part" = main ]; then
    timed "the steps beside" join_lanes
  fi
  # The stable-toolchain garbage test always runs. These go deeper where the tools are
  # installed. Alone, last: a target's budget is time, so what runs beside it would cut how
  # much it tries.
  if [ -d testing/fuzz ] && command -v cargo-fuzz > /dev/null && rustup toolchain list | grep -q '^nightly'; then
    main "fuzz, short budget" 148 scripts/fuzz-short.sh
  fi
else
  echo "no crates yet"
fi

if [ -n "$part" ]; then
  echo "gate part $part passed (a part is not stamped)"
  exit 0
fi
# What each lane did: passed, passed with some items reused (`passed(K-of-N-reused)`), or not
# run, and why. The commit carries this line; the stamp holds it, for scripts/push.sh to check.
lanes_line="Gate-Lanes: main=passed"
for name in matrix other-systems size devices; do
  if [ -n "${dropped[$name]:-}" ]; then
    lanes_line+=" $name=not-run(${dropped[$name]// /-})"
  elif [ -f "$GATE_LANE_STATE_DIR/$name.status" ]; then
    lanes_line+=" $name=$(cat "$GATE_LANE_STATE_DIR/$name.status")"
  else
    lanes_line+=" $name=passed"
  fi
done
if [ -n "$checked_tree" ]; then
  if [ "$(tree_now)" = "$checked_tree" ]; then
    stamps="$(git rev-parse --git-dir)/orion-gate-passed"
    mkdir -p "$stamps"
    echo "$lanes_line" > "$stamps/$checked_tree"
  else
    echo "the tree changed while the gate ran: not stamped, so it cannot be pushed" >&2
    exit 1
  fi
fi
echo "$lanes_line"
echo "gate passed"
