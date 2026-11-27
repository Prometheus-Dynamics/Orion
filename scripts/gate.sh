#!/usr/bin/env bash
# The check every PR into next must pass. CI runs exactly this, in parts.
#
#   scripts/gate.sh                 everything; stamps the tree for scripts/push.sh if it passes
#   scripts/gate.sh --part <part>   one part, and no stamp: `main` (every step not in a lane), or
#                                   a lane: `clippy`, `shipped`, `matrix`, `other-systems`,
#                                   `size`, `devices`
#   scripts/gate.sh --drop LANE:WHY a lane not run, recorded so (`not-run(WHY)`)
#
# The parts together are the whole gate. CI runs each on a machine of its own, at once.
#
# In the build lanes, an item that passed before on exactly these inputs is not run again
# (scripts/lane-reuse.py: the feature matrix's builds, the no_std and other-system checks, the
# device sizes, and each package's tests, lints and fuzzing); a lane is never run in full here for
# having gone without one (CI runs everything, on every push). The tree's stamp holds what each lane did, the
# `Gate-Lanes:` line the commit carries (scripts/push.sh checks it).
#
# Nothing whose result is known runs again. A tree whose code key (scripts/code-key.sh: every
# file but the notes) passed before builds and tests the same, so on one the gate checks only
# what reads notes (boundaries, links, the PR list) and stamps the tree; on a tree that passed
# whole, it says so and runs nothing.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# The one nice level for all heavy work (scripts/work-env.sh): everything the gate starts runs
# at it, nothing lower.
. scripts/work-env.sh
at_gate_nice
part=""
declare -A dropped=()
usage="usage: scripts/gate.sh [--part main|clippy|shipped|matrix|other-systems|size|devices] [--drop LANE:WHY]..."
while [ "$#" -gt 0 ]; do
  case "$1 ${2:-}" in
    "--part main" | "--part clippy" | "--part shipped" | "--part matrix" | "--part other-systems" | "--part size" | "--part devices")
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
# Debug and test builds are incremental (cargo's default): a change rebuilds only what it
# touched. The steps whose output is measured pin it off themselves.

# Each step has two bounds, from the two numbers after its label, WALL:CPU: the slowest it has
# taken in a gate that passed, here or in CI, in seconds and in CPU seconds (its whole process
# tree's, as scripts/bounded.py counts them; the CPU number from cold builds too, with nothing
# reused). Each bound is three times its number, and never under a minute. Past its CPU bound a
# step is slow; past its wall bound with CPU to spare it was starved of the machine. A step
# runs under scripts/bounded.py, in a session of its own, its output through a file: past its
# bounds, it and everything it started are ended (TERM, then KILL) and the gate fails, naming
# it and which bound it passed. A step that ends with a process of its own still running fails too, so no process it
# leaves can hold the gate's output open. Each step has a TMPDIR of its own, and fails if it
# leaves anything there.
#
# A hang is found apart from those bounds: a step whose processes use no CPU for a minute
# (STALL_SECONDS) has stalled, and is ended then, named so, however long its bound. Each step
# says the CPU seconds it used.
#
# A step written without its numbers does not run (exit 2): it would be bounded by nothing
# anyone chose. On a machine slower than any these numbers came from, GATE_STEP_BOUND_SCALE (a
# whole number, 1 if unset) multiplies every bound, the stall's among them.
#
# bounded <label> <slowest seconds>:<slowest CPU seconds> <command...>: a command, or a
# function of this script.
STALL_SECONDS=60
bounded() {
  local label="$1" scale="${GATE_STEP_BOUND_SCALE:-1}" bound cpu stall
  if ! [[ "${2:-}" =~ ^([1-9][0-9]*):([1-9][0-9]*)$ ]]; then
    echo "$label: no bounds: '${2:-}' is not its slowest seconds and CPU seconds, WALL:CPU" >&2
    exit 2
  fi
  bound=$((BASH_REMATCH[1] * 3))
  cpu=$((BASH_REMATCH[2] * 3))
  if ! [[ "$scale" =~ ^[1-9][0-9]*$ ]]; then
    echo "$label: GATE_STEP_BOUND_SCALE must be a whole number, not '$scale'" >&2
    exit 2
  fi
  shift 2
  [ "$bound" -ge 60 ] || bound=60
  [ "$cpu" -ge 60 ] || cpu=60
  bound=$((bound * scale))
  cpu=$((cpu * scale))
  stall=$((STALL_SECONDS * scale))
  if declare -F "$1" > /dev/null; then
    scripts/bounded.py "$bound" "$label" --cpu "$cpu" --stall "$stall" -- \
      bash -c 'set -euo pipefail; "$@"' "$label" "$@"
  else
    scripts/bounded.py "$bound" "$label" --cpu "$cpu" --stall "$stall" -- "$@"
  fi
}

# step <label> <slowest seconds>:<slowest CPU seconds> <command...>
step() {
  local label="$1" start status
  echo "==> $label"
  start=$SECONDS
  # Run plainly, then its status kept by hand: under `set -e` a failure ends the gate here; where
  # a caller has `-e` ignored, the time printed after must not stand for the step's status.
  bounded "$@"
  status=$?
  echo "    $label: $((SECONDS - start)) s"
  return "$status"
}

# A part of a bounded step, timed: its bound is the step's.
timed() {
  local label="$1" start status
  shift
  echo "==> $label"
  start=$SECONDS
  # A function in a shell of its own with `-e` on, as `bounded` runs one: where a caller has
  # `-e` ignored, it would be ignored inside the function too, and its last command alone
  # would say how it went.
  if declare -F "$1" > /dev/null; then
    bash -c 'set -euo pipefail; "$@"' "$label" "$@"
  else
    "$@"
  fi
  status=$?
  echo "    $label: $((SECONDS - start)) s"
  return "$status"
}

# A step of the main part: run unless the gate was asked for a lane alone.
main() {
  if [ -z "$part" ] || [ "$part" = main ]; then
    step "$@"
  fi
}

# once FILE... -- COMMAND...: COMMAND, unless it passed before on exactly these files, this git
# and this Python. For the gate's own tools' self-tests: what they test is the files, so with
# the files the same, the result is known (the one-minute rule: nothing known runs again).
once() {
  local files=() hash stamp
  while [ "$1" != -- ]; do
    files+=("$1")
    shift
  done
  shift
  hash="$({ printf '%s\0' "$@" "$(git --version)" "$(python3 --version)"; cat "${files[@]}"; } | sha256sum | cut -c1-16)"
  stamp="$(git rev-parse --path-format=absolute --git-common-dir)/orion-lane-records/once-$hash"
  if [ -f "$stamp" ]; then
    echo "    passed before on these files: not run again"
    return 0
  fi
  # Only a pass is kept: a failure returns its status, and nothing is written. Run plainly, not
  # as `"$@" || return`, which would have `-e` ignored inside a function run so.
  # A function in a shell of its own with `-e` on, as `timed` runs one.
  local status
  if declare -F "$1" > /dev/null; then
    bash -c 'set -euo pipefail; "$@"' once "$@"
  else
    "$@"
  fi
  status=$?
  [ "$status" -eq 0 ] || return "$status"
  mkdir -p "${stamp%/*}"
  touch "$stamp"
}
# Here, not with the rest below: the first steps run it.
export -f once

# The tests that make namespaces of their own (interfaces made and taken away; accounts that
# hold what they should and no more) say so and pass on a machine that cannot make one. The
# gate's machines can, so here one that could not run fails, saying why. Not ORION_: every
# ORION_ variable is a setting to the programs the tests start. Not GATE_: those are kept out of
# the lanes' keys, and nothing built may read them (lane-reuse.py own-vars); this one is read by
# tests alone, and a test run's key holds it (lane-reuse.py TEST_ENV).
export NAMESPACE_TESTS=required

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
# GATE_LANE_SCHEDULE: how the lanes share the machine, for measuring which is faster. Unset or
# `parallel`, as always: every lane starts at once, each with cargo's -j of its own.
# `matrix-after-tests`: the feature matrix starts when the tests step ends, passed or failed;
# a failed tests step still waits for every lane and prints what each said, so a red matrix is
# never hidden behind it. `shared-jobs`: every cargo and rustc of the gate takes its jobs from
# one jobserver, as many as the processors the gate has, beside each one's own -j.
# GATE_TRACE_LANES=FILE writes each lane's command line there as it starts, and when.
schedule="${GATE_LANE_SCHEDULE:-parallel}"
case "$schedule" in
  parallel | matrix-after-tests | shared-jobs) ;;
  *) echo "gate: GATE_LANE_SCHEDULE is parallel, matrix-after-tests or shared-jobs, not '$schedule'" >&2; exit 2 ;;
esac
deferred=()
if [ "$schedule" = shared-jobs ]; then
  jobs_fifo="$lane_logs/jobserver"
  mkfifo "$jobs_fifo"
  exec {jobs_fd}<> "$jobs_fifo"
  # One token fewer than the processors: each process that takes jobs holds one of its own.
  jobs_tokens=$(($(nproc) - 1))
  [ "$jobs_tokens" -ge 1 ] || jobs_tokens=1
  printf '%*s' "$jobs_tokens" '' | tr ' ' '+' >&"$jobs_fd"
  export CARGO_MAKEFLAGS="-j$((jobs_tokens + 1)) --jobserver-auth=fifo:$jobs_fifo"
  echo "lane schedule: shared-jobs, $jobs_tokens tokens beside each process's own"
elif [ "$schedule" != parallel ]; then
  echo "lane schedule: $schedule"
fi
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

# lane <name> <label> <WALL:CPU> <command...> [-- <label> <WALL:CPU> <command...>]...
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
  elif [ -z "$part" ] && [ "$schedule" = matrix-after-tests ] && [ "$name" = matrix ]; then
    deferred=("$name" "$@")
  elif [ -z "$part" ]; then
    lane_start "$name" now "$@"
  fi
}
lane_start() {
  local name="$1" when="$2"
  shift 2
  if [ -n "${GATE_TRACE_LANES:-}" ]; then
    { printf 'lane %s %s:' "$name" "$when"; printf ' %q' "$@"; echo; } >> "$GATE_TRACE_LANES"
  fi
  lane_steps "$@" > "$lane_logs/$name" 2>&1 &
  lane_pids+=("$!")
  lane_names+=("$name")
}
# The lanes held for after the tests step (GATE_LANE_SCHEDULE=matrix-after-tests), started.
start_deferred() {
  [ "${#deferred[@]}" -eq 0 ] || lane_start "${deferred[0]}" "after-tests" "${deferred[@]:1}"
  deferred=()
}
# tests_step <label> <WALL:CPU> <command...>: a step, then the lanes held for after it started,
# passed or failed. Failed, every lane runs to its end and what each said is printed before
# the step's status is returned: a red lane is never hidden behind it.
tests_step() {
  local status=0
  step "$@" || status=$?
  start_deferred
  if [ "$status" -ne 0 ]; then
    join_lanes || true
    return "$status"
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
      local start=$SECONDS status
      bounded "$label" "$slowest" "${command[@]}"
      status=$?
      # A red step ends its lane with its status, `-e` or not: no later step runs, and the
      # lane's status is never the last step's alone.
      [ "$status" -eq 0 ] || exit "$status"
      echo "    $label: $((SECONDS - start)) s"
    done
  )
}

# Waits for every lane, prints what each said, and fails if one did.
# Timed here, not by `timed`: it waits for this shell's children, so it runs in this shell.
join_lanes() {
  local failed=0 i start=$SECONDS
  echo "==> the steps beside"
  for i in "${!lane_pids[@]}"; do
    wait "${lane_pids[$i]}" || failed=1
    cat "$lane_logs/${lane_names[$i]}"
  done
  lane_pids=()
  echo "    the steps beside: $((SECONDS - start)) s"
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
#
# A package whose tests passed before on exactly these inputs (scripts/lane-reuse.py, lane
# `tests`: every crate they build, every file outside the crates, what they say they read, the
# programs on this machine they start) is not tested again: its result is known. The rest run
# together, built with every feature, as `--workspace` built them.
members() {
  local listing
  listing="$(scripts/workspace.py members)"
  echo "$listing"
}
tests() {
  local state="$GATE_LANE_STATE_DIR/tests" listing names=() run=() selected=() docs=() keyed=() name
  listing="$(members)"
  mapfile -t names < <(cut -d' ' -f1 <<< "$listing")
  scripts/lane-reuse.py plan tests "$state" "${names[@]}"
  mapfile -t selected < <(awk -F'\t' '$3 == "run" {print $1}' "$state")
  # The doctests of those that have any (scripts/workspace.py doctested): a crate with none
  # has a known result, no test, and its rustdoc pass is not made. Chosen by the sources' key
  # alone: rustdoc builds and runs a doctest in one pass, so it has no binary to key before.
  if [ "${#selected[@]}" -gt 0 ]; then
    listing="$(scripts/workspace.py doctested "${selected[@]}")"
    if [ "$listing" != none ]; then
      while read -r name; do
        docs+=(-p "$name")
      done <<< "$listing"
    fi
  fi
  # Those to run are built first, and each whose test binaries passed before, the same in all
  # they run, is reused (scripts/lane-reuse.py binary-plan): a comment, or code no test of a
  # package links, runs none of its tests. The run after builds nothing again.
  if [ "${#selected[@]}" -gt 0 ]; then
    for name in "${selected[@]}"; do
      run+=(-p "$name")
    done
    cargo test "${run[@]}" --all-features --no-run --message-format=json-render-diagnostics > "$state.artifacts"
    scripts/lane-reuse.py binary-plan "$state" "$state.artifacts"
    mapfile -t selected < <(awk -F'\t' '$3 == "run" {print $1}' "$state")
    run=()
    for name in "${selected[@]}"; do
      run+=(-p "$name")
    done
  fi
  if [ "${#run[@]}" -eq 0 ] && [ "${#docs[@]}" -eq 0 ]; then
    echo "    every package's tests passed before, on these inputs or these binaries"
  elif [ "${#run[@]}" -eq 0 ]; then
    cargo test "${docs[@]}" --all-features --doc
  elif cargo nextest --version > /dev/null 2>&1; then
    local doc_log doc_pid doc_status=0 status=0
    doc_log="$(mktemp)"
    if [ "${#docs[@]}" -gt 0 ]; then
      cargo test "${docs[@]}" --all-features --doc > "$doc_log" 2>&1 &
      doc_pid=$!
    fi
    cargo nextest run "${run[@]}" --all-features --no-tests=warn || status=$?
    [ -z "${doc_pid:-}" ] || wait "$doc_pid" || doc_status=$?
    cat "$doc_log"
    rm -f "$doc_log"
    [ "$doc_status" -eq 0 ] || echo "    the doctests failed" >&2
    [ "$status" -eq 0 ] && [ "$doc_status" -eq 0 ] || return 1
  else
    cargo test "${run[@]}" --all-features
  fi
  scripts/lane-reuse.py record "$state" "${names[@]}"
  # And each by its binaries, those keyed so.
  if [ -s "$state.bin" ]; then
    mapfile -t keyed < <(cut -f1 "$state.bin")
    scripts/lane-reuse.py record "$state.bin" "${keyed[@]}"
  fi
  scripts/lane-reuse.py status tests "$state" > "$state.status"
}

# Every crate linted, tests included. A function: a lane takes `--` between its steps, so the
# lint's own `--` cannot be written in one.
#
# A package linted before on exactly these inputs is not linted again (scripts/lane-reuse.py,
# lane `clippy`).
clippy_all() {
  local state="$GATE_LANE_STATE_DIR/clippy" listing names=() run=()
  listing="$(members)"
  mapfile -t names < <(cut -d' ' -f1 <<< "$listing")
  scripts/lane-reuse.py plan clippy "$state" "${names[@]}"
  mapfile -t run < <(awk -F'\t' '$3 == "run" {print "-p"; print $1}' "$state")
  if [ "${#run[@]}" -eq 0 ]; then
    echo "    every package was linted before on these inputs"
  else
    cargo clippy "${run[@]}" --all-targets --all-features -- -D warnings
  fi
  scripts/lane-reuse.py record "$state" "${names[@]}"
  scripts/lane-reuse.py status clippy "$state" > "$state.status"
}

# The comm core's tests again, built as each shipped binary builds it, and those binaries' own
# tests alone, so with only what they ship. `--all-features` above is no build that ships: a
# test that holds only with every feature on, or a part that needs one the binary leaves out,
# shows only here. scripts/shipped-features.py reads the sets from the binaries' manifests.
#
# Each item passed before on exactly these inputs is not run again (scripts/lane-reuse.py, lane
# `shipped`).
shipped_tests() {
  local binary line run=(cargo test) state="$GATE_LANE_STATE_DIR/shipped" items=()
  declare -A features=()
  if cargo nextest --version > /dev/null 2>&1; then
    run=(cargo nextest run --no-tests=fail)
  fi
  for binary in orion-bin orion-recovery-bin; do
    line="$(scripts/shipped-features.py "$binary" orion-comm-app)" || return 1
    features[$binary]="$line"
    items+=("orion-comm-app $line" "$binary")
  done
  scripts/lane-reuse.py plan shipped "$state" "${items[@]}"
  for binary in orion-bin orion-recovery-bin; do
    local args=()
    read -ra args <<< "${features[$binary]}"
    lane_item "$state" "orion-comm-app ${features[$binary]}" "orion-comm-app as $binary builds it: ${args[*]}" \
      "${run[@]}" -p orion-comm-app "${args[@]}"
    lane_item "$state" "$binary" "$binary alone" "${run[@]}" -p "$binary"
  done
  scripts/lane-reuse.py status shipped "$state" > "$state.status"
}

# Every filter in .config/nextest.toml (a priority, a test group) matches a test: a test renamed
# or moved into a shared binary would otherwise lose it without a word. Against the list nextest
# gives for the binaries the tests just ran; nothing to check without nextest.
test_filters() {
  if cargo nextest --version > /dev/null 2>&1; then
    local state="$GATE_LANE_STATE_DIR/tests" fresh="" listing run=()
    listing="$(mktemp)"
    mapfile -t run < <(awk -F'\t' '$3 == "run" {print "-p"; print $1}' "$state")
    if [ "${#run[@]}" -gt 0 ]; then
      fresh="$(mktemp)"
      cargo nextest list "${run[@]}" --all-features --message-format json > "$fresh"
    fi
    # Those that ran, as listed now; those reused, as listed when they last ran.
    scripts/lane-reuse.py listing "$state" ${fresh:+"$fresh"} "$listing"
    scripts/check-test-filters.py "$listing"
    # What ran was built only from what its key holds.
    [ -z "$fresh" ] || scripts/lane-reuse.py depinfo "$state" "$fresh"
    rm -f "$listing" ${fresh:+"$fresh"}
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
  stamps="$(git rev-parse --git-dir)/orion-gate-passed"
  code_stamp="$stamps/code-$(scripts/code-key.sh "$checked_tree")"
  tree_stamp="$stamps/tree-$checked_tree"
fi

# The code passed before: only what reads the notes runs, and the tree is stamped. Not a pass
# 3 days old or more: what the machine has (its compilers, the programs tests run) is no part
# of a code key. The gate then runs, and each lane's keys, which hold those, say what to redo.
if [ -z "$part" ] && [ -n "$checked_tree" ] && [ -n "$(find "$code_stamp" -mmin -4320 2> /dev/null)" ]; then
  if [ -e "$tree_stamp" ]; then
    echo "this tree passed before: nothing changed, nothing to run"
  else
    echo "the code passed before (key ${code_stamp##*/code-}): only what reads the notes runs"
    for check in scripts/check-boundaries.sh scripts/check-links.sh "scripts/pr-status.py --offline" "scripts/code-key.sh --check"; do
      timed "$check" $check
    done
    [ "$(tree_now)" = "$checked_tree" ] || { echo "the tree changed while the gate ran: not stamped" >&2; exit 1; }
    : > "$tree_stamp"
  fi
  cat "$code_stamp"
  echo "gate passed"
  exit 0
fi

# The gate's own wrappers, on planted failures: a red command anywhere fails its lane or step,
# and nothing is recorded as passed after one (scripts/gate-self-test.sh).
main "the gate's wrappers' self-test" 6:3 once scripts/gate.sh scripts/gate-self-test.sh -- scripts/gate-self-test.sh
# A release takes a lane left to CI only where CI passed it at that commit.
main "release check self-test" 3:1 once scripts/release-check.sh -- scripts/release-check.sh --self-test
main "push guard" 3:1 once scripts/push.sh scripts/code-key.sh -- scripts/push.sh --self-test
# The target directory is this worktree's own: one built from another worktree's files can
# hold crates cargo takes as fresh for sources that are not these.
main "target owner self-test" 3:1 scripts/target-owner.sh --self-test
step "target owner" 20:10 scripts/target-owner.sh "$target_dir" "$PWD"
main "step bounds self-test" 4:5 once scripts/bounded.py -- scripts/bounded.py --self-test
# A test binary's key moves with what it maps to run, and with all of a file it cannot read as ELF.
main "binary key self-test" 6:3 once scripts/elf-alloc-key.py -- scripts/elf-alloc-key.py --self-test
# A step with no number, or a scale that is not one, refuses to run.
bounds_refuse() {
  local number scale status
  for number in x "" 0 5 5: :5 0:5 5:0 5:x; do
    status=0
    (bounded probe "$number" true) > /dev/null 2>&1 || status=$?
    [ "$status" -eq 2 ] || { echo "a bound of '$number': exit $status, not 2" >&2; return 1; }
  done
  for scale in 1.5 0 2; do
    status=0
    (GATE_STEP_BOUND_SCALE="$scale" bounded probe 1:1 true) > /dev/null 2>&1 || status=$?
    [ "$status" -eq "$([ "$scale" = 2 ] && echo 0 || echo 2)" ] ||
      { echo "a scale of '$scale': exit $status" >&2; return 1; }
  done
}
# Exported: `timed` runs a function in a shell of its own.
export STALL_SECONDS
export -f bounded bounds_refuse
if [ -z "$part" ] || [ "$part" = main ]; then
  timed "steps without a bound refuse" bounds_refuse
fi

# The client builds on Windows and macOS as well as Linux. Clippy for each, tests included:
# what needs a comm core is Linux's and drops out. CI's hosted runners build and run them on
# those systems (`.github/workflows/next.yml`). And a crate that must build without std says
# so in its manifest:
# [package.metadata.orion]
# no_std = true
other_systems() {
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
  local status
  timed "$label" "$@"
  status=$?
  # Recorded only once it passed.
  [ "$status" -eq 0 ] || return "$status"
  scripts/lane-reuse.py record "$state" "$item"
}

# The device sizes, unless they passed before on these inputs.
device_size() {
  local state="$GATE_LANE_STATE_DIR/devices"
  scripts/lane-reuse.py plan devices "$state" device-size
  lane_item "$state" device-size "device size" scripts/device-size.sh
  scripts/lane-reuse.py status devices "$state" > "$state.status"
}

# The keys' self-test (scripts/lane-reuse.py --self-test): each kind of input changed in a copy
# of the tree moves a key. Its result is known while the scripts that make keys are the same, so
# it runs when one of them changed since it last passed here.
keys_self_test() {
  local proved
  proved="$(git rev-parse --path-format=absolute --git-common-dir)/orion-lane-records/self-test-$(cat scripts/lane-reuse.py scripts/size-key.py scripts/elf-alloc-key.py scripts/gate.sh | sha256sum | cut -c1-16)"
  if [ -f "$proved" ]; then
    echo "    passed before on these scripts"
    return 0
  fi
  scripts/lane-reuse.py --self-test
  mkdir -p "$(dirname "$proved")"
  touch "$proved"
}

# Every fuzz target for its short budget, unless that ran before on exactly these inputs
# (scripts/lane-reuse.py, lane `fuzz`): the same targets, built from the same code, given the
# same seconds.
fuzz_short() {
  local state="$GATE_LANE_STATE_DIR/fuzz"
  scripts/lane-reuse.py plan fuzz "$state" "fuzz-short 3"
  # A target directory of its own (a subvolume, scripts/work-area.sh): its builds are
  # nightly's, with sanitizers, and share nothing with the rest.
  lane_item "$state" "fuzz-short 3" "every target, 3 s each" \
    env CARGO_TARGET_DIR="$target_dir/fuzz" scripts/fuzz-short.sh 3
  # Its runs reused by its binaries (scripts/fuzz-short.sh): said so on the gate's line.
  if [ -f "$state.bin" ] && awk -F'\t' '$3 == "reused" {f = 1} END {exit !f}' "$state.bin"; then
    awk -F'\t' -v OFS='\t' '{$3 = "reused"; print}' "$state" > "$state.tmp" && mv "$state.tmp" "$state"
  fi
  scripts/lane-reuse.py status fuzz "$state" > "$state.status"
}

# The functions a step runs in a shell of its own (`bounded`), and what they call.
export -f lockfile members keys_self_test tests clippy_all shipped_tests fuzz_short test_filters other_systems timed lane_item device_size

# The workspace glob matches nothing until the first crate lands.
if compgen -G "crates/*/*/Cargo.toml" > /dev/null; then
  # Before anything builds: a build not held to the lockfile could write it.
  main lockfile 8:2 lockfile

  # Beside the rest, each in target directories of its own, so none waits on another's lock on
  # it. All at the gate's one nice level (scripts/work-env.sh): none lower. Lanes once ran a
  # level lower, to leave the size gate's links the processors (I21); on a machine the projects
  # share, a level lower is a sliver of a processor, and its minute cannot be kept.
  lane clippy clippy 163:371 env CARGO_TARGET_DIR="$target_dir/clippy" bash -c 'set -euo pipefail; clippy_all' --
  lane shipped "the shipped builds' tests" 300:349 env CARGO_TARGET_DIR="$target_dir/shipped" \
    bash -c 'set -euo pipefail; shipped_tests' --
  # Every crate with optional features: none, each alone, all.
  lane matrix "feature matrix" 605:1535 scripts/feature-matrix.sh
  lane other-systems "no_std and the client on other systems" 416:157 other_systems
  if [ -x scripts/size-gate.sh ] && [ -d crates/bins/orion-recovery ]; then
    # The whole gate here leaves a new key's builds to CI (scripts/size-gate.sh, CI only);
    # asked for the lane alone, or on CI, it builds.
    lane size "size gate" 610:1812 env SIZE_GATE_CI_ONLY="$([ -z "$part" ] && echo 1 || echo 0)" scripts/size-gate.sh
  fi
  devices=()
  if [ -x scripts/device-size.sh ] && [ -d crates/device/probe ]; then
    devices+=("device size" 422:110 env CARGO_TARGET_DIR="$target_dir/device-lane" bash -c 'set -euo pipefail; device_size' --)
  fi
  if [ -x scripts/mcu-template.sh ] && [ -d examples/mcu-template ]; then
    devices+=("mcu template" 140:144 env CARGO_TARGET_DIR="$target_dir" scripts/mcu-template.sh --)
  fi
  if [ -x scripts/packaging-check.sh ] && [ -d packaging ]; then
    devices+=(packaging 6:1 scripts/packaging-check.sh --)
  fi
  [ "${#devices[@]}" -gt 0 ] && lane devices "${devices[@]}"

  # Checks that build nothing: the main part's first steps, while the lanes build. One that
  # fails ends the gate and its lanes at once, so they still fail first.
  if [ -x scripts/check-boundaries.sh ]; then
    main boundaries 6:2 scripts/check-boundaries.sh
    main boundaries-self-test 12:2 once scripts/check-boundaries.sh -- scripts/check-boundaries.sh --self-test
  fi
  main "notes are read by no build" 1:1 scripts/code-key.sh --check
  main "test filters self-test" 1:1 scripts/check-test-filters.py --self-test
  main "test files" 1:1 scripts/check-test-filters.py --files
  # A library is built for its unit tests exactly when it has some: none compiled to run nothing,
  # none marked `test = false` with a test that would never run.
  main "libraries built for their unit tests" 3:2 scripts/workspace.py lib-tests
  # Every PR brief reads: an id for a name, a status, and needs that name briefs. AGENTS.md sends
  # whoever picks up a PR to this list, so it must not rot unseen.
  main "pr list" 1:1 scripts/pr-status.py --offline

  if [ -x scripts/check-links.sh ]; then
    main links 2:1 scripts/check-links.sh
  fi
  main format 12:3 cargo fmt --all --check
  main "the lane keys' self-test" 25:12 keys_self_test

  if [ "$schedule" = matrix-after-tests ] && [ -z "$part" ]; then
    tests_step tests 630:647 tests
  else
    main tests 630:647 tests
  fi
  main "test filters" 8:6 test_filters
  main "nothing built reads the gate's own variables" 5:1 scripts/lane-reuse.py own-vars
  main "tests read only what their key holds" 10:5 scripts/lane-reuse.py reads
  main "the check compiled out" 17:17 cargo test -q -p orion-check --no-default-features --lib
  # In this shell, where the lanes are its children: each step in them has its own bound.
  if [ -z "$part" ] || [ "$part" = main ]; then
    join_lanes
  fi
  # The stable-toolchain garbage test always runs. These go deeper where the tools are
  # installed. Alone, last: a target's budget is time, so what runs beside it would cut how
  # much it tries.
  if [ -d testing/fuzz ] && command -v cargo-fuzz > /dev/null && rustup toolchain list | grep -q '^nightly'; then
    main "fuzz, short budget" 148:264 fuzz_short
  elif [ -z "${dropped[fuzz]:-}" ]; then
    # Not passed, if it could not run: the line says so, and a release refuses it.
    dropped[fuzz]="no cargo-fuzz or nightly here"
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
for name in tests clippy shipped matrix other-systems size devices fuzz; do
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
    mkdir -p "$stamps"
    echo "$lanes_line" > "$code_stamp"
    : > "$tree_stamp"
  else
    echo "the tree changed while the gate ran: not stamped, so it cannot be pushed" >&2
    exit 1
  fi
fi
echo "$lanes_line"
echo "gate passed"
