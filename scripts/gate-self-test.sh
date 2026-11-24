#!/usr/bin/env bash
# The gate's own wrappers, proved on planted failures: each run as the gate runs it (its
# functions, taken from scripts/gate.sh as they are, in a shell with the gate's `set -euo
# pipefail`), and again called where `-e` is ignored (`f || exit`), where a wrapper that leaned
# on `-e` would let the time it prints after a red command stand for a pass.
#
#   scripts/gate-self-test.sh [GATE]    GATE: the gate script to take the wrappers from
#
# Red is: a lane whose first, last or only step fails (also started after `&&`, and with its
# steps split on `--`); a step, a function step whose first command fails, a timed part (a
# function too). A failing lane item writes no pass record; a failing `once` command no stamp.
# Green is: lanes and steps of passing commands, every step run. Every case is checked by what
# it printed too, not by its status alone. Records and stamps go to a git directory of its own.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
gate="${1:-scripts/gate.sh}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
{
  echo 'part=""; declare -A dropped=(); STALL_SECONDS=60'
  for name in bounded step timed once lane lane_steps join_lanes lane_item; do
    sed -n "/^$name() {/,/^}/p" "$gate" | grep -q . || { echo "gate-self-test: no $name() in $gate" >&2; exit 2; }
    sed -n "/^$name() {/,/^}/p" "$gate"
  done
  echo 'export -f once timed lane_item'
} > "$work/wrappers.sh"
git init -q --bare "$work/git"
failures=0
fail() { echo "FAIL: $*"; failures=$((failures + 1)); }

# run red|green LABEL BODY [MARKER...]: BODY in the gate's shell, at top level and inside an
# `-e`-ignored call; each must end as said and print every MARKER (a line, as a regex).
run() {
  local want="$1" label="$2" body="$3" where out status marker
  shift 3
  for where in top ignored; do
    out="$work/out"
    mkdir -p "$work/lanes"
    local call="$body"
    [ "$where" = top ] || call="planted() { $body; }; planted || exit \$?"
    status=0
    GIT_DIR="$work/git" bash -c "set -euo pipefail; . '$work/wrappers.sh'
      lane_logs='$work/lanes'; lane_pids=(); lane_names=()
      GATE_LANE_STATE_DIR='$work/lanes'; export GATE_LANE_STATE_DIR
      two_commands() { false; true; }; export -f two_commands
      $call" > "$out" 2>&1 || status=$?
    if [ "$want" = red ] && [ "$status" -eq 0 ]; then
      fail "$label ($where): passed"
    elif [ "$want" = green ] && [ "$status" -ne 0 ]; then
      fail "$label ($where): failed, status $status"
    fi
    for marker in "$@"; do
      grep -qE "$marker" "$out" || fail "$label ($where): no line '$marker'"
    done
    if [ "$want" = red ] && grep -q '^==> two' "$out"; then
      fail "$label ($where): a step ran after a red one"
    fi
  done
}

run red "a lane, its first step red" 'lane t one 1:1 false -- two 1:1 true; join_lanes' 'FAILED: one'
run red "a lane, its last step red" 'lane t one 1:1 true -- last 1:1 false; join_lanes' 'one: [0-9]+ s' 'FAILED: last'
run red "a lane, its only step red" 'lane t one 1:1 false; join_lanes' 'FAILED: one'
run red "a lane started after &&, its first step red" \
  '[ 1 -gt 0 ] && lane t one 1:1 false -- two 1:1 true; join_lanes' 'FAILED: one'
run red "a step" 'step one 1:1 false' '^==> one'
run red "a function step, its first command red, its last green" 'step one 1:1 two_commands' '^==> one'
run red "a timed part" 'timed one false' '^==> one'
run red "a timed function, its first command red, its last green" 'timed one two_commands' '^==> one'
run green "a lane of two green steps" 'lane t one 1:1 true -- two 1:1 true; join_lanes' \
  'one: [0-9]+ s' 'two: [0-9]+ s'
run green "a step" 'step one 1:1 true' 'one: [0-9]+ s'

# A pass record only after a pass: a red lane item writes none, a green one does.
printf 'red\tplanted-red\trun\ngreen\tplanted-green\trun\n' > "$work/lanes/state"
echo gate-self-test > "$work/lanes/state.lane"
run red "a lane item" "lane_item '$work/lanes/state' red red false"
run green "a lane item" "lane_item '$work/lanes/state' green green true"
[ ! -e "$work/git/orion-lane-records/gate-self-test/planted-red" ] || fail "a red lane item was recorded as passed"
[ -e "$work/git/orion-lane-records/gate-self-test/planted-green" ] || fail "a green lane item was not recorded"
# A `once` stamp only after a pass.
run red "a once command" "once '$gate' -- two_commands"
if compgen -G "$work/git/orion-lane-records/once-*" > /dev/null; then
  fail "a red once command was stamped as passed"
fi

[ "$failures" -eq 0 ] || exit 1
echo "gate self-test passed"
