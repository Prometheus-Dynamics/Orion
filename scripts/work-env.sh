# The work area's settings, sourced by the scripts that run heavy work (scripts/gate.sh,
# scripts/run-gate.sh, scripts/ab-target.sh). Not run: `. scripts/work-env.sh`.
#
# One nice level for all heavy work on a machine the projects share: every gate, build, test
# run and A/B at 10, never lower or higher, and nothing inside one at another. With levels
# mixed, work at 19 got a sliver of a processor beside builds at 10 (weights 15 and 110), and
# no one's minute could be measured or kept. GATE_: kept out of every lane's key, and read by
# nothing built (scripts/lane-reuse.py own-vars).
export GATE_NICE=10

# at_gate_nice: this shell and what it starts at GATE_NICE, or fail saying why. Absolute
# (`--priority`), not relative: started at 0, it goes to 10, never to 0 + 10 + 10. A process may
# not lower its own nice level, so one started above GATE_NICE fails rather than measure wrong.
at_gate_nice() {
  local now
  now="$(ps -o nice= -p "$$" | tr -d ' ')"
  if [ "$now" -gt "$GATE_NICE" ]; then
    echo "started at nice $now: heavy work runs at nice $GATE_NICE (scripts/work-env.sh); start it there" >&2
    return 1
  fi
  renice --priority "$GATE_NICE" -p "$$" > /dev/null
  at_gate_weight
}

# at_gate_weight: this shell's cgroup at CPU weight 100, the default, and not idle; or fail
# saying why. Nice ranks processes inside one cgroup only; between systemd's units and scopes
# their weights share the processors, and weights of 1, 50 and 100 are in use on the machine.
# Every project's heavy unit or scope runs at CPUWeight=100 with nice 10 inside it: no
# override, and no idle weight. Not on CI, whose runner is its own.
at_gate_weight() {
  local cgroup weight idle
  [ -z "${CI:-}" ] || return 0
  cgroup="/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)"
  weight="$(cat "$cgroup/cpu.weight" 2> /dev/null || echo 100)"
  idle="$(cat "$cgroup/cpu.idle" 2> /dev/null || echo 0)"
  if [ "$weight" != 100 ] || [ "$idle" != 0 ]; then
    echo "started in $cgroup at CPU weight $weight, idle $idle: heavy work runs at CPUWeight=100, not idle (scripts/work-env.sh); start it there" >&2
    return 1
  fi
}
