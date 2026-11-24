#!/usr/bin/env bash
# Runs the whole gate as a worker's machine runs it, and records what it took and under what
# load: the gate's own output, then one line with the load before and after, the wall time and
# the CPU it used. Every gate must end in under a minute at any load; this line is how one that
# did not is found.
#
#   scripts/run-gate.sh TARGET_DIR LOG
#
# GATE_CPUS (a taskset list, every processor if unset) and GATE_JOBS (cargo's -j, cargo's default
# if unset) keep it to a share of a machine others use too.
set -euo pipefail
[ $# -eq 2 ] || { echo "usage: scripts/run-gate.sh TARGET_DIR LOG" >&2; exit 2; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$root/scripts/work-env.sh"
at_gate_nice
export CARGO_TARGET_DIR="$1"
log="$2"
[ -z "${GATE_JOBS:-}" ] || export CARGO_BUILD_JOBS="$GATE_JOBS"
pin=()
[ -z "${GATE_CPUS:-}" ] || pin=(taskset -c "$GATE_CPUS")
load() { cut -d' ' -f1-3 /proc/loadavg; }
before="$(load)"
echo "gate-start: $(date -Is), load $before, nice $GATE_NICE" > "$log"
status=0
/usr/bin/time -f "%e s wall %U user %S sys" -o "$log.time" "${pin[@]}" "$root/scripts/gate.sh" >> "$log" 2>&1 || status=$?
echo "exit $status nice $GATE_NICE load-before $before load-after $(load) time $(cat "$log.time")" >> "$log"
rm -f "$log.time"
exit "$status"
