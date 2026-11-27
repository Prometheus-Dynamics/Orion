#!/usr/bin/env bash
# What a gate costs after a change: a comment appended to each FILE, the whole gate run as a
# worker's machine runs it (scripts/run-gate.sh, at the one nice level), the files put back; RUNS
# times. Then each run's wall time, CPU time and load, and the median and the slowest.
#
#   scripts/measure-change.sh FILE[,FILE...] RUNS TARGET_DIR LOG_DIR
#
# Several files, comma-separated, are one change: a batch. A different comment each run, so each
# is a change: `#` in a .py, .sh or .toml file, `<!-- -->` in .html, `//` in any other.
# GATE_CPUS and GATE_JOBS go to run-gate.sh. MEASURE_CHANGE picks another kind of change, again
# different each run:
#   comment       a comment line appended (the default)
#   unused-fn     a pub fn nothing calls appended to each FILE, all .rs: code that is built, and
#                 never runs
#   used=TEXT     TEXT, a string in each FILE, made longer: code that runs, and reads it
# Run it from a committed tree: an uncommitted file is a change too, and the gates would measure it.
# The one-minute rule: every gate ends in under 60 s, the slowest of twenty included.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
. scripts/work-env.sh
at_gate_nice
[ "$#" -eq 4 ] || { echo "usage: scripts/measure-change.sh FILE[,FILE...] RUNS TARGET_DIR LOG_DIR" >&2; exit 2; }
IFS=, read -r -a files <<< "$1"
runs="$2" target="$3" logs="$4"
[ "${#files[@]}" -gt 0 ] || { echo "measure-change: no FILE" >&2; exit 2; }
kind="${MEASURE_CHANGE:-comment}"
for file in "${files[@]}"; do
  [ -f "$file" ] || { echo "measure-change: no file $file" >&2; exit 2; }
  case "$kind" in
    comment) ;;
    unused-fn) [[ "$file" == *.rs ]] || { echo "measure-change: unused-fn needs .rs files, not $file" >&2; exit 2; } ;;
    used=?*) grep -qF -- "${kind#used=}" "$file" || { echo "measure-change: $file lacks '${kind#used=}'" >&2; exit 2; } ;;
    *) echo "measure-change: MEASURE_CHANGE is comment, unused-fn or used=TEXT, not '$kind'" >&2; exit 2 ;;
  esac
done
git diff --quiet || { echo "measure-change: the worktree has changes; commit or set them aside" >&2; exit 1; }
saved="$(mktemp -d)"
for number in "${!files[@]}"; do
  cp -f "${files[$number]}" "$saved/$number"
done
# Put back with a fresh mtime (cp -f, not -p): an old one could make cargo reuse a build of
# the change.
put_back() {
  local number
  for number in "${!files[@]}"; do
    cp -f "$saved/$number" "${files[$number]}"
  done
}
trap 'put_back; rm -rf "$saved"' EXIT
mkdir -p "$logs"
stamp="$(date +%Y%m%dT%H%M%S)"
walls=()
for run in $(seq 1 "$runs"); do
  for file in "${files[@]}"; do
    case "$kind" in
      comment)
        case "$file" in
          *.py | *.sh | *.toml) printf '\n# measure-change %s run %s\n' "$stamp" "$run" >> "$file" ;;
          *.html) printf '\n<!-- measure-change %s run %s -->\n' "$stamp" "$run" >> "$file" ;;
          *) printf '\n// measure-change %s run %s\n' "$stamp" "$run" >> "$file" ;;
        esac ;;
      unused-fn)
        printf '\n/// Called by nothing: measure-change.\npub fn measure_change_%s_%s(x: u32) -> u32 {\n    x.wrapping_mul(%s).rotate_left(7)\n}\n' \
          "${stamp,,}" "$run" "$((2654435761 + run))" >> "$file" ;;
      used=*)
        python3 - "$file" "${kind#used=}" "$stamp $run" <<'PY'
import sys
path, text, mark = sys.argv[1:]
with open(path) as file:
    source = file.read()
with open(path, "w") as file:
    file.write(source.replace(text, f"{text} ({mark})", 1))
PY
        ;;
    esac
  done
  log="$logs/measure-$stamp-$run.log"
  status=0
  scripts/run-gate.sh "$target" "$log" > /dev/null 2>&1 || status=$?
  put_back
  line="$(tail -1 "$log")"
  wall="$(sed -n 's/.* time \([0-9.]*\) s wall.*/\1/p' <<< "$line")"
  cpu="$(sed -n 's/.* s wall \([0-9.]*\) user \([0-9.]*\) sys.*/\1 \2/p' <<< "$line" | awk '{print $1 + $2}')"
  loads="$(sed -n 's/.*load-before \([0-9.]*\) .*load-after \([0-9.]*\) .*/\1 to \2/p' <<< "$line")"
  echo "run $run ($kind): exit $status, ${wall:-?} s wall, ${cpu:-?} CPU-s, nice ${GATE_NICE:-10}, load $loads ($log)"
  [ "$status" -eq 0 ] || { echo "run $run failed: see $log" >&2; exit 1; }
  walls+=("$wall")
done
printf '%s\n' "${walls[@]}" | sort -n | awk '{w[NR] = $1} END {
  median = (NR % 2) ? w[(NR + 1) / 2] : (w[NR / 2] + w[NR / 2 + 1]) / 2
  printf "median %.1f s, slowest %.1f s, of %d runs\n", median, w[NR], NR
}'
