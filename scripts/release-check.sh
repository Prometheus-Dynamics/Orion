#!/usr/bin/env bash
# Refuses (non-zero, naming them) unless every lane of the gate ran in full at COMMIT: its
# `Gate-Lanes:` line says `passed` for each, with nothing reused or not run. Part of tagging a
# release; it never runs a gate.
#   scripts/release-check.sh COMMIT
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
[ "$#" -eq 1 ] || { echo "usage: scripts/release-check.sh COMMIT" >&2; exit 2; }
line="$(git log -1 --format=%B "$1" | grep '^Gate-Lanes:' | tail -1 || true)"
[ -n "$line" ] || { echo "refused: $1 has no Gate-Lanes line" >&2; exit 1; }
short=0
for lane in main tests clippy shipped matrix other-systems size devices fuzz; do
  value="$(sed -n "s/.* $lane=\([^ ]*\).*/\1/p" <<< "$line")"
  if [ "$value" != passed ]; then
    echo "refused: $lane at $1 is ${value:-missing}, not passed in full" >&2
    short=1
  fi
done
[ "$short" -eq 0 ] && echo "every lane passed in full at $1"
