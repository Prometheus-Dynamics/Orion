#!/bin/sh
# Per build lane: the commit it last ran at in full, and whether it may reuse items or must run
# in full (scripts/lane-reuse.py).
#   scripts/lanes-behind.sh [LANE...]
cd "$(dirname "$0")/.." || exit 2
exec python3 scripts/lane-reuse.py behind "$@"
