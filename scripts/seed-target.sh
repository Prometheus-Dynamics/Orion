#!/usr/bin/env bash
# Starts a new worktree's target directory as a copy of another's, so its first gate builds
# only what differs: the workspace's own crates (their paths differ) and what changed since.
#
#   scripts/seed-target.sh FROM TO
#
# A copy, never a share: each worktree keeps a target directory of its own, and cargo's
# fingerprints decide what is rebuilt, as in any target directory. cargo goes by file times, so
# the copy says it was seeded, and the gate makes the new worktree's files the newest before
# it builds in it. On a filesystem that clones
# files (btrfs, XFS) the copy takes seconds and no space until either side changes a file;
# elsewhere it is a whole copy, and the script says so first. Prune FROM first
# (scripts/prune-target.sh --delete) to copy less.
set -euo pipefail
[ "$#" -eq 2 ] || { echo "usage: scripts/seed-target.sh FROM TO" >&2; exit 2; }
from="$1" to="$2"
[ -d "$from" ] || { echo "no $from" >&2; exit 1; }
if [ -e "$to" ] && [ -n "$(ls -A "$to" 2> /dev/null)" ]; then
  echo "$to is not empty: not overwritten" >&2
  exit 1
fi
mkdir -p "$(dirname "$to")"
probe="$(find "$from" -type f -size +1k -print -quit)"
if [ -n "$probe" ] && ! cp --reflink=always "$probe" "$to.probe" 2> /dev/null; then
  echo "this filesystem does not clone files: $(du -sh "$from" | cut -f1) will be copied whole"
fi
rm -f "$to.probe"
# An empty TO is replaced, not copied into.
[ -d "$to" ] && rmdir "$to"
cp -a --reflink=auto "$from" "$to"
# Whose it was: the first gate in the new worktree takes it over, and gives that worktree's
# files the time now first, so nothing in the copy is newer than the sources it is used for
# (scripts/target-owner.sh).
echo "seeded from $(cat "$from/.orion-worktree" 2> /dev/null || echo "$from")" > "$to/.orion-worktree"
echo "seeded $to from $from"
