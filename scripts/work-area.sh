#!/usr/bin/env bash
# Makes this machine's work area for the repository, or says what it lacks: worktrees, build
# targets, caches and logs on the fast disk, outside the checkout.
#
#   scripts/work-area.sh [AREA]           make what is missing; AREA is ORION_WORK_AREA,
#                                         /mnt/nvme-wd/orion if unset
#   scripts/work-area.sh --check [AREA]   say what is missing or wrong, change nothing
#
#   AREA/wt/        worktrees, one per branch worked on
#   AREA/target/    the gate's build targets (CARGO_TARGET_DIR): a btrfs subvolume of its own
#   AREA/target/size-gate, AREA/target/fuzz     each a subvolume of its own too
#   AREA/cache/     what is kept between runs and can be made again (scripts/board-ssh.sh)
#   AREA/logs/      the gates' logs (scripts/run-gate.sh)
#
# Why subvolumes: a btrfs filesystem's files sit in one b-tree per subvolume, and every
# compiler creating, opening and listing files in a target takes that tree's locks. On a
# disk shared with other builds, a core change's test build spent most of its time in
# uninterruptible waits on them (btrfs_inode_lock, getdents64, filename_create: rustc was in
# such a wait 4 to 5 times as often as it ran). A target in a subvolume of its own has a tree
# of its own. No mount changes and no root: a subvolume is made like a directory.
#
# An existing target is never moved into a subvolume (a move keeps the files in the old tree):
# a target is built fresh in one, or copied in with `cp --reflink=always`, which shares the
# data and costs no space. Removing a subvolume is `rm -rf` of its files, then `rmdir` of the
# empty subvolume, which its owner may do (`btrfs subvolume delete` needs root, or the
# filesystem's user_subvol_rm_allowed, and is refused otherwise).
set -euo pipefail
check=no
if [ "${1:-}" = --check ]; then
  check=yes
  shift
fi
area="${1:-${ORION_WORK_AREA:-/mnt/nvme-wd/orion}}"
missing=0

is_subvolume() {
  # A subvolume's root directory has inode number 256 on btrfs.
  [ "$(stat -f -c %T "$1" 2> /dev/null)" = btrfs ] && [ "$(stat -c %i "$1")" = 256 ]
}

directory() {
  if [ -d "$1" ]; then
    return
  fi
  if [ "$check" = yes ]; then
    echo "missing: $1"
    missing=1
  else
    mkdir -p "$1"
    echo "made $1"
  fi
}

subvolume() {
  local path="$1"
  if is_subvolume "$path"; then
    return
  fi
  if [ "$(stat -f -c %T "$(dirname "$path")" 2> /dev/null)" != btrfs ]; then
    # Not btrfs: a plain directory, and nothing to gain from a subvolume.
    directory "$path"
    return
  fi
  if [ -e "$path" ]; then
    echo "not a subvolume: $path (build one fresh beside it, or copy it with cp --reflink=always, then swap)" >&2
    missing=1
    return
  fi
  if [ "$check" = yes ]; then
    echo "missing: $path (a subvolume)"
    missing=1
  else
    btrfs subvolume create "$path" > /dev/null
    echo "made subvolume $path"
  fi
}

directory "$area"
directory "$area/wt"
subvolume "$area/target"
if is_subvolume "$area/target"; then
  subvolume "$area/target/size-gate"
  subvolume "$area/target/fuzz"
fi
directory "$area/cache"
directory "$area/logs"
if [ "$missing" = 0 ]; then
  echo "work area $area: complete"
fi
exit "$missing"
