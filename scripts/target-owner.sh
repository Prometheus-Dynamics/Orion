#!/usr/bin/env bash
# Keeps a target directory to the worktree it was built from.
#
#   scripts/target-owner.sh TARGET [WORKTREE]     (WORKTREE: this one, unless said)
#   scripts/target-owner.sh --self-test
#
# cargo decides what is fresh by file times. In a target directory that another worktree
# built from newer files, a crate can be taken as fresh for this worktree's older, different
# sources: a gate then passes or fails on code that is not in the tree. So a target directory
# records the worktree it belongs to (TARGET/.orion-worktree), and:
#
# - one that belongs to another worktree is refused, saying whose it is;
# - one that was seeded from another's (scripts/seed-target.sh) is taken over: every tracked
#   file of this worktree is given the time now first, so nothing built from the other
#   worktree's files is newer than this one's sources, and cargo builds this worktree's own;
# - a new one, or one from before this record, is taken as this worktree's.
set -euo pipefail

owner() {
    local target="$1" worktree="$2" record="$1/.orion-worktree" said
    mkdir -p "$target"
    if [ -f "$record" ]; then
        said="$(cat "$record")"
        case "$said" in
            "$worktree") return 0 ;;
            "seeded from "*)
                echo "target: $target was ${said}: this worktree's files are given the time now, so all of it is built from them"
                (cd "$worktree" && git ls-files -z | xargs -0 -r touch -c --)
                ;;
            *)
                echo "target: $target belongs to the worktree $said, not $worktree." >&2
                echo "  cargo takes a crate as fresh by file times, so it may hold what was built from other sources." >&2
                echo "  Use a target directory of this worktree's own (scripts/seed-target.sh makes one from another's)." >&2
                return 1
                ;;
        esac
    fi
    printf '%s\n' "$worktree" > "$record"
}

self_test() {
    local dir a b out
    dir="$(mktemp -d)"
    trap 'rm -rf "$dir"' RETURN
    a="$dir/a" b="$dir/b"
    for tree in "$a" "$b"; do
        mkdir -p "$tree"
        git -C "$tree" init -q
        echo one > "$tree/file"
        git -C "$tree" add file
    done
    # A new target is this worktree's, and stays so.
    owner "$dir/target" "$a" && owner "$dir/target" "$a" || { echo "self-test: its own target was refused" >&2; return 1; }
    [ "$(cat "$dir/target/.orion-worktree")" = "$a" ] || { echo "self-test: not recorded" >&2; return 1; }
    # Another worktree's is refused, by name, and left as it was.
    if out="$(owner "$dir/target" "$b" 2>&1)"; then
        echo "self-test: another worktree's target was taken" >&2; return 1
    fi
    [[ "$out" == *"belongs to the worktree $a"* ]] || { echo "self-test: not said whose: $out" >&2; return 1; }
    [ "$(cat "$dir/target/.orion-worktree")" = "$a" ] || { echo "self-test: the record changed" >&2; return 1; }
    # A seeded one is taken over, with this worktree's files made newer than what it holds.
    mkdir -p "$dir/seeded"
    echo "seeded from $a" > "$dir/seeded/.orion-worktree"
    touch -d '2001-01-01' "$b/file"
    touch "$dir/seeded/built"
    owner "$dir/seeded" "$b" > /dev/null || { echo "self-test: a seeded target was refused" >&2; return 1; }
    [ "$b/file" -nt "$dir/seeded/built" ] || { echo "self-test: the sources are older than the seed" >&2; return 1; }
    [ "$(cat "$dir/seeded/.orion-worktree")" = "$b" ] || { echo "self-test: a seeded target was not taken over" >&2; return 1; }
    echo "target owner self-test passed"
}

if [ "${1:-}" = --self-test ]; then
    self_test
    exit
fi
[ "$#" -ge 1 ] || { echo "usage: scripts/target-owner.sh TARGET [WORKTREE] | --self-test" >&2; exit 2; }
owner "$1" "$(realpath "${2:-$(dirname "${BASH_SOURCE[0]}")/..}")"
