#!/usr/bin/env bash
# Pushes only what the gate passed on. scripts/gate.sh, when it passes, stamps the exact tree it
# checked (tracked and new files, as `git add -A` would commit them, and only if nothing changed
# while it ran). This refuses unless every commit to be pushed has a stamped tree.
#
# A working branch `pr/<id>` is not landed, so it pushes without stamps: the landing gate, run on
# the merged tree before the push to `next`, is the proof, and a worker does not repeat it. Every
# other branch needs its stamps.
#
#   scripts/push.sh REMOTE BRANCH    push HEAD to REMOTE/BRANCH, if every new commit passed
#   scripts/push.sh --self-test      prove it refuses an unstamped commit and lets a stamped one by
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

stamps() { echo "$(git rev-parse --git-dir)/orion-gate-passed"; }

if [ "${1:-}" = "--self-test" ]; then
  work="$(mktemp -d "${TMPDIR:-/tmp}/orion-push.XXXXXX")"
  trap 'rm -rf "$work"' EXIT
  git init -q --bare "$work/remote.git"
  git init -q -b next "$work/tree"
  cd "$work/tree"
  git config user.email test@example.invalid
  git config user.name test
  echo one > file && git add -A && git commit -q -m one
  git remote add origin "$work/remote.git"
  # Not stamped: refused, and nothing reaches the remote.
  if "$here/push.sh" origin next > "$work/out" 2>&1; then
    echo "self-test FAILED: an unstamped commit was pushed"; exit 1
  fi
  grep -q "the gate has not passed" "$work/out" || { echo "self-test FAILED: wrong refusal"; cat "$work/out"; exit 1; }
  [ -z "$(git --git-dir="$work/remote.git" rev-parse -q --verify refs/heads/next || true)" ] \
    || { echo "self-test FAILED: the remote got the branch"; exit 1; }
  # Stamped: pushed.
  mkdir -p "$(stamps)" && touch "$(stamps)/$(git rev-parse 'HEAD^{tree}')"
  "$here/push.sh" origin next > "$work/out" 2>&1 || { echo "self-test FAILED: a stamped commit was refused"; cat "$work/out"; exit 1; }
  # Two new commits, only the last stamped: refused, since the first did not pass.
  echo two > file && git commit -qam two
  echo three > file && git commit -qam three
  touch "$(stamps)/$(git rev-parse 'HEAD^{tree}')"
  if "$here/push.sh" origin next > "$work/out" 2>&1; then
    echo "self-test FAILED: a commit the gate never passed went with a later one"; exit 1
  fi
  # A working branch takes unstamped commits; the landing branch still refuses them.
  echo four > file && git commit -qam four
  "$here/push.sh" origin pr/X1 > "$work/out" 2>&1 || { echo "self-test FAILED: a working branch was refused"; cat "$work/out"; exit 1; }
  if "$here/push.sh" origin next > "$work/out" 2>&1; then
    echo "self-test FAILED: an unstamped commit landed after a working-branch push"; exit 1
  fi
  echo "push guard self-test passed"
  exit 0
fi

[ $# -eq 2 ] || { echo "usage: scripts/push.sh REMOTE BRANCH" >&2; exit 2; }
remote="$1" branch="$2"
case "$branch" in
  pr/?*) exec git push "$remote" "HEAD:refs/heads/$branch" ;;
esac
git fetch -q "$remote" "$branch" 2>/dev/null || true
if git rev-parse -q --verify "refs/remotes/$remote/$branch" > /dev/null; then
  range="$remote/$branch..HEAD"
else
  range="HEAD"
fi
missing=0
for commit in $(git rev-list "$range"); do
  tree="$(git rev-parse "$commit^{tree}")"
  if [ ! -e "$(stamps)/$tree" ]; then
    echo "refused: the gate has not passed on $(git log -1 --format='%h %s' "$commit")" >&2
    missing=1
  fi
done
[ "$missing" -eq 0 ] || { echo "run scripts/gate.sh on exactly that tree, then commit it" >&2; exit 1; }
exec git push "$remote" "HEAD:refs/heads/$branch"
