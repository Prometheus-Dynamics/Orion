#!/usr/bin/env bash
# Refuses (non-zero, naming them) unless every lane of the gate ran in full at COMMIT: its
# `Gate-Lanes:` line says `passed` for each, with nothing reused or not run. Part of tagging a
# release; it never runs a gate.
#
# One not-run is taken: `not-run(ci-only:...)`, a lane the gate here left to CI (the size
# gate's builds of new keys, scripts/size-gate.sh), and only where CI ran that lane at that
# very commit and it passed (`gate (LANE)`, asked of GitHub with `gh`). Any other not-run, or a
# ci-only lane CI did not pass at that commit, is refused as before.
#   scripts/release-check.sh COMMIT
#   scripts/release-check.sh --self-test    every answer, on planted commits and a planted `gh`
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# ci_passed COMMIT LANE: CI ran `gate (LANE)` at exactly COMMIT, and it passed.
ci_passed() {
  local sha run
  sha="$(git rev-parse "$1^{commit}")"
  for run in $(gh run list --commit "$sha" --json databaseId --jq '.[].databaseId'); do
    if gh run view "$run" --json headSha,jobs \
      --jq "select(.headSha == \"$sha\") | .jobs[] | select(.name == \"gate ($2)\") | .conclusion" |
      grep -qx success; then
      return 0
    fi
  done
  return 1
}

check() {
  local commit="$1" line short=0 lane value
  line="$(git log -1 --format=%B "$commit" | grep '^Gate-Lanes:' | tail -1 || true)"
  [ -n "$line" ] || { echo "refused: $commit has no Gate-Lanes line" >&2; return 1; }
  for lane in main tests clippy shipped matrix other-systems size devices fuzz; do
    value="$(sed -n "s/.* $lane=\([^ ]*\).*/\1/p" <<< "$line")"
    if [ "$value" = passed ]; then
      continue
    fi
    if [[ "$value" == "not-run(ci-only:"* ]]; then
      if ci_passed "$commit" "$lane"; then
        echo "$lane at $commit: left to CI here, and CI's gate ($lane) passed at it"
        continue
      fi
      echo "refused: $lane at $commit is $value, and CI's gate ($lane) did not pass at it" >&2
      short=1
      continue
    fi
    echo "refused: $lane at $commit is ${value:-missing}, not passed in full" >&2
    short=1
  done
  [ "$short" -eq 0 ] || return 1
  echo "every lane passed in full at $commit"
}

if [ "${1:-}" = --self-test ]; then
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' EXIT
  failures=0
  fail() { echo "self-test FAILED: $*"; failures=$((failures + 1)); }
  all="main=passed tests=passed clippy=passed shipped=passed matrix=passed other-systems=passed devices=passed fuzz=passed"
  git init -q "$work/repo"
  commit() { git -C "$work/repo" -c user.name=t -c user.email=t@t commit -q --allow-empty -m "$1" -m "Gate-Lanes: $2"; git -C "$work/repo" rev-parse HEAD; }
  full="$(commit full "$all size=passed")"
  ci="$(commit ci "$all size=not-run(ci-only:3-of-3-size-keys-new)")"
  other="$(commit other "$all size=not-run(no-zig-here)")"
  reused="$(commit reused "$all size=passed(2-of-3-reused)")"
  # A planted gh: one run per commit, its size job's conclusion from $work/size-<sha>.
  mkdir "$work/bin"
  cat > "$work/bin/gh" << 'GH'
#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
  "run list") sha="${4:?}"; echo "${sha:0:8}" ;;
  "run view")
    sha="$(git -C "$GH_WORK/repo" rev-list --all | grep "^$3")"
    conclusion="$(cat "$GH_WORK/size-$sha" 2> /dev/null || echo none)"
    printf '{"headSha":"%s","jobs":[{"name":"gate (main)","conclusion":"success"},{"name":"gate (size)","conclusion":"%s"}]}' "$sha" "$conclusion" |
      jq -r "$7"
    ;;
  *) exit 2 ;;
esac
GH
  chmod +x "$work/bin/gh"
  run() { (cd "$work/repo" && PATH="$work/bin:$PATH" GH_WORK="$work" check "$1") > "$work/out" 2>&1; }
  run "$full" || fail "every lane passed, and refused"
  run "$ci" && fail "a ci-only lane with no CI run was taken"
  echo failure > "$work/size-$ci"
  run "$ci" && fail "a ci-only lane CI failed was taken"
  echo success > "$work/size-$ci"
  run "$ci" || fail "a ci-only lane CI passed at that commit was refused: $(cat "$work/out")"
  echo success > "$work/size-$other"
  run "$other" && fail "a not-run other than ci-only was taken, CI green or not"
  run "$reused" && fail "a lane with items reused was taken"
  [ "$failures" -eq 0 ] || exit 1
  echo "release check self-test passed"
  exit 0
fi

[ "$#" -eq 1 ] || { echo "usage: scripts/release-check.sh COMMIT | --self-test" >&2; exit 2; }
check "$1"
