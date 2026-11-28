#!/usr/bin/env bash
# The repo's own Cargo home, so a gate reads its crates from nothing it shares with other work
# (the user's ~/.cargo, held by every other project's builds too) and never from the network:
#
#   scripts/cargo-home.sh DIR             DIR/home with every crate that every lock file names,
#                                         fetched once per set of lock files; prints the record
#   scripts/cargo-home.sh DIR lane NAME   prints DIR/lanes/NAME: one lane's copy of DIR/home,
#                                         made again when the fetch it was copied from is not
#                                         the current one
#
# The fetch is `cargo fetch --locked` in the directory of each lock file in git (the examples'
# and the fuzz targets' among them), and of no other: a lock file git does not hold is not a
# lock anyone else has. The record
# (DIR/fetch.log) holds each lock file's sha256, the sources the crates come from, the number
# of crate files and a sha256 over theirs. It is fetched again only when a lock file, cargo or
# this script changes; otherwise nothing is read but the lock files.
#
# The home's config says offline, and never cleans its cache: a build that needs a crate the
# lock files do not name fails at once, saying so, where it would have fetched it.
#
# A lane's copy is at the same path every time: test binaries hold the paths of the crate
# sources they were built from, so a copy at a new path would change what every lane built. On
# a file system with reflinks (btrfs, XFS) a copy costs only what the lanes later write to it.
set -euo pipefail
[ $# -eq 1 ] || { [ $# -eq 3 ] && [ "$2" = lane ]; } || {
  echo "usage: scripts/cargo-home.sh DIR [lane NAME]" >&2
  exit 2
}
dir="$(realpath -m "$1")"
home="$dir/home"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if [ $# -eq 3 ]; then
  name="$3"
  [[ "$name" =~ ^[A-Za-z0-9_.-]+$ ]] || { echo "cargo-home: a lane's name, not '$name'" >&2; exit 2; }
  [ -s "$home/.orion-fetch" ] || { echo "cargo-home: $home has not been fetched" >&2; exit 1; }
  lane="$dir/lanes/$name"
  if ! cmp -s "$home/.orion-fetch" "$lane/.orion-fetch" 2> /dev/null; then
    mkdir -p "$dir/lanes"
    # Made beside it under a name of this process's own, then put in its place. Ended before
    # that (a gate that stops ends its lanes), it removes what it made.
    trap 'rm -rf "$lane.new.$$"' EXIT
    trap 'exit 143' TERM
    trap 'exit 130' INT
    rm -rf "$lane.new.$$"
    mkdir "$lane.new.$$"
    for part in registry git config.toml .orion-fetch; do
      [ ! -e "$home/$part" ] || cp -a --reflink=auto "$home/$part" "$lane.new.$$/"
    done
    if [ -e "$lane" ]; then
      mv "$lane" "$lane.old.$$"
    fi
    mv "$lane.new.$$" "$lane"
    rm -rf "$lane.old.$$"
  fi
  echo "$lane"
  exit 0
fi

# What a copy left when it could not remove it (killed outright): each `<lane>.new.<pid>` and
# `<lane>.old.<pid>` whose process is gone.
if [ -d "$dir/lanes" ]; then
  for left in "$dir/lanes"/*.new.* "$dir/lanes"/*.old.*; do
    [ -e "$left" ] || continue
    pid="${left##*.}"
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    if ! kill -0 "$pid" 2> /dev/null; then
      echo "cargo-home: removed $left (its copy ended before it was done)"
      rm -rf "$left"
    fi
  done
fi

# The lock files: those in git.
locks=()
while IFS= read -r lock; do
  locks+=("$lock")
done < <(git ls-files -- 'Cargo.lock' '*/Cargo.lock')

# What the fetch is of: every lock file by content, cargo, and this script.
fetch_key() {
  {
    echo "cargo $(cargo -V)"
    sha256sum scripts/cargo-home.sh
    for lock in "${locks[@]}"; do
      sha256sum "$lock"
    done
  } | sha256sum | cut -d' ' -f1
}
key="$(fetch_key)"
if [ "$(cat "$home/.orion-fetch" 2> /dev/null)" = "$key" ] && [ -s "$dir/fetch.log" ]; then
  cat "$dir/fetch.log"
  exit 0
fi

mkdir -p "$home"
printf '[net]\noffline = true\n\n[cache]\nauto-clean-frequency = "never"\n' > "$home/config.toml"
rm -f "$home/.orion-fetch"
for lock in "${locks[@]}"; do
  # Online for the fetch alone, whatever the home's config says.
  (cd "$(dirname "$lock")" && CARGO_HOME="$home" CARGO_NET_OFFLINE=false cargo fetch --locked --quiet)
done

{
  echo "fetch $key"
  echo "home $home"
  echo "cargo $(cargo -V)"
  for lock in "${locks[@]}"; do
    echo "lock $lock $(sha256sum "$lock" | cut -d' ' -f1)"
  done
  sed -n 's/^source = "\(.*\)"$/\1/p' "${locks[@]}" | sed 's/#.*//; s/?.*//' | sort -u | sed 's/^/source /'
  crates="$(cd "$home/registry/cache" && find . -name '*.crate' | sort)"
  echo "crates $(grep -c . <<< "$crates")"
  echo "crate sums $(cd "$home/registry/cache" && xargs -d '\n' sha256sum <<< "$crates" | sha256sum | cut -d' ' -f1)"
  echo "git checkouts $(find "$home/git/checkouts" -mindepth 2 -maxdepth 2 -type d 2> /dev/null | sed "s|$home/git/checkouts/||" | sort | tr '\n' ' ')"
} > "$dir/fetch.log.new"
mv "$dir/fetch.log.new" "$dir/fetch.log"
echo "$key" > "$home/.orion-fetch"
cat "$dir/fetch.log"
