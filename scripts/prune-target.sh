#!/usr/bin/env bash
# What the gate's own builds no longer use, in its debug target directory: builds of earlier
# revisions and of dependencies since moved on, which cargo never removes.
#
#   scripts/prune-target.sh             says what would go, and how much
#   scripts/prune-target.sh --delete    removes it
#
# Live is what cargo names for the gate's debug builds (clippy, the tests, the doctests, the
# check compiled out), asked again with JSON messages: an up-to-date build costs seconds and
# names every artifact it would use. Everything else under debug/deps, build and .fingerprint
# goes, and incremental caches untouched for a day. Removing something still wanted costs a
# rebuild, never a wrong result. Run it when the gate has just passed: what it asks is built.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
delete=no
[ "${1:-}" = "--delete" ] && delete=yes
target_dir="${CARGO_TARGET_DIR:-$PWD/target}"
debug="$target_dir/debug"
[ -d "$debug" ] || { echo "no $debug"; exit 0; }

live=$(mktemp)
trap 'rm -f "$live"' EXIT
{
  cargo clippy --workspace --all-targets --all-features --message-format json -q -- -D warnings
  cargo test --workspace --all-features --no-run --message-format json -q
  cargo test --workspace --all-features --doc --no-run --message-format json -q 2> /dev/null || true
  cargo test -p orion-check --no-default-features --lib --no-run --message-format json -q
} > "$live"

python3 - "$debug" "$delete" "$live" <<'PY'
import json, os, re, shutil, sys, time
debug, delete, live_file = sys.argv[1], sys.argv[2] == "yes", sys.argv[3]
hashed = re.compile(r"-([0-9a-f]{16})(?:\.|$)")
live = set()
for line in open(live_file):
    try:
        message = json.loads(line)
    except ValueError:
        continue
    paths = list(message.get("filenames") or [])
    if message.get("executable"):
        paths.append(message["executable"])
    if message.get("out_dir"):
        paths.append(message["out_dir"])
    for path in paths:
        for part in path.split(os.sep):
            found = hashed.search(part)
            if found:
                live.add(found.group(1))
# A binary or an example is named where cargo copies it out, without its hash: the copy is a
# hard link to the hashed file, which is found by its inode.
# One with no hashed file at all (a C library) keeps the newest fingerprint of its package that
# records that target.
named = set()
unhashed = set()
for line in open(live_file):
    try:
        message = json.loads(line)
    except ValueError:
        continue
    for path in list(message.get("filenames") or []) + [message.get("executable") or ""]:
        if path and not hashed.search(os.path.basename(path)) and os.path.exists(path):
            named.add(os.lstat(path).st_ino)
            # `source#name@version`, or `source/name#version` when the name is the directory's.
            source, _, rest = message["package_id"].partition("#")
            package = rest.split("@")[0] if "@" in rest else source.rstrip("/").rsplit("/", 1)[-1]
            unhashed.add((package, message["target"]["name"].replace("-", "_")))
prints = os.path.join(debug, ".fingerprint")
for package, target in unhashed:
    kept = [(os.lstat(os.path.join(prints, d)).st_mtime, d) for d in os.listdir(prints)
            if d.startswith(package + "-") and hashed.search(d)
            and any(f.endswith("-" + target) for f in os.listdir(os.path.join(prints, d)))]
    if kept:
        live.add(hashed.search(max(kept)[1]).group(1))
for sub in ("deps", "examples"):
    base = os.path.join(debug, sub)
    if os.path.isdir(base):
        for entry in os.listdir(base):
            found = hashed.search(entry)
            if found and os.lstat(os.path.join(base, entry)).st_ino in named:
                live.add(found.group(1))
if not live:
    sys.exit("cargo named nothing: not pruning")

def size(path):
    if os.path.isdir(path) and not os.path.islink(path):
        return sum(os.lstat(os.path.join(d, f)).st_size for d, _, fs in os.walk(path) for f in fs)
    return os.lstat(path).st_size

def newest(path):
    stamps = [os.lstat(path).st_mtime]
    for d, _, fs in os.walk(path):
        stamps.extend(os.lstat(os.path.join(d, f)).st_mtime for f in fs)
    return max(stamps)

doomed = []
for sub in ("deps", "build", ".fingerprint", "examples"):
    base = os.path.join(debug, sub)
    if not os.path.isdir(base):
        continue
    for entry in os.listdir(base):
        found = hashed.search(entry)
        if found and found.group(1) not in live:
            doomed.append(os.path.join(base, entry))
base = os.path.join(debug, "incremental")
if os.path.isdir(base):
    day = time.time() - 86400
    doomed.extend(os.path.join(base, e) for e in os.listdir(base)
                  if newest(os.path.join(base, e)) < day)
total = sum(size(p) for p in doomed)
if os.environ.get("PRUNE_LIST"):
    for path in doomed:
        print("   " + path)
print(f"{len(live)} live builds; {len(doomed)} entries not used, {total / 1e9:.1f} GB")
if delete:
    for path in doomed:
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
    print("removed")
PY
