#!/usr/bin/env bash
# Which test binaries a change moves: by the whole file's hash, and by the key of what runs
# (scripts/elf-alloc-key.py, the allocated sections). For each change, every test binary the
# gate's tests step builds (`cargo test --workspace --all-features --no-run`) is built, timed,
# and keyed both ways against the build of the committed tree. The tree is put back after each.
#
#   scripts/alloc-key-probe.sh TARGET_DIR OUT_DIR
#   scripts/alloc-key-probe.sh --fuzz TARGET_DIR OUT_DIR   the fuzz targets instead, built as
#                                                         scripts/fuzz-short.sh builds them
#
# The changes, each to orion-types, which most crates build on:
#   comment-end   a comment line appended to its lib.rs (no line of code moves)
#   comment-top   a comment line put first in error.rs (every line after it moves)
#   used          an error message changed: "malformed data" (code that runs, and reads it)
#   used-hex      the escape's hex digits made lower case (code names and decoders run)
#   unused-fn     a pub fn nothing calls, appended to its lib.rs
#
# OUT_DIR gets, per change, `<change>.keys` (alloc key, file hash, binary) and `<change>.build`
# (wall, user, system seconds of the build), and a summary on stdout. Measurement only: no
# test runs. TARGET_DIR must be warm for the committed tree, and is not the gate's own.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
. scripts/work-env.sh
at_gate_nice
fuzz=no
if [ "${1:-}" = --fuzz ]; then
  fuzz=yes
  shift
fi
[ "$#" -eq 2 ] || { echo "usage: scripts/alloc-key-probe.sh [--fuzz] TARGET_DIR OUT_DIR" >&2; exit 2; }
target="$1" out="$2"
mkdir -p "$out"
git diff --quiet || { echo "alloc-key-probe: the worktree has changes; commit or set them aside" >&2; exit 1; }
saved="$(mktemp -d)"
files=(crates/core/types/src/lib.rs crates/core/types/src/error.rs crates/core/types/src/escape.rs)
for f in "${files[@]}"; do cp -f "$f" "$saved/${f//\//_}"; done
# Put back with a fresh mtime (cp -f, not -p): an old one could make cargo reuse a build of a change.
restore() { for f in "${files[@]}"; do cp -f "$saved/${f//\//_}" "$f"; done; }
trap 'restore; rm -rf "$saved"' EXIT

build() { # NAME: build every test binary, time it, key each
  local name="$1" list
  list="$(mktemp)"
  if [ "$fuzz" = yes ]; then
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$target" /usr/bin/time -o "$out/$name.build" -f '%e %U %S' \
      taskset -c "${GATE_CPUS:-6-31}" cargo +nightly fuzz build --fuzz-dir testing/fuzz > "$out/$name.log" 2>&1
    for path in testing/fuzz/fuzz_targets/*.rs; do
      echo "$target/x86_64-unknown-linux-gnu/release/$(basename "$path" .rs)"
    done > "$list"
    xargs -a "$list" scripts/elf-alloc-key.py > "$out/$name.keys"
    rm -f "$list"
    return
  fi
  CARGO_TARGET_DIR="$target" /usr/bin/time -o "$out/$name.build" -f '%e %U %S' \
    taskset -c "${GATE_CPUS:-6-31}" cargo test --workspace --all-features --no-run -j "${GATE_JOBS:-13}" \
    --message-format=json 2> "$out/$name.log" |
    jq -r 'select(.reason == "compiler-artifact" and .executable != null) | .executable' | sort -u > "$list"
  xargs -a "$list" scripts/elf-alloc-key.py > "$out/$name.keys"
  rm -f "$list"
}

build base
build base-again
printf '\n// alloc-key-probe: a comment\n' >> crates/core/types/src/lib.rs
build comment-end
restore
{ echo '// alloc-key-probe: a comment'; cat "$saved/crates_core_types_src_error.rs"; } > crates/core/types/src/error.rs
build comment-top
restore
sed -i 's/"malformed data"/"malformed data!"/' crates/core/types/src/error.rs
grep -q '"malformed data!"' crates/core/types/src/error.rs || { echo "alloc-key-probe: the used change was not made" >&2; exit 1; }
build used
restore
sed -i 's/b"0123456789ABCDEF"/b"0123456789abcdef"/' crates/core/types/src/escape.rs
grep -q 'b"0123456789abcdef"' crates/core/types/src/escape.rs || { echo "alloc-key-probe: the hex change was not made" >&2; exit 1; }
build used-hex
restore
printf '\n/// Called by nothing: alloc-key-probe.\npub fn alloc_key_probe_unused(x: u32) -> u32 {\n    x.wrapping_mul(2_654_435_761).rotate_left(7)\n}\n' >> crates/core/types/src/lib.rs
build unused-fn
restore

summary() { # CHANGE
  local change="$1" binaries files keys
  binaries="$(wc -l < "$out/base.keys")"
  # Binaries by path: each line is KEY FILEHASH PATH.
  files="$(join -1 3 -2 3 <(sort -k3 "$out/base.keys") <(sort -k3 "$out/$change.keys") | awk '$3 != $5' | wc -l)"
  keys="$(join -1 3 -2 3 <(sort -k3 "$out/base.keys") <(sort -k3 "$out/$change.keys") | awk '$2 != $4' | wc -l)"
  read -r wall user sys < "$out/$change.build"
  printf '%-12s build %6.1f s wall %7.1f CPU-s   of %s binaries: file hash moved %4s, alloc key moved %4s\n' \
    "$change" "$wall" "$(echo "$user + $sys" | bc -l)" "$binaries" "$files" "$keys"
}
echo "nice $GATE_NICE, load $(cut -d' ' -f1-3 /proc/loadavg)"
for change in base-again comment-end comment-top used used-hex unused-fn; do summary "$change"; done
