#!/usr/bin/env bash
# The size gate: each form Orion ships in, built as it ships (profile `service`) and held to
# a number of bytes.
#
# - `orion-recovery`, the comm core alone, small enough for an initramfs;
# - `orion`, the comm core, node logic and the command line: what a board installs;
# - `liborion_c.so`, the C library programs load.
#
# The microcontroller form has its own gate, `scripts/device-size.sh`.
#
#   scripts/size-gate.sh
#
# Each is built on its own: built together, cargo gives every one the features of all, and
# `orion-recovery` would carry what `orion` has.
#
# Always gated: the builds for this machine. Gated as well where they can be built: the static
# musl files that go on a board, for each board target installed. Those need a C compiler for
# the target (the SSH way in brings `ring`): CC_<triple> if set, else scripts/zigcc/<triple> when
# `zig` is on PATH, and that zig must be 0.15.2 (another is refused, with what to do). armv7
# and riscv64 build and are gated, and have not run on a board.
#
# A change that pushes a size past its limit either wins the bytes back or raises the
# limit here, in the same commit, with the reason.
set -euo pipefail
# Never incremental, whatever the caller set: the bytes measured are of a build made whole, as it ships.
export CARGO_INCREMENTAL=0
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Bytes. Each is what was measured when it was last set, and a small margin: 20-30 KB, rounded
# to 10,000. A limit is raised only with the reason, in docs/prs, and what was cut first.
#                         this machine (x86_64 gnu), C40 after its cut
#   orion-recovery        3,255,208
#   orion                 6,560,752   (5,936,216 at I6: +624,536, a tenth; by crate and
#                                      symbol in docs/prs/C41.md)
#   liborion_c.so           735,656
RECOVERY_HOST=3270000
ORION_HOST=6590000
CAPI_HOST=740000
# A board's files, static musl. aarch64, the one run on a board, as measured at C31 (files the
# settings name, paths and commands, in both: +40,400 and +37,360 since C30, after a cut;
# docs/prs/C31.md); the others as at C41, until they run on one:
#                                   orion-recovery   orion
#   aarch64-unknown-linux-musl      2,733,120        5,308,480
#   armv7-unknown-linux-musleabihf  2,782,344        5,327,160   (never run on a board)
#   riscv64gc-unknown-linux-musl    3,017,416        5,668,376   (never run on a board)
BOARDS=(
    "aarch64-unknown-linux-musl     2760000          5330000"
    "armv7-unknown-linux-musleabihf 2810000          5350000"
    "riscv64gc-unknown-linux-musl   3040000          5690000"
)

target_dir="${CARGO_TARGET_DIR:-target}"
failures=0

# Each build has a target directory of its own, under this one, so all of them run at once: a
# fat-LTO link is one thread, and cargo holds a target directory for one build at a time. Each
# says what it measured in a file of its own, read back in order once all have ended.
lanes="$target_dir/size-gate"
results=$(mktemp -d)
trap 'rm -rf "$results"' EXIT
pids=()

# build <name> <package> <file> <label> <limit> [target]: builds <package> for [target] (this
# machine without one) and holds <file> to <limit> bytes; a board's file must be static.
build() {
    local name="$1" package="$2" file="$3" label="$4" limit="$5" target="${6:-}"
    local dir="$lanes/$name" out="$results/$name"
    (
        if [ -n "$target" ]; then
            CARGO_TARGET_DIR="$dir" cargo build --quiet --locked --profile service \
                --target "$target" -p "$package" || exit 1
            file="$dir/$target/service/$file"
        else
            CARGO_TARGET_DIR="$dir" cargo build --quiet --profile service -p "$package" || exit 1
            file="$dir/service/$file"
        fi
        size=$(stat -c %s "$file")
        failed=0
        if [ "$size" -gt "$limit" ]; then
            echo "    FAIL: $label is $size bytes, over its limit of $limit"
            failed=1
        else
            echo "    $label: $size bytes (limit $limit)"
        fi
        if [ -n "$target" ] && command -v file > /dev/null && ! file "$file" | grep -q "statically linked"; then
            echo "    FAIL: $file is not statically linked"
            failed=1
        fi
        exit "$failed"
    ) > "$out" 2>&1 &
    pids+=("$!:$name")
}

build recovery-host orion-recovery-bin orion-recovery "orion-recovery for this machine" "$RECOVERY_HOST"
build orion-host orion-bin orion "orion for this machine" "$ORION_HOST"
build capi-host orion-capi liborion_c.so "liborion_c.so for this machine" "$CAPI_HOST"

installed=$(rustup target list --installed 2>/dev/null || true)

# The board files are built and measured with this zig and no other: another version builds C
# differently, and the numbers above stop meaning anything.
ZIG_VERSION=0.15.2
zig_checked=""
check_zig() {
    [ -n "$zig_checked" ] && return 0
    local found
    found=$(zig version 2>/dev/null || echo "none")
    if [ "$found" != "$ZIG_VERSION" ]; then
        echo "    FAIL: zig $found is on PATH; the board files are built with zig $ZIG_VERSION only." >&2
        echo "          Install zig $ZIG_VERSION, or set CC_<target> to another C compiler for that target." >&2
        failures=$((failures + 1))
        return 1
    fi
    zig_checked=yes
}
for line in "${BOARDS[@]}"; do
    read -r board recovery_limit orion_limit <<< "$line"
    triple=$(echo "$board" | tr 'a-z-' 'A-Z_')
    cc_var="CC_$(echo "$board" | tr '-' '_')"
    if ! grep -qx "$board" <<< "$installed"; then
        echo "    $board: not built here, no Rust target"
        continue
    fi
    if [ -z "${!cc_var:-}" ]; then
        if command -v zig > /dev/null; then
            check_zig || break
            export "$cc_var=$PWD/scripts/zigcc/$board"
            export "AR_$(echo "$board" | tr '-' '_')=$PWD/scripts/zigcc/$board-ar"
        else
            echo "    $board: not built here, no C compiler ($cc_var, or zig on PATH)"
            continue
        fi
    fi
    linker="CARGO_TARGET_${triple}_LINKER"
    export "$linker=${!linker:-rust-lld}"
    build "recovery-$board" orion-recovery-bin orion-recovery \
        "orion-recovery for a board ($board, static)" "$recovery_limit" "$board"
    build "orion-$board" orion-bin orion "orion for a board ($board, static)" "$orion_limit" "$board"
done

for entry in "${pids[@]}"; do
    pid="${entry%%:*}" name="${entry#*:}"
    if ! wait "$pid"; then
        failures=$((failures + 1))
    fi
    cat "$results/$name"
done

[ "$failures" -eq 0 ]
