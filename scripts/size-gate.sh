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
# `zig` is on PATH. armv7 and riscv64 build and are gated, and have not run on a board.
#
# A change that pushes a size past its limit either wins the bytes back or raises the
# limit here, in the same commit, with the reason.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Bytes. Each is what was measured when it was last set, and a tenth more.
#                         this machine (x86_64 gnu)
#   orion-recovery        2,973,432
#   orion                 5,936,216
#   liborion_c.so           670,696
RECOVERY_HOST=3270000
ORION_HOST=6530000
CAPI_HOST=740000
# A board's files, static musl, as measured (orion-recovery with the USB link, C13/J6):
#                                   orion-recovery   orion
#   aarch64-unknown-linux-musl      2,698,416        5,237,912
#   armv7-unknown-linux-musleabihf  2,606,688        5,020,784   (never run on a board)
#   riscv64gc-unknown-linux-musl    2,821,472        5,351,312   (never run on a board)
BOARDS=(
    "aarch64-unknown-linux-musl     2970000          5770000"
    "armv7-unknown-linux-musleabihf 2870000          5530000"
    "riscv64gc-unknown-linux-musl   3110000          5890000"
)

target_dir="${CARGO_TARGET_DIR:-target}"
failures=0

gate() {
    local label="$1" file="$2" limit="$3" size
    size=$(stat -c %s "$file")
    if [ "$size" -gt "$limit" ]; then
        echo "    FAIL: $label is $size bytes, over its limit of $limit"
        failures=$((failures + 1))
    else
        echo "    $label: $size bytes (limit $limit)"
    fi
}

static() {
    local file="$1"
    if command -v file > /dev/null && ! file "$file" | grep -q "statically linked"; then
        echo "    FAIL: $file is not statically linked"
        failures=$((failures + 1))
    fi
}

cargo build --quiet --profile service -p orion-recovery-bin
gate "orion-recovery for this machine" "$target_dir/service/orion-recovery" "$RECOVERY_HOST"
cargo build --quiet --profile service -p orion-bin
gate "orion for this machine" "$target_dir/service/orion" "$ORION_HOST"
cargo build --quiet --profile service -p orion-capi
gate "liborion_c.so for this machine" "$target_dir/service/liborion_c.so" "$CAPI_HOST"

installed=$(rustup target list --installed 2>/dev/null || true)
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
            export "$cc_var=$PWD/scripts/zigcc/$board"
            export "AR_$(echo "$board" | tr '-' '_')=$PWD/scripts/zigcc/$board-ar"
        else
            echo "    $board: not built here, no C compiler ($cc_var, or zig on PATH)"
            continue
        fi
    fi
    linker="CARGO_TARGET_${triple}_LINKER"
    export "$linker=${!linker:-rust-lld}"
    cargo build --quiet --locked --profile service --target "$board" -p orion-recovery-bin
    gate "orion-recovery for a board ($board, static)" "$target_dir/$board/service/orion-recovery" "$recovery_limit"
    static "$target_dir/$board/service/orion-recovery"
    cargo build --quiet --locked --profile service --target "$board" -p orion-bin
    gate "orion for a board ($board, static)" "$target_dir/$board/service/orion" "$orion_limit"
    static "$target_dir/$board/service/orion"
done

[ "$failures" -eq 0 ]
