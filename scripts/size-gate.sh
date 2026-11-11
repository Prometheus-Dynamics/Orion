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
# Always gated: the builds for this machine. Gated as well where they can be built: the
# static aarch64 musl files that go on a board. That needs the Rust target and a C compiler
# for it, named in CC_aarch64_unknown_linux_musl (zig cc does).
#
# A change that pushes a size past its limit either wins the bytes back or raises the
# limit here, in the same commit, with the reason.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Bytes. Each is what was measured when it was last set, and a tenth more.
#                         this machine (x86_64 gnu)    a board (aarch64 musl, static)
#   orion-recovery        2,973,432                    2,634,312
#   orion                 5,936,216                    5,032,768
#   liborion_c.so           670,696                    -
RECOVERY_HOST=3270000
RECOVERY_BOARD=2900000
ORION_HOST=6530000
ORION_BOARD=5540000
CAPI_HOST=740000

target_dir="${CARGO_TARGET_DIR:-target}"
board=aarch64-unknown-linux-musl
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

if rustup target list --installed 2>/dev/null | grep -qx "$board" && [ -n "${CC_aarch64_unknown_linux_musl:-}" ]; then
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER="${CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER:-rust-lld}"
    cargo build --quiet --profile service --target "$board" -p orion-recovery-bin
    gate "orion-recovery for a board ($board, static)" "$target_dir/$board/service/orion-recovery" "$RECOVERY_BOARD"
    static "$target_dir/$board/service/orion-recovery"
    cargo build --quiet --profile service --target "$board" -p orion-bin
    gate "orion for a board ($board, static)" "$target_dir/$board/service/orion" "$ORION_BOARD"
    static "$target_dir/$board/service/orion"
else
    echo "    the board's files were not built here: no $board target, or no C compiler named for it"
fi

[ "$failures" -eq 0 ]
