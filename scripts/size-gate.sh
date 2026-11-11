#!/usr/bin/env bash
# The size gate: `orion-recovery`, the comm core alone, must stay small enough for an
# initramfs. Built as it ships (profile `service`) and held to a number of bytes.
#
#   scripts/size-gate.sh
#
# Always gated: the build for this machine. Gated as well where it can be built: the
# static aarch64 musl file that goes on a board. That needs the Rust target and a C
# compiler for it, named in CC_aarch64_unknown_linux_musl (zig cc does).
#
# A change that pushes a size past its limit either wins the bytes back or raises the
# limit here, in the same commit, with the reason.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Bytes. Each is what was measured when it was last set, and a tenth more.
#   this machine (x86_64 gnu):       measured 2,973,432
#   aarch64-unknown-linux-musl:      measured 2,634,088
HOST_LIMIT=3270000
BOARD_LIMIT=2900000

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

cargo build --quiet --profile service -p orion-recovery-bin
gate "orion-recovery for this machine" "$target_dir/service/orion-recovery" "$HOST_LIMIT"

board=aarch64-unknown-linux-musl
if rustup target list --installed 2>/dev/null | grep -qx "$board" && [ -n "${CC_aarch64_unknown_linux_musl:-}" ]; then
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER="${CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER:-rust-lld}" \
        cargo build --quiet --profile service --target "$board" -p orion-recovery-bin
    file="$target_dir/$board/service/orion-recovery"
    gate "orion-recovery for a board ($board, static)" "$file" "$BOARD_LIMIT"
    if command -v file > /dev/null && ! file "$file" | grep -q "statically linked"; then
        echo "    FAIL: the board's file is not statically linked"
        failures=$((failures + 1))
    fi
else
    echo "    the board's file was not built here: no $board target, or no C compiler named for it"
fi

[ "$failures" -eq 0 ]
