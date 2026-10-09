#!/usr/bin/env bash
# Builds examples/mcu-template as a bare-metal staticlib and reports the flash/RAM it costs.
#
# The staticlib is linked with rust-lld (--gc-sections, rooted at the template's C entry points)
# into a throwaway ELF, so the numbers are what a firmware actually pays for the device session,
# the wire codec, the framing layers, and the template glue, not the unused archive contents.
#
# Targets default to a Cortex-M0+ (thumbv6m), a Cortex-M4F, and RISC-V32 MCUs with and without
# atomics; override with MCU_SIZE_TARGETS="target-a target-b". Missing targets are installed with
# rustup. Variants:
#   uart        the default minimal device (no allocator), UART C entry points
#   can         the same over classic CAN
#   uart-alloc  the full-record variant with the example 8 KiB heap (only on targets with
#               compare-and-swap, which the example heap needs)
# Override with MCU_SIZE_VARIANTS="uart can".
#
# Columns: flash = .text + .rodata + .data, static RAM = .data + .bss (of which heap), and the
# part of flash that is compiler-builtins mem* routines (shared with any C runtime).
#
# MCU_SIZE_CHECK_BUDGET=1 fails if a minimal UART build exceeds its flash or RAM budget below
# (CI). MCU_SIZE_BREAKDOWN=1 prints flash per component for each build. MCU_SIZE_BUILD_ONLY=1
# only builds (no rust-lld / size needed); MCU_SIZE_ELF_DIR=<dir> keeps the linked ELFs (for
# `nm --size-sort -S` and friends).
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
template="$root_dir/examples/mcu-template"
read -r -a targets <<<"${MCU_SIZE_TARGETS:-thumbv6m-none-eabi thumbv7em-none-eabihf riscv32imc-unknown-none-elf riscv32imac-unknown-none-elf}"
read -r -a variants <<<"${MCU_SIZE_VARIANTS:-uart can uart-alloc}"

# Budgets for the minimal UART device (RX = TX = 128), about 15-20% above what it needs today.
# Raise them only together with an explanation of what the extra bytes buy.
declare -A flash_budget=(
    [thumbv6m-none-eabi]=9472
    [thumbv7em-none-eabihf]=9216
    [riscv32imc-unknown-none-elf]=11520
    [riscv32imac-unknown-none-elf]=11520
)
declare -A ram_budget=(
    [thumbv6m-none-eabi]=1056
    [thumbv7em-none-eabihf]=1056
    [riscv32imc-unknown-none-elf]=1056
    [riscv32imac-unknown-none-elf]=1056
)

sysroot="$(rustc --print sysroot)"
host="$(rustc -vV | sed -n 's/^host: //p')"
lld="$sysroot/lib/rustlib/$host/bin/rust-lld"
size_tool="${SIZE:-size}"
nm_tool="${NM:-nm}"

installed_targets="$(rustup target list --installed 2>/dev/null || true)"
for target in "${targets[@]}"; do
    if [[ -n "$installed_targets" ]] && ! grep -qx "$target" <<<"$installed_targets"; then
        rustup target add "$target"
    fi
done

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cat >"$work/link.x" <<'EOF'
MEMORY
{
  FLASH : ORIGIN = 0x00000000, LENGTH = 1M
  RAM   : ORIGIN = 0x20000000, LENGTH = 256K
}
SECTIONS
{
  .text   : { *(.text .text.*) } > FLASH
  .rodata : { *(.rodata .rodata.* .srodata .srodata.*) } > FLASH
  .data   : { *(.data .data.* .sdata .sdata.*) } > RAM AT > FLASH
  .bss (NOLOAD) : { *(.bss .bss.* .sbss .sbss.* COMMON) } > RAM
  /DISCARD/ : { *(.ARM.exidx .ARM.exidx.* .ARM.extab.* .eh_frame .comment) }
}
EOF

common_roots=(orion_init orion_publish orion_poll orion_lease_count)
declare -A variant_features=(
    [uart]="standalone,ffi-uart"
    [can]="standalone,ffi-can"
    [uart-alloc]="standalone,ffi-uart,global-heap"
)
declare -A variant_roots=(
    [uart]="orion_rx orion_tx"
    [can]="orion_can_rx orion_can_tx"
    [uart-alloc]="orion_rx orion_tx"
)

# Prints "component bytes" lines for the code and read-only data of an ELF whose flash total is
# $2; what no symbol covers (literal pools, alignment, mapping-symbol gaps) is "unattributed".
breakdown() {
    "$nm_tool" --size-sort -S -C -t d "$1" | awk -v flash="$2" '
        $3 ~ /^[TtRr]$/ && $4 !~ /^[$.]/ {
            size = $2 + 0; name = $0; sub(/^[^ ]+ [^ ]+ [^ ]+ /, "", name)
            if (name ~ /compiler_builtins|^__aeabi|^mem(cpy|set|move|cmp)$|^__(mul|div|udiv|ash|lsh|clz)/) c = "compiler-builtins"
            else if (name ~ /orion_link::device/) c = "device session"
            else if (name ~ /orion_link::wire/) c = "wire codec (views)"
            else if (name ~ /orion_link::(stream|packet|frame|crc|source|transport)/) c = "framing (COBS/CAN, CRC)"
            else if (name ~ /orion_link::message|serde|postcard|orion_control_plane|orion_core/) c = "postcard/serde + records"
            else if (name ~ /alloc::|heap|Heap/) c = "alloc + heap"
            else if (name ~ /^core::|^<core::/) c = "core"
            else if (name ~ /orion_mcu_template|^orion_/) c = "template + C API"
            else if (name ~ /^OUTLINED_FUNCTION/) c = "outlined (shared) code"
            else c = "other"
            total[c] += size
        }
        END {
            for (c in total) { printf "    %-26s %6d\n", c, total[c]; sum += total[c] }
            printf "    %-26s %6d\n", "unattributed", flash - sum
        }'
}

budget_failures=()
printf '%-30s %-10s %10s %10s %8s %10s\n' "target" "link" "flash (B)" "RAM (B)" "heap" "mem* (B)"
for target in "${targets[@]}"; do
    for variant in "${variants[@]}"; do
        if [[ "$variant" == "uart-alloc" && ( "$target" == thumbv6m-* || "$target" == riscv32imc-* ) ]]; then
            continue # the example heap needs compare-and-swap
        fi
        (cd "$template" && cargo build --locked --release --quiet --target "$target" \
            --no-default-features --features "${variant_features[$variant]}")
        archive="${CARGO_TARGET_DIR:-$template/target}/$target/release/liborion_mcu_template.a"
        if [[ "${MCU_SIZE_BUILD_ONLY:-0}" == "1" ]]; then
            echo "built $archive ($variant)"
            continue
        fi
        undefined=()
        for symbol in "${common_roots[@]}" ${variant_roots[$variant]}; do
            undefined+=("--undefined=$symbol")
        done
        elf="$work/$target-$variant.elf"
        "$lld" -flavor gnu -T "$work/link.x" --gc-sections -e orion_poll "${undefined[@]}" \
            -o "$elf" "$archive"
        if [[ -n "${MCU_SIZE_ELF_DIR:-}" ]]; then
            mkdir -p "$MCU_SIZE_ELF_DIR"
            cp "$elf" "$MCU_SIZE_ELF_DIR/"
        fi
        read -r text data bss _ < <("$size_tool" "$elf" | tail -n 1)
        flash=$((text + data))
        ram=$((data + bss))
        heap="$("$nm_tool" -S -t d "$elf" | awk '/HEAP/ { print $2 + 0; found = 1 } END { if (!found) print 0 }')"
        builtins="$(breakdown "$elf" "$flash" | awk '/compiler-builtins/ { print $2 }')"
        printf '%-30s %-10s %10d %10d %8d %10d\n' "$target" "$variant" "$flash" "$ram" "$heap" "${builtins:-0}"
        if [[ "${MCU_SIZE_BREAKDOWN:-0}" == "1" ]]; then
            breakdown "$elf" "$flash" | sort
        fi
        if [[ "$variant" == "uart" && -n "${flash_budget[$target]:-}" ]]; then
            if ((flash > flash_budget[$target])); then
                budget_failures+=("$target uart flash $flash B > budget ${flash_budget[$target]} B")
            fi
            if ((ram > ram_budget[$target])); then
                budget_failures+=("$target uart RAM $ram B > budget ${ram_budget[$target]} B")
            fi
        fi
    done
done

if [[ "${MCU_SIZE_CHECK_BUDGET:-0}" == "1" ]]; then
    if ((${#budget_failures[@]} > 0)); then
        printf 'error: MCU size budget exceeded:\n' >&2
        printf '  %s\n' "${budget_failures[@]}" >&2
        exit 1
    fi
    echo "MCU size budgets met (minimal UART device)."
fi
