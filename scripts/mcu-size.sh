#!/usr/bin/env bash
# Builds examples/mcu-template as a bare-metal staticlib and reports the flash/RAM it costs.
#
# The staticlib is linked with rust-lld (--gc-sections, rooted at the template's C entry points)
# into a throwaway ELF, so the numbers are what a firmware actually pays for the device session,
# postcard, the framing layers, the Orion record types it touches, and the example heap — not the
# unused archive contents.
#
# Targets default to a Cortex-M4F and a RISC-V32 MCU; override with
# MCU_SIZE_TARGETS="target-a target-b". Variants: the UART (COBS) and the CAN entry points.
# Set MCU_SIZE_BUILD_ONLY=1 to only build (no rust-lld / size needed), and MCU_SIZE_ELF_DIR=<dir>
# to keep the linked ELFs (for `nm --size-sort -S` and friends).
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
template="$root_dir/examples/mcu-template"
read -r -a targets <<<"${MCU_SIZE_TARGETS:-thumbv7em-none-eabihf riscv32imac-unknown-none-elf}"

sysroot="$(rustc --print sysroot)"
host="$(rustc -vV | sed -n 's/^host: //p')"
lld="$sysroot/lib/rustlib/$host/bin/rust-lld"
size_tool="${SIZE:-size}"

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
declare -A variant_features=([uart]="ffi-uart" [can]="ffi-can")
declare -A variant_roots=([uart]="orion_rx orion_tx" [can]="orion_can_rx orion_can_tx")

printf '%-30s %-6s %10s %10s %14s\n' "target" "link" "flash (B)" "RAM (B)" "RAM excl. heap"
for target in "${targets[@]}"; do
    for variant in uart can; do
        (cd "$template" && cargo build --locked --release --quiet --target "$target" \
            --no-default-features --features "standalone,${variant_features[$variant]}")
        archive="$template/target/$target/release/liborion_mcu_template.a"
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
        printf '%-30s %-6s %10d %10d %14d\n' "$target" "$variant" "$flash" "$ram" "$((ram - 8192))"
    done
done
