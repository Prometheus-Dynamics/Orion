#!/usr/bin/env bash
# Size gate for a whole device firmware: builds examples/stm32g4-device (an STM32G4 that speaks
# the Orion link over FDCAN or UART and publishes one record and one stream) and checks it
# against the committed baseline in examples/stm32g4-device/size-baseline.txt.
#
# Per variant:
#   flash       .vector_table + .text + .rodata + .data (what is written to the chip)
#   static RAM  .data + .bss
#   stack       the worst-case stack depth from `-Z emit-stack-sizes` and the call graph
#               (scripts/stack-depth.py), main plus the deepest exception with its frame. The
#               firmware also reports the measured high-water mark as `stack.high_water_bytes`.
#
# A variant fails if flash, static RAM or stack exceeds its baseline by more than the tolerance
# below; a change that grows the firmware updates the baseline in the same commit (run with
# DEVICE_SIZE_UPDATE=1), so every growth is a visible line in the diff. Shrinking is reported and
# never fails. DEVICE_SIZE_BREAKDOWN=1 prints flash by source component (an extra build with line
# info, which does not change the code). The stack bound needs a nightly toolchain with the
# thumbv7em-none-eabihf target: STACK_TOOLCHAIN, by default the pinned nightly-2026-10-15, so the
# bound does not move with nightly (CI installs the same one). Without it the stack column is
# skipped with a warning.
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
crate="$root_dir/examples/stm32g4-device"
baseline="$crate/size-baseline.txt"
target=thumbv7em-none-eabihf
target_dir="${CARGO_TARGET_DIR:-$root_dir/target}/device-size"
stack_toolchain="${STACK_TOOLCHAIN:-nightly-2026-10-15}"
flash_tolerance=64
ram_tolerance=32

# variant name -> cargo feature flags
declare -A features=(
    [can]="--no-default-features --features link-can,small-mem"
    [uart]="--no-default-features --features link-uart,small-mem"
    [can-hw-crc]="--no-default-features --features link-can,small-mem,hw-crc"
    [can-builtin-mem]="--no-default-features --features link-can"
)
variants=(can uart can-hw-crc can-builtin-mem)

section() { awk -v s="$2" '$1 == s { print $2; found = 1 } END { if (!found) print 0 }' <<<"$1"; }

have_stack=1
if ! rustup run "$stack_toolchain" rustc --print target-libdir --target "$target" >/dev/null 2>&1 ||
    [[ ! -d "$(rustup run "$stack_toolchain" rustc --print target-libdir --target "$target" 2>/dev/null)" ]]; then
    echo "warning: no $stack_toolchain toolchain with $target; skipping the stack bound" >&2
    have_stack=0
fi

declare -A base_flash base_ram base_stack
if [[ -f "$baseline" ]]; then
    while read -r name flash ram stack; do
        [[ -z "$name" || "$name" == \#* ]] && continue
        base_flash[$name]=$flash base_ram[$name]=$ram base_stack[$name]=$stack
    done <"$baseline"
fi

fail=0
results=()
printf '%-16s %8s %8s %8s %8s\n' variant flash ram stack total-ram
for v in "${variants[@]}"; do
    # shellcheck disable=SC2086
    (cd "$crate" && cargo build --locked --release --quiet --target "$target" \
        --target-dir "$target_dir/stable" ${features[$v]})
    elf="$target_dir/stable/$target/release/orion-stm32g4-device"
    sizes="$(size -A "$elf")"
    data=$(section "$sizes" .data)
    flash=$(($(section "$sizes" .vector_table) + $(section "$sizes" .text) + $(section "$sizes" .rodata) + data))
    ram=$((data + $(section "$sizes" .bss)))
    stack=-
    if ((have_stack)); then
        # shellcheck disable=SC2086
        (cd "$crate" && RUSTFLAGS="-C link-arg=-Tlink.x -Z emit-stack-sizes" \
            cargo "+$stack_toolchain" build --release --quiet --target "$target" \
            --target-dir "$target_dir/stack" ${features[$v]})
        stack="$(python3 "$root_dir/scripts/stack-depth.py" \
            "$target_dir/stack/$target/release/orion-stm32g4-device" |
            awk '/^worst case:/ { print $3 }')"
    fi
    total=-
    [[ "$stack" != - ]] && total=$((ram + stack))
    printf '%-16s %8d %8d %8s %8s' "$v" "$flash" "$ram" "$stack" "$total"
    if [[ -n "${base_flash[$v]:-}" ]]; then
        printf '   (baseline %d / %d / %s)' "${base_flash[$v]}" "${base_ram[$v]}" "${base_stack[$v]}"
        if ((flash > base_flash[$v] + flash_tolerance)); then
            printf '  FLASH +%d' $((flash - base_flash[$v])); fail=1
        fi
        if ((ram > base_ram[$v] + ram_tolerance)); then
            printf '  RAM +%d' $((ram - base_ram[$v])); fail=1
        fi
        if [[ "$stack" != - && "${base_stack[$v]}" != - ]] && ((stack > base_stack[$v] + ram_tolerance)); then
            printf '  STACK +%d' $((stack - base_stack[$v])); fail=1
        fi
    elif [[ -z "${DEVICE_SIZE_UPDATE:-}" ]]; then
        printf '   (no baseline)'; fail=1
    fi
    printf '\n'
    results+=("$v $flash $ram $stack")

    if [[ -n "${DEVICE_SIZE_BREAKDOWN:-}" ]]; then
        # shellcheck disable=SC2086
        (cd "$crate" && CARGO_PROFILE_RELEASE_DEBUG=2 cargo build --locked --release --quiet \
            --target "$target" --target-dir "$target_dir/lines" ${features[$v]})
        python3 "$root_dir/scripts/flash-by-source.py" \
            "$target_dir/lines/$target/release/orion-stm32g4-device" | sed 's/^/    /'
    fi
done

if [[ -n "${DEVICE_SIZE_UPDATE:-}" ]]; then
    {
        echo "# variant flash static-ram stack (bytes); written by DEVICE_SIZE_UPDATE=1 scripts/device-size.sh"
        printf '%s\n' "${results[@]}"
    } >"$baseline"
    echo "updated $baseline"
    exit 0
fi
if ((fail)); then
    echo "error: the device firmware grew past its baseline; if the growth is intended, run" >&2
    echo "  DEVICE_SIZE_UPDATE=1 scripts/device-size.sh and commit the baseline with the reason" >&2
    exit 1
fi
