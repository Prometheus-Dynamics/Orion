#!/usr/bin/env bash
# What the device side of the wire costs a microcontroller, held to a budget.
#
#   scripts/device-size.sh
#
# Builds crates/device/probe, the smallest firmware that is still one, for each
# microcontroller target, and links it with only what its entry points reach: the numbers
# are what a firmware pays, not what the archive holds. Flash is code and constants and
# initial values; RAM is what is static. A stack is the firmware's own.
#
# DEVICE_SIZE_TARGETS and DEVICE_SIZE_VARIANTS (stream, can, topics, actions, firmware)
# narrow it. Each variant past can is the stream device with one more layer. A change that goes
# past a budget either wins the bytes back or raises the budget here, with the reason.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

read -r -a targets <<<"${DEVICE_SIZE_TARGETS:-thumbv6m-none-eabi thumbv7em-none-eabihf riscv32imc-unknown-none-elf riscv32imac-unknown-none-elf}"
read -r -a variants <<<"${DEVICE_SIZE_VARIANTS:-stream can topics actions firmware}"

# Bytes: flash and RAM for each target and variant, a tenth above what was measured.
# Measured: v6m 6262/512 and 6438/544, v7em 5758/512 and 5962/544, riscv 6742/512 and
# 6840/544 (stream/can); with one topic, v6m 8090, v7em 7510, riscv 8946, all 544 RAM;
# with one action, v6m 7862, v7em 7430, riscv 8698, all 552 RAM; with firmware (SHA-256,
# two slots, the boot record and the bootloader's choice), v6m 12318, v7em 12606, riscv
# 13892, all 712 RAM.
# About 1.4 KB of the flash is memmove from compiler_builtins.
declare -A budget=(
    [thumbv6m-none-eabi/stream]="6900 570"
    [thumbv7em-none-eabihf/stream]="6340 570"
    [riscv32imc-unknown-none-elf/stream]="7420 570"
    [riscv32imac-unknown-none-elf/stream]="7420 570"
    [thumbv6m-none-eabi/topics]="8900 600"
    [thumbv6m-none-eabi/actions]="8650 610"
    [thumbv6m-none-eabi/firmware]="13550 790"
    [thumbv6m-none-eabi/can]="7090 600"
    [thumbv7em-none-eabihf/topics]="8260 600"
    [thumbv7em-none-eabihf/actions]="8180 610"
    [thumbv7em-none-eabihf/firmware]="13870 790"
    [thumbv7em-none-eabihf/can]="6560 600"
    [riscv32imc-unknown-none-elf/topics]="9840 600"
    [riscv32imc-unknown-none-elf/actions]="9570 610"
    [riscv32imc-unknown-none-elf/firmware]="15280 790"
    [riscv32imc-unknown-none-elf/can]="7530 600"
    [riscv32imac-unknown-none-elf/topics]="9840 600"
    [riscv32imac-unknown-none-elf/actions]="9570 610"
    [riscv32imac-unknown-none-elf/firmware]="15280 790"
    [riscv32imac-unknown-none-elf/can]="7530 600"
)

sysroot="$(rustc --print sysroot)"
host="$(rustc -vV | sed -n 's/^host: //p')"
tools="$sysroot/lib/rustlib/$host/bin"
if [ ! -x "$tools/rust-lld" ] || [ ! -x "$tools/llvm-size" ]; then
    echo "    not measured: rust-lld and llvm-size are not installed (rustup component add llvm-tools)"
    exit 0
fi
installed="$(rustup target list --installed 2>/dev/null || true)"
target_dir="${CARGO_TARGET_DIR:-target}"
work="$target_dir/device-size"
mkdir -p "$work"
failures=0
measured=0

for target in "${targets[@]}"; do
    if ! grep -qx "$target" <<<"$installed"; then
        echo "    $target: not installed here, not measured"
        continue
    fi
    for variant in "${variants[@]}"; do
        features=()
        [ "$variant" = stream ] || features=(--features "$variant")
        cargo build --quiet --profile device -p orion-device-probe --target "$target" "${features[@]}"
        elf="$work/$target-$variant.elf"
        # Only what the entry points reach is kept, as a firmware's own link keeps it.
        "$tools/rust-lld" -flavor gnu --gc-sections -e orion_probe_poll \
            -u orion_probe_poll -u orion_probe_receive -u orion_probe_transmit -u orion_probe_publish -u orion_probe_boot \
            -o "$elf" "$target_dir/$target/device/liborion_device_probe.a"
        read -r text data bss < <("$tools/llvm-size" -B "$elf" | awk 'NR==2 {print $1, $2, $3}')
        flash=$((text + data))
        ram=$((data + bss))
        read -r flash_limit ram_limit <<<"${budget[$target/$variant]}"
        measured=$((measured + 1))
        if [ "$flash" -gt "$flash_limit" ] || [ "$ram" -gt "$ram_limit" ]; then
            echo "    FAIL: $target $variant: flash $flash (budget $flash_limit), RAM $ram (budget $ram_limit)"
            failures=$((failures + 1))
        else
            echo "    $target $variant: flash $flash (budget $flash_limit), RAM $ram (budget $ram_limit)"
        fi
    done
done

[ "$measured" -gt 0 ] || echo "    no microcontroller target is installed here: nothing was measured"

# A whole firmware: examples/stm32g4-device, held to its committed baseline
# (examples/stm32g4-device/size-baseline.txt) per variant.
#
#   flash       .vector_table + .text + .rodata + .data (what is written to the chip)
#   static RAM  .data + .bss
#   stack       the worst case from `-Z emit-stack-sizes` and the call graph
#               (scripts/stack-depth.py): main plus the deepest exception with its frame. The
#               firmware publishes the measured high-water mark as topic `stack`.
#
# A variant fails past its baseline by more than the tolerance; a change that grows the firmware
# updates the baseline in the same commit (DEVICE_SIZE_UPDATE=1), so every growth is a line in
# the diff. The stack bound needs STACK_TOOLCHAIN (pinned, so the bound does not move with
# nightly) with the target; without it the column is skipped.
firmware="$PWD/examples/stm32g4-device"
baseline="$firmware/size-baseline.txt"
g4=thumbv7em-none-eabihf
g4_dir="$(cd "$target_dir" && pwd)/stm32g4-size"
stack_toolchain="${STACK_TOOLCHAIN:-nightly-2026-10-15}"
flash_tolerance=64
ram_tolerance=32
declare -A g4_features=(
    [can]="link-can,small-mem"
    [uart]="link-uart,small-mem"
    [can-hw-crc]="link-can,small-mem,hw-crc"
    [can-builtin-mem]="link-can"
)
g4_variants=(can uart can-hw-crc can-builtin-mem)

if ! grep -qx "$g4" <<<"$installed"; then
    echo "    stm32g4 firmware: $g4 not installed here, not measured"
    [ "$failures" -eq 0 ]
    exit
fi
section() { awk -v s="$2" '$1 == s { print $2; found = 1 } END { if (!found) print 0 }' <<<"$1"; }
have_stack=1
if ! libdir="$(rustup run "$stack_toolchain" rustc --print target-libdir --target "$g4" 2>/dev/null)" ||
    [ ! -d "$libdir" ]; then
    echo "    stm32g4 firmware: no $stack_toolchain with $g4, stack bound not measured"
    have_stack=0
fi
declare -A base
if [ -f "$baseline" ]; then
    while read -r name flash ram stack; do
        [[ -z "$name" || "$name" == \#* ]] && continue
        base[$name]="$flash $ram $stack"
    done <"$baseline"
fi
# Its own lints: it is not a workspace member, so the workspace's steps never see it.
(cd "$firmware" && cargo fmt --check)
for v in can uart; do
    (cd "$firmware" && cargo clippy --quiet --release --target "$g4" --target-dir "$g4_dir/stable" \
        --no-default-features --features "${g4_features[$v]},hw-crc" -- -D warnings)
done
results=()
for v in "${g4_variants[@]}"; do
    (cd "$firmware" && cargo build --quiet --release --target "$g4" --target-dir "$g4_dir/stable" \
        --no-default-features --features "${g4_features[$v]}")
    sizes="$(size -A "$g4_dir/stable/$g4/release/orion-stm32g4-device")"
    data=$(section "$sizes" .data)
    flash=$(($(section "$sizes" .vector_table) + $(section "$sizes" .text) + $(section "$sizes" .rodata) + data))
    ram=$((data + $(section "$sizes" .bss)))
    stack=-
    if ((have_stack)); then
        (cd "$firmware" && RUSTFLAGS="-C link-arg=-Tlink.x -Z emit-stack-sizes" \
            cargo "+$stack_toolchain" build --quiet --release --target "$g4" \
            --target-dir "$g4_dir/stack" --no-default-features --features "${g4_features[$v]}")
        stack="$(python3 scripts/stack-depth.py "$g4_dir/stack/$g4/release/orion-stm32g4-device" |
            awk '/^worst case:/ { print $3 }')"
    fi
    results+=("$v $flash $ram $stack")
    line="    stm32g4 $v: flash $flash, static RAM $ram, stack $stack"
    if [ -n "${base[$v]:-}" ]; then
        read -r base_flash base_ram base_stack <<<"${base[$v]}"
        line+=" (baseline $base_flash / $base_ram / $base_stack)"
        if ((flash > base_flash + flash_tolerance)); then
            line+="  FAIL: flash +$((flash - base_flash))"
            failures=$((failures + 1))
        fi
        if ((ram > base_ram + ram_tolerance)); then
            line+="  FAIL: RAM +$((ram - base_ram))"
            failures=$((failures + 1))
        fi
        if [[ "$stack" != - && "$base_stack" != - ]] && ((stack > base_stack + ram_tolerance)); then
            line+="  FAIL: stack +$((stack - base_stack))"
            failures=$((failures + 1))
        fi
    elif [ -z "${DEVICE_SIZE_UPDATE:-}" ]; then
        line+="  FAIL: no baseline"
        failures=$((failures + 1))
    fi
    echo "$line"
done

if [ -n "${DEVICE_SIZE_UPDATE:-}" ]; then
    {
        echo "# variant flash static-ram stack (bytes); written by DEVICE_SIZE_UPDATE=1 scripts/device-size.sh"
        printf '%s\n' "${results[@]}"
    } >"$baseline"
    echo "    updated $baseline"
fi
if [ "$failures" -ne 0 ]; then
    echo "    a size grew past its budget or baseline: win the bytes back, or raise it with the reason" >&2
    echo "    (the firmware's: DEVICE_SIZE_UPDATE=1 scripts/device-size.sh)" >&2
fi
[ "$failures" -eq 0 ]
