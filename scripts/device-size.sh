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
# DEVICE_SIZE_TARGETS and DEVICE_SIZE_VARIANTS (stream, can, topics, actions) narrow it. Each
# variant past stream is the stream device with one more layer. A change that goes
# past a budget either wins the bytes back or raises the budget here, with the reason.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

read -r -a targets <<<"${DEVICE_SIZE_TARGETS:-thumbv6m-none-eabi thumbv7em-none-eabihf riscv32imc-unknown-none-elf riscv32imac-unknown-none-elf}"
read -r -a variants <<<"${DEVICE_SIZE_VARIANTS:-stream can topics actions}"

# Bytes: flash and RAM for each target and variant, a tenth above what was measured.
# Measured: v6m 6262/512 and 6438/544, v7em 5758/512 and 5962/544, riscv 6742/512 and
# 6840/544 (stream/can); with one topic, v6m 8090, v7em 7510, riscv 8946, all 544 RAM;
# with one action, v6m 7862, v7em 7430, riscv 8698, all 552 RAM.
# About 1.4 KB of the flash is memmove from compiler_builtins.
declare -A budget=(
    [thumbv6m-none-eabi/stream]="6900 570"
    [thumbv7em-none-eabihf/stream]="6340 570"
    [riscv32imc-unknown-none-elf/stream]="7420 570"
    [riscv32imac-unknown-none-elf/stream]="7420 570"
    [thumbv6m-none-eabi/topics]="8900 600"
    [thumbv6m-none-eabi/actions]="8650 610"
    [thumbv6m-none-eabi/can]="7090 600"
    [thumbv7em-none-eabihf/topics]="8260 600"
    [thumbv7em-none-eabihf/actions]="8180 610"
    [thumbv7em-none-eabihf/can]="6560 600"
    [riscv32imc-unknown-none-elf/topics]="9840 600"
    [riscv32imc-unknown-none-elf/actions]="9570 610"
    [riscv32imc-unknown-none-elf/can]="7530 600"
    [riscv32imac-unknown-none-elf/topics]="9840 600"
    [riscv32imac-unknown-none-elf/actions]="9570 610"
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
            -u orion_probe_poll -u orion_probe_receive -u orion_probe_transmit -u orion_probe_publish \
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
[ "$failures" -eq 0 ]
