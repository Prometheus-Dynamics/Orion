#!/usr/bin/env bash
# The MCU template (examples/mcu-template) builds and passes its tests in every
# configuration, and builds for every microcontroller target.
#
#   scripts/mcu-template.sh
#
# It is not a workspace member, so the workspace's own steps never see it. The C API over a
# UART (the default), over CAN, and with firmware slots are each linted and tested on the
# host; the slots test defines the port's flash hooks, so it runs alone. Then each
# configuration is built for each target that is installed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../examples/mcu-template"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-../../target}/mcu-template"

uart=()
can=(--no-default-features --features standalone,ffi-can)
slots=(--features ffi-slots)

cargo fmt --check
cargo clippy -q --all-targets "${uart[@]}" -- -D warnings
cargo clippy -q --all-targets "${can[@]}" -- -D warnings
cargo clippy -q --lib --test slots "${slots[@]}" -- -D warnings
cargo test -q "${uart[@]}"
cargo test -q "${can[@]}"
cargo test -q "${slots[@]}" --test slots

installed="$(rustup target list --installed 2>/dev/null || true)"
for target in thumbv6m-none-eabi thumbv7em-none-eabihf riscv32imc-unknown-none-elf riscv32imac-unknown-none-elf; do
    if ! grep -qx "$target" <<<"$installed"; then
        echo "    $target: not installed here, not built"
        continue
    fi
    for config in uart can slots; do
        declare -n features="$config"
        cargo build -q --release --target "$target" "${features[@]}"
        echo "    $target $config: built"
    done
done
