#!/usr/bin/env bash
# Verifies the `no_std` + `alloc` build of Orion's model crates.
#
# 1. Builds every no_std crate (and the `orion` facade's model-level features) with
#    `--no-default-features` for bare-metal targets, one crate per cargo invocation so workspace
#    feature unification cannot hide a crate that silently needs `std`.
# 2. Runs clippy on each crate without `std` (host target).
# 3. Runs the host-side tests of each crate without `std`, including the canonical-encoding test
#    that proves no_std builds produce the same rkyv bytes as std builds.
#
# Targets default to a Cortex-M4F, a RISC-V32 MCU, and a Cortex-M33; override with
# NO_STD_TARGETS="target-a target-b". Install them with `rustup target add <target>`.
# Set NO_STD_SKIP_HOST=1 to only run the cross builds.
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

crates=(
    orion-core
    orion-data-plane
    orion-control-plane
    orion-auth
    orion-runtime
    orion-cluster
)
facade_features="core,auth,control-plane,data-plane,runtime,cluster,macros"
read -r -a targets <<<"${NO_STD_TARGETS:-thumbv7em-none-eabihf riscv32imac-unknown-none-elf thumbv8m.main-none-eabihf}"

installed_targets="$(rustup target list --installed 2>/dev/null || true)"
for target in "${targets[@]}"; do
    if [[ -n "$installed_targets" ]] && ! grep -qx "$target" <<<"$installed_targets"; then
        echo "error: target $target is not installed (rustup target add $target)" >&2
        exit 1
    fi
done

for target in "${targets[@]}"; do
    for crate in "${crates[@]}"; do
        echo "==> no_std build: $crate ($target)"
        cargo build --locked -p "$crate" --no-default-features --target "$target"
    done
    echo "==> no_std build: orion facade [$facade_features] ($target)"
    cargo build --locked -p orion --no-default-features --features "$facade_features" --target "$target"
done

if [[ "${NO_STD_SKIP_HOST:-0}" == "1" ]]; then
    exit 0
fi

for crate in "${crates[@]}"; do
    echo "==> no_std clippy: $crate"
    cargo clippy --locked -p "$crate" --no-default-features --all-targets -- -D warnings
done

for crate in "${crates[@]}"; do
    echo "==> no_std host tests: $crate"
    cargo test --locked -p "$crate" --no-default-features
done

echo "no_std checks passed for: ${crates[*]} (targets: ${targets[*]})"
