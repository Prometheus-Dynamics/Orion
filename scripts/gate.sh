#!/usr/bin/env bash
# The check every PR into next must pass. CI runs exactly this.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"

step() {
  local label="$1" start
  shift
  echo "==> $label"
  start=$SECONDS
  "$@"
  echo "    $label: $((SECONDS - start)) s"
}

# The workspace glob matches nothing until the first crate lands.
if compgen -G "crates/*/*/Cargo.toml" > /dev/null; then
  step format cargo fmt --all --check
  step clippy cargo clippy --workspace --all-targets --all-features -- -D warnings
  step tests cargo test --workspace --all-features
  step "the check compiled out" cargo test -q -p orion-check --no-default-features --lib

  # A crate that must build without std says so in its manifest:
  # [package.metadata.orion]
  # no_std = true
  mapfile -t no_std_crates < <(grep -l '^no_std = true' crates/*/*/Cargo.toml | xargs -r -n1 dirname | xargs -r -n1 basename)
  for target in thumbv7em-none-eabihf riscv32imac-unknown-none-elf; do
    for crate in "${no_std_crates[@]}"; do
      step "$crate on $target" cargo check -q -p "orion-$crate" --no-default-features --target "$target"
    done
  done
else
  echo "no crates yet"
fi

if [ -x scripts/check-boundaries.sh ]; then
  step boundaries scripts/check-boundaries.sh
  step boundaries-self-test scripts/check-boundaries.sh --self-test
fi

echo "gate passed"
