#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

echo "==> Applying formatting"
cargo fmt --all

echo "==> Applying clippy fixes"
cargo clippy --fix --allow-dirty --allow-staged --workspace --all-targets --all-features -- -W clippy::all

echo "==> Re-applying formatting"
cargo fmt --all

echo "==> Verifying repo state"
"$root_dir/scripts/ci.sh"
