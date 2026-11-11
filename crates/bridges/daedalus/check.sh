#!/usr/bin/env bash
# The bridge builds, lints and passes its tests against the Daedalus commit it pins, with the
# daemon from that same commit.
#
#   crates/bridges/daedalus/check.sh
#
# It is not a workspace member (Daedalus is a workspace of its own), so the workspace's steps
# never see it. The daemon is installed once under the target directory and reused while the
# pinned commit stays the same.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
target_dir="${CARGO_TARGET_DIR:-../../../target}"
mkdir -p "$target_dir"
target_dir="$(cd "$target_dir" && pwd)"
export CARGO_TARGET_DIR="$target_dir/bridge-daedalus"

rev="$(sed -n 's/^daedalus-loader = .*rev = "\([0-9a-f]*\)".*/\1/p' Cargo.toml)"
daemon_root="$target_dir/bridge-daedalus-daemon/$rev"
if [ ! -x "$daemon_root/bin/daedalusd" ]; then
    CARGO_TARGET_DIR="$target_dir/bridge-daedalus-daemon/build" cargo install --quiet --locked \
        --git https://github.com/Prometheus-Dynamics/Daedalus --rev "$rev" daedalusd --root "$daemon_root"
fi

cargo fmt --check
cargo clippy --quiet --all-targets -- -D warnings
DAEDALUSD="$daemon_root/bin/daedalusd" cargo test --quiet
