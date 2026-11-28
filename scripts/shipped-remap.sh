#!/usr/bin/env bash
# The one cargo argument that keeps the Cargo home's path out of what a build makes:
#
#   cargo build --profile service --target TRIPLE "$(scripts/shipped-remap.sh TRIPLE)" ...
#   cargo build --profile service "$(scripts/shipped-remap.sh)" ...     (this machine)
#
# A dependency's source is under the Cargo home (registry/src, git/checkouts), and its paths
# are in what it compiles to: panic messages, file!(). They are written as /cargo-home instead,
# so where the home is (~/.cargo, a copy of it, another user's) changes no byte of a shipped
# binary. The workspace's own sources are already relative.
#
# A --config target rustflags entry adds to the flags .cargo/config.toml gives that target;
# RUSTFLAGS would replace them.
set -euo pipefail
triple="${1:-$(rustc -vV | sed -n 's/^host: //p')}"
home="${CARGO_HOME:-$HOME/.cargo}"
flags="\"--remap-path-prefix=$home=/cargo-home\""
real="$(realpath -m "$home")"
[ "$real" = "$home" ] || flags="$flags, \"--remap-path-prefix=$real=/cargo-home\""
echo "--config=target.$triple.rustflags=[$flags]"
