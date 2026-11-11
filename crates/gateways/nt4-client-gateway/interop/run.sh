#!/usr/bin/env bash
# Interop test of the NT4 gateway, client form, against WPILib's ntcore (the pyntcore package). Not part of
# the gate: it needs Python 3, and network access to pip the first time.
#
#   crates/gateways/nt4-client-gateway/interop/run.sh
#
# An ntcore server runs with a topic of its own. A comm core joins it as a client with one
# topic mapped each way. The server must see the board's topic with its NT4 type and a value
# that follows, and the server's topic must be published on the board, also when it changes.
#
# The venv is the one the NT4 crate's interop uses, under nt4-interop in the cargo target
# directory. Port: NT4_INTEROP_CLIENT_GATEWAY_PORT (5834).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "$here/../../../.." && pwd)"
target="${CARGO_TARGET_DIR:-$root_dir/target}"
work="$target/nt4-interop"
venv="$work/venv"
port="${NT4_INTEROP_CLIENT_GATEWAY_PORT:-5834}"
failures=0

mkdir -p "$work"
if [[ ! -x "$venv/bin/python" ]]; then
    echo "==> creating venv with pyntcore in $venv"
    python3 -m venv "$venv"
    "$venv/bin/pip" install --quiet pyntcore
fi

echo "==> building the gateway demo"
(cd "$root_dir" && cargo build --quiet -p orion-nt4-client-gateway --example nt4-client-gateway-demo)

board="$work/client-gateway-board"
rm -rf "$board"
mkdir -p "$board"
demo_pid=""
cleanup() {
    [[ -n "$demo_pid" ]] && kill "$demo_pid" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

check() {
    local description="$1" file="$2" pattern="$3"
    if grep -qE "$pattern" "$file"; then
        echo "    ok: $description"
    else
        echo "    FAIL: $description (no match for /$pattern/ in $file)"
        failures=$((failures + 1))
    fi
}

"$target/debug/examples/nt4-client-gateway-demo" "$board" "127.0.0.1:$port" > "$work/client-gateway.out" 2>&1 &
demo_pid=$!

# pyntcore writes networktables.json into the working directory; keep it out of the checkout.
cd "$work"
if "$venv/bin/python" -I "$here/ntcore_side.py" --port "$port" > "$work/client-gateway-ntcore.out" 2>&1; then
    echo "    ok: ntcore server finished"
else
    echo "    FAIL: ntcore server"
    failures=$((failures + 1))
fi
sed 's/^/    /' "$work/client-gateway-ntcore.out"
sleep 0.5
check "the board joined the server" "$work/client-gateway.out" '^joined the server$'
check "the board's topic has its NT4 type on the server" "$work/client-gateway-ntcore.out" \
    '/Tank/level type=double '
check "the server's topic was published on the board" "$work/client-gateway.out" \
    '^robot\.state value = false$'
check "and followed when it changed" "$work/client-gateway.out" '^robot\.state value = true$'

if [[ $failures -gt 0 ]]; then
    echo "==> nt4 client gateway interop: $failures check(s) failed"
    exit 1
fi
echo "==> nt4 client gateway interop: all checks passed"
