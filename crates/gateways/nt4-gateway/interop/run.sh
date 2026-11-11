#!/usr/bin/env bash
# Interop test of the NT4 gateway against WPILib's ntcore (the pyntcore package). Not part of
# the gate: it needs Python 3, and network access to pip the first time.
#
#   crates/gateways/nt4-gateway/interop/run.sh
#
# A comm core runs the gateway with one topic mapped each way. An ntcore client must see the
# mapped topic with its NT4 type and a value that follows the board's, and what it writes
# back must be published on the board.
#
# The venv is the one the NT4 crate's interop uses, under nt4-interop in the cargo target
# directory. Port: NT4_INTEROP_GATEWAY_PORT (5833).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "$here/../../../.." && pwd)"
target="${CARGO_TARGET_DIR:-$root_dir/target}"
work="$target/nt4-interop"
venv="$work/venv"
port="${NT4_INTEROP_GATEWAY_PORT:-5833}"
failures=0

mkdir -p "$work"
if [[ ! -x "$venv/bin/python" ]]; then
    echo "==> creating venv with pyntcore in $venv"
    python3 -m venv "$venv"
    "$venv/bin/pip" install --quiet pyntcore
fi

echo "==> building the gateway demo"
(cd "$root_dir" && cargo build --quiet -p orion-nt4-gateway --example nt4-gateway-demo)

board="$work/gateway-board"
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

"$target/debug/examples/nt4-gateway-demo" "$board" "$port" > "$work/gateway.out" 2>&1 &
demo_pid=$!
sleep 1

# pyntcore writes networktables.json into the working directory; keep it out of the checkout.
cd "$work"
if "$venv/bin/python" -I "$here/ntcore_side.py" --port "$port" > "$work/gateway-ntcore.out" 2>&1; then
    echo "    ok: ntcore client finished"
else
    echo "    FAIL: ntcore client"
    failures=$((failures + 1))
fi
sed 's/^/    /' "$work/gateway-ntcore.out"
sleep 0.5
check "the mapped topic has its NT4 type" "$work/gateway-ntcore.out" '/Tank/level type=double '
check "the client's value was published on the board" "$work/gateway.out" \
    '^panel\.setpoint value = 7\.5$'

if [[ $failures -gt 0 ]]; then
    echo "==> nt4 gateway interop: $failures check(s) failed"
    exit 1
fi
echo "==> nt4 gateway interop: all checks passed"
