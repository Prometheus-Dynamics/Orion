#!/usr/bin/env bash
# Interop test of orion-nt4 against WPILib's ntcore (the pyntcore package). Optional: not part of
# the default CI, it needs Python 3 and network access to pip the first time.
#
#   ./scripts/nt4-interop.sh
#
# Two directions, each asserted on the output:
#   1. orion server (nt4-server) <- ntcore client: the client publishes a double[] and a string,
#      and receives a double and a string the server holds; the server must record both uploads.
#   2. ntcore server -> orion client (nt4-dump): double, boolean, string[], int and raw values.
#
# The venv lives in target/nt4-interop (ignored by git). Ports: NT4_INTEROP_SERVER_PORT (5832),
# NT4_INTEROP_WPI_PORT (5831).
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

work="$root_dir/target/nt4-interop"
venv="$work/venv"
server_port="${NT4_INTEROP_SERVER_PORT:-5832}"
wpi_port="${NT4_INTEROP_WPI_PORT:-5831}"
failures=0

mkdir -p "$work"
# pyntcore writes networktables.json into the working directory; keep it out of the checkout.
cd "$work"
if [[ ! -x "$venv/bin/python" ]]; then
    echo "==> creating venv with pyntcore in $venv"
    python3 -m venv "$venv"
    "$venv/bin/pip" install --quiet pyntcore
fi
python="$venv/bin/python"

echo "==> building the nt4 examples"
cargo build --quiet -p orion-nt4 --examples
bin="$root_dir/target/debug/examples"

server_pid=""
wpi_pid=""
stdin_fd_open=0
cleanup() {
    if [[ $stdin_fd_open == 1 ]]; then exec 3>&-; fi
    [[ -n "$server_pid" ]] && kill "$server_pid" 2>/dev/null || true
    [[ -n "$wpi_pid" ]] && kill "$wpi_pid" 2>/dev/null || true
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

echo "==> direction 1: ntcore client -> orion server"
rm -f "$work/stdin"
mkfifo "$work/stdin"
# nt4-server exits when its stdin closes, so keep the FIFO open on fd 3 until the end.
"$bin/nt4-server" --bind "127.0.0.1:$server_port" \
    --topic /interop/server/number=double:1.5 \
    --topic /interop/server/label=string:from-orion \
    < "$work/stdin" > "$work/server.out" 2>&1 &
server_pid=$!
exec 3> "$work/stdin"
stdin_fd_open=1
sleep 1

# A second orion client watches the relay: the uploads must reach it with server-clock timestamps.
timeout 12 "$bin/nt4-dump" --port "$server_port" --prefix /interop/client/ --name orion-relay \
    > "$work/relay.out" 2>&1 &
relay_pid=$!
sleep 1

if "$python" -I "$root_dir/scripts/nt4_interop.py" ntcore-client --port "$server_port" \
    > "$work/ntcore-client.out" 2>&1; then
    echo "    ok: ntcore client finished"
    sed 's/^/    /' "$work/ntcore-client.out"
else
    echo "    FAIL: ntcore client"
    sed 's/^/    /' "$work/ntcore-client.out"
    failures=$((failures + 1))
fi
sleep 1
wait "$relay_pid" 2>/dev/null || true
check "relay delivered the double[] upload" "$work/relay.out" \
    '/interop/client/array @[0-9]+ = DoubleArray\(\[1\.0, 2\.5\]\)'
check "relay delivered the string upload" "$work/relay.out" \
    '/interop/client/text @[0-9]+ = String\("hello-from-ntcore"\)'
# The server restamps relayed values with its own clock, which starts near 0 at server start, so a
# relayed timestamp is small (under 10^12 us, about 11 days), not the WPILib client's Unix time.
check "relayed timestamps are on the server clock" "$work/relay.out" \
    '/interop/client/array @[0-9]{1,12} = '
check "server recorded the double[] upload" "$work/server.out" \
    '/interop/client/array <- DoubleArray\(\[1\.0, 2\.5\]\) \(from a client\)'
check "server recorded the string upload" "$work/server.out" \
    '/interop/client/text <- String\("hello-from-ntcore"\) \(from a client\)'

exec 3>&-
stdin_fd_open=0
kill "$server_pid" 2>/dev/null || true
wait "$server_pid" 2>/dev/null || true
server_pid=""

echo "==> direction 2: ntcore server -> orion client"
"$python" -I "$root_dir/scripts/nt4_interop.py" ntcore-server --port "$wpi_port" --seconds 12 \
    > "$work/ntcore-server.out" 2>&1 &
wpi_pid=$!
sleep 2
timeout 7 "$bin/nt4-dump" --port "$wpi_port" --prefix /interop/wpi/ --name orion-interop \
    > "$work/dump.out" 2>&1 || true
kill "$wpi_pid" 2>/dev/null || true
wait "$wpi_pid" 2>/dev/null || true
wpi_pid=""
echo "    dump output:"
sed 's/^/    /' "$work/dump.out" | head -30
check "client connected" "$work/dump.out" '^connected$'
check "double value arrived" "$work/dump.out" '/interop/wpi/double @[0-9]+ = Double\(2\.25\)'
check "boolean value arrived" "$work/dump.out" '/interop/wpi/bool @[0-9]+ = Boolean\(true\)'
check "string[] value arrived" "$work/dump.out" '/interop/wpi/names @[0-9]+ = StringArray\(\["a", "b"\]\)'
check "int value arrived" "$work/dump.out" '/interop/wpi/count @[0-9]+ = Int\(7\)'
check "raw value arrived" "$work/dump.out" '/interop/wpi/raw @[0-9]+ = Raw\(\[1, 2\]\)'

if [[ $failures -gt 0 ]]; then
    echo "==> nt4 interop: $failures check(s) failed"
    exit 1
fi
echo "==> nt4 interop: all checks passed"
