#!/usr/bin/env bash
# Checks packaging/:
#
#   scripts/packaging-check.sh
#
# - the systemd units load (`systemd-analyze verify`), and node logic's unit and the comm core's
#   stay hardened (`systemd-analyze security`, at most their limits);
# - node logic's unit does not set RestrictSUIDSGID, under which systemd refuses openat2;
# - the sysusers entry parses (`systemd-sysusers --dry-run`).
# - the BusyBox init scripts parse (`sh -n`), and the supervisor's failure rule holds: it starts
#   a service that ends again, after a growing wait, and a TERM stops it;
# - each settings example is read by the binary it is for, with no key unknown to it
#   (the binaries' own tests: `orion-bin`, `orion-recovery-bin`).
#
# A tool that is not installed is skipped with a line saying so: the gate runs on machines
# without systemd.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Exposure limits. The comm core is not root and starts no sessions, so it is scored; it
# keeps the clock, devices and the network, which the score counts (4.7 at C52). The login
# helper is not scored: each login is the user's own shell, and what the score counts would
# follow it (packaging/README.md).
NODE_LIMIT=2.5
COMM_LIMIT=5.0

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failures=0

if command -v systemd-analyze > /dev/null; then
    root="$work/root"
    mkdir -p "$root/usr/bin" "$root/usr/lib/systemd/system"
    printf '#!/bin/sh\n' > "$root/usr/bin/orion"
    chmod 755 "$root/usr/bin/orion"
    cp packaging/systemd/*.service packaging/systemd/*.socket "$root/usr/lib/systemd/system/"
    # The targets the units order against, from this machine.
    cp /usr/lib/systemd/system/*.target "$root/usr/lib/systemd/system/" 2> /dev/null || true
    if out=$(systemd-analyze verify --man=no --recursive-errors=no --root="$root" \
        orion-comm.service orion-node.service orion-login.socket orion-login.service 2>&1) && [ -z "$out" ]; then
        echo "    units: load"
    else
        echo "    FAIL: units do not load:"
        echo "$out" | sed 's/^/      /'
        failures=$((failures + 1))
    fi
    for scored in "orion-node.service $NODE_LIMIT" "orion-comm.service $COMM_LIMIT"; do
        read -r unit limit <<< "$scored"
        score=$(systemd-analyze security --offline=true --root="$root" "$unit" 2>&1 \
            | sed -n "s/.*Overall exposure level for $unit: \([0-9.]*\).*/\1/p")
        if [ -z "$score" ]; then
            echo "    FAIL: no exposure score for $unit"
            failures=$((failures + 1))
        elif awk -v s="$score" -v l="$limit" 'BEGIN { exit !(s <= l) }'; then
            echo "    $unit exposure: $score (limit $limit)"
        else
            echo "    FAIL: $unit exposure is $score, over $limit"
            failures=$((failures + 1))
        fi
    done
else
    echo "    units not checked: no systemd-analyze"
fi

# Node logic opens everything under a root or the store with openat2, and systemd answers
# that call with "not implemented" for a unit with RestrictSUIDSGID=yes: every push, fetch and
# listing in a root then fails (I98, seen on a board). No tool is needed to look.
if grep -Eq '^[[:space:]]*RestrictSUIDSGID[[:space:]]*=[[:space:]]*(yes|true|on|1)[[:space:]]*$' \
    packaging/systemd/orion-node.service; then
    echo "    FAIL: orion-node.service sets RestrictSUIDSGID: systemd then refuses openat2, and no file under a root can be opened"
    failures=$((failures + 1))
else
    echo "    orion-node.service: openat2 is not refused"
fi

if command -v systemd-sysusers > /dev/null; then
    mkdir -p "$work/users/etc"
    if out=$(systemd-sysusers --dry-run --root="$work/users" packaging/systemd/orion.sysusers 2>&1) \
        && echo "$out" | grep -q "orion"; then
        echo "    sysusers: parses"
    else
        echo "    FAIL: orion.sysusers:"
        echo "$out" | sed 's/^/      /'
        failures=$((failures + 1))
    fi
else
    echo "    sysusers not checked: no systemd-sysusers"
fi

# A login keeps the helper's bounding set, and a root login is all of root: the helper's unit
# sets none. The comm core's account is one sysusers makes, and its unit makes the directory
# it listens in: without either a board's first start fails (217/USER) or goes into recovery.
if grep -q '^CapabilityBoundingSet=' packaging/systemd/orion-login.service; then
    echo "    FAIL: orion-login.service bounds its capabilities, and every root login with them"
    failures=$((failures + 1))
else
    echo "    login helper: no bounding set, so a root login is all of root"
fi
comm_user=$(sed -n 's/^User=//p' packaging/systemd/orion-comm.service)
if grep -q "^u $comm_user " packaging/systemd/orion.sysusers \
    && grep -q '^RuntimeDirectory=' packaging/systemd/orion-comm.service; then
    echo "    comm core: its account ($comm_user) in sysusers, its runtime directory in its unit"
else
    echo "    FAIL: orion-comm.service runs as $comm_user: not in orion.sysusers, or no RuntimeDirectory="
    failures=$((failures + 1))
fi

for script in packaging/busybox/orion-supervise packaging/busybox/S4*; do
    if ! out=$(sh -n "$script" 2>&1); then
        echo "    FAIL: $script does not parse:"
        echo "$out" | sed 's/^/      /'
        failures=$((failures + 1))
    fi
done
echo "    busybox scripts: parse"

# The supervisor, with a stand-in for the binary that ends at once: started, started again
# after 1 s and then 2 s, and stopped by TERM within a moment.
fake="$work/fake-orion"
printf '#!/bin/sh\necho run >> "%s/runs"\nexit 3\n' "$work" > "$fake"
chmod 755 "$fake"
# Recovery mode runs the binary as it is; the others run it as `orion comm` or as the orion user.
SUPERVISE_BIN="$fake" ORION_SOCKET="$work/none.sock" sh packaging/busybox/orion-supervise recovery \
    > "$work/supervise.log" 2>&1 &
supervisor=$!
sleep 4
kill -TERM "$supervisor"
stopped=0
for _ in 1 2 3 4 5 6 7 8 9 10; do
    kill -0 "$supervisor" 2> /dev/null || { stopped=1; break; }
    sleep 0.2
done
wait "$supervisor" 2> /dev/null || true
runs=$(wc -l < "$work/runs" 2> /dev/null || echo 0)
if [ "$stopped" -eq 1 ] && [ "$runs" -eq 3 ] && grep -q "again in 2 s" "$work/supervise.log"; then
    echo "    supervisor: restarts after 1 s then 2 s, stops on TERM"
else
    echo "    FAIL: supervisor: $runs runs in 4 s (3 expected), stopped: $stopped"
    sed 's/^/      /' "$work/supervise.log"
    failures=$((failures + 1))
fi

[ "$failures" -eq 0 ]
