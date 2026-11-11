#!/usr/bin/env bash
# Checks packaging/:
#
#   scripts/packaging-check.sh
#
# - the systemd units load (`systemd-analyze verify`), and node logic's unit stays hardened
#   (`systemd-analyze security`, at most EXPOSURE_LIMIT);
# - the sysusers entry parses (`systemd-sysusers --dry-run`).
#
# A tool that is not installed is skipped with a line saying so: the gate runs on machines
# without systemd.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The comm core is not scored: it lets SSH logins in as the users they name, so most of what
# the score counts would follow every login into its shell (packaging/README.md).
EXPOSURE_LIMIT=2.5

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failures=0

if command -v systemd-analyze > /dev/null; then
    root="$work/root"
    mkdir -p "$root/usr/bin" "$root/usr/lib/systemd/system"
    printf '#!/bin/sh\n' > "$root/usr/bin/orion"
    chmod 755 "$root/usr/bin/orion"
    cp packaging/systemd/*.service "$root/usr/lib/systemd/system/"
    # The targets the units order against, from this machine.
    cp /usr/lib/systemd/system/*.target "$root/usr/lib/systemd/system/" 2> /dev/null || true
    if out=$(systemd-analyze verify --man=no --recursive-errors=no --root="$root" \
        orion-comm.service orion-node.service 2>&1) && [ -z "$out" ]; then
        echo "    units: load"
    else
        echo "    FAIL: units do not load:"
        echo "$out" | sed 's/^/      /'
        failures=$((failures + 1))
    fi
    score=$(systemd-analyze security --offline=true --root="$root" orion-node.service 2>&1 \
        | sed -n 's/.*Overall exposure level for orion-node.service: \([0-9.]*\).*/\1/p')
    if [ -z "$score" ]; then
        echo "    FAIL: no exposure score for orion-node.service"
        failures=$((failures + 1))
    elif awk -v s="$score" -v l="$EXPOSURE_LIMIT" 'BEGIN { exit !(s <= l) }'; then
        echo "    orion-node.service exposure: $score (limit $EXPOSURE_LIMIT)"
    else
        echo "    FAIL: orion-node.service exposure is $score, over $EXPOSURE_LIMIT"
        failures=$((failures + 1))
    fi
else
    echo "    units not checked: no systemd-analyze"
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

[ "$failures" -eq 0 ]
