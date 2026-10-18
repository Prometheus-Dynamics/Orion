#!/usr/bin/env bash
# Fails when product names leak into Orion. Orion is a generic control-plane library: products
# (PhotonVision, HeliOS, Atlas, Raze, Lemnos, Styx, Eidos, ...) and the boards they run on belong
# in the products' own repositories, and generic examples use role names instead ("device-agent",
# "vision device", "aarch64 board").
#
# Allowlisted (see ALLOW below): the orion-nt4 crate and its interop scripts, which implement the
# FRC NetworkTables 4 wire protocol whose own vocabulary (WPILib, FRC, robot) is part of the
# protocol, and CHANGELOG.md, which records history.
#
# Only git-tracked text files are scanned. Run from anywhere: ./scripts/check-neutral-names.sh
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

# Product and board names, matched case-insensitively as whole words.
PATTERN='\b(photon|photonvision|helios|atlas|lemnos|lemnosd|styx|eidos|raze|daedalus|cm5|raspberry|bcm2712|roborio|wpilib|frc|robots?|apriltag|tag36h11|opencv|libcamera)\b'

# Paths (prefixes or globs) where the names are allowed.
ALLOW=(
    'crates/nt4/*'
    'scripts/nt4-interop.sh'
    'scripts/nt4_interop.py'
    'CHANGELOG.md'
    'scripts/check-neutral-names.sh'
)

allowed() {
    local path="$1" pattern
    for pattern in "${ALLOW[@]}"; do
        # shellcheck disable=SC2053
        if [[ "$path" == $pattern ]]; then
            return 0
        fi
    done
    return 1
}

violations=0
while IFS= read -r path; do
    [[ -f "$path" ]] || continue
    grep -Iq . "$path" 2>/dev/null || continue
    allowed "$path" && continue
    if hits="$(grep -inE "$PATTERN" "$path" || true)" && [[ -n "$hits" ]]; then
        while IFS= read -r line; do
            echo "$path:$line"
        done <<<"$hits"
        violations=$((violations + 1))
    fi
done < <(git ls-files)

if (( violations > 0 )); then
    echo "check-neutral-names: product names in $violations file(s); generalize them or move them to the product's repository" >&2
    exit 1
fi
echo "check-neutral-names: ok"
