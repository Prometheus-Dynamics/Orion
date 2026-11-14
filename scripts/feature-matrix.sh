#!/usr/bin/env bash
# Every crate with optional features builds with none of them, with each one alone, and with
# all of them. A use of something a feature brings, outside that feature's cfg, fails here.
#
# With crate names as arguments, only those crates are checked.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# One line for each build: the crate, then the flags.
mapfile -t builds < <(cargo metadata --locked --no-deps --format-version 1 | python3 -c '
import json, sys
only = set(sys.argv[1:])
for package in json.load(sys.stdin)["packages"]:
    name = package["name"]
    features = sorted(f for f in package["features"] if f != "default")
    if not features or (only and name not in only):
        continue
    print(name, "--no-default-features")
    for feature in features:
        print(name, "--no-default-features", "--features", feature)
    print(name, "--all-features")
' "$@")

failed=0
log=$(mktemp)
for build in "${builds[@]}"; do
  read -r crate flags <<< "$build"
  # shellcheck disable=SC2086
  if ! cargo check -q --locked -p "$crate" $flags 2> "$log"; then
    echo "FAILED: cargo check -p $crate $flags"
    grep -E '^error' -A6 "$log" | head -40
    failed=$((failed + 1))
  fi
done
rm -f "$log"
echo "feature matrix: ${#builds[@]} builds, $failed failed"
[ "$failed" -eq 0 ]
