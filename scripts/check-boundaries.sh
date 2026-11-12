#!/usr/bin/env bash
# The rules a compiler can't enforce: which crates may depend on which, what never comes back, where
# `unsafe` may live, how long a file may be, and where product names may appear.
#
#   scripts/check-boundaries.sh               check this tree
#   scripts/check-boundaries.sh --self-test   prove each rule fails on a deliberately broken tree
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
script="$root/scripts/check-boundaries.sh"

if [ "${1:-}" = "--self-test" ]; then
  # Copies only tracked and new text files the rules read, so a build directory never slows it down.
  work="$(mktemp -d "${TMPDIR:-/tmp}/orion-boundaries.XXXXXX")"
  trap 'rm -rf "$work"' EXIT
  fresh() {
    rm -rf "$work/tree"
    mkdir -p "$work/tree/scripts" "$work/tree/crates/core/base/src" "$work/tree/crates/comm/alpha/src" \
      "$work/tree/crates/node/beta/src" "$work/tree/crates/client/gamma/src"
    cp "$script" "$work/tree/scripts/check-boundaries.sh"
    for crate in core/base comm/alpha node/beta client/gamma; do
      printf '[package]\nname = "orion-%s"\nversion = "0.0.0"\n\n[lints]\nworkspace = true\n' "${crate##*/}" \
        >"$work/tree/crates/$crate/Cargo.toml"
      echo '//! A sample.' >"$work/tree/crates/$crate/src/lib.rs"
    done
    (cd "$work/tree" && git init -q && git add -A)
  }
  run() { (cd "$work/tree" && git add -A && ORION_BOUNDARIES_ROOT="$work/tree" bash scripts/check-boundaries.sh) >"$work/out" 2>&1; }
  dep() { printf '\n[dependencies]\n%s\n' "$2" >>"$work/tree/crates/$1/Cargo.toml"; }
  failures=0
  expect_fail() {
    local name="$1" want="$2"
    if run; then
      echo "self-test FAILED: '$name' was not caught"
      failures=$((failures + 1))
    elif ! grep -q "$want" "$work/out"; then
      echo "self-test FAILED: '$name' failed for the wrong reason:"
      cat "$work/out"
      failures=$((failures + 1))
    else
      echo "caught: $name"
    fi
  }

  fresh
  run || { echo "self-test FAILED: the clean sample tree does not pass"; cat "$work/out"; exit 1; }
  echo "passes: clean sample tree"

  fresh; dep comm/alpha 'orion-beta = { path = "../../node/beta" }'
  expect_fail "comm depending on node" "comm and node never depend on each other"
  fresh; dep node/beta 'orion-alpha = { path = "../../comm/alpha" }'
  expect_fail "node depending on comm" "comm and node never depend on each other"
  fresh; dep core/base 'orion-gamma = { path = "../../client/gamma" }'
  expect_fail "core depending on client" "depends on something above it"
  fresh; dep client/gamma 'rkyv = "0.8"'
  expect_fail "rkyv dependency" "rkyv"
  fresh; dep client/gamma 'quinn = "0.11"'
  expect_fail "QUIC dependency" "not allowed here"
  fresh; dep comm/alpha 'hyper = "1"'
  expect_fail "HTTP dependency" "not allowed here"
  fresh; dep client/gamma 'rustls = "0.23"'
  expect_fail "rustls dependency" "not allowed here"
  fresh; echo 'pub fn f() { unsafe { core::hint::unreachable_unchecked() } }' >>"$work/tree/crates/comm/alpha/src/lib.rs"
  expect_fail "unsafe outside the allowed crates" "unsafe outside"
  fresh; sed -i 's/workspace = true/# no shared lints/' "$work/tree/crates/comm/alpha/Cargo.toml"
  expect_fail "crate dropping the shared lints" "must inherit"
  fresh; seq 801 | sed 's#^#// #' >"$work/tree/crates/comm/alpha/src/long.rs"
  expect_fail "file over 800 lines" "over 800 lines"
  fresh; echo '// made for the raze board' >>"$work/tree/crates/comm/alpha/src/lib.rs"
  expect_fail "product name" "product name"

  [ "$failures" -eq 0 ] || exit 1
  echo "self-test passed"
  exit 0
fi

exec python3 -I - "${ORION_BOUNDARIES_ROOT:-$root}" <<'PY'
import pathlib
import re
import subprocess
import sys
import tomllib

root = pathlib.Path(sys.argv[1])
errors = []

# Crates allowed to use `unsafe`. Everywhere else the workspace lint forbids it.
UNSAFE_OK = ("crates/core/ring", "crates/core/fdpass", "crates/core/measure", "crates/client/capi", "crates/device/")
UNSAFE_FILES = ("crates/core/wire/src/crc.rs", "crates/testing/sample-executor/src/spawn.rs")
# Network stacks the design names, and the only crates that may hold them.
NET_DEPS = {"hyper", "axum", "reqwest", "ureq", "http", "h2", "h3", "quinn", "quiche", "rustls", "tokio-rustls", "tonic", "actix-web"}
NET_OK = ("crates/node/files-pull",)
# Other libraries and products go by their role (the camera library, the device library, the
# graph runtime, the product).
PRODUCT = re.compile(
    r"\b(photon|photonvision|helios|atlas|lemnos|lemnosd|styx|eidos|raze|daedalus|daedalusd|cm5|raspberry|bcm2712|roborio|wpilib|frc|robots?|apriltag|opencv|libcamera)\b",
    re.I,
)
# Where the names stay: the NT4 crates, whose protocol's own vocabulary they are; the record of
# the NT4 client run, which quotes that vocabulary from a real server; this file; and the lock
# file's dependency names.
PRODUCT_OK = (
    "scripts/check-boundaries.sh", "crates/gateways/nt4", "testing/board-runs/F8.md", "Cargo.lock",
)
LIMIT = 800


def tier(path):
    return path.relative_to(root / "crates").parts[0]


manifests = sorted((root / "crates").glob("*/*/Cargo.toml"))
fuzz = root / "testing" / "fuzz" / "Cargo.toml"
for manifest in manifests + ([fuzz] if fuzz.exists() else []):
    rel = manifest.parent.relative_to(root).as_posix()
    data = tomllib.loads(manifest.read_text())
    tables = [data.get(key, {}) for key in ("dependencies", "dev-dependencies", "build-dependencies")]
    tables += [t.get(key, {}) for t in data.get("target", {}).values() for key in ("dependencies", "dev-dependencies")]
    this_tier = rel.split("/")[1] if rel.startswith("crates/") else None
    for table in tables:
        for name, spec in table.items():
            package = spec.get("package", name) if isinstance(spec, dict) else name
            if package == "rkyv" or package.startswith("rkyv"):
                errors.append(f"{rel}: rkyv left the codebase and does not come back")
            if package in NET_DEPS and not rel.startswith(NET_OK):
                errors.append(f"{rel}: {package} is not allowed here (only {', '.join(NET_OK)})")
            if not (isinstance(spec, dict) and "path" in spec) or this_tier is None:
                continue
            target = (manifest.parent / spec["path"]).resolve()
            try:
                other = target.relative_to((root / "crates").resolve()).parts[0]
            except ValueError:
                continue
            is_dev = table is data.get("dev-dependencies")
            if {this_tier, other} == {"comm", "node"}:
                errors.append(f"{rel}: comm and node never depend on each other ({name})")
            if this_tier in ("core", "device") and other not in ("core", "device", "testing") and not is_dev:
                errors.append(f"{rel}: depends on something above it ({name}, in {other})")
            if this_tier == "core" and other == "device":
                errors.append(f"{rel}: depends on something above it ({name}, in {other})")
    allowed = rel.startswith(UNSAFE_OK) or any(f.startswith(rel + "/") for f in UNSAFE_FILES)
    if rel.startswith("crates/") and not allowed and data.get("lints", {}).get("workspace") is not True:
        errors.append(f"{rel}: must inherit the workspace lints ([lints] workspace = true)")

listed = subprocess.run(
    ["git", "-C", str(root), "ls-files", "--cached", "--others", "--exclude-standard"],
    capture_output=True, text=True, check=True,
).stdout.splitlines()
for rel in listed:
    path = root / rel
    if not path.is_file() or path.is_symlink():
        continue
    try:
        text = path.read_text()
    except (UnicodeDecodeError, OSError):
        continue
    if rel.endswith(".rs"):
        lines = text.count("\n")
        if lines > LIMIT:
            errors.append(f"{rel}: over {LIMIT} lines ({lines})")
        if rel.startswith("crates/") and not rel.startswith(UNSAFE_OK) and rel not in UNSAFE_FILES:
            for number, line in enumerate(text.splitlines(), 1):
                code = line.split("//", 1)[0]
                if re.search(r"\bunsafe\b", code) and "tests/ui/" not in rel:
                    errors.append(f"{rel}:{number}: unsafe outside the allowed crates")
    if not rel.startswith(PRODUCT_OK):
        for number, line in enumerate(text.splitlines(), 1):
            match = PRODUCT.search(line)
            if match:
                errors.append(f"{rel}:{number}: product name '{match.group(0)}'")

if errors:
    print("boundary check failed:", file=sys.stderr)
    for error in errors:
        print("  " + error, file=sys.stderr)
    sys.exit(1)
print("boundaries ok")
PY
