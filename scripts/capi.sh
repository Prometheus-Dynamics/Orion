#!/usr/bin/env bash
# liborion (docs/capi.md).
#
#   scripts/capi.sh header   regenerate crates/capi/include/orion/orion.h and the MCU template's
#                            examples/mcu-template/include/orion_mcu.h (needs cbindgen)
#   scripts/capi.sh check    the headers are current, the shared ABI header is the ABI crate's,
#                            the exported symbols are crates/capi/symbols.txt, the SONAME carries
#                            ORION_ABI_MAJOR, and a C program runs against liborion.a and
#                            liborion.so, the MCU header compiles in each build, and the Rust
#                            wrapper (crates/liborion-rs) passes its tests in both link modes
#
# CBINDGEN overrides the cbindgen binary; CC the C compiler.
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"
capi=crates/capi
header="$capi/include/orion/orion.h"
cbindgen="${CBINDGEN:-cbindgen}"
cc="${CC:-cc}"
target_dir="${CARGO_TARGET_DIR:-$root_dir/target}"

mcu=examples/mcu-template
mcu_header="$mcu/include/orion_mcu.h"

generate() {
    "$cbindgen" --quiet --config "$capi/cbindgen.toml" --crate orion-capi --output "$1" "$capi"
}

generate_mcu() {
    "$cbindgen" --quiet --config "$mcu/cbindgen.toml" --crate orion-mcu-template --output "$1" "$mcu"
}

case "${1:-}" in
header)
    generate "$header"
    generate_mcu "$mcu_header"
    echo "capi: wrote $header and $mcu_header"
    ;;
check)
    scratch="$(mktemp -d)"
    trap 'rm -rf "$scratch"' EXIT

    generate "$scratch/orion.h"
    generate_mcu "$scratch/orion_mcu.h"
    for pair in "$header:$scratch/orion.h" "$mcu_header:$scratch/orion_mcu.h"; do
        if ! diff -u "${pair%%:*}" "${pair#*:}"; then
            echo "capi: ${pair%%:*} is out of date; run scripts/capi.sh header" >&2
            exit 1
        fi
    done

    abi_dir="$(cargo metadata --format-version 1 --locked |
        python3 -c 'import json, os, sys
for p in json.load(sys.stdin)["packages"]:
    if p["name"].endswith("-abi"):
        print(os.path.dirname(p["manifest_path"]))')"
    shared="$(find "$capi/include" -name abi.h)"
    upstream="$abi_dir/include/${shared#"$capi/include/"}"
    if ! cmp "$shared" "$upstream"; then
        echo "capi: $shared differs from the ABI crate's $upstream" >&2
        exit 1
    fi

    cargo build --locked -p liborion
    lib="$target_dir/debug"

    nm -D --defined-only "$lib/liborion.so" | awk '$2 == "T" { print $3 }' | sort >"$scratch/symbols"
    if ! diff -u "$capi/symbols.txt" "$scratch/symbols"; then
        echo "capi: exported symbols changed: update $capi/symbols.txt and note the change" \
            "(docs/capi.md, Versions)" >&2
        exit 1
    fi

    major="$(awk '$2 == "ORION_ABI_MAJOR" { print $3 }' "$header")"
    soname="$(readelf -d "$lib/liborion.so" | sed -n 's/.*SONAME.*\[\(.*\)\]/\1/p')"
    if [[ "$soname" != "liborion.so.$major" ]]; then
        echo "capi: SONAME is '$soname', want liborion.so.$major" >&2
        exit 1
    fi

    for defines in "-DORION_MCU_UART" "-DORION_MCU_CAN" \
        "-DORION_MCU_UART -DORION_MCU_XFER -DORION_MCU_SLOTS"; do
        # -Wno-type-limits: the minor check is always true while ORION_MCU_API_MINOR is 0.
        # shellcheck disable=SC2086
        "$cc" -std=c11 -Wall -Wextra -Werror -Wno-type-limits -fsyntax-only $defines \
            -I "$mcu/include" \
            "$mcu/tests/c/header.c"
    done

    flags=(-std=c11 -Wall -Wextra -Werror -I "$capi/include")
    "$cc" "${flags[@]}" "$capi/tests/c/smoke.c" "$lib/liborion.a" -lpthread -ldl -lm \
        -o "$scratch/smoke-static"
    "$scratch/smoke-static"
    ln -sf "$lib/liborion.so" "$scratch/$soname"
    "$cc" "${flags[@]}" "$capi/tests/c/smoke.c" -L "$lib" -lorion -o "$scratch/smoke-shared"
    LD_LIBRARY_PATH="$scratch" "$scratch/smoke-shared"

    # The Rust wrapper: static (the core compiled in), then shared, against this liborion.so.1
    # (and the test binary must really need it).
    cargo test --locked -q -p liborion-rs
    shared_tests="$(LIBORION_LIB_DIR="$lib" cargo test --locked -q -p liborion-rs \
        --no-default-features --features shared --no-run --message-format=json |
        python3 -c 'import json, sys
for line in sys.stdin:
    m = json.loads(line)
    # Integration tests only: the unit-test binary of the library calls nothing, so needs nothing.
    if m.get("reason") == "compiler-artifact" and m.get("executable") and m["target"]["kind"] == ["test"]:
        print(m["executable"])')"
    if [[ -z "$shared_tests" ]]; then
        echo "capi: no liborion-rs test binary in the shared build" >&2
        exit 1
    fi
    for test in $shared_tests; do
        if ! readelf -d "$test" | grep -q "NEEDED.*\[$soname\]"; then
            echo "capi: $test does not link $soname" >&2
            exit 1
        fi
        LD_LIBRARY_PATH="$scratch" "$test" -q
    done

    # A consumer outside the workspace, through the wrapper only, in both modes.
    consumer=(--locked -q --manifest-path examples/liborion-consumer/Cargo.toml
        --target-dir "$target_dir")
    cargo build "${consumer[@]}"
    "$lib/liborion-consumer"
    LIBORION_LIB_DIR="$lib" cargo build "${consumer[@]}" --no-default-features --features shared
    if ! readelf -d "$lib/liborion-consumer" | grep -q "NEEDED.*\[$soname\]"; then
        echo "capi: the shared liborion-consumer does not link $soname" >&2
        exit 1
    fi
    LD_LIBRARY_PATH="$scratch" "$lib/liborion-consumer"
    echo "capi: ok"
    ;;
*)
    echo "usage: $0 header|check" >&2
    exit 2
    ;;
esac
