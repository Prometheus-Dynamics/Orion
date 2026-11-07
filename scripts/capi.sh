#!/usr/bin/env bash
# liborion (docs/capi.md).
#
#   scripts/capi.sh header   regenerate crates/capi/include/orion/orion.h (needs cbindgen)
#   scripts/capi.sh check    the header is current, the shared ABI header is the ABI crate's,
#                            the exported symbols are crates/capi/symbols.txt, the SONAME carries
#                            ORION_ABI_MAJOR, and a C program runs against liborion.a and
#                            liborion.so
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

generate() {
    "$cbindgen" --quiet --config "$capi/cbindgen.toml" --crate orion-capi --output "$1" "$capi"
}

case "${1:-}" in
header)
    generate "$header"
    echo "capi: wrote $header"
    ;;
check)
    scratch="$(mktemp -d)"
    trap 'rm -rf "$scratch"' EXIT

    generate "$scratch/orion.h"
    if ! diff -u "$header" "$scratch/orion.h"; then
        echo "capi: $header is out of date; run scripts/capi.sh header" >&2
        exit 1
    fi

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

    flags=(-std=c11 -Wall -Wextra -Werror -I "$capi/include")
    "$cc" "${flags[@]}" "$capi/tests/c/smoke.c" "$lib/liborion.a" -lpthread -ldl -lm \
        -o "$scratch/smoke-static"
    "$scratch/smoke-static"
    ln -sf "$lib/liborion.so" "$scratch/$soname"
    "$cc" "${flags[@]}" "$capi/tests/c/smoke.c" -L "$lib" -lorion -o "$scratch/smoke-shared"
    LD_LIBRARY_PATH="$scratch" "$scratch/smoke-shared"
    echo "capi: ok"
    ;;
*)
    echo "usage: $0 header|check" >&2
    exit 2
    ;;
esac
