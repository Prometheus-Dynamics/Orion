#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_dir="$root_dir/target/package"
tmp_dir="${ORION_PACKAGE_VERIFY_DIR:-}"

packages=(
    orion-core
    orion-control-plane
    orion-auth
    orion-data-plane
    orion-runtime
    orion-transport-common
    orion-service
    orion-macros
    orion-cluster
    orion-transport-http
    orion-transport-ipc
    orion-transport-tcp
    orion-transport-quic
    orion-client
    orion
    orion-node
    orionctl
)

if [[ -z "$tmp_dir" ]]; then
    tmp_dir="$(mktemp -d)"
    cleanup_tmp=1
else
    cleanup_tmp=0
    rm -rf "$tmp_dir"
    mkdir -p "$tmp_dir"
fi

if [[ "$cleanup_tmp" -eq 1 ]]; then
    trap 'rm -rf "$tmp_dir"' EXIT
fi

mkdir -p "$target_dir" "$tmp_dir/packages"

printf 'Packaging publishable crates without verification...\n'
cargo package \
    --manifest-path "$root_dir/Cargo.toml" \
    --workspace \
    --allow-dirty \
    --no-verify >/dev/null

printf 'Unpacking package tarballs into temporary verification workspace...\n'
for package in "${packages[@]}"; do
    package_id="$(cargo pkgid --manifest-path "$root_dir/Cargo.toml" -p "$package")"
    version="${package_id##*#}"
    version="${version##*@}"
    crate_file="$target_dir/${package}-${version}.crate"
    package_dir="$tmp_dir/packages/$package"

    if [[ ! -f "$crate_file" ]]; then
        printf 'Expected package tarball not found: %s\n' "$crate_file" >&2
        exit 1
    fi

    mkdir -p "$package_dir"
    archive_file="$tmp_dir/${package}.tar"
    gzip_log="$tmp_dir/${package}.gzip.log"
    set +e
    gzip -dc "$crate_file" >"$archive_file" 2>"$gzip_log"
    gzip_status=$?
    set -e
    if [[ "$gzip_status" -ne 0 && "$gzip_status" -ne 2 ]]; then
        printf 'Failed to decompress package tarball: %s\n' "$crate_file" >&2
        cat "$gzip_log" >&2
        exit "$gzip_status"
    fi
    tar -xf "$archive_file" -C "$package_dir" --strip-components=1
done

{
    printf '[workspace]\n'
    printf 'resolver = "3"\n'
    printf 'members = [\n'
    for package in "${packages[@]}"; do
        printf '    "packages/%s",\n' "$package"
    done
    printf ']\n\n'
    printf '[patch.crates-io]\n'
    for package in "${packages[@]}"; do
        printf '%s = { path = "packages/%s" }\n' "$package" "$package"
    done
} >"$tmp_dir/Cargo.toml"

if [[ -f "$root_dir/Cargo.lock" ]]; then
    cp "$root_dir/Cargo.lock" "$tmp_dir/Cargo.lock"
fi

printf 'Checking unpacked package tarballs as an isolated workspace...\n'
cargo check \
    --manifest-path "$tmp_dir/Cargo.toml" \
    --workspace \
    --all-targets \
    --all-features

printf 'Package tarball verification passed.\n'
