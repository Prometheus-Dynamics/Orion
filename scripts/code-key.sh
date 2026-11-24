#!/usr/bin/env bash
# The code key of a tree: a hash of every file in it but the notes. The notes are what CI's push
# trigger ignores (.github/workflows/next.yml): every `*.md`, everything under `docs/`, and the
# `LICENSE-*` files at the root. No build or test reads a note (`--check` proves it of the Rust
# sources), so two trees with one code key build and test the same: a gate that passed on one
# passed on the other, and only the checks that read notes (boundaries, links, the PR list) need
# running again.
#
#   scripts/code-key.sh TREE-ISH    the code key of a tree or commit
#   scripts/code-key.sh --check     fails if a Rust source names a note
set -euo pipefail

if [ "${1:-}" = "--check" ]; then
  if git grep -nE '"[^"]*\.md"|"(\.\./)*docs/|include_(str|bytes)!\([^)]*\.md' -- '*.rs'; then
    echo "a Rust source names a note: the code key leaves notes out, so none may be read" >&2
    exit 1
  fi
  echo "no Rust source names a note"
  exit 0
fi
[ $# -eq 1 ] || { echo "usage: scripts/code-key.sh TREE-ISH | --check" >&2; exit 2; }
git ls-tree -r --full-tree "$1" |
  awk -F'\t' '{ p = $2; gsub(/^"|"$/, "", p) } !(p ~ /\.md$/ || p ~ /^docs\// || p ~ /^LICENSE-[^\/]*$/)' |
  sha256sum | cut -c1-64
