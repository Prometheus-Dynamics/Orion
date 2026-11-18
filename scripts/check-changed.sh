#!/usr/bin/env bash
# The gate's own checks, for the crates a change touched and every workspace crate that depends
# on them: format, clippy, the tests and doctests, and the feature matrix. For the inner loop.
# A subset of scripts/gate.sh, never a stand-in for it: the gate still runs whole before a
# merge, and only it stamps a tree for scripts/push.sh.
#
#   scripts/check-changed.sh            the crates with files changed against HEAD
#   scripts/check-changed.sh CRATE...   those crates
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

step() {
  local label="$1" start
  shift
  echo "==> $label"
  start=$SECONDS
  "$@"
  echo "    $label: $((SECONDS - start)) s"
}

changed=("$@")
if [ "${#changed[@]}" -eq 0 ]; then
  mapfile -t changed < <(git status --porcelain --untracked-files=all | cut -c4- | sed 's/.* -> //')
  [ "${#changed[@]}" -gt 0 ] || { echo "nothing changed"; exit 0; }
  set -- --paths "${changed[@]}"
else
  set -- --crates "${changed[@]}"
fi

# The crates named, or owning a changed path, and every workspace crate depending on them.
mapfile -t crates < <(cargo metadata --locked --format-version 1 | python3 -c '
import json, os, sys
meta = json.load(sys.stdin)
members = set(meta["workspace_members"])
packages = {p["id"]: p for p in meta["packages"] if p["id"] in members}
root = meta["workspace_root"]
mode, args = sys.argv[1], sys.argv[2:]
start = set()
if mode == "--crates":
    names = {p["name"]: i for i, p in packages.items()}
    for name in args:
        if name not in names:
            sys.exit(f"no workspace crate {name}")
        start.add(names[name])
else:
    dirs = sorted(((os.path.dirname(p["manifest_path"]), i) for i, p in packages.items()),
                  key=lambda d: -len(d[0]))
    for path in args:
        full = os.path.join(root, path)
        owner = next((i for d, i in dirs if full == d or full.startswith(d + os.sep)), None)
        if owner is None:
            # Outside every crate (the manifests, scripts, docs): only the whole gate says.
            print("!" + path)
        else:
            start.add(owner)
users = {i: set() for i in packages}
for node in meta["resolve"]["nodes"]:
    if node["id"] in packages:
        for dep in node["deps"]:
            if dep["pkg"] in packages:
                users[dep["pkg"]].add(node["id"])
seen, todo = set(start), list(start)
while todo:
    for user in users[todo.pop()]:
        if user not in seen:
            seen.add(user)
            todo.append(user)
for i in sorted(seen, key=lambda i: packages[i]["name"]):
    lib = any(k in ("lib", "rlib", "staticlib", "cdylib", "dylib", "proc-macro")
              for t in packages[i]["targets"] for k in t["kind"])
    print(packages[i]["name"] + (" lib" if lib else ""))
' "$@")

outside=()
selected=()
libraries=()
for line in "${crates[@]}"; do
  case "$line" in
    "!"*) outside+=("${line#!}") ;;
    *" lib")
      selected+=("${line% lib}")
      libraries+=(-p "${line% lib}")
      ;;
    *) selected+=("$line") ;;
  esac
done
if [ "${#outside[@]}" -gt 0 ]; then
  echo "changed outside any crate, which only the whole gate checks:"
  printf '    %s\n' "${outside[@]}"
fi
[ "${#selected[@]}" -gt 0 ] || { echo "no crate to check"; exit 0; }
echo "checking ${#selected[@]} crates: ${selected[*]}"
packages=()
for crate in "${selected[@]}"; do
  packages+=(-p "$crate")
done

step format cargo fmt --check "${packages[@]}"
# The feature matrix has target directories of its own: it runs beside the rest.
matrix_log=$(mktemp)
trap 'rm -f "$matrix_log"' EXIT
start_matrix=$SECONDS
scripts/feature-matrix.sh "${selected[@]}" > "$matrix_log" 2>&1 &
matrix=$!
step clippy cargo clippy "${packages[@]}" --all-targets --all-features -- -D warnings
if cargo nextest --version > /dev/null 2>&1; then
  step tests cargo nextest run "${packages[@]}" --all-features --no-tests=pass
  if [ "${#libraries[@]}" -gt 0 ]; then
    step doctests cargo test "${libraries[@]}" --all-features --doc
  fi
else
  step tests cargo test "${packages[@]}" --all-features
fi
echo "==> feature matrix"
if wait "$matrix"; then
  tail -n 1 "$matrix_log"
  echo "    feature matrix: $((SECONDS - start_matrix)) s"
else
  cat "$matrix_log"
  exit 1
fi
echo "the checks of these crates passed; the whole gate still runs before a merge"
