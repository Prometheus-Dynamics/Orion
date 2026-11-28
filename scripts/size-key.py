#!/usr/bin/env python3
"""Content keys for the size gate's builds (scripts/size-gate.sh).

A build's key is a hash of everything that can change the bytes it makes:

- every file, by content, in the directory of each workspace crate in the package's dependency
  closure (normal and build dependencies, on every platform: a superset, never less). Tracked
  files and new ones; files git ignores are not read by any build here;
- `Cargo.lock` (every registry and git dependency, by version and checksum or revision), the
  root `Cargo.toml` (profiles, features, patches, workspace dependencies), `rust-toolchain.toml`,
  every cargo config file cargo reads (`.cargo/config.toml` here and in each directory above it,
  and in CARGO_HOME), and where CARGO_HOME is (a dependency's paths are in its panic messages);
- this script and scripts/size-gate.sh (what it builds, and how), so a change to either
  measures everything again;
- the toolchain (`rustc -vV`, `cargo -V`), the package, the target, and the C compiler and
  linker: `cc --version` for this machine, zig's version and the wrapper scripts for a board;
- every environment variable a build or a build script reads by convention (CARGO_*, RUST*,
  CC*, CXX*, AR*, CFLAGS*, LDFLAGS*, TARGET_*, HOST_*, ZIG*, PKG_CONFIG*, SOURCE_DATE_EPOCH),
  but CARGO_TARGET_DIR, with the worktree's path written as <root>.

The worktree's path itself is not in the key: the gate's self-test and every re-measure check
that a build's bytes do not depend on it.

    size-key.py ROOT NAME:PACKAGE:TARGET...    one line per build: NAME KEY
    size-key.py --explain ROOT NAME:PACKAGE:TARGET
                                               each input and its hash, then the key

TARGET is `host` for a build for this machine.
"""

import hashlib
import json
import os
import re
import subprocess
import sys

# Raised when what goes into a key changes, so no older entry is read as a newer one.
KEY_VERSION = "1"

ENV = re.compile(
    r"^(CARGO_|RUST|CC|CXX|AR(_|$)|CFLAGS|CXXFLAGS|LDFLAGS|TARGET_|HOST_|ZIG|PKG_CONFIG|SOURCE_DATE_EPOCH)"
)
# Where builds go, and how many jobs run at once (the jobserver a shared-jobs gate hands every
# cargo): neither changes what is built. Nor where the crates are read from (the gate's own Cargo
# home and each lane's copy of it, scripts/cargo-home.sh): the lockfile holds every crate's
# checksum, and the home's config is keyed by content (shown_config). A measured size is the
# exception: shipped binaries hold the paths of the crate sources they were built from, so the
# size key holds the home's path as well (Workspace.shared).
NOT_IN_KEY = {"CARGO_TARGET_DIR", "CARGO_MAKEFLAGS", "CARGO_HOME"}


def run(args, cwd):
    try:
        return subprocess.run(args, cwd=cwd, capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        return f"<{args[0]} failed: {error}>"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_digest(path):
    try:
        with open(path, "rb") as file:
            return digest(file.read())
    except FileNotFoundError:
        return "absent"


def cargo_home():
    return os.environ.get("CARGO_HOME") or os.path.join(os.path.expanduser("~"), ".cargo")


def cargo_configs(root):
    """Every cargo config file cargo reads for a build in ROOT, nearest first."""
    found = []
    here = os.path.abspath(root)
    while True:
        for name in ("config.toml", "config"):
            path = os.path.join(here, ".cargo", name)
            if os.path.isfile(path):
                found.append(path)
        parent = os.path.dirname(here)
        if parent == here:
            break
        here = parent
    for name in ("config.toml", "config"):
        path = os.path.join(cargo_home(), name)
        if os.path.isfile(path) and path not in found:
            found.append(path)
    return found


def shown_config(path, root):
    """A cargo config's path as a key shows it: from the root, or from the Cargo home, whichever
    holds it, so the same config read from another copy of the home is the same input."""
    home = os.path.join(cargo_home(), "")
    if path.startswith(home):
        return "<CARGO_HOME>/" + path[len(home):]
    return path.replace(root, "<root>", 1) if path.startswith(root) else path


class Workspace:
    def __init__(self, root):
        self.root = os.path.abspath(root)
        # Not --offline: on a machine that has not fetched the dependencies yet (a fresh CI
        # runner) cargo fetches what the lockfile names, as the build after it would. A key is
        # never made from a failed listing.
        listing = subprocess.run(
            ["cargo", "metadata", "--locked", "--format-version", "1"],
            cwd=self.root,
            capture_output=True,
            text=True,
        )
        if listing.returncode != 0:
            sys.exit(f"size-key.py: cargo metadata failed:\n{listing.stderr}")
        meta = json.loads(listing.stdout)
        self.packages = {p["id"]: p for p in meta["packages"]}
        self.members = set(meta["workspace_members"])
        self.by_name = {self.packages[i]["name"]: i for i in self.members}
        self.deps = {}
        for node in meta["resolve"]["nodes"]:
            self.deps[node["id"]] = [
                dep["pkg"]
                for dep in node["deps"]
                if any(kind["kind"] != "dev" for kind in dep["dep_kinds"])
            ]
        self.digests = {}
        self.shared_cache = None
        self.listing = None

    def closure(self, package):
        start = self.by_name[package]
        seen, todo = set(), [start]
        while todo:
            current = todo.pop()
            if current in seen:
                continue
            seen.add(current)
            todo.extend(self.deps.get(current, []))
        return sorted(
            os.path.relpath(os.path.dirname(self.packages[i]["manifest_path"]), self.root)
            for i in seen
            if i in self.members
        )

    def listed(self):
        """Every file git sees (tracked, or untracked and not ignored), listed once."""
        if self.listing is None:
            listed = subprocess.run(
                ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                cwd=self.root,
                capture_output=True,
                check=True,
            ).stdout
            self.listing = sorted({name.decode() for name in listed.split(b"\0") if name})
        return self.listing

    def files(self, directory):
        # One listing for every directory: a git run for each of a closure's directories was
        # most of a key's time (298 of them for five builds, 3.9 s of 6.1).
        prefix = directory.rstrip("/") + "/"
        names = [name for name in self.listed() if name.startswith(prefix)]
        out = []
        for name in names:
            if name not in self.digests:
                self.digests[name] = file_digest(os.path.join(self.root, name))
            out.append((name, self.digests[name]))
        return out

    def shared(self):
        if self.shared_cache is None:
            inputs = [("key version", KEY_VERSION)]
            # size-gate.sh builds what is measured (its profiles, flags and toolchains): a change
            # to it measures everything again.
            names = ("Cargo.lock", "Cargo.toml", "rust-toolchain.toml", "scripts/size-key.py", "scripts/size-gate.sh")
            for name in names:
                inputs.append((name, file_digest(os.path.join(self.root, name))))
            for path in cargo_configs(self.root):
                inputs.append((f"cargo config {shown_config(path, self.root)}", file_digest(path)))
            inputs.append(("CARGO_HOME", cargo_home()))
            inputs.append(("rustc -vV", digest(run(["rustc", "-vV"], self.root).encode())))
            inputs.append(("cargo -V", digest(run(["cargo", "-V"], self.root).encode())))
            env = sorted(
                (name, value.replace(self.root, "<root>"))
                for name, value in os.environ.items()
                if ENV.match(name) and name not in NOT_IN_KEY
            )
            inputs.append(("environment", digest(json.dumps(env).encode())))
            self.shared_cache = inputs
        return self.shared_cache

    def inputs(self, package, target):
        inputs = list(self.shared())
        inputs.append(("package", package))
        inputs.append(("target", target))
        if target == "host":
            inputs.append(("cc --version", digest(run(["cc", "--version"], self.root).encode())))
        else:
            inputs.append(("zig version", run(["zig", "version"], self.root).strip()))
            for suffix in ("", "-ar"):
                wrapper = os.path.join("scripts", "zigcc", target + suffix)
                inputs.append((wrapper, file_digest(os.path.join(self.root, wrapper))))
        for directory in self.closure(package):
            for name, file_hash in self.files(directory):
                inputs.append((name, file_hash))
        return inputs

    def key(self, package, target):
        return digest(json.dumps(self.inputs(package, target)).encode())


def main(argv):
    explain = bool(argv) and argv[0] == "--explain"
    if explain:
        argv = argv[1:]
    if len(argv) < 2:
        print(__doc__.strip().splitlines()[-6], file=sys.stderr)
        return 2
    workspace = Workspace(argv[0])
    for spec in argv[1:]:
        name, package, target = spec.split(":")
        if explain:
            for label, value in workspace.inputs(package, target):
                print(f"{label}\t{value}")
        print(name, workspace.key(package, target))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
