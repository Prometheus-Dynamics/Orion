#!/usr/bin/env python3
"""Whether checking crates together builds each exactly as checking it alone does.

    same-features.py STAMPS FLAGS TARGET... -- GROUP...

A GROUP is crate names joined by commas. `cargo check -p A -p B` resolves features once for
the whole graph: a package A builds with no features may get some because B asks for them,
and then A is checked as a build it never has alone. For each TARGET, with FLAGS (one string,
such as `--no-default-features`), every package each crate of a group builds must have the
same features in the group's resolution as in that crate's own: otherwise this names them and
fails.

The answer depends only on the manifests, the lockfile, the toolchain, the flags, the targets
and the groups, so a check that passed is stamped under STAMPS with a key over those (and this
script), and not made again until one of them changes. Without the stamp (CI) it is made every
time.
"""

import glob
import hashlib
import os
import subprocess
import sys


def key(root, flags, targets, groups):
    sha = hashlib.sha256()
    files = ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", os.path.relpath(__file__, root)]
    files += sorted(glob.glob("crates/*/*/Cargo.toml", root_dir=root))
    for name in files:
        sha.update(name.encode() + b"\0")
        try:
            with open(os.path.join(root, name), "rb") as file:
                sha.update(file.read())
        except OSError:
            sha.update(b"<none>")
    version = subprocess.run(["cargo", "-V"], capture_output=True, text=True, cwd=root).stdout
    sha.update("\0".join([version, flags, *targets, "--", *groups]).encode())
    return sha.hexdigest()


def resolved(root, target, flags, crates):
    """Each package the crates build (normal and build dependencies), and its features."""
    args = ["cargo", "tree", "-q", "--locked", "--target", target, *flags.split()]
    args += ["-e", "normal,build", "-f", "{p}|{f}", "--prefix", "none"]
    for crate in crates:
        args += ["-p", crate]
    out = subprocess.run(args, capture_output=True, text=True, cwd=root, check=True).stdout
    found = {}
    for line in out.splitlines():
        line = line.removesuffix(" (*)").strip()
        if "|" in line:
            package, features = line.split("|", 1)
            found[package] = features
    return found


def main(argv):
    if "--" not in argv or argv.index("--") < 3:
        print("usage: same-features.py STAMPS FLAGS TARGET... -- GROUP...", file=sys.stderr)
        return 2
    split = argv.index("--")
    stamps, flags, targets, groups = argv[0], argv[1], argv[2:split], argv[split + 1 :]
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    stamp = os.path.join(stamps, key(root, flags, targets, groups))
    if os.path.exists(stamp):
        print("    the same features together as alone (checked for these manifests before)")
        return 0
    differ = 0
    for target in targets:
        for group in groups:
            crates = group.split(",")
            if len(crates) < 2:
                continue
            together = resolved(root, target, flags, crates)
            for crate in crates:
                for package, features in resolved(root, target, flags, [crate]).items():
                    if together.get(package) != features:
                        differ += 1
                        print(
                            f"    {target}: {crate} builds {package} with [{features}] alone, "
                            f"[{together.get(package)}] beside {', '.join(c for c in crates if c != crate)}"
                        )
    if differ:
        print(f"    {differ} differ: check those crates apart, or in another group")
        return 1
    os.makedirs(stamps, exist_ok=True)
    open(stamp, "w").close()
    print("    the same features together as alone")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
