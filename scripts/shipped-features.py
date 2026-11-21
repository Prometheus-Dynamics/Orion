#!/usr/bin/env python3
"""The features a shipped binary builds a crate with, as cargo arguments.

    shipped-features.py BIN CRATE

Prints `--features A,B`, after `--no-default-features` when BIN turns the defaults off, as
BIN's manifest asks for CRATE. The gate tests CRATE that way too, beside `--all-features`:
a build that leaves features out is the one shipped, and only it can show a test or a check
that holds only with them all. Read from the manifests, so a change to what is shipped
changes what is tested with it.
"""

import json
import subprocess
import sys


def main(argv):
    if len(argv) != 2:
        print("usage: shipped-features.py BIN CRATE", file=sys.stderr)
        return 2
    binary, crate = argv
    meta = json.loads(
        subprocess.run(
            ["cargo", "metadata", "--locked", "--offline", "--no-deps", "--format-version", "1"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )
    for package in meta["packages"]:
        if package["name"] != binary:
            continue
        for dep in package["dependencies"]:
            if dep["name"] == crate and dep["kind"] is None:
                args = [] if dep["uses_default_features"] else ["--no-default-features"]
                if dep["features"]:
                    args += ["--features", ",".join(dep["features"])]
                print(" ".join(args))
                return 0
        print(f"{binary} does not depend on {crate}", file=sys.stderr)
        return 1
    print(f"no package {binary}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
