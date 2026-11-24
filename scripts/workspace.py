#!/usr/bin/env python3
"""What the scripts ask of the workspace's crates, from `cargo metadata`. A file of its own, not
`python3 -c '...'` in a shell script: there a quote in the program ends it early, and a program
cut short can print nothing and look like a workspace with nothing in it. Each command fails,
saying so, when what it finds is empty.

    workspace.py members                 every crate: NAME, then `lib` if it has a library
    workspace.py matrix [CRATE...]       one line per feature-matrix build: CRATE FLAGS...
    workspace.py affected --crates C...  the crates named and every workspace crate using them,
    workspace.py affected --paths P...   or owning a changed path, as `members` prints them;
                                         a path outside every crate printed as !PATH
    workspace.py featured CRATE...       of the crates named, those with optional features;
                                         prints `none` when there is none
    workspace.py doctested [CRATE...]    of the crates with a library (all, or those named),
                                         those whose documentation holds Rust that rustdoc
                                         runs as a test; prints `none` when there is none
    workspace.py lib-tests               fails unless each library is built for its unit
                                         tests exactly when it has some: `[lib] test = false`
                                         on one with none, and never on one with any

A library's unit tests are those in its own sources (`#[test]`, `#[tokio::test]` and the like,
or `cfg(test)`). Built for them, a library with none is compiled again, in test mode, to run
nothing: 49 crates were, 7% of a core change's compile time. Marked `test = false`, one that
gains a test would never run it; so this fails both ways.

A crate's doctests are the code blocks in its library's doc comments that rustdoc compiles: a
fence with no language, or one rustdoc takes for Rust (`rust`, `no_run`, `should_panic`,
`compile_fail`, `edition...`). `ignore`, `text` and other languages are not run. A crate with
none has a known result, no test, and its rustdoc pass is not made.
"""

import re

import json
import os
import subprocess
import sys

LIBRARY = ("lib", "rlib", "staticlib", "cdylib", "dylib", "proc-macro")


def metadata(deps):
    command = ["cargo", "metadata", "--locked", "--format-version", "1"]
    if not deps:
        command.append("--no-deps")
    listing = subprocess.run(command, capture_output=True, text=True)
    if listing.returncode != 0:
        sys.exit(f"workspace.py: cargo metadata failed:\n{listing.stderr}")
    return json.loads(listing.stdout)


def line(package):
    lib = any(kind in LIBRARY for target in package["targets"] for kind in target["kind"])
    return package["name"] + (" lib" if lib else "")


def members():
    meta = metadata(deps=False)
    return [line(p) for p in sorted(meta["packages"], key=lambda p: p["name"])]


def matrix(only):
    meta = metadata(deps=False)
    names = {p["name"] for p in meta["packages"]}
    unknown = sorted(set(only) - names)
    if unknown:
        sys.exit(f"workspace.py: no workspace crate {', '.join(unknown)}")
    out = []
    for package in meta["packages"]:
        name = package["name"]
        features = sorted(f for f in package["features"] if f != "default")
        if not features or (only and name not in only):
            continue
        out.append(f"{name} --no-default-features")
        out += [f"{name} --no-default-features --features {feature}" for feature in features]
        out.append(f"{name} --all-features")
    return out


def affected(mode, args):
    meta = metadata(deps=True)
    ids = set(meta["workspace_members"])
    packages = {p["id"]: p for p in meta["packages"] if p["id"] in ids}
    root = meta["workspace_root"]
    start, out = set(), []
    if mode == "--crates":
        names = {p["name"]: i for i, p in packages.items()}
        for name in args:
            if name not in names:
                sys.exit(f"workspace.py: no workspace crate {name}")
            start.add(names[name])
    elif mode == "--paths":
        dirs = sorted(
            ((os.path.dirname(p["manifest_path"]), i) for i, p in packages.items()),
            key=lambda d: -len(d[0]),
        )
        for path in args:
            full = os.path.join(root, path)
            owner = next((i for d, i in dirs if full == d or full.startswith(d + os.sep)), None)
            if owner is None:
                # Outside every crate (the manifests, scripts, docs): only the whole gate says.
                out.append("!" + path)
            else:
                start.add(owner)
    else:
        sys.exit(__doc__)
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
    out += [line(packages[i]) for i in sorted(seen, key=lambda i: packages[i]["name"])]
    return out


RUST_FENCE = re.compile(r"^(rust|no_run|should_panic|compile_fail|edition\d+|test_harness|standalone_crate)$")
DOC_LINE = re.compile(r"^\s*(//!|///)\s?(.*)$")


def runs_doctests(directory):
    """Whether any doc comment under DIRECTORY/src opens a fence rustdoc runs."""
    for base, _, files in os.walk(os.path.join(directory, "src")):
        for name in files:
            if not name.endswith(".rs"):
                continue
            inside = False
            with open(os.path.join(base, name), errors="replace") as file:
                for text in file:
                    doc = DOC_LINE.match(text)
                    if not doc:
                        continue
                    body = doc.group(2).strip()
                    if not body.startswith(("```", "~~~")):
                        continue
                    if inside:
                        inside = False
                        continue
                    inside = True
                    info = [word for word in re.split(r"[,\s]+", body.lstrip("`~")) if word]
                    if "ignore" in info:
                        continue
                    if not info or all(RUST_FENCE.match(word) for word in info):
                        return True
    return False


def doctested(only):
    meta = metadata(deps=False)
    names = {p["name"] for p in meta["packages"]}
    unknown = sorted(set(only) - names)
    if unknown:
        sys.exit(f"workspace.py: no workspace crate {', '.join(unknown)}")
    out = []
    for package in sorted(meta["packages"], key=lambda p: p["name"]):
        if only and package["name"] not in only:
            continue
        if not line(package).endswith(" lib"):
            continue
        if runs_doctests(os.path.dirname(package["manifest_path"])):
            out.append(package["name"])
    return out or ["none"]


def featured(only):
    meta = metadata(deps=False)
    names = {p["name"] for p in meta["packages"]}
    unknown = sorted(set(only) - names)
    if unknown:
        sys.exit(f"workspace.py: no workspace crate {', '.join(unknown)}")
    out = sorted(
        p["name"] for p in meta["packages"]
        if p["name"] in only and any(f != "default" for f in p["features"])
    )
    return out or ["none"]


UNIT_TEST = re.compile(r"#\[[\w:]*test\b|cfg(_attr)?\(\s*test\b")


def has_unit_tests(source):
    """Whether any .rs file in the library's directory (its src_path's) holds a unit test."""
    for base, _, files in os.walk(os.path.dirname(source)):
        for name in files:
            if name.endswith(".rs"):
                with open(os.path.join(base, name), errors="replace") as file:
                    if UNIT_TEST.search(file.read()):
                        return True
    return False


def lib_tests():
    meta = metadata(deps=False)
    wrong = []
    checked = 0
    for package in sorted(meta["packages"], key=lambda p: p["name"]):
        for target in package["targets"]:
            if not any(kind in LIBRARY for kind in target["kind"]):
                continue
            checked += 1
            tested = target.get("test", True)
            has = has_unit_tests(target["src_path"])
            if tested and not has:
                wrong.append(f"{package['name']}: no unit test, but built for them: add `[lib] test = false`")
            elif has and not tested:
                wrong.append(f"{package['name']}: has unit tests, never run (`[lib] test = false`): remove it")
    if checked == 0:
        print("workspace.py: no library: nothing would be checked", file=sys.stderr)
        return 1
    if wrong:
        print("\n".join(wrong), file=sys.stderr)
        return 1
    print(f"each of {checked} libraries is built for unit tests exactly when it has some")
    return 0


def main(argv):
    if argv == ["lib-tests"]:
        return lib_tests()
    if argv == ["members"]:
        out, what = members(), "no workspace crate"
    elif argv[:1] == ["matrix"]:
        out, what = matrix(argv[1:]), "no crate with features" + (f" among {' '.join(argv[1:])}" if argv[1:] else "")
    elif argv[:1] == ["featured"] and len(argv) >= 2:
        out, what = featured(argv[1:]), ""
    elif argv[:1] == ["doctested"]:
        out, what = doctested(argv[1:]), ""
    elif argv[:1] == ["affected"] and len(argv) >= 3:
        out, what = affected(argv[1], argv[2:]), "nothing affected"
    else:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    if not out:
        # Empty is never an answer: a listing that found nothing checked nothing.
        print(f"workspace.py: {what}: nothing would be checked", file=sys.stderr)
        return 1
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
