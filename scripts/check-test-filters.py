#!/usr/bin/env python3
"""Every filter in .config/nextest.toml matches at least one test, and every test file is built.

A filter that matches nothing says nothing: a test renamed, or moved into a binary it shares,
loses its priority or its test group without a word. Each `test(...)`, `package(...)`,
`binary(...)` and `binary_id(...)` in each override's filter must match a test in the list, and
so must the filter as a whole when it holds only those joined by `|`.

A crate whose test files share one binary (`autotests = false`, tests/all.rs) builds only the
files it names: a new tests/<file>.rs left out of all.rs would never build or run. Each
tests/*.rs in such a crate must be named by a `#[path = "<file>"]` in tests/all.rs, or be a
`[[test]]` of its own.

    check-test-filters.py LIST.json     LIST.json from `cargo nextest list --message-format json`
    check-test-filters.py --files       every test file of a crate without autotests is built
    check-test-filters.py --self-test   a filter naming a missing test fails, and a test file
                                        left out of tests/all.rs fails; the rest pass
"""

import fnmatch
import glob
import json
import os
import re
import sys
import tempfile
import tomllib

ATOM = re.compile(r"\b(test|package|binary_id|binary)\(([^()]*)\)")
DEFAULT = {"test": "~", "package": "glob", "binary": "glob", "binary_id": "glob"}


def tests_of(listing):
    """(package, binary, binary id, test name) for every test in a nextest list."""
    return [
        (suite["package-name"], suite["binary-name"], binary_id, name)
        for binary_id, suite in listing["rust-suites"].items()
        for name in suite["testcases"]
    ]


def matches(kind, pattern, test):
    package, binary, binary_id, name = test
    value = {"test": name, "package": package, "binary": binary, "binary_id": binary_id}[kind]
    if pattern.startswith("="):
        return value == pattern[1:]
    if pattern.startswith("~"):
        return pattern[1:] in value
    if pattern.startswith("/") and pattern.endswith("/") and len(pattern) > 1:
        return re.search(pattern[1:-1], value) is not None
    if pattern.startswith("#"):
        return fnmatch.fnmatchcase(value, pattern[1:])
    if DEFAULT[kind] == "~":
        return pattern in value
    return fnmatch.fnmatchcase(value, pattern)


def check(config, tests):
    """The problems: one line for each filter or part of one that matches no test."""
    problems = []
    for profile_name, profile in config.get("profile", {}).items():
        for number, override in enumerate(profile.get("overrides", []), 1):
            text = override.get("filter")
            if text is None:
                continue
            where = f"profile.{profile_name}.overrides #{number}"
            atoms = ATOM.findall(text)
            if not atoms:
                problems.append(f"{where}: no test(...), package(...) or binary(...) to check in {text.strip()!r}")
                continue
            for kind, pattern in atoms:
                if not any(matches(kind, pattern.strip(), test) for test in tests):
                    problems.append(f"{where}: {kind}({pattern}) matches no test")
    return problems


def unbuilt(root):
    """Test files in crates without autotests that nothing builds."""
    problems = []
    for manifest in sorted(glob.glob(os.path.join(root, "crates", "*", "*", "Cargo.toml"))):
        with open(manifest, "rb") as file:
            package = tomllib.load(file)
        if package.get("package", {}).get("autotests", True):
            continue
        crate = os.path.dirname(manifest)
        built = set()
        for target in package.get("test", []):
            path = target.get("path", os.path.join("tests", target["name"] + ".rs"))
            built.add(os.path.normpath(os.path.join(crate, path)))
            if os.path.isfile(os.path.join(crate, path)):
                with open(os.path.join(crate, path)) as file:
                    for named in re.findall(r'#\[path\s*=\s*"([^"]+)"\]', file.read()):
                        built.add(os.path.normpath(os.path.join(crate, os.path.dirname(path), named)))
        for test_file in sorted(glob.glob(os.path.join(crate, "tests", "*.rs"))):
            if os.path.normpath(test_file) not in built:
                problems.append(f"{os.path.relpath(test_file, root)} is built by nothing: name it in tests/all.rs")
    return problems


def self_test():
    tests = [
        ("orion-a", "all", "orion-a::all", "sink::moves_a_file"),
        ("orion-b", "verbs", "orion-b::verbs", "put_and_remove"),
    ]

    def config(filter_text):
        return {"profile": {"default": {"overrides": [{"filter": filter_text, "priority": 1}]}}}

    failures = []
    for text in [
        "test(=sink::moves_a_file) | test(=put_and_remove)",
        "test(~moves) | test(/^put_/) | package(orion-a) | binary(verbs) | binary_id(=orion-b::verbs)",
    ]:
        if check(config(text), tests):
            failures.append(f"{text!r} failed: {check(config(text), tests)}")
    # Planted: a name that is not there, and the old name of a test now under its module.
    for text in ["test(=put_and_remove) | test(=no_such_test)", "test(=moves_a_file)", "package(orion-c)"]:
        if not check(config(text), tests):
            failures.append(f"{text!r} passed, and names no test")
    # A crate whose files share a binary: one file named, one planted and left out.
    with tempfile.TemporaryDirectory() as root:
        crate = os.path.join(root, "crates", "x", "y")
        os.makedirs(os.path.join(crate, "tests"))
        with open(os.path.join(crate, "Cargo.toml"), "w") as file:
            file.write('[package]\nautotests = false\nname = "y"\n\n[[test]]\nname = "all"\npath = "tests/all.rs"\n')
        for name, text in [("all.rs", '#[path = "one.rs"]\nmod one;\n'), ("one.rs", ""), ("two.rs", "")]:
            with open(os.path.join(crate, "tests", name), "w") as file:
                file.write(text)
        found = unbuilt(root)
        if [p for p in found if "two.rs" in p] != found or not found:
            failures.append(f"a test file left out of all.rs: {found}")
        os.remove(os.path.join(crate, "tests", "two.rs"))
        if unbuilt(root):
            failures.append(f"every file named, yet: {unbuilt(root)}")
    if failures:
        print("self-test FAILED:", *failures, sep="\n  ")
        return 1
    print("test filters self-test passed")
    return 0


def main(argv):
    if argv == ["--self-test"]:
        return self_test()
    if argv == ["--files"]:
        problems = unbuilt(".")
        for problem in problems:
            print(f"FAIL: {problem}")
        if problems:
            return 1
        print("every test file is built")
        return 0
    if len(argv) != 1:
        print(__doc__.strip().splitlines()[-2], file=sys.stderr)
        return 2
    with open(".config/nextest.toml", "rb") as file:
        config = tomllib.load(file)
    with open(argv[0]) as file:
        tests = tests_of(json.load(file))
    problems = check(config, tests)
    for problem in problems:
        print(f"FAIL: .config/nextest.toml {problem}")
    if problems:
        return 1
    print(f"every nextest filter matches a test ({len(tests)} tests)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
