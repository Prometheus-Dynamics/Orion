#!/usr/bin/env python3
"""Which items of the gate's build lanes passed before on exactly these inputs, and need not run.

    lane-reuse.py plan LANE STATE ITEM...    key each ITEM; STATE gets one line each: ITEM, key,
                                             `run` or `reused`. Prints what is reused.
    lane-reuse.py record STATE ITEM          ITEM passed: record its key (the gate only, after it
                                             passed)
    lane-reuse.py status LANE STATE          `passed`, or `passed(K-of-N-reused)`; and,
                                             if every item ran, records that the lane ran here
    lane-reuse.py behind [LANE...]           per lane, the commit it last ran in full at, and
                                             how far this checkout is past it
    lane-reuse.py --explain LANE ITEM        each input of ITEM's key
    lane-reuse.py binary-plan STATE ARTIFACTS
                                             the tests lane: of the items STATE has to run,
                                             those whose test binaries (ARTIFACTS: cargo's JSON
                                             of their build) passed before are reused; their
                                             binary keys go to STATE.bin
    lane-reuse.py binary-plan-item LANE STATE ITEM -- EXE...
                                             one item keyed by its binaries (the fuzz targets)
    lane-reuse.py own-vars                   fails if a Rust source (a build script, a test)
                                             reads one of the gate's own variables
    lane-reuse.py --self-test                in a copy of this tree, changes each kind of input
                                             and checks a key moves (or, for a note or a crate
                                             not built, that it does not)

Only lanes whose result is a function of their inputs are here: checks and builds (the feature
matrix, the no_std crates and the client on other systems, the device sizes). A lane that runs
tests runs every time. An item's key is a hash of everything its result depends on:

- every file, by content, of each workspace crate it builds (normal and build dependencies on
  every platform, and the dev-dependencies of a crate it lints with its tests);
- the lockfile's entry of each package it builds, so a dependency's change reruns only what
  builds it;
- the root manifest, the toolchain file, cargo's config files, the toolchain (`rustc -vV`,
  `cargo -V`, and clippy's for a lint), the C compiler, and the environment a build reads
  (scripts/size-key.py's list, `CARGO_INCREMENTAL` among them). A variable that names a file (a
  path, or a tool such as `RUSTC_WRAPPER` found on PATH) is keyed by that file's content too.
  The gate's own variables (`GATE_*`, `SIZE_GATE_*`) are never in a key, and `own-vars` checks
  that nothing built reads them;
- the item itself (its command line), and the scripts that run and judge it: scripts/gate.sh,
  this script, scripts/size-key.py, and the lane's own (feature-matrix.sh, same-features.py,
  device-size.sh). A change to any reruns the whole lane;
- for the device sizes, the stm32g4 example's files and the nightly that measures its stack.

The worktree's path is not in a key: the device sizes come out the same bytes from two target
directories (I45), and a key names inputs, not where they are built. So records are kept in the
repository's common git directory, shared by its worktrees: `orion-lane-records/LANE/KEY`, each
holding a line with the hash of the scripts above, which an older or hand-made record lacks and
so is never read as a pass.

A lane that selects no item fails (`plan` and `status`), never passes: only `--drop LANE:WHY`
leaves a lane out, and the gate's line says `not-run(WHY)`.

A lane reuses for as long as its inputs are the same: no lane runs in full here for having gone
some commits or days without one. Every run here must end within a minute at any load (the
one-minute rule, 2026-11-24), and a run in full is minutes of work whose result is already known.
What a key could miss is found elsewhere: CI runs every lane in full, with nothing reused, on every
push, and scripts/release-check.sh refuses a release that any lane did not pass in full at that
commit. GATE_LANE_REUSE=0 runs everything here. An item with no key (a kind this script does not
know) runs every time and is named; each lane says how many of its items had no key or were not
reused. `behind` still says when each lane last ran in full here.
"""

import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import time

FORMAT = "orion-lane-record 1"
LANES = ("matrix", "other-systems", "devices", "tests", "shipped", "clippy", "fuzz")
LANE_SCRIPTS = {
    "matrix": ["scripts/feature-matrix.sh"],
    "other-systems": ["scripts/same-features.py"],
    "devices": ["scripts/device-size.sh"],
    "tests": [".config/nextest.toml"],
    "shipped": [".config/nextest.toml", "scripts/shipped-features.py"],
    "clippy": [],
    "fuzz": ["scripts/fuzz-short.sh"],
}
# What a test may run that the machine has, not the repository: a test's key holds each one's
# content, so a new version runs the tests again.
PROGRAMS = (
    "sh", "bash", "unshare", "setpriv", "ip", "id", "kill", "ssh", "ssh-keygen", "scp", "sftp",
    "sshd", "bwrap", "readelf", "nm", "cc", "firefox", "chromium", "chromium-browser",
    "google-chrome",
)
# The notes: what scripts/code-key.sh leaves out. No build or test reads one.
NOTE = re.compile(r"(\.md$|^docs/|^LICENSE-[^/]*$)")
# The variables a test reads that the build does not: the settings of the programs tests start
# (ORION_..., and the markers a test sets for itself when it runs itself again), whether the
# namespace tests must run, and the rest named below. A test run's key holds their values;
# `reads` fails on a variable a crate reads that neither this nor the build's own list covers.
TEST_ENV = re.compile(r"^(ORION_|NAMESPACE_TESTS$|UPDATE_VECTORS$|DRILL_|WATCHDOG_|NOTIFY_SOCKET$)")
# The gate's own variables: where it keeps its state, how it scales its bounds, whether it reuses.
# None changes what a build makes, and each gate sets some afresh, so none is in a key.
GATE_OWN = ("GATE_", "SIZE_GATE_")
# Variables that name a program, found on PATH when not given as a path.
TOOLS = {"RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "RUSTDOC", "CARGO", "CC", "CXX", "AR"}
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_size_key():
    spec = importlib.util.spec_from_file_location("size_key", os.path.join(ROOT, "scripts", "size-key.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


size_key = load_size_key()
digest, file_digest, run = size_key.digest, size_key.file_digest, size_key.run


def load_elf_key():
    spec = importlib.util.spec_from_file_location("elf_key", os.path.join(ROOT, "scripts", "elf-alloc-key.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Keys by what runs. A test binary's allocated sections (scripts/elf-alloc-key.py) are the code
# and data it runs with: a change that moves none of them (a comment, a doc line, code no test
# binary links) cannot change what its tests do. So a lane item whose source key moved is keyed
# again by its binaries, once built, and reused if those passed before. What a binary reads when
# it runs is in that key too: every file of each crate it builds that is not Rust source (a
# fixture, a page, a manifest), what its package says its tests read, the programs it may start,
# and the environment it reads. A package whose sources may reach outside the crates, or name a
# file by CARGO_MANIFEST_DIR at run time (its own sources among what it may read), is never
# keyed so: only its sources' key reuses it. Kept apart from the source keys: LANE-bin.
BINARY_LANES = {"tests": "tests-bin", "fuzz": "fuzz-bin"}
RUNTIME_SOURCE = re.compile(r"CARGO_MANIFEST_DIR")


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()


def records():
    return os.path.join(git("rev-parse", "--path-format=absolute", "--git-common-dir"), "orion-lane-records")


def scripts_hash():
    names = ["scripts/gate.sh", "scripts/lane-reuse.py", "scripts/size-key.py"]
    return digest("".join(f"{name}\0{file_digest(os.path.join(ROOT, name))}\0" for name in names).encode())


def lock_entries():
    """(name, version) -> that package's entry in Cargo.lock, as written."""
    entries = {}
    with open(os.path.join(ROOT, "Cargo.lock")) as file:
        text = file.read()
    for block in text.split("[[package]]")[1:]:
        fields = dict(
            line.split(" = ", 1) for line in block.strip().splitlines() if " = " in line and not line.startswith(" ")
        )
        entries[(fields.get("name", "").strip('"'), fields.get("version", "").strip('"'))] = block.strip()
    return entries


class Keys(size_key.Workspace):
    def __init__(self):
        # size_key.Workspace's state, from one listing: a listing takes seconds.
        listed_before = os.environ.get("LANE_REUSE_METADATA")
        if listed_before:
            # The self-test only: the listing of a tree whose manifests, lockfile and toolchain
            # file are these, its root written as @ROOT@. Concurrent listings wait on cargo's
            # package-cache lock, one after another (30 at once: 23 s).
            with open(listed_before) as file:
                meta = json.loads(file.read().replace("@ROOT@", ROOT))
        else:
            listing = subprocess.run(
                ["cargo", "metadata", "--locked", "--format-version", "1"],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            if listing.returncode != 0:
                sys.exit(f"lane-reuse.py: cargo metadata failed:\n{listing.stderr}")
            meta = json.loads(listing.stdout)
        self.root = ROOT
        self.packages = {p["id"]: p for p in meta["packages"]}
        self.members = set(meta["workspace_members"])
        self.by_name = {self.packages[i]["name"]: i for i in self.members}
        nodes = meta["resolve"]["nodes"]
        self.deps = {
            node["id"]: [dep["pkg"] for dep in node["deps"] if any(k["kind"] != "dev" for k in dep["dep_kinds"])]
            for node in nodes
        }
        self.with_dev = {node["id"]: [dep["pkg"] for dep in node["deps"]] for node in nodes}
        self.digests = {}
        self.shared_cache = None
        self.lock = lock_entries()
        self.common_cache = None
        # Every file git would see, listed once: a key reads only these, by directory.
        listed = subprocess.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=ROOT,
            capture_output=True,
            check=True,
        ).stdout
        self.listed = sorted({name.decode() for name in listed.split(b"\0") if name})
        self.by_directory = {}

    def files(self, directory):
        if directory not in self.by_directory:
            prefix = directory.rstrip("/") + "/"
            out = []
            for name in self.listed:
                if name.startswith(prefix):
                    if name not in self.digests:
                        self.digests[name] = file_digest(os.path.join(ROOT, name))
                    out.append((name, self.digests[name]))
            self.by_directory[directory] = out
        return self.by_directory[directory]

    def packages_built(self, crates, dev=False):
        """Every package the crates build: theirs, and, with dev, their dev-dependencies'."""
        todo = [self.by_name[crate] for crate in crates]
        if dev:
            todo += [dep for crate in crates for dep in self.with_dev[self.by_name[crate]]]
        seen = set()
        while todo:
            current = todo.pop()
            if current not in seen:
                seen.add(current)
                todo.extend(self.deps.get(current, []))
        return seen

    def common(self):
        if self.common_cache is None:
            self.common_cache = self.common_inputs()
        return list(self.common_cache)

    def common_inputs(self):
        inputs = [("format", FORMAT), ("scripts", scripts_hash())]
        for name in ("Cargo.toml", "rust-toolchain.toml"):
            inputs.append((name, file_digest(os.path.join(ROOT, name))))
        for path in size_key.cargo_configs(ROOT):
            shown = path.replace(ROOT, "<root>", 1) if path.startswith(ROOT) else path
            inputs.append((f"cargo config {shown}", file_digest(path)))
        inputs.append(("CARGO_HOME", size_key.cargo_home()))
        inputs.append(("rustc -vV", digest(run(["rustc", "-vV"], ROOT).encode())))
        inputs.append(("cargo -V", digest(run(["cargo", "-V"], ROOT).encode())))
        inputs.append(("cc --version", digest(run(["cc", "--version"], ROOT).encode())))
        env = sorted((name, value.replace(ROOT, "<root>"), named_file(name, value)) for name, value in key_environment())
        inputs.append(("environment", digest(json.dumps(env).encode())))
        return inputs

    def built(self, packages):
        inputs = []
        for package_id in sorted(packages, key=lambda i: (self.packages[i]["name"], self.packages[i]["version"])):
            package = self.packages[package_id]
            entry = self.lock.get((package["name"], package["version"]), "absent")
            inputs.append((f"Cargo.lock {package['name']} {package['version']}", digest(entry.encode())))
            if package_id in self.members:
                directory = os.path.relpath(os.path.dirname(package["manifest_path"]), ROOT)
                inputs.extend(self.files(directory))
        return inputs

    def inputs(self, lane, item):
        inputs = self.common()
        inputs.append(("lane", lane))
        inputs.append(("item", item))
        for script in LANE_SCRIPTS[lane]:
            inputs.append((script, file_digest(os.path.join(ROOT, script))))
        words = item.split()
        if lane == "matrix":
            # CRATE FLAGS...: `cargo check -p CRATE FLAGS`.
            inputs += self.built(self.packages_built([words[0]]))
        elif lane == "other-systems":
            # check|clippy TARGET CRATE,CRATE...: a no_std group checked, or the client linted
            # with its tests, for TARGET.
            kind, target, crates = words[0], words[1], words[2].split(",")
            inputs.append(("target", target))
            if kind == "clippy":
                if not hasattr(self, "clippy"):
                    self.clippy = run(["cargo", "clippy", "-V"], ROOT).strip()
                inputs.append(("clippy -V", self.clippy))
            inputs += self.built(self.packages_built(crates, dev=kind == "clippy"))
        elif lane == "devices" and item == "device-size":
            inputs += self.built(self.packages_built(["orion-device-probe"]))
            inputs += self.files("examples/stm32g4-device")
            # The nightly device-size.sh measures the stack with: its default, or STACK_TOOLCHAIN.
            with open(os.path.join(ROOT, "scripts", "device-size.sh")) as file:
                default = re.search(r'STACK_TOOLCHAIN:-([^}"]+)', file.read())
            toolchain = os.environ.get("STACK_TOOLCHAIN") or (default.group(1) if default else "nightly")
            stack = run(["rustup", "run", toolchain, "rustc", "-vV"], ROOT)
            inputs.append((f"{toolchain} rustc -vV", digest(stack.encode())))
            tools = run(["rustc", "--print", "sysroot"], ROOT).strip()
            for tool in ("rust-lld", "llvm-size"):
                path = os.path.join(tools, "lib", "rustlib", "x86_64-unknown-linux-gnu", "bin", tool)
                inputs.append((tool, file_digest(path)))
        elif lane == "fuzz":
            # fuzz-short SECONDS: every target, for that budget each. The fuzz crate (its own
            # workspace), every workspace crate it builds, the nightly that builds it and
            # cargo-fuzz.
            inputs += self.files("testing/fuzz")
            with open(os.path.join(ROOT, "testing", "fuzz", "Cargo.toml")) as file:
                named = re.findall(r'^([a-z0-9_-]+)\s*=\s*\{[^}]*path\s*=', file.read(), re.M)
            inputs += self.built(self.packages_built(named))
            inputs.append(("nightly rustc -vV", digest(run(["rustc", "+nightly", "-vV"], ROOT).encode())))
            inputs.append(("cargo fuzz --version", run(["cargo", "fuzz", "--version"], ROOT).strip()))
        elif lane in ("tests", "shipped", "clippy"):
            # PACKAGE [FLAGS...]: its tests run, or it is linted with its tests. Every crate its
            # tests build; every file outside the crates (pages, examples, the programs' own
            # files a test reads); the crates a package says its tests read
            # (`[package.metadata.orion] test-reads`, which `reads` checks); and, for a test
            # run, the programs on this machine it may start.
            package = words[0]
            inputs += self.built(self.packages_built([package], dev=True))
            # Every file outside the crates, for a package whose sources may reach one; for one
            # that names none, nothing outside its crates is read, and a script or a page
            # changed does not run its tests again (reaches_outside).
            if self.reaches_outside(package):
                inputs.append(("outside the crates", self.outside()))
            else:
                inputs.append(("outside the crates", "never reached"))
            for directory in self.test_reads(package):
                inputs.extend(self.files(directory))
            if lane == "clippy":
                if not hasattr(self, "clippy"):
                    self.clippy = run(["cargo", "clippy", "-V"], ROOT).strip()
                inputs.append(("clippy -V", self.clippy))
            else:
                inputs.append(("programs", self.programs()))
                inputs.append(("what the tests read of the environment", sorted(
                    (name, value) for name, value in os.environ.items() if TEST_ENV.match(name)
                )))
        else:
            raise NoKey(f"no key for {lane} item {item!r}")
        return inputs

    def binary_keyable(self, package):
        """Whether PACKAGE's tests can be keyed by their binaries: nothing they read at run time
        is left out of that key."""
        if self.reaches_outside(package):
            return False
        home = os.path.relpath(os.path.dirname(self.packages[self.by_name[package]]["manifest_path"]), ROOT)
        for name, _ in self.files(home):
            if name.endswith(".rs"):
                with open(os.path.join(ROOT, name), errors="replace") as file:
                    if RUNTIME_SOURCE.search(file.read()):
                        return False
        return True

    def binary_inputs(self, lane, item, executables, read):
        """ITEM's key by its EXECUTABLES (paths) and the files READ at run time (name, digest)."""
        if not hasattr(self, "elf_key"):
            self.elf_key = load_elf_key()
        inputs = self.common()
        inputs.append(("lane", BINARY_LANES[lane]))
        inputs.append(("item", item))
        for script in LANE_SCRIPTS[lane] + ["scripts/elf-alloc-key.py"]:
            inputs.append((script, file_digest(os.path.join(ROOT, script))))
        for path in sorted(executables, key=os.path.basename):
            with open(path, "rb") as file:
                inputs.append((f"binary {os.path.basename(path)}", self.elf_key.alloc_key(file.read())))
        inputs += read
        return inputs

    def test_binary_inputs(self, package, executables):
        built = self.built(self.packages_built([package], dev=True))
        read = [(name, value) for name, value in built if not name.endswith(".rs")]
        for directory in self.test_reads(package):
            read.extend(self.files(directory))
        read.append(("programs", self.programs()))
        read.append(("what the tests read of the environment", sorted(
            (name, value) for name, value in os.environ.items() if TEST_ENV.match(name)
        )))
        return self.binary_inputs("tests", package, executables, read)

    def outside(self):
        """One digest of every file outside the crates but the notes and CI's own."""
        if not hasattr(self, "outside_cache"):
            names = [
                name
                for name in self.listed
                if not name.startswith(("crates/", ".github/")) and not NOTE.search(name)
            ]
            self.outside_cache = digest(
                "".join(f"{name}\0{file_digest(os.path.join(ROOT, name))}\0" for name in names).encode()
            )
        return self.outside_cache

    def reaches_outside(self, package):
        """Whether any Rust source of PACKAGE (library, tests, examples, build script) may reach
        a file outside the crates at run time: names a path outside them (a top-level entry
        other than crates/, or `..`), walks up (`.parent()`), finds its own binary or the
        target (`current_exe`, `CARGO_TARGET_TMPDIR`), or runs cargo. Broad on purpose: a
        package flagged wrongly only runs its tests more often. What it reads when compiled is
        held by its `.d` files (`depinfo`)."""
        if not hasattr(self, "reaching"):
            self.reaching = {}
            tops = sorted({
                name.split("/")[0] for name in self.listed
                if not name.startswith("crates/") and not NOTE.search(name)
            })
            self.reach_tops = tops
        if package not in self.reaching:
            home = os.path.relpath(os.path.dirname(self.packages[self.by_name[package]]["manifest_path"]), ROOT)
            found = False
            for name, _ in self.files(home):
                if not name.endswith(".rs"):
                    continue
                with open(os.path.join(ROOT, name), errors="replace") as file:
                    text = file.read()
                if REACH.search(text) or any(reaches(literal, self.reach_tops) for literal in LITERAL.findall(text)):
                    found = True
                    break
            self.reaching[package] = found
        return self.reaching[package]

    def test_reads(self, package):
        """The directories a package's tests read beyond what they build."""
        meta = (self.packages[self.by_name[package]].get("metadata") or {}).get("orion") or {}
        return list(meta.get("test-reads", []))

    def programs(self):
        if not hasattr(self, "programs_cache"):
            found = []
            for name in PROGRAMS:
                path = shutil.which(name) or ""
                found.append((name, path, file_digest(os.path.realpath(path)) if path else ""))
            found.append(("kernel", os.uname().release))
            self.programs_cache = digest(json.dumps(found).encode())
        return self.programs_cache

    def key(self, lane, item):
        try:
            inputs = self.inputs(lane, item)
        except (KeyError, IndexError) as error:
            # A crate not in the workspace, or a line not of the lane's shape: nothing to key.
            raise NoKey(f"no key for {lane} item {item!r}: {error!r}") from error
        return digest(json.dumps(inputs).encode())


class NoKey(Exception):
    pass


def key_environment():
    """The variables a build reads, as (name, value): never the gate's own."""
    return [
        (name, value)
        for name, value in os.environ.items()
        if (size_key.ENV.match(name) or name.startswith(("DEVICE_SIZE_", "STACK_TOOLCHAIN")))
        and name not in size_key.NOT_IN_KEY
        and not name.startswith(GATE_OWN)
    ]


def named_file(name, value):
    """The content digest of the file a variable names, or "" if it names none."""
    path = value if os.path.isabs(value) else os.path.join(ROOT, value)
    if value and not os.path.isfile(path) and name in TOOLS and os.sep not in value:
        path = shutil.which(value) or ""
    return file_digest(path) if value and os.path.isfile(path) else ""


def own_vars():
    """Every place a Rust source names one of the gate's own variables."""
    found = subprocess.run(
        ["git", "grep", "-nE", r"\b(SIZE_)?GATE_[A-Z_]+", "--", "*.rs"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if found:
        print("a Rust source reads the gate's own variables, which no key holds:", file=sys.stderr)
        print(found, file=sys.stderr)
        return 1
    print("no Rust source reads the gate's own variables")
    return 0


def record_path(lane, key):
    return os.path.join(records(), lane, key)


def passed_before(lane, key):
    try:
        with open(record_path(lane, key)) as file:
            return file.read() == f"{FORMAT} {scripts_hash()}\n"
    except OSError:
        return False


def last_full_run(lane):
    """(commit, seconds since) of the lane's last full run here, or None."""
    try:
        with open(os.path.join(records(), f"{lane}.ran")) as file:
            commit, when = file.read().split()
        return commit, time.time() - float(when)
    except (OSError, ValueError):
        return None


def behind(lane):
    """Why the lane must run in full, or None."""
    if os.environ.get("CI"):
        return "on CI"
    if os.environ.get("GATE_LANE_REUSE") == "0":
        return "GATE_LANE_REUSE=0"
    return None


def plan(lane, state, items):
    if not items:
        # A lane that selects nothing checked nothing: it fails, and is never recorded as
        # passed. A lane left out on purpose is `--drop LANE:WHY`, which says not-run.
        sys.exit(f"lane-reuse.py: {lane} selected no item: nothing would be checked")
    keys = Keys()
    why = behind(lane)
    reused = 0
    with open(state, "w") as file:
        for item in items:
            try:
                key = keys.key(lane, item)
            except NoKey:
                # Runs every time, and is never recorded: said, so it is not taken for reuse.
                print(f"    no key: {item} (runs every time)")
                file.write(f"{item}\t-\trun\n")
                continue
            use = why is None and passed_before(lane, key)
            reused += use
            file.write(f"{item}\t{key}\t{'reused' if use else 'run'}\n")
            if use:
                print(f"    reused: {item} (passed before on these inputs)")
    if why:
        print(f"    {lane} runs in full: {why}")
    print(f"    {lane}: {reused} of {len(items)} reused; {len(items) - reused} of {len(items)} items had no key or were not reused")
    return 0


def binary_plan(state, artifacts):
    """The tests lane, after its build: each item to run whose binaries passed before is reused."""
    keys = Keys()
    executables = {}
    with open(artifacts) as file:
        for line in file:
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if message.get("reason") == "compiler-artifact" and message.get("executable"):
                package = keys.packages.get(message["package_id"])
                if package:
                    executables.setdefault(package["name"], set()).add(message["executable"])
    why = behind("tests")
    rows = read_state(state)
    reused = 0
    with open(state + ".bin", "w") as binary:
        for row in rows:
            name, key, how = row
            package = name.split()[0]
            if how != "run" or key == "-" or package not in executables or not keys.binary_keyable(package):
                continue
            inputs = keys.test_binary_inputs(package, executables[package])
            binary_key = digest(json.dumps(inputs).encode())
            use = why is None and passed_before(BINARY_LANES["tests"], binary_key)
            binary.write(f"{name}\t{binary_key}\t{'reused' if use else 'run'}\n")
            if use:
                row[2] = "reused"
                reused += 1
                print(f"    reused: {name} (its test binaries passed before, the same in all they run)")
    with open(state + ".bin.lane", "w") as file:
        file.write(BINARY_LANES["tests"] + "\n")
    with open(state, "w") as file:
        for row in rows:
            file.write("\t".join(row) + "\n")
    print(f"    tests: {reused} more reused by their binaries")
    return 0


def binary_plan_item(lane, state, item, executables):
    """One item keyed by its binaries, and the files beside them it reads (the fuzz targets and
    their corpora: every file under testing/fuzz that is not Rust source)."""
    keys = Keys()
    read = [(name, value) for name, value in keys.files("testing/fuzz") if not name.endswith(".rs")]
    inputs = keys.binary_inputs(lane, item, executables, read)
    key = digest(json.dumps(inputs).encode())
    use = behind(lane) is None and passed_before(BINARY_LANES[lane], key)
    with open(state, "w") as file:
        file.write(f"{item}\t{key}\t{'reused' if use else 'run'}\n")
    with open(state + ".lane", "w") as file:
        file.write(BINARY_LANES[lane] + "\n")
    if use:
        print(f"    reused: {item} (its binaries passed before, the same in all they run)")
    return 0


def read_state(state):
    with open(state) as file:
        return [line.rstrip("\n").split("\t") for line in file if line.strip()]


def record(state, item):
    for name, key, _ in read_state(state):
        if name == item:
            break
    else:
        sys.exit(f"lane-reuse.py: {item!r} is not in {state}")
    if key == "-":
        return 0
    with open(state + ".lane") as file:
        lane = file.read().strip()
    path = record_path(lane, key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".tmp", "w") as file:
        file.write(f"{FORMAT} {scripts_hash()}\n")
    os.replace(path + ".tmp", path)
    return 0


def status(lane, state):
    rows = read_state(state)
    if not rows:
        sys.exit(f"lane-reuse.py: {lane} had no item: not passed")
    reused = sum(1 for _, _, how in rows if how == "reused")
    if reused == 0:
        os.makedirs(records(), exist_ok=True)
        with open(os.path.join(records(), f"{lane}.ran"), "w") as file:
            file.write(f"{git('rev-parse', 'HEAD')} {time.time()}\n")
        print("passed")
    else:
        print(f"passed({reused}-of-{len(rows)}-reused)")
    return 0


def listing(state, fresh, out):
    """Every test of the tests lane: those that ran from FRESH (`cargo nextest list` of them),
    kept by their key; those reused from what was kept when they last ran."""
    suites = {}
    ran = {}
    if fresh:
        with open(fresh) as file:
            ran = json.load(file)["rust-suites"]
    kept = os.path.join(records(), "tests-list")
    os.makedirs(kept, exist_ok=True)
    for name, key, how in read_state(state):
        if how == "run":
            mine = {binary: suite for binary, suite in ran.items() if suite["package-name"] == name}
            suites.update(mine)
            if key != "-":
                with open(os.path.join(kept, key + ".tmp"), "w") as file:
                    json.dump(mine, file)
                os.replace(os.path.join(kept, key + ".tmp"), os.path.join(kept, key))
        else:
            try:
                with open(os.path.join(kept, key)) as file:
                    suites.update(json.load(file))
            except OSError:
                sys.exit(f"lane-reuse.py: {name} was reused, but its tests were never listed: run with GATE_LANE_REUSE=0")
    with open(out, "w") as file:
        json.dump({"rust-suites": suites}, file)
    return 0


REACH = re.compile(r'\.parent\(\)|current_exe|CARGO_TARGET_TMPDIR|Command::new\(\s*"cargo"|env!\(\s*"CARGO"\s*\)')
LITERAL = re.compile(r'"((?:[^"\\]|\\.)*)"')


def reaches(literal, tops):
    """Whether a string in a source names a path outside the crates."""
    if literal == ".." or "../" in literal or "/.." in literal:
        return True
    return any(literal == top or literal.startswith(top + "/") or ("/" + top + "/") in literal for top in tops)


READS = re.compile(
    r'#\[path\s*=\s*"([^"]+)"\]|include_(?:str|bytes)!\(\s*"([^"]+)"|CARGO_MANIFEST_DIR"\)\)?\s*\.join\(\s*"([^"]+)"|concat!\(env!\("CARGO_MANIFEST_DIR"\),\s*"([^"]+)"'
)


ENV_READ = re.compile(r'(?:env::var(?:_os)?|env!|option_env!)\(\s*(?:"([A-Z_0-9]+)"|([A-Z_][A-Z_0-9]*)\s*\))')
ENV_CONST = re.compile(r'const ([A-Z_][A-Z_0-9]*): &str = "([^"]*)";')


def env_reads(text):
    """The variables a source reads by name: a literal, or a constant of the same file."""
    consts = dict(ENV_CONST.findall(text))
    for match in ENV_READ.finditer(text):
        yield match.group(1) or consts.get(match.group(2), match.group(2))


def reads():
    """Fails if a package's sources name a file outside what its key holds: another crate's
    directory that is not one it builds, and not one it says its tests read; or read a variable
    of the environment no key holds (TEST_ENV, and the build's own)."""
    keys = Keys()
    crates = os.path.join(ROOT, "crates")
    problems = []
    for package_id in sorted(keys.members):
        package = keys.packages[package_id]
        home = os.path.dirname(package["manifest_path"])
        allowed = [
            os.path.dirname(keys.packages[built]["manifest_path"])
            for built in keys.packages_built([package["name"]], dev=True)
            if built in keys.members
        ]
        allowed += [os.path.join(ROOT, directory) for directory in keys.test_reads(package["name"])]
        for name in keys.files(os.path.relpath(home, ROOT)):
            if not name[0].endswith(".rs"):
                continue
            path = os.path.join(ROOT, name[0])
            with open(path, errors="replace") as file:
                text = file.read()
            for variable in env_reads(text):
                if not (TEST_ENV.match(variable) or size_key.ENV.match(variable)):
                    problems.append(f"{name[0]}: reads ${variable}, which no key holds: add it to TEST_ENV")
            for match in READS.finditer(text):
                said = next(group for group in match.groups() if group)
                base = home if match.group(3) or match.group(4) else os.path.dirname(path)
                target = os.path.normpath(os.path.join(base, said.lstrip("/") if match.group(4) else said))
                if os.path.commonpath([target, crates]) != crates and target != os.path.dirname(crates) and not crates.startswith(target + os.sep):
                    continue  # outside the crates: in every test key
                if any(os.path.commonpath([target, where]) == where for where in allowed):
                    continue
                # The root itself: a walk of the crates, which `test-reads = ["crates"]` holds.
                if target == ROOT and "crates" in keys.test_reads(package["name"]):
                    continue
                problems.append(f"{name[0]}: {said!r} is {os.path.relpath(target, ROOT) or '.'}, which its key does not hold")
    if problems:
        print("a package reads what its tests' key does not hold; add it to its manifest's", file=sys.stderr)
        print("[package.metadata.orion] test-reads, or build what it reads:", file=sys.stderr)
        print("\n".join(problems), file=sys.stderr)
        return 1
    print("every package's tests read only what their key holds")
    return 0


def depinfo(state, fresh):
    """Fails if a test binary that ran was built from a file its package's key does not hold:
    every file in rustc's dep-info for it (BINARY.d, beside it) is in a crate its tests build,
    outside the crates, in what it says its tests read, or outside the repository (the
    registry's and the toolchain's, which Cargo.lock and the toolchain in the key pin)."""
    keys = Keys()
    with open(fresh) as file:
        suites = json.load(file)["rust-suites"]
    ran = {name for name, _, how in read_state(state) if how == "run"}
    crates = os.path.join(ROOT, "crates") + os.sep
    problems, read = [], 0
    for suite in suites.values():
        name = suite["package-name"]
        if name not in ran:
            continue
        allowed = [
            os.path.dirname(keys.packages[built]["manifest_path"]) + os.sep
            for built in keys.packages_built([name], dev=True)
            if built in keys.members
        ]
        allowed += [os.path.join(ROOT, directory) + os.sep for directory in keys.test_reads(name)]
        binary = suite["binary-path"]
        base = os.path.join(os.path.dirname(binary), os.path.basename(binary).split(".")[0] + ".d")
        try:
            with open(base) as file:
                text = file.read()
        except OSError:
            problems.append(f"{name}: no dep-info at {base}")
            continue
        read += 1
        for line in text.splitlines():
            if ":" not in line or line.startswith(" "):
                continue
            path = os.path.realpath(line.split(":", 1)[0].replace("\\ ", " "))
            if not path.startswith(crates):
                continue  # outside the crates: in every key, or not the repository's
            if not any(path.startswith(where) for where in allowed):
                problems.append(f"{name}: built from {os.path.relpath(path, ROOT)}, which its key does not hold")
    if problems:
        print("a test binary was built from what its key does not hold; declare it in its", file=sys.stderr)
        print("manifest's [package.metadata.orion] test-reads:", file=sys.stderr)
        print("\n".join(sorted(set(problems))), file=sys.stderr)
        return 1
    print(f"every test binary that ran was built from what its key holds ({read} read)")
    return 0


def manifest_dir_packages(keys):
    """The workspace packages any of whose Rust sources mention CARGO_MANIFEST_DIR."""
    found = []
    for package_id in keys.members:
        package = keys.packages[package_id]
        home = os.path.relpath(os.path.dirname(package["manifest_path"]), ROOT)
        for name, _ in keys.files(home):
            if name.endswith(".rs"):
                with open(os.path.join(ROOT, name), errors="replace") as file:
                    if RUNTIME_SOURCE.search(file.read()):
                        found.append(package["name"])
                        break
    return sorted(found)


def plan_self_test():
    """binary-plan end to end, in a copy of the tree that --self-test made: planted cargo
    artifacts and test state, records in the copy's own git directory, and what runs again
    after each change. Prints one line per case, then `plan self-test passed` or the failures."""
    import contextlib
    import io
    import tempfile

    work = tempfile.mkdtemp(prefix="orion-plan.")
    failures = []
    try:
        keys = Keys()
        leaf, other = "orion-device-probe", "orion-ctl"
        builds_leaf = [keys.packages[i]["name"] for i in keys.members
                       if keys.by_name[leaf] in keys.packages_built([keys.packages[i]["name"]], dev=True)
                       and keys.packages[i]["name"] != leaf]
        if builds_leaf:
            sys.exit(f"plan self-test: {leaf} is built by {builds_leaf}: pick a package nothing builds")
        six = manifest_dir_packages(keys)
        if not six:
            sys.exit("plan self-test: no package mentions CARGO_MANIFEST_DIR: plant one")
        # Each package's executables: a test binary, and for the leaf a binary of its own its
        # tests run (named to sort after the test, so a key of the first alone would miss it).
        exes = {}
        for package in [leaf, other] + six:
            directory = os.path.join(work, "exe", package)
            os.makedirs(directory)
            exes[package] = [os.path.join(directory, "a-test")]
            shutil.copyfile(shutil.which("true"), exes[package][0])
        exes[leaf].append(os.path.join(work, "exe", leaf, "b-bin"))
        shutil.copyfile(shutil.which("true"), exes[leaf][1])
        artifacts = os.path.join(work, "artifacts")
        with open(artifacts, "w") as file:
            file.write("not json: cargo's other output\n")
            for package, paths in exes.items():
                package_id = keys.by_name[package]
                file.write(json.dumps({"reason": "compiler-artifact", "package_id": package_id, "executable": None}) + "\n")
                for path in paths:
                    file.write(json.dumps({"reason": "compiler-artifact", "package_id": package_id, "executable": path}) + "\n")
        state = os.path.join(work, "state")

        def plan():
            with open(state, "w") as file:
                for package in [leaf, other] + six:
                    file.write(f"{package}\tsource-{package}\trun\n")
            with contextlib.redirect_stdout(io.StringIO()):
                binary_plan(state, artifacts)
            keyed = {name for name, _, _ in read_state(state + ".bin")}
            return {name: how for name, _, how in read_state(state)}, keyed

        def expect(label, runs, also=lambda got, keyed: None):
            got, keyed = plan()
            for package in [leaf, other] + six:
                want = "run" if package in runs or package in six else "reused"
                if got[package] != want:
                    failures.append(f"{label}: {package} {got[package]}, not {want}")
            for package in six:
                if package in keyed:
                    failures.append(f"{label}: {package} mentions CARGO_MANIFEST_DIR, and was keyed by its binaries")
            also(got, keyed)
            if not any(f.startswith(label + ":") for f in failures):
                print(f"    plan: {label}: {', '.join(sorted(p for p in (leaf, other) if p in runs)) or 'nothing'} ran")

        def record_all():
            for name, _, _ in read_state(state + ".bin"):
                record(state + ".bin", name)

        expect("nothing passed before", {leaf, other})
        record_all()
        expect("the same binaries, passed before", set())
        fixture = os.path.join(ROOT, os.path.relpath(os.path.dirname(keys.packages[keys.by_name[leaf]]["manifest_path"]), ROOT), "tests", "planted-fixture.txt")
        made = not os.path.isdir(os.path.dirname(fixture))
        os.makedirs(os.path.dirname(fixture), exist_ok=True)
        with open(fixture, "w") as file:
            file.write("a fixture its tests read\n")
        try:
            expect(f"a fixture of {leaf}, which nothing else builds", {leaf})
        finally:
            os.remove(fixture)
            if made:
                os.rmdir(os.path.dirname(fixture))
        expect("the fixture taken away again", set())
        shutil.copyfile(shutil.which("false"), exes[leaf][1])
        try:
            expect(f"only the code of a binary {leaf}'s tests run", {leaf})
        finally:
            shutil.copyfile(shutil.which("true"), exes[leaf][1])
        os.environ["ORION_PLANTED"] = "changed"
        try:
            expect("a value of the environment the tests read", {leaf, other})
        finally:
            del os.environ["ORION_PLANTED"]
        os.environ["CI"] = "1"
        try:
            expect("on CI, where a lane runs in full", {leaf, other})
        finally:
            del os.environ["CI"]
        planted = os.path.join(ROOT, "crates/client/ctl/tests/all.rs")
        with open(planted) as file:
            before = file.read()
        with open(planted, "a") as file:
            file.write('\nconst _PLANTED: &str = env!("CARGO_MANIFEST_DIR");\n')
        try:
            expect(f"a seventh package, {other}, made to mention CARGO_MANIFEST_DIR", {other},
                   lambda got, keyed: other in keyed and failures.append(
                       f"{other} mentions CARGO_MANIFEST_DIR, and was keyed by its binaries"))
        finally:
            with open(planted, "w") as file:
                file.write(before)
        # A test binary that is not ELF: keyed by all of its bytes, so one byte changed runs it.
        with open(exes[leaf][0], "w") as file:
            file.write("#!/bin/sh\nexit 0\n")
        if load_elf_key().alloc_key(open(exes[leaf][0], "rb").read()) != "file-" + hashlib.sha256(b"#!/bin/sh\nexit 0\n").hexdigest():
            failures.append("a test binary not ELF was not keyed by all of its content")
        expect("a test binary that is not ELF, new", {leaf})
        record_all()
        expect("that binary, passed before", set())
        with open(exes[leaf][0], "w") as file:
            file.write("#!/bin/sh\nexit 1\n")
        expect("that binary, one byte changed", {leaf})
        print(f"    plan: on the source key alone: {', '.join(six)}")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    if failures:
        print("plan self-test FAILED:", *failures, sep="\n  ")
        return 1
    print("plan self-test passed")
    return 0


def self_test():
    """Each kind of input a test's key holds, changed in a copy of this tree: the key moves; a
    note, or a crate the package does not build, leaves it where it was. Every case runs in a
    copy of its own, all at once: none waits on another."""
    import tempfile

    work = tempfile.mkdtemp(prefix="orion-keys.")
    copy = os.path.join(work, "tree")
    runs = {}
    try:
        listed = subprocess.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=ROOT, capture_output=True, check=True,
        ).stdout.split(b"\0")
        for name in (n.decode() for n in listed if n):
            if os.path.isfile(os.path.join(ROOT, name)):
                os.makedirs(os.path.dirname(os.path.join(copy, name)), exist_ok=True)
                shutil.copy2(os.path.join(ROOT, name), os.path.join(copy, name))
        subprocess.run(["git", "init", "-q"], cwd=copy, check=True)
        shims = os.path.join(work, "bin")
        os.makedirs(shims)
        env = {k: v for k, v in os.environ.items() if k != "CI" and not k.startswith("GATE_")}
        # One target for every listing: the copy's own would be made, empty, for each.
        env["CARGO_TARGET_DIR"] = os.path.join(work, "target")

        # One listing for every case that leaves the manifests, the lockfile and the toolchain
        # file as they are; a case that changes one lists its own tree.
        listing = subprocess.run(
            ["cargo", "metadata", "--locked", "--format-version", "1"],
            cwd=copy, capture_output=True, text=True, env=env, check=True,
        ).stdout
        metadata = os.path.join(work, "metadata.json")
        with open(metadata, "w") as file:
            file.write(listing.replace(copy, "@ROOT@"))
        own_listing = re.compile(r"(^|/)(Cargo\.toml|Cargo\.lock|rust-toolchain\.toml)$")

        def start(tag, argv, change=None, extra=None):
            """Run lane-reuse.py ARGV in a copy of its own, CHANGE made to it first."""
            root = os.path.join(work, f"case-{len(runs)}")
            subprocess.run(["cp", "-a", copy, root], check=True)
            run_env = dict(env, **(extra or {}))
            if change:
                change(root)
            if not (change and own_listing.search(change.name)):
                run_env["LANE_REUSE_METADATA"] = metadata
            runs[tag] = subprocess.Popen(
                [sys.executable, os.path.join(root, "scripts", "lane-reuse.py"), *argv],
                cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=run_env,
            )

        def result(tag):
            out, err = runs[tag].communicate()
            if runs[tag].returncode not in (0, 1) or not out.strip():
                sys.exit(f"self-test: {tag} failed:\n{out}{err}")
            return out.rstrip().splitlines()

        def touch(name, text="\n// changed by the keys' self-test\n"):
            def change(root):
                with open(os.path.join(root, name), "a") as file:
                    file.write(text)
            change.name = name
            return change

        def replace(name, old, new):
            def change(root):
                path = os.path.join(root, name)
                with open(path) as file:
                    before = file.read()
                assert old in before, f"{name} lacks {old!r}"
                with open(path, "w") as file:
                    file.write(before.replace(old, new, 1))
            change.name = name
            return change

        keys = Keys.__new__(Keys)  # only for the closure, read from this tree
        Keys.__init__(keys)
        built = {
            os.path.relpath(os.path.dirname(keys.packages[i]["manifest_path"]), ROOT)
            for i in keys.packages_built(["orion-ctl"], dev=True) if i in keys.members
        }
        apart = sorted(
            os.path.relpath(os.path.dirname(keys.packages[i]["manifest_path"]), ROOT)
            for i in keys.members
        )
        apart = next(d for d in apart if d not in built)
        lock = re.search(r'name = "zeroize"\nversion = "[^"]+"\nsource = "[^"]+"\nchecksum = "([0-9a-f]+)"', open(os.path.join(copy, "Cargo.lock")).read())
        with open(os.path.join(shims, "ssh"), "w") as file:
            file.write("#!/bin/sh\nexit 0\n")
        os.chmod(os.path.join(shims, "ssh"), 0o755)
        # Two binaries of one name, other bytes: the key must move by what is in them, not the name.
        one, other = os.path.join(work, "one", "tests"), os.path.join(work, "other", "tests")
        for path, source in ((one, "true"), (other, "false")):
            os.makedirs(os.path.dirname(path))
            shutil.copyfile(shutil.which(source), path)

        cases = [
            ("a file of the crate itself", True, touch("crates/client/ctl/src/lib.rs"), None),
            ("a file of a crate it builds", True, touch("crates/client/orion/src/lib.rs"), None),
            ("a file of a crate only its tests build", True, touch("crates/testing/stubs/src/lib.rs"), None),
            # orion-ctl's sources name no path outside its crates: a page changed is not its.
            ("a file outside the crates, which it never reaches", False, touch("web/video-example/video.js"), None),
            ("the tests' own settings", True, touch(".config/nextest.toml", "\n# changed\n"), None),
            ("the gate", True, touch("scripts/gate.sh", "\n# changed\n"), None),
            ("the root manifest", True, touch("Cargo.toml", "\n# changed\n"), None),
            ("the toolchain file", True, touch("rust-toolchain.toml", "\n# changed\n"), None),
            # Its entry in the lockfile as written: a line added to it, which cargo reads past.
            ("a locked dependency it builds", lock is not None,
             lock and replace("Cargo.lock", lock.group(0), lock.group(0) + "\n# changed"), None),
            ("the build's environment", True, None, {"RUSTFLAGS": "-Cdebuginfo=0"}),
            ("the environment a test reads", True, None, {"NAMESPACE_TESTS": "changed"}),
            ("a program a test starts", True, None, {"PATH": shims + os.pathsep + os.environ.get("PATH", "")}),
            ("a note", False, touch("docs/design.md", "\nchanged\n"), None),
            (f"a crate it does not build ({apart})", False, touch(os.path.join(apart, "Cargo.toml"), "\n# changed\n"), None),
        ]
        binary_cases = [
            ("by its binaries: a Rust source of the crate itself", False, touch("crates/client/ctl/src/lib.rs"), one),
            ("by its binaries: a Rust source of a crate it builds", False, touch("crates/client/orion/src/lib.rs"), one),
            ("by its binaries: another binary of the same name", True, None, other),
            ("by its binaries: a manifest of a crate it builds", True,
             touch("crates/client/orion/Cargo.toml", "\n# changed\n"), one),
            ("by its binaries: the tests' own settings", True, touch(".config/nextest.toml", "\n# changed\n"), one),
            ("by its binaries: a note", False, touch("docs/design.md", "\nchanged\n"), one),
        ]
        # Every crate that reads a file by CARGO_MANIFEST_DIR today also reaches outside, so that
        # check is planted: in a crate that is otherwise keyed so.
        unkeyed = [
            ("orion-web-tests", "reaches outside the crates", None),
            ("orion-ctl", "reads a file by CARGO_MANIFEST_DIR",
             touch("crates/client/ctl/tests/all.rs", '\nconst _PLANTED: &str = env!("CARGO_MANIFEST_DIR");\n')),
        ]
        start("plan", ["--self-test-plan"])
        start("base", ["--explain", "tests", "orion-ctl"])
        # The same inputs in another copy, elsewhere: one key, wherever the tree is.
        start("base elsewhere", ["--explain", "tests", "orion-ctl"])
        for label, _, change, extra in cases:
            start(label, ["--explain", "tests", "orion-ctl"], change, extra)
        # A package whose sources reach outside the crates (the browser tests load web/).
        start("web base", ["--explain", "tests", "orion-web-tests"])
        start("web", ["--explain", "tests", "orion-web-tests"], touch("web/video-example/video.js"))
        # What a package says its tests read: any crate, for one that reads them all.
        start("stubs base", ["--explain", "tests", "orion-stubs"])
        start("stubs", ["--explain", "tests", "orion-stubs"], touch(os.path.join(apart, "Cargo.toml"), "\n# changed\n"))
        start("binary base", ["--explain-binary", "orion-ctl", one])
        for label, _, change, executable in binary_cases:
            start(label, ["--explain-binary", "orion-ctl", executable], change)
        for package, why, change in unkeyed:
            start(f"unkeyed {package}", ["--explain-binary", package, one], change)

        key = lambda tag: result(tag)[-1].split()[-1]
        failures = []
        base = key("base")
        if key("base elsewhere") != base:
            failures.append("the same inputs gave two keys")
        for label, moves, _, _ in cases:
            if (key(label) != base) != moves:
                failures.append(f"{label}: the key {'did not move' if moves else 'moved'}")
            else:
                print(f"    {label}: {'moved' if moves else 'stayed'}")
        for tag, label in (("web", "a file outside the crates, for a package that reaches it"),
                           ("stubs", "a crate a package says its tests read")):
            if key(tag) == key(f"{tag} base"):
                failures.append(f"{label}: the key did not move")
            else:
                print(f"    {label}: moved")
        base = result("binary base")[-1]
        for label, moves, _, _ in binary_cases:
            if (result(label)[-1] != base) != moves:
                failures.append(f"{label}: the key {'did not move' if moves else 'moved'}")
            else:
                print(f"    {label}: {'moved' if moves else 'stayed'}")
        for package, why, _ in unkeyed:
            if result(f"unkeyed {package}")[-1] != "not keyed by its binaries":
                failures.append(f"{package} {why}, and was keyed by its binaries")
            else:
                print(f"    by its binaries: {package} ({why}): not keyed so")
        lines = result("plan")
        print(*(line for line in lines if line.startswith("    ")), sep="\n")
        if lines[-1] != "plan self-test passed":
            failures += [line.strip() for line in lines if not line.startswith("    plan:")]
    finally:
        for process in runs.values():
            if process.poll() is None:
                process.kill()
                process.wait()
        shutil.rmtree(work, ignore_errors=True)
    if failures:
        print("keys self-test FAILED:", *failures, sep="\n  ")
        return 1
    print("keys self-test passed")
    return 0


def main(argv):
    if len(argv) >= 3 and argv[0] == "plan":
        with open(argv[2] + ".lane", "w") as file:
            file.write(argv[1] + "\n")
        return plan(argv[1], argv[2], argv[3:])
    if len(argv) >= 3 and argv[0] == "record":
        # One or more items, read from one state file.
        for item in argv[2:]:
            record(argv[1], item)
        return 0
    if len(argv) == 3 and argv[0] == "binary-plan":
        return binary_plan(argv[1], argv[2])
    if len(argv) >= 6 and argv[0] == "binary-plan-item" and argv[4] == "--":
        return binary_plan_item(argv[1], argv[2], argv[3], argv[5:])
    if len(argv) == 3 and argv[0] == "status":
        return status(argv[1], argv[2])
    if argv and argv[0] == "behind":
        for lane in argv[1:] or LANES:
            ran = last_full_run(lane)
            where = f"last full run at {ran[0][:12]}, {ran[1] / 3600:.1f} h ago" if ran else "no full run here"
            print(f"{lane}: {where}; {behind(lane) or 'may reuse'}")
        return 0
    if argv == ["own-vars"]:
        return own_vars()
    if argv == ["reads"]:
        return reads()
    if argv == ["--self-test"]:
        return self_test()
    if len(argv) == 3 and argv[0] == "depinfo":
        return depinfo(argv[1], argv[2])
    if len(argv) in (3, 4) and argv[0] == "listing":
        return listing(argv[1], argv[2] if len(argv) == 4 else "", argv[-1])
    if argv == ["--self-test-plan"]:
        return plan_self_test()
    if len(argv) >= 3 and argv[0] == "--explain-binary":
        # PACKAGE EXE...: its tests' key by those binaries, or why it has none.
        keys = Keys()
        if not keys.binary_keyable(argv[1]):
            print("not keyed by its binaries")
            return 0
        print("key", digest(json.dumps(keys.test_binary_inputs(argv[1], argv[2:])).encode()))
        return 0
    if len(argv) == 3 and argv[0] == "--explain":
        keys = Keys()
        for label, value in keys.inputs(argv[1], argv[2]):
            print(f"{label}\t{value}")
        print("key", keys.key(argv[1], argv[2]))
        return 0
    print(__doc__.strip().splitlines()[2], file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
