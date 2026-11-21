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
    lane-reuse.py own-vars                   fails if a Rust source (a build script, a test)
                                             reads one of the gate's own variables

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

A lane is never reused for longer than this: one whose last full run is 10 or more commits
behind HEAD, or 3 days old, runs in full, every item, and so does one that has never run in full
here, and every lane when GATE_LANE_REUSE=0. An item with no key (a kind this script does not
know) runs every time and is named; each lane says how many of its items had no key or were not
reused. CI runs everything. scripts/release-check.sh refuses a release that
any lane did not pass, in full, at that commit.
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
BEHIND_COMMITS = 10
BEHIND_SECONDS = 3 * 24 * 3600
LANES = ("matrix", "other-systems", "devices")
LANE_SCRIPTS = {
    "matrix": ["scripts/feature-matrix.sh"],
    "other-systems": ["scripts/same-features.py"],
    "devices": ["scripts/device-size.sh"],
}
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
        else:
            raise NoKey(f"no key for {lane} item {item!r}")
        return inputs

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
    ran = last_full_run(lane)
    if ran is None:
        return "never run in full here"
    commit, age = ran
    try:
        count = int(git("rev-list", "--first-parent", "--count", f"{commit}..HEAD"))
    except subprocess.CalledProcessError:
        return f"its last full run, at {commit[:12]}, is not in this history"
    if count >= BEHIND_COMMITS:
        return f"{count} commits since its last full run"
    if age >= BEHIND_SECONDS:
        return f"{age / 86400:.1f} days since its last full run"
    return None


def plan(lane, state, items):
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
    reused = sum(1 for _, _, how in rows if how == "reused")
    if reused == 0:
        os.makedirs(records(), exist_ok=True)
        with open(os.path.join(records(), f"{lane}.ran"), "w") as file:
            file.write(f"{git('rev-parse', 'HEAD')} {time.time()}\n")
        print("passed")
    else:
        print(f"passed({reused}-of-{len(rows)}-reused)")
    return 0


def main(argv):
    if len(argv) >= 3 and argv[0] == "plan":
        with open(argv[2] + ".lane", "w") as file:
            file.write(argv[1] + "\n")
        return plan(argv[1], argv[2], argv[3:])
    if len(argv) == 3 and argv[0] == "record":
        return record(argv[1], argv[2])
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
