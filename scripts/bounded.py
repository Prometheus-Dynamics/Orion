#!/usr/bin/env python3
"""Runs one gate step within a bound, and leaves nothing of it running.

    bounded.py SECONDS LABEL -- COMMAND...
    bounded.py --self-test

The step runs in a session and process group of its own, with /dev/null for input and a file
for its output and errors. The file is copied to this script's output as it grows. A process
the step leaves behind can hold that file, never the gate's own output, so it cannot keep the
gate's reader waiting.

This script is the step's subreaper (Linux). A process the step starts and leaves behind, even
one in a session of its own, becomes this script's child when its parent ends, so it can be
found. When the step ends with processes still running, or passes its bound, each process
under this script is sent TERM, and KILL a few seconds later if it is still running. Exit
status:

- past its bound: 124, with `TIMED OUT: LABEL` and the bound;
- ended, but processes it started were still running: 125, naming each (they are ended too);
- ended, but it left something in its TMPDIR: 125, naming each entry;
- otherwise the step's own.

Each step gets a TMPDIR of its own, empty, with a short name (a socket under it must fit in
107 bytes), under the TMPDIR this script was given. What a step makes there, it removes: one
gate's tests left 176 entries, 139 MB, in a shared one, and a full tmpfs fails unrelated steps
with errors that say nothing of it. Whatever is left is named, then removed.

TERM, INT or HUP to this script ends the step and every process under it the same way.

`--self-test` plants a sleeper, a process that ignores TERM, an orphan that holds the output,
a stop from outside and a step that leaves a file in its TMPDIR, and checks each ends within its
bound with nothing left running, and the leftover is named.
"""

import ctypes
import shutil
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time

# Seconds between TERM and KILL. The self-test shortens it for its own runs.
GRACE = float(os.environ.get("ORION_BOUND_GRACE", "5"))
PR_SET_CHILD_SUBREAPER = 36


class Bound(Exception):
    pass


class Stopped(Exception):
    def __init__(self, number):
        super().__init__(number)
        self.number = number


def become_subreaper():
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        return libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) == 0
    except (OSError, AttributeError):
        return False


def parents():
    """pid -> (parent pid, state) for every process."""
    found = {}
    if not os.path.isdir("/proc"):
        return found
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", "rb") as file:
                stat = file.read()
        except OSError:
            continue
        fields = stat[stat.rindex(b")") + 2 :].split(None, 2)
        found[int(name)] = (int(fields[1]), fields[0])
    return found


def command_line(pid):
    """What PID runs. A process caught in exec reads empty for a moment, until its new
    arguments are set: it is read again, and named by its short name if it stays empty."""
    for _ in range(20):
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as file:
                line = file.read().replace(b"\0", b" ").decode(errors="replace").strip()
        except OSError:
            return "?"
        if line:
            return line
        time.sleep(0.005)
    try:
        with open(f"/proc/{pid}/comm") as file:
            return f"[{file.read().strip()}]"
    except OSError:
        return "?"


def under(root):
    """The live processes below ROOT, nearest first."""
    table = parents()
    children = {}
    for pid, (parent, _) in table.items():
        children.setdefault(parent, []).append(pid)
    out, todo = [], list(children.get(root, []))
    while todo:
        pid = todo.pop(0)
        if table[pid][1] != b"Z":
            out.append(pid)
        todo.extend(children.get(pid, []))
    return out


def anything_left(subreaper, group):
    """Whether a process of the step may still run: cheap, and never no when it is yes."""
    if subreaper:
        try:
            tasks = os.listdir("/proc/self/task")
        except OSError:
            return True
        for task in tasks:
            try:
                with open(f"/proc/self/task/{task}/children") as file:
                    if file.read().split():
                        return True
            except OSError:
                return True
        return False
    try:
        os.killpg(group, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def reap():
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return


def send(group, pids, number):
    if group is not None:
        try:
            os.killpg(group, number)
        except (ProcessLookupError, PermissionError):
            pass
    for pid in pids:
        try:
            os.kill(pid, number)
        except (ProcessLookupError, PermissionError):
            pass


def end_all(group):
    """TERM to the step's group and every process under this script; KILL to what is left."""
    me = os.getpid()
    send(group, under(me), signal.SIGTERM)
    deadline = time.monotonic() + GRACE
    while time.monotonic() < deadline:
        reap()
        if not under(me):
            return
        time.sleep(0.05)
    for _ in range(50):
        left = under(me)
        if not left:
            break
        send(group, left, signal.SIGKILL)
        time.sleep(0.02)
        reap()
    reap()


def copy(path, stop, out):
    """Copies the file at PATH to OUT as it grows, until STOP is set and all of it is copied."""
    with open(path, "rb") as file:
        while True:
            chunk = file.read(65536)
            if chunk:
                out.write(chunk)
                out.flush()
                continue
            if stop.is_set():
                return
            stop.wait(0.1)


def run(bound, label, command):
    subreaper = become_subreaper()

    def on_alarm(number, frame):
        raise Bound()

    def on_stop(number, frame):
        raise Stopped(number)

    for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(number, on_stop)
    signal.signal(signal.SIGALRM, on_alarm)

    log = tempfile.NamedTemporaryFile(prefix="orion-step.", delete=False)
    scratch = tempfile.mkdtemp(prefix="g")
    stop = threading.Event()
    copier = threading.Thread(target=copy, args=(log.name, stop, sys.stdout.buffer), daemon=True)
    group = None

    def drain():
        stop.set()
        if copier.is_alive():
            copier.join()

    try:
        try:
            signal.alarm(bound)
            try:
                child = subprocess.Popen(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=log,
                    close_fds=True,
                    start_new_session=True,
                    env=dict(os.environ, TMPDIR=scratch),
                )
            except OSError as error:
                print(f"    {label}: cannot run {command[0]}: {error}", flush=True)
                return 127
            group = child.pid
            copier.start()
            status = None
            while status is None:
                ended, raw = os.waitpid(-1, 0)
                if ended == child.pid:
                    status = os.waitstatus_to_exitcode(raw)
                    child.returncode = status
            left = []
            if anything_left(subreaper, group):
                left = [(pid, command_line(pid)) for pid in under(os.getpid())]
            if left:
                end_all(group)
            signal.alarm(0)
            drain()
            if left:
                print(f"    {label}: ended, but left these running (now ended):", flush=True)
                for pid, line in left:
                    print(f"      {pid} {line[:200]}", flush=True)
                return 125
            files = sorted(os.listdir(scratch))
            if files:
                print(f"    {label}: ended, but left these in its TMPDIR (now removed):", flush=True)
                for name in files[:50]:
                    print(f"      {name}", flush=True)
                if len(files) > 50:
                    print(f"      and {len(files) - 50} more", flush=True)
                return 125
            return 128 - status if status < 0 else status
        except Bound:
            end_all(group)
            drain()
            how = "every process under it" if subreaper else "its process group"
            print(f"    {label}: TIMED OUT after {bound} s (bound {bound} s); {how} ended", flush=True)
            return 124
        except Stopped as stopped:
            signal.alarm(0)
            end_all(group)
            drain()
            print(f"    {label}: stopped by signal {stopped.number}; everything under it ended", flush=True)
            return 128 + stopped.number
    finally:
        drain()
        log.close()
        os.unlink(log.name)
        shutil.rmtree(scratch, ignore_errors=True)


def self_test():
    """Each planted case at once, with a short grace: ends in time, nothing left running."""
    marker = f"orion-bounded-self-test-{os.getpid()}"
    env = dict(os.environ, ORION_BOUND_GRACE="1")
    me = [sys.executable, os.path.abspath(__file__)]
    cases = {
        # name: (bound, shell command, exit wanted, most seconds, what the output must hold)
        "passes its status and output": (20, "echo out; echo err >&2; exit 3", 3, 10, "err"),
        "a sleeper": (1, f"exec -a {marker}-sleeper sleep 1000", 124, 6, "TIMED OUT"),
        "one that ignores TERM, in a session of its own": (
            1,
            f"setsid bash -c 'trap \"\" TERM; exec -a {marker}-deaf sleep 1000' & sleep 1000",
            124,
            7,
            "TIMED OUT",
        ),
        # The step ends, as a helper exiting under `set -e` would, leaving an orphan that holds
        # what was its output. The reader below must still see the end. The step ends only
        # once the orphan runs as the marker: caught still in exec, it would not be named so
        # (docs/flakes.md).
        "an orphan holding the output": (
            20,
            f"setsid bash -c 'exec -a {marker}-orphan sleep 1000' & "
            f"until grep -qa '^{marker}-orphan' /proc/$!/cmdline; do sleep 0.01; done; "
            "echo started; exit 0",
            125,
            6,
            marker,
        ),
        # Given an empty TMPDIR of its own, apart from this script's; what it leaves is named.
        "one that leaves a file in its TMPDIR": (
            20,
            f'[ "$TMPDIR" != "{tempfile.gettempdir()}" ] && [ -z "$(ls -A "$TMPDIR")" ] '
            f"&& touch \"$TMPDIR/{marker}-left\"",
            125,
            10,
            f"left these in its TMPDIR (now removed):\n      {marker}-left",
        ),
    }
    # A case's time counts from when its step runs, seen by the name its process takes: on a
    # loaded machine the interpreter alone can take seconds to start (docs/flakes.md).
    starts = {
        "a sleeper": f"{marker}-sleeper",
        "one that ignores TERM, in a session of its own": f"{marker}-deaf",
        "an orphan holding the output": f"{marker}-orphan",
        "a stop from outside": f"{marker}-stop",
    }
    results, failures = {}, []

    def running(root, prefix):
        """Whether a process under ROOT runs with arguments that begin with PREFIX. Only
        ROOT's tree is read, never every process on the machine: it is asked every 10 ms."""
        todo = [root]
        while todo:
            pid = todo.pop()
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as file:
                    if pid != root and file.read().startswith(prefix.encode()):
                        return True
                for task in os.listdir(f"/proc/{pid}/task"):
                    with open(f"/proc/{pid}/task/{task}/children") as file:
                        todo.extend(int(child) for child in file.read().split())
            except OSError:
                pass
        return False

    def one(name, bound, script, stopped=False):
        start = time.monotonic()
        process = subprocess.Popen(
            me + [str(bound), name, "--", "bash", "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
        )
        if name in starts:
            waited = time.monotonic() + 30
            while (
                not running(process.pid, starts[name])
                and process.poll() is None
                and time.monotonic() < waited
            ):
                time.sleep(0.01)
            start = time.monotonic()
        if stopped:
            # Stopped once its step runs, which is after it can take the stop: a stop sent
            # before, while a loaded machine still starts it, would find no handler there
            # (docs/flakes.md).
            process.send_signal(signal.SIGTERM)
        # Read to the end of the output: this is what blocks if an orphan holds it.
        output = process.stdout.read().decode(errors="replace")
        status = process.wait()
        results[name] = (status, time.monotonic() - start, output)

    threads = [
        threading.Thread(target=one, args=(name, bound, script))
        for name, (bound, script, _, _, _) in cases.items()
    ]
    stop_script = f"setsid bash -c 'exec -a {marker} sleep 1000' & exec -a {marker}-stop sleep 1000"
    threads.append(threading.Thread(target=one, args=("a stop from outside", 100, stop_script, True)))
    cases["a stop from outside"] = (100, stop_script, 128 + signal.SIGTERM, 7, "stopped by signal")
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
        if thread.is_alive():
            failures.append("a case did not end within 60 s: its output is held")
    for name, (_, _, want, most, holds) in cases.items():
        if name not in results:
            continue
        status, took, output = results[name]
        if status != want:
            failures.append(f"{name}: exit {status}, not {want}: {output!r}")
        if took > most:
            failures.append(f"{name}: took {took:.1f} s, more than {most} s")
        if holds not in output:
            failures.append(f"{name}: output lacks {holds!r}: {output!r}")
    def says(pid):
        """PID's arguments, read once: every process on the machine is read, kernel threads
        among them, whose arguments are always empty."""
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as file:
                return file.read().decode(errors="replace")
        except OSError:
            return ""

    left = [pid for pid in parents() if marker in says(pid)]
    if left:
        failures.append(f"left running: {[command_line(pid) for pid in left]}")
        for pid in left:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    if failures:
        print("self-test FAILED:", *failures, sep="\n  ")
        return 1
    print(f"step bounds self-test passed ({len(cases)} cases)")
    return 0


def main(argv):
    if argv == ["--self-test"]:
        return self_test()
    if len(argv) < 4 or argv[2] != "--" or not argv[0].isdigit() or int(argv[0]) < 1:
        print("usage: bounded.py SECONDS LABEL -- COMMAND...", file=sys.stderr)
        return 2
    return run(int(argv[0]), argv[1], argv[3:])


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
