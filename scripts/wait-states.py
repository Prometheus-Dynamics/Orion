#!/usr/bin/env python3
"""Runs COMMAND and says where its threads spent their time: running, ready to run but
waiting for a processor, sleeping, or in uninterruptible wait (D) and on what.

    scripts/wait-states.py [--every SECONDS] -- COMMAND...

A build that takes minutes of wall time on little CPU time is either starved (its processes
ready, no processor free) or blocked (in D: a filesystem lock, the disk). The two need
different fixes, and the load average does not tell them apart; lock waits do not show in the
kernel's I/O pressure either. Every SECONDS (default 1) it reads, for each process under
COMMAND, its state and wait channel (`/proc/PID/stat`, `/proc/PID/wchan`) and its CPU time and
time ready to run (`/proc/PID/schedstat`). A process that starts and ends between two samples
is not seen, and one's times are as last read: the sums are a floor. At the end it prints:

- the share of process-samples in each state, and the wait channels of those in D;
- the CPU time and the time ready-to-run-but-not-running, summed over the processes seen.

Its exit status is COMMAND's. It only reads /proc, and signals nothing.
"""

import collections
import os
import subprocess
import sys
import time


def children_of(root):
    """Every process under ROOT, ROOT included, from /proc/*/stat's parent field."""
    parents = {}
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat") as file:
                rest = file.read().rsplit(") ", 1)[1].split()
        except (OSError, IndexError):
            continue
        parents.setdefault(int(rest[1]), []).append(int(name))
    found, todo = [], [root]
    while todo:
        pid = todo.pop()
        found.append(pid)
        todo.extend(parents.get(pid, []))
    return found


def sample(pid):
    """(state, wait channel, comm, (cpu ns, waited ns)) of each thread of PID: a process's
    own state is its main thread's, which sleeps while its others work (rustc's codegen)."""
    out = []
    try:
        threads = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return out
    for tid in threads:
        base = f"/proc/{pid}/task/{tid}"
        try:
            with open(f"{base}/stat") as file:
                text = file.read()
            with open(f"{base}/wchan") as file:
                channel = file.read().strip() or "-"
            with open(f"{base}/schedstat") as file:
                ran, waited = (int(x) for x in file.read().split()[:2])
        except (OSError, ValueError):
            continue
        comm = text[text.index("(") + 1 : text.rindex(")")]
        out.append((tid, text.rsplit(") ", 1)[1][0], channel, comm, (ran, waited)))
    return out


def main(argv):
    every = 1.0
    if argv[:1] == ["--every"]:
        every = float(argv[1])
        argv = argv[2:]
    if argv[:1] != ["--"] or len(argv) < 2:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    child = subprocess.Popen(argv[1:])
    states = collections.Counter()
    channels = collections.Counter()
    by_name = collections.Counter()
    last = {}  # thread -> (comm, (cpu ns, waited ns)), its latest reading
    started = time.monotonic()
    while child.poll() is None:
        for pid in children_of(child.pid):
            for tid, state, channel, comm, times in sample(pid):
                states[state] += 1
                by_name[(comm, state)] += 1
                if state == "D":
                    channels[channel] += 1
                last[tid] = (comm, times)
        time.sleep(every)
    took = time.monotonic() - started
    total = sum(states.values())
    print(f"wait-states: {took:.0f} s, {total} thread-samples every {every:g} s")
    names = {"R": "running or ready", "S": "sleeping", "D": "uninterruptible (D)"}
    for state, count in states.most_common():
        print(f"  {names.get(state, state)}: {count} ({100 * count / max(total, 1):.0f}%)")
    if channels:
        print("  D waits on:")
        for channel, count in channels.most_common(10):
            print(f"    {channel}: {count}")
    cpu = sum(t[0] for _, t in last.values()) / 1e9
    waited = sum(t[1] for _, t in last.values()) / 1e9
    print(f"  of the threads seen: {cpu:.0f} CPU-s run, {waited:.0f} s ready but waiting for a processor")
    busy = collections.Counter()
    for (name, state), count in by_name.items():
        if state in "RD":
            busy[(name, state)] += count
    print("  most samples running or ready (R) and in D, by program: " + ", ".join(
        f"{name} {state} {count}" for (name, state), count in busy.most_common(8)))
    compilers = {n: c for (n, s), c in by_name.items() if s == "D" and n in ("rustc", "cc", "ld", "rust-lld", "ld.lld", "mold")}
    if compilers:
        print("  compilers and linkers in D (samples): " + ", ".join(f"{n} {c}" for n, c in sorted(compilers.items())))
    return child.returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
