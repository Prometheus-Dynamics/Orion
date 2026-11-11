#!/usr/bin/env python3
"""Worst-case stack depth of a Cortex-M ELF from `-Z emit-stack-sizes` and its call graph.

Usage: stack-depth.py <elf> [root ...]   (roots default to the cortex-m-rt main and handlers)

Frame sizes come from `llvm-readobj --stack-sizes`, call edges from `bl`/`b.w`/`b` branches in
`llvm-objdump -d`. Indirect calls (`blx rN`) are followed to every function whose name matches
STACK_INDIRECT (a regex; by default the wire codec's `Encode::encode` impls, the only `dyn` calls
the device tier makes). An indirect call in a function not matched by STACK_INDIRECT_FROM is
reported, and the result is then a lower bound. Recursion is reported and cut.
Exceptions add the hardware frame (32 bytes, 104 with the FPU context) on top of main's depth.
"""
import os, re, subprocess, sys

elf = sys.argv[1]
roots = sys.argv[2:]

def run(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout

sizes = {}
names_at = {}
cur = None
for line in run("llvm-readobj", "--stack-sizes", elf).splitlines():
    m = re.search(r"Functions: \[(.*)\]", line)
    if m:
        cur = [n.strip() for n in m.group(1).split(",")]
        continue
    m = re.search(r"Size: (0x[0-9A-Fa-f]+)", line)
    if m and cur:
        for n in cur:
            sizes[n] = int(m.group(1), 16)
        cur = None

calls, indirect = {}, set()
func = None
for line in run("llvm-objdump", "-d", "--no-show-raw-insn", elf).splitlines():
    m = re.match(r"^[0-9a-f]+ <(.+)>:$", line)
    if m:
        func = m.group(1)
        calls.setdefault(func, set())
        continue
    if func is None:
        continue
    m = re.search(r"\t(bl|b\.w|b|blx)\s+0x[0-9a-f]+ <([^>+]+)", line)
    if m and m.group(2) != func:
        calls[func].add(m.group(2))
    elif re.search(r"\tblx\s+r\d+", line):
        indirect.add(func)

targets_re = re.compile(os.environ.get("STACK_INDIRECT", r"Encode6encode"))
targets = {f for f in calls if targets_re.search(f)}
from_re = re.compile(os.environ.get("STACK_INDIRECT_FROM", r"orion_"))
unknown = {f for f in indirect if not from_re.search(f)}
for f in indirect - unknown:
    calls[f] |= targets - {f}
indirect = unknown

def depth(f, stack=()):
    if f in stack:
        return 0, [f + " (recursion)"], False
    best, path, ind = 0, [], f in indirect
    for g in calls.get(f, ()):
        d, p, i = depth(g, stack + (f,))
        ind |= i
        if d > best:
            best, path = d, p
    return sizes.get(f, 0) + best, [f] + path, ind

if not roots:
    roots = [n for n in ("__cortex_m_rt_main", ) if n in calls] or \
            [n for n in calls if n.endswith("___cortex_m_rt_main") or n.endswith("__cortex_m_rt_main")]
    roots += [n for n in ("SysTick", "HardFault_", "DefaultHandler_") if n in calls]
total = 0
main_depth = 0
for r in roots:
    d, path, ind = depth(r)
    tag = " (lower bound: indirect calls)" if ind else ""
    if "main" in r:
        main_depth = d
        print(f"main: {d} B{tag}")
    else:
        print(f"{r}: {d} B + 104 B exception frame{tag}")
        total = max(total, d + 104)
    for p in path[:12]:
        print(f"    {sizes.get(p, 0):5d}  {p[:100]}")
print(f"worst case: {main_depth + total} B (main + deepest handler)")
