#!/usr/bin/env python3
"""Flash bytes of an ELF's code by the source file each instruction came from (needs debug line
info; it does not change the code), so inlined code is charged to where it was written.

Usage: flash-by-source.py <elf>    Prints "component bytes" lines, largest first.
"""
import re, subprocess, sys

COMPONENTS = [
    (r"crates/link/src/device/", "device session"),
    (r"crates/link/src/wire/streams", "streams codec"),
    (r"crates/link/src/wire/", "wire codec"),
    (r"crates/link/src/(crc|frame)", "frame + CRC-32C"),
    (r"crates/link/src/(packet|transport|stream|source)", "framing (CAN segments / COBS)"),
    (r"crates/link/src/", "link (other)"),
    (r"examples/stm32g4-device/src/(fdcan|usart)", "FDCAN / USART driver"),
    (r"examples/stm32g4-device/src/", "application"),
    (r"compiler_builtins|compiler-builtins", "compiler-builtins (mem*)"),
    (r"cortex-m-rt", "cortex-m-rt (reset, vectors)"),
    (r"library/core/src/(fmt|panic)", "core fmt + panic"),
    (r"library/core/", "core (slices, iterators)"),
]

out = subprocess.run(["llvm-objdump", "-d", "-l", "--no-show-raw-insn", sys.argv[1]],
                     check=True, capture_output=True, text=True).stdout
insns = []  # (addr, component)
current = "unknown"
for line in out.splitlines():
    m = re.match(r"^; (/.*|[^ ].*):\d+", line)
    if m:
        path = m.group(1)
        current = next((c for pat, c in COMPONENTS if re.search(pat, path)), path.split("/")[-1])
        continue
    m = re.match(r"^\s*([0-9a-f]+):\s", line)
    if m:
        insns.append((int(m.group(1), 16), current))
    elif re.match(r"^[0-9a-f]+ <", line):
        current = "unknown"
total = {}
for (a, c), (b, _) in zip(insns, insns[1:]):
    if 0 < b - a <= 8:
        total[c] = total.get(c, 0) + (b - a)
for c, n in sorted(total.items(), key=lambda kv: -kv[1]):
    print(f"{c:34s} {n:6d}")
print(f"{'total code':34s} {sum(total.values()):6d}")
