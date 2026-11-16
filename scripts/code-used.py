#!/usr/bin/env python3
"""Which code a program touched, by crate and by symbol.

    scripts/code-used.py <pages> <binary with symbols> [<pages at an earlier build> <its binary>]

<pages> is what `orion-drill pages read <pid> orion` printed on the board: a file offset and 0
or 1 per page of code. <binary with symbols> is the same build with its symbols kept
(`CARGO_PROFILE_SERVICE_STRIP=false`); stripping takes symbols, not code, so the offsets match.
Each touched page's bytes are shared out among the symbols it holds, by how much of it each
covers, and summed by crate: the first path segment of the symbol, `std`, `core` and `alloc`
counted apart. With a second pair, the two are compared crate by crate.
"""

import collections
import re
import subprocess
import sys


def loads(binary):
    """The executable LOAD segments: (file offset, virtual address, size)."""
    out = subprocess.run(["readelf", "-lW", binary], capture_output=True, text=True, check=True).stdout
    segments = []
    for line in out.splitlines():
        parts = line.split()
        if parts[:1] == ["LOAD"] and "E" in parts[6:-1]:
            segments.append((int(parts[1], 16), int(parts[2], 16), int(parts[5], 16)))
    return segments


def symbols(binary):
    """Function symbols as (file offset, size, name), from the binary's symbol table."""
    out = subprocess.run(
        ["nm", "-S", "--defined-only", "--demangle", binary],
        capture_output=True, text=True, check=True,
    ).stdout
    segments = loads(binary)
    found = []
    for line in out.splitlines():
        parts = line.split(" ", 3)
        if len(parts) < 4 or parts[2] not in "tTwW":
            continue
        address, size, name = int(parts[0], 16), int(parts[1], 16), parts[3]
        for offset, vaddr, length in segments:
            if vaddr <= address < vaddr + length:
                found.append((address - vaddr + offset, size, name))
                break
    found.sort()
    return found


def crate(name):
    """The crate a demangled symbol belongs to."""
    name = name.lstrip("<&*")
    match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)::", name)
    return match.group(1) if match else "(other)"


def pages(path):
    """The page size and the offsets of touched pages."""
    touched, offsets = [], []
    for line in open(path):
        parts = line.split()
        if len(parts) == 2 and parts[0].startswith("0x"):
            offset = int(parts[0], 16)
            offsets.append(offset)
            if parts[1] == "1":
                touched.append(offset)
    offsets.sort()
    size = min(b - a for a, b in zip(offsets, offsets[1:]) if b > a)
    return size, touched


def account(pages_path, binary):
    """Touched bytes by crate and by symbol."""
    size, touched = pages(pages_path)
    table = symbols(binary)
    by_crate, by_symbol = collections.Counter(), collections.Counter()
    starts = [entry[0] for entry in table]
    import bisect
    for page in touched:
        end = page + size
        covered = 0
        index = max(bisect.bisect_right(starts, page) - 1, 0)
        while index < len(table) and table[index][0] < end:
            start, length, name = table[index]
            overlap = min(start + length, end) - max(start, page)
            if overlap > 0:
                by_crate[crate(name)] += overlap
                by_symbol[name] += overlap
                covered += overlap
            index += 1
        if covered < size:
            by_crate["(no symbol)"] += size - covered
    return size, len(touched), by_crate, by_symbol


def main():
    if len(sys.argv) not in (3, 5):
        print(__doc__)
        sys.exit(2)
    size, count, crates, syms = account(sys.argv[1], sys.argv[2])
    print(f"pages of {size // 1024} KiB touched: {count}, {count * size // 1024} KiB")
    if len(sys.argv) == 5:
        _, before_count, before, _ = account(sys.argv[3], sys.argv[4])
        print(f"earlier: {before_count} pages")
        print(f"{'crate':40} {'earlier KiB':>12} {'now KiB':>10} {'change':>8}")
        names = sorted(set(crates) | set(before), key=lambda n: -(crates[n] - before[n]))
        for name in names[:40]:
            print(f"{name:40} {before[name] / 1024:12.1f} {crates[name] / 1024:10.1f} "
                  f"{(crates[name] - before[name]) / 1024:+8.1f}")
        return
    print(f"{'crate':40} {'KiB':>8}")
    for name, used in crates.most_common(40):
        print(f"{name:40} {used / 1024:8.1f}")
    print()
    print("symbols with the most touched bytes:")
    for name, used in syms.most_common(40):
        print(f"{used / 1024:8.1f}  {name[:110]}")


if __name__ == "__main__":
    main()
