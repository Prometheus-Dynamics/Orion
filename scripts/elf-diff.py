#!/usr/bin/env python3
"""Where two builds of one file differ: by ELF section, and the strings each has alone.

    elf-diff.py A B

For the size gate's path check: when a build made at another path has other bytes, this says
what in them depends on the path, so the cause can be named. Uses `readelf` for the sections
where it is installed; the byte and string counts need nothing.
"""

import re
import subprocess
import sys


def sections(path):
    try:
        out = subprocess.run(["readelf", "-SW", path], capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return []
    found = []
    for line in out.splitlines():
        match = re.match(r"\s*\[\s*\d+\]\s+(\S+)\s+\S+\s+[0-9a-f]+\s+([0-9a-f]+)\s+([0-9a-f]+)", line)
        if match:
            found.append((match.group(1), int(match.group(2), 16), int(match.group(3), 16)))
    return found


def strings(data, least=8):
    return set(re.findall(rb"[\x20-\x7e]{%d,}" % least, data))


def main(argv):
    if len(argv) != 2:
        print(__doc__.strip().splitlines()[2].strip(), file=sys.stderr)
        return 2
    a, b = (open(path, "rb").read() for path in argv)
    print(f"      sizes: {len(a)} and {len(b)} bytes")
    differing = [i for i in range(min(len(a), len(b))) if a[i] != b[i]]
    print(f"      bytes that differ in the common length: {len(differing)}")
    by_section = {}
    table = sections(argv[0])
    for offset in differing:
        name = next((n for n, start, size in table if start <= offset < start + size), "(outside any section)")
        by_section[name] = by_section.get(name, 0) + 1
    for name, count in sorted(by_section.items(), key=lambda item: -item[1])[:12]:
        print(f"      {name}: {count} bytes")
    only_a, only_b = strings(a) - strings(b), strings(b) - strings(a)
    for label, found in (("first only", only_a), ("second only", only_b)):
        print(f"      strings in the {label}: {len(found)}")
        for text in sorted(found)[:15]:
            print(f"        {text[:160].decode()}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
