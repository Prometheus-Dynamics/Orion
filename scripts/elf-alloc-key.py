#!/usr/bin/env python3
"""What a program does when run, as a key: its ELF's allocated sections, nothing else.

    elf-alloc-key.py FILE...          one line each: the alloc key, the whole file's SHA-256, FILE
    elf-alloc-key.py --sections FILE  each allocated section: name, address, flags, size, digest
    elf-alloc-key.py --self-test      planted ELF changes (needs cc and objcopy)

A test binary's bytes change with what never runs: debug info, the symbol table, the build-id,
a comment that moved no code. Its allocated sections (SHF_ALLOC: what the loader maps) are what
runs and what it reads. The key is each such section's name, address, flags and bytes, in
order; a NOBITS section (.bss, .tbss) by its size, having no bytes. `.note.gnu.build-id` is left
out: it is a hash of the whole link, debug info included. The ELF header's entry point and type
go in too, so a binary that starts elsewhere is another key.

Two binaries with one key load the same bytes at the same addresses, so they run the same.

A file this cannot read as a 64-bit little-endian ELF (truncated, malformed, another format) is
keyed by its whole content instead, `file-` and its SHA-256: never by less than all of it, so
it can only miss a record, never match one it should not.
"""

import hashlib
import struct
import sys

SHF_ALLOC = 0x2
SHT_NOBITS = 8
LEFT_OUT = {".note.gnu.build-id"}


def sections(data):
    if data[:4] != b"\x7fELF":
        raise ValueError("not an ELF file")
    if data[4] != 2 or data[5] != 1:
        raise ValueError("not a 64-bit little-endian ELF file")
    e_type, _machine, _version, e_entry = struct.unpack_from("<HHIQ", data, 16)
    e_shoff = struct.unpack_from("<Q", data, 40)[0]
    e_shentsize, e_shnum, e_shstrndx = struct.unpack_from("<HHH", data, 58)
    if e_shnum == 0 or e_shoff + e_shnum * e_shentsize > len(data) or e_shstrndx >= e_shnum:
        raise ValueError("no section headers, or they run past the end of the file")
    headers = []
    for i in range(e_shnum):
        headers.append(struct.unpack_from("<IIQQQQIIQQ", data, e_shoff + i * e_shentsize))
    names = headers[e_shstrndx]
    names_at = names[4]

    def name_of(offset):
        end = data.index(b"\0", names_at + offset)
        return data[names_at + offset : end].decode()

    out = []
    for name_offset, kind, flags, address, offset, size, _link, _info, _align, _entsize in headers:
        if not flags & SHF_ALLOC:
            continue
        name = name_of(name_offset)
        if name in LEFT_OUT:
            continue
        if kind != SHT_NOBITS and offset + size > len(data):
            raise ValueError(f"section {name} runs past the end of the file")
        body = b"" if kind == SHT_NOBITS else data[offset : offset + size]
        out.append((name, address, flags, kind, size, body))
    return (e_type, e_entry), out


def alloc_key(data):
    try:
        head, found = sections(data)
    except (ValueError, struct.error, IndexError, UnicodeDecodeError):
        return "file-" + hashlib.sha256(data).hexdigest()
    sha = hashlib.sha256()
    sha.update(b"elf-alloc-key 1\0%d\0%d\0" % head)
    for name, address, flags, kind, size, body in found:
        sha.update(b"%s\0%d\0%d\0%d\0%d\0" % (name.encode(), address, flags, kind, size))
        sha.update(hashlib.sha256(body).digest())
    return sha.hexdigest()


def self_test():
    import os
    import subprocess
    import tempfile

    failures = []
    work = tempfile.mkdtemp(prefix="orion-elf-key.")
    try:
        source = os.path.join(work, "p.c")
        with open(source, "w") as file:
            file.write('#include <stdio.h>\nint main(void) { puts("planted string"); return 0; }\n')
        base = os.path.join(work, "base")
        subprocess.run(["cc", "-O1", "-o", base, source], check=True)

        def key_of(path):
            with open(path, "rb") as file:
                return alloc_key(file.read())

        def whole(path):
            with open(path, "rb") as file:
                return hashlib.sha256(file.read()).hexdigest()

        def objcopy(name, *args):
            path = os.path.join(work, name)
            # objcopy warns that an allocated section added is in no segment: so planted.
            subprocess.run(["objcopy", *args, base, path], check=True, stderr=subprocess.DEVNULL)
            return path

        note = os.path.join(work, "note")
        with open(note, "wb") as file:
            file.write(b"a note no loader maps")
        k = key_of(base)
        if k.startswith("file-") or key_of(base) != k:
            failures.append("the base binary has no section key, or not the same one twice")
        added = objcopy("added", "--add-section", f".orion.planted={note}")
        if whole(added) == whole(base):
            failures.append("objcopy added no section")
        if key_of(added) != k:
            failures.append("a section not allocated, added, moved the key")
        alloc = objcopy("alloc", "--add-section", f".orion.planted={note}",
                        "--set-section-flags", ".orion.planted=alloc,load,readonly,data")
        if key_of(alloc) == k:
            failures.append("the same section, allocated, left the key as it was")
        with open(base, "rb") as file:
            data = bytearray(file.read())
        at = data.find(b"planted string")
        if at < 0:
            failures.append("the planted string is not in the binary")
        else:
            data[at] ^= 1
            if alloc_key(bytes(data)) == k:
                failures.append("a byte of .rodata changed left the key as it was")
        with open(base, "rb") as file:
            data = file.read()
        for what, broken in (
            ("truncated", data[: len(data) // 2]),
            ("empty", b""),
            ("not an ELF file", b"#!/bin/sh\nexit 0\n"),
            ("section headers past its end", data[:40] + struct.pack("<Q", len(data) * 4) + data[48:]),
        ):
            got = alloc_key(broken)
            if got != "file-" + hashlib.sha256(broken).hexdigest():
                failures.append(f"a {what} file was not keyed by its whole content: {got}")
    finally:
        subprocess.run(["rm", "-rf", work], check=False)
    for failure in failures:
        print(f"self-test FAILED: {failure}")
    if failures:
        return 1
    print("elf alloc key self-test passed")
    return 0


def main(argv):
    if argv[:1] == ["--self-test"]:
        return self_test()
    if argv[:1] == ["--sections"]:
        with open(argv[1], "rb") as file:
            _, found = sections(file.read())
        for name, address, flags, kind, size, body in found:
            print(f"{name}\t{address:#x}\t{flags:#x}\t{size}\t{hashlib.sha256(body).hexdigest()[:16]}")
        return 0
    if not argv:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    for path in argv:
        with open(path, "rb") as file:
            data = file.read()
        print(alloc_key(data), hashlib.sha256(data).hexdigest(), path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
