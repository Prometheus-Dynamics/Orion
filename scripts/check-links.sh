#!/usr/bin/env bash
# Every link in the repository's Markdown goes somewhere: a file or directory that exists, and
# for `file.md#part` or `#part`, a heading there with that anchor (as GitHub makes them).
# Links to the web are not followed.
#
#   scripts/check-links.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
python3 - << 'PY'
import re, subprocess, sys
from pathlib import Path

root = Path.cwd()
files = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard", "*.md"],
                       capture_output=True, text=True, check=True).stdout.split()
LINK = re.compile(r"(?<!\!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)|!\[[^\]]*\]\(([^)\s]+)\)")

def prose(text):
    """The text outside fenced code blocks and inline code."""
    out, fenced = [], False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
            out.append("")
            continue
        out.append("" if fenced else re.sub(r"`[^`]*`", "", line))
    return out

def slug(heading):
    text = re.sub(r"`", "", heading.strip().lower())
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")

anchors = {}
def anchors_of(path):
    if path not in anchors:
        seen, found = {}, set()
        for line in prose(path.read_text(encoding="utf-8")):
            m = re.match(r"#{1,6}\s+(.*)", line)
            if not m:
                continue
            base = slug(m.group(1))
            n = seen.get(base, 0)
            seen[base] = n + 1
            found.add(base if n == 0 else f"{base}-{n}")
        anchors[path] = found
    return anchors[path]

errors = []
for name in files:
    path = root / name
    if not path.is_file():
        continue
    for number, line in enumerate(prose(path.read_text(encoding="utf-8")), 1):
        for m in LINK.finditer(line):
            target = m.group(1) or m.group(2)
            if re.match(r"^[a-z][a-z0-9+.-]*:", target):
                continue
            file_part, _, anchor = target.partition("#")
            to = path if not file_part else (path.parent / file_part).resolve()
            if not to.exists():
                errors.append(f"{name}:{number}: {target}: no such file")
            elif anchor and to.suffix == ".md" and anchor not in anchors_of(to):
                errors.append(f"{name}:{number}: {target}: no heading #{anchor}")
for error in errors:
    print(f"    {error}")
print(f"    {len(files)} files, {len(errors)} broken links")
sys.exit(1 if errors else 0)
PY
