#!/usr/bin/env python3
"""Shows which planned PRs are merged, open, ready to start, blocked, or dropped.

Reads docs/prs/*.md for each PR's needs, and GitHub for pr/<id> branches into next.

--offline reads the briefs alone and fails on one it cannot read: a name that is not an id, no
status, or a need that names no brief. The gate runs it so, so the list cannot rot unseen.
"""
import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
REPO = "Prometheus-Dynamics/Orion"


# A PR's id: its group's letter, a number, and a letter for one split off it (C35a).
ID = re.compile(r"([A-Z])(\d+)([a-z]?)")


def sort_key(pr_id):
    letter, number, part = ID.fullmatch(pr_id).groups()
    return (letter, int(number), part)


def read_briefs():
    """Each brief's needs, status and title, and what could not be read."""
    briefs, problems = {}, []
    for path in sorted((ROOT / "docs" / "prs").glob("*.md")):
        if not ID.fullmatch(path.stem):
            problems.append(f"{path.name}: the name is not a PR id")
            continue
        text = path.read_text()
        # Written as a line of its own or as an item of a list, both in use.
        status = re.search(r"^(?:- )?\*\*Status:\*\* (\w+)", text, re.M)
        if status is None:
            problems.append(f"{path.name}: no **Status:** line")
        # A brief written after the plan names no needs: it needs nothing.
        needs = re.search(r"^(?:- )?\*\*Needs:\*\* (.*)$", text, re.M)
        needs = needs.group(1).strip() if needs else "nothing"
        briefs[path.stem] = dict(
            needs=[] if needs in ("nothing", "none") else [n.strip() for n in needs.split(",")],
            done=status is not None and status.group(1) == "done",
            dropped=status is not None and status.group(1) == "dropped",
            title=text.splitlines()[0].lstrip("# ").strip(),
        )
    for pr_id, brief in briefs.items():
        kept = []
        for need in brief["needs"]:
            if " " in need:
                # Words about something outside Orion, such as another library's pin: not a PR
                # here, so nothing to wait for in this list.
                continue
            if need not in briefs:
                problems.append(f"{pr_id}.md: needs {need}, which has no brief")
            kept.append(need)
        brief["needs"] = kept
    return briefs, problems


def github_state():
    try:
        out = subprocess.run(
            ["gh", "pr", "list", "--repo", REPO, "--base", "next", "--state", "all",
             "--limit", "1000", "--json", "headRefName,state"],
            capture_output=True, text=True, check=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"could not read GitHub ({error}); treating every PR as not started", file=sys.stderr)
        return {}
    states = {}
    for pr in json.loads(out):
        match = re.fullmatch(r"pr/([A-Z]\d+[a-z]?)", pr["headRefName"])
        if match and states.get(match.group(1)) != "MERGED":
            states[match.group(1)] = pr["state"]
    return states


def main():
    offline = sys.argv[1:] == ["--offline"]
    if sys.argv[1:] and not offline:
        sys.exit("usage: pr-status.py [--offline]")
    briefs, problems = read_briefs()
    if problems:
        print("pr-status: briefs that cannot be read:", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        sys.exit(1)
    states = {} if offline else github_state()
    merged = {i for i, b in briefs.items() if b["done"] or states.get(i) == "MERGED"}
    groups = {"merged": [], "open": [], "ready": [], "blocked": [], "dropped": []}
    for pr_id in sorted(briefs, key=sort_key):
        if briefs[pr_id]["dropped"]:
            # Closed without being kept: neither to start nor waiting on anything.
            groups["dropped"].append(pr_id)
        elif pr_id in merged:
            groups["merged"].append(pr_id)
        elif states.get(pr_id) == "OPEN":
            groups["open"].append(pr_id)
        elif all(n in merged for n in briefs[pr_id]["needs"]):
            groups["ready"].append(pr_id)
        else:
            groups["blocked"].append(pr_id)
    print(f"merged ({len(groups['merged'])}): {' '.join(groups['merged'])}")
    print(f"open ({len(groups['open'])}): {' '.join(groups['open'])}")
    print(f"blocked ({len(groups['blocked'])})")
    print(f"dropped ({len(groups['dropped'])}): {' '.join(groups['dropped'])}")
    print(f"ready ({len(groups['ready'])}):")
    for pr_id in groups["ready"]:
        print(f"  {pr_id}  {briefs[pr_id]['title']}")


main()
