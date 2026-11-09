#!/usr/bin/env python3
"""Shows which planned PRs are merged, open, ready to start, or blocked.

Reads docs/prs/*.md for each PR's needs, and GitHub for pr/<id> branches into next.
"""
import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
REPO = "Prometheus-Dynamics/Orion"


def sort_key(pr_id):
    return (pr_id[0], int(pr_id[1:]))


def read_briefs():
    briefs = {}
    for path in (ROOT / "docs" / "prs").glob("*.md"):
        text = path.read_text()
        needs = re.search(r"^\*\*Needs:\*\* (.*)$", text, re.M).group(1)
        done = re.search(r"^\*\*Status:\*\* done", text, re.M) is not None
        title = text.splitlines()[0].lstrip("# ").strip()
        briefs[path.stem] = dict(
            needs=[] if needs == "nothing" else [n.strip() for n in needs.split(",")],
            done=done,
            title=title,
        )
    return briefs


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
        match = re.fullmatch(r"pr/([A-I]\d+)", pr["headRefName"])
        if match and states.get(match.group(1)) != "MERGED":
            states[match.group(1)] = pr["state"]
    return states


def main():
    briefs = read_briefs()
    states = github_state()
    merged = {i for i, b in briefs.items() if b["done"] or states.get(i) == "MERGED"}
    groups = {"merged": [], "open": [], "ready": [], "blocked": []}
    for pr_id in sorted(briefs, key=sort_key):
        if pr_id in merged:
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
    print(f"ready ({len(groups['ready'])}):")
    for pr_id in groups["ready"]:
        print(f"  {pr_id}  {briefs[pr_id]['title']}")


main()
