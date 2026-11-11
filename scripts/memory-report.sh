#!/bin/sh
# What processes hold in memory, split the way the budget is: code, shared rings, and data.
#
#   scripts/memory-report.sh PID...
#
# Runs on a board (busybox sh and awk are enough). From each process's /proc/PID/smaps, by
# proportional set size (Pss), so a page two processes share is counted half in each and once
# in the total:
#
# - code: mappings of a file that can be executed (the program and its libraries' text). The
#   kernel maps text around each fault (64 KiB by default), so this is most of the text that is
#   in the page cache, touched or not;
# - used: the part of code the process has touched (Referenced) since its reference bits were
#   last cleared (echo 1 > /proc/PID/clear_refs), shared out like Pss;
# - rings: shared memory made with memfd_create (topic rings, frame pools, marks);
# - data: everything else: heap, stacks, other anonymous memory, the program's own static
#   data, and read-only data of its files.
#
# Prints a line a process and a total, in KiB.
set -eu
[ "$#" -gt 0 ] || { echo "usage: $0 PID..." >&2; exit 2; }
for pid in "$@"; do
    name=$(tr '\0' ' ' < "/proc/$pid/cmdline" | cut -c1-60)
    awk -v pid="$pid" -v name="$name" '
        function flush() {
            if (kind == "") return
            sum[kind] += pss
            if (kind == "code" && rss > 0) used += pss * ref / rss
        }
        /^[0-9a-f]+-[0-9a-f]+ / {
            flush()
            perms = $2; file = $6; rss = 0; pss = 0; ref = 0
            kind = "data"
            if (index(file, "/memfd:") == 1) kind = "rings"
            else if (substr(perms, 3, 1) == "x" && substr(file, 1, 1) == "/") kind = "code"
            next
        }
        /^Rss:/ { rss = $2 }
        /^Pss:/ { pss = $2 }
        /^Referenced:/ { ref = $2 }
        END {
            flush()
            printf "%-8s code %6d  used %6d  rings %6d  data %6d  %s\n", pid, sum["code"], used, sum["rings"], sum["data"], name
        }
    ' "/proc/$pid/smaps"
done | tee /tmp/memory-report.$$
awk '{ c += $3; u += $5; r += $7; d += $9 } END { printf "total    code %6d  used %6d  rings %6d  data %6d\n", c, u, r, d }' /tmp/memory-report.$$
rm -f /tmp/memory-report.$$
