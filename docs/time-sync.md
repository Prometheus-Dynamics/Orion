# Time sync

The node can keep the board's clock right by itself, so an image does not need
`systemd-timesyncd` (2.8 MB PSS on the test board) or any other NTP daemon. It is the feature
`time-sync` of `orion-node` (included in `full`), configured with the `ORION_NODE_TIME_*`
variables in [node-env.md](node-env.md#time-sync).

## Sources

- **SNTP servers** (RFC 4330, client mode): any NTP server, `host[:port]`.
- **Another Orion node**, a fleet peer, written `peer://host[:port]`. A node started with `ORION_NODE_TIME_SERVE=addr:port` answers SNTP on
  that port, over UDP and also over TCP (the same 48 bytes each way, one exchange per
  connection). A board with no internet takes its time from the controller or a PC that has it.
  A source written `tcp://host:port` is asked over TCP, so time also crosses TCP-only paths,
  for example an SSH tunnel.

A serving node says how good its clock is:

| Its clock | Answer |
| --- | --- |
| synchronized by this node's own time sync | its source's stratum plus one |
| synchronized by another daemon (the kernel's `STA_UNSYNC` is clear) | stratum 3 |
| not synchronized, `ORION_NODE_TIME_SERVE_AUTHORITY=1` | stratum 10: this node is the network's reference |
| not synchronized | "unsynchronized" (leap indicator 3, stratum 16): clients ignore it |

Queries are unauthenticated, as SNTP is. A request and its answer are the same size, so a
serving node is no amplifier.

## Modes

- `report`: measures the offset to the sources and publishes it, never touching the clock. Safe
  next to timesyncd, for an A/B before switching an image over.
- `discipline`: also adjusts the clock. It needs `CAP_SYS_TIME`. While `systemd-timesyncd`
  runs, the node only reports, so two daemons never fight over the clock.

## Rules

They follow timesyncd:
- Offsets up to 0.4 s are slewed through the kernel's clock discipline (`adjtimex`), which also
  marks the clock synchronized (`STA_UNSYNC` cleared), so `timedatectl` and the node's
  [clock facts](observability.md#clock-facts) show it.
- A larger offset is stepped (`clock_settime`), but only once two samples agree within 0.1 s, so
  one bad answer cannot move the clock.
- After the first synchronization the clock is never stepped backwards: a large negative offset
  is reported and left alone.
- Answers that took over 1 s to come back are not used.
- The poll interval starts at 2 s until synchronized, then runs from
  `ORION_NODE_TIME_POLL_MIN_MS` (64 s), doubling up to `ORION_NODE_TIME_POLL_MAX_MS` (1024 s)
  while the offset stays under 50 ms.
- The last synchronized time is saved to `<state dir>/time-sync.last`. At start, a clock behind
  it (a board without a working RTC) is stepped forward to it before the first sample.

## An operator's clock

A board on a network with no internet can take its time from the computer managing it, over the
Orion connection that computer already has. The board needs no address for it.

- On the node: put `operator` in `ORION_NODE_TIME_SERVERS`, first if it should win
  (`ORION_NODE_TIME_SERVERS=operator,pool.ntp.org`).
- On the computer: `RemoteOperator::serve_time(stratum)` runs until aborted, at the pace the node
  asks for (`offer_time` is one round). `stratum` is 3 for a computer kept by NTP.
- The operator must be allowed the action `clock.set`; serving the time is the same power as
  setting it. Any other operator is refused.

The exchange is two signed requests. The node opens with a token holding its monotonic time; the
operator answers at once with its clock, and the node measures the round trip on its own clock.
When no operator is offering, the next source in the list is used.

## Wrong sources

List order is rank. A source whose time cannot be right is passed over for the next one, and
`time.error` says why:

- before this build existed (never earlier than 2026-01-01; `SOURCE_DATE_EPOCH` when the build
  sets it): applies to every source;
- from a fleet peer only (`peer://host[:port]`, or `tcp://host:port`): over a day behind the last
  synchronized time, or over 400 days past it. Internet NTP and `operator` are not held to this,
  so a board left off for a week still takes the right time.

## Synchronized flag

`/run/orion/time-synced` (`ORION_NODE_TIME_SYNCED_FILE`, `off` for none) exists while the node
disciplines the clock and it is synchronized, the role of timesyncd's
`/run/systemd/timesync/synchronized`. It is removed at start, at shutdown and when adjusting the
clock fails. `report` mode never creates it.

## Status

The time sync publishes these status keys on the node (publisher `node:time`), with a
time-to-live of three polls:

| Key | Type | Meaning |
| --- | --- | --- |
| `time.mode` | string | `report` or `discipline` (`report` while timesyncd runs) |
| `time.synced` | bool | this node has synchronized the clock |
| `time.source` | string | the source of the last sample |
| `time.offset_ms` | f64 | source time minus local time at the last sample |
| `time.delay_ms` | f64 | the last sample's round trip |
| `time.stratum` | uint | the source's stratum |
| `time.last_action` | string | `stepped`, `slewed`, `would step`, `waiting for a second sample`, ... |
| `time.error` | string | why the last poll gave no sample, or empty |

## Switching an image over

1. Turn on `ORION_NODE_TIME_SYNC=report` with the image's sources next to timesyncd, and compare
   `time.offset_ms` with what timesyncd reports.
2. Mask `systemd-timesyncd.service`, set `ORION_NODE_TIME_SYNC=discipline`, and give the node
   `CAP_SYS_TIME`: `packaging/systemd/orion-node.service.d/time-sync.conf.example` is the drop-in for the
   packaged unit.

## Cost

x86_64 release: +93 KB on the default `orion-node` build (3,350,296 to 3,443,768 B), +84 KB on
`full` (6,056,992 to 6,141,456 B). About half is the code (the sync loop, the UDP and TCP
servers, name lookups); the rest is unwinding tables and constants. No new crates: the clock calls
come through `libc`. At run time: one UDP socket and one TCP listener when serving, a query socket
per poll, and a timer.

## Measured on the test board

2026-11-07, test board #2: no RTC, timesyncd active but not synchronized (no reachable server).
Its only network is the controller's link, and the controller's clock was a year off, so the
source was a node on the PC serving its synchronized clock, reached over TCP through an SSH
tunnel. In `report` mode, the board measured itself 13,711 s (3.8 h) behind, with a 1 ms
round trip, stratum 3. Two samples 2 s apart agreed, so `discipline` would have stepped. It did
not, because timesyncd was running.
