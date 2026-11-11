# Operating a board

What to know to run Orion on a board: how it is started, how to get in, what to watch, and what
happens when something dies. The details are in the reference pages each section links to.

## Started by systemd

A board runs `orion comm` and `orion node` as two services from the files in
[`packaging/`](../packaging/README.md): the comm core as root with group `orion`, node logic as
the `orion` user, both reading `/etc/orion/orion.conf` ([settings.md](settings.md)).

- The comm core is reported started once it serves, tells systemd it is alive every 15 s, and
  is restarted if it stops saying so. Both services are always restarted, after 1 s and then
  up to 30 s apart, and never given up on.
- The services log to standard error, so to the journal: `journalctl -u orion-comm`.
  `ORION_LOG=debug` in a drop-in says more.
- `systemctl status orion-comm` shows the comm core's own status line: what it serves, or
  `recovery: <why>`.

## Getting in

- **SSH**, answered by the comm core itself when `ssh.listen` is set: logins as the board's own
  users, with sftp, scp and `ssh -L`. No sshd is needed ([comm.md](comm.md), "The SSH way in").
- **Programs and tools** connect to the socket on the board, or to `peer.listen` across the
  network: `orion --address orion+tcp://board:port node show`.
- **Browsers** connect to `ws.listen` ([comm.md](comm.md), "The WebSocket carrier").
- **When the settings or the state directory are broken**, the comm core does not stop: it
  opens the recovery way in, SSH with the keys in `/etc/orion/recovery_keys`, and stays up
  until it is fixed ([comm.md](comm.md), "Recovery"). `orion-recovery`, the comm core alone, is
  the same way in for an initramfs or a flasher image.

## What to watch

There is no metrics endpoint: the board's vital signs and Orion's own state are topics, read
like any other ([comm.md](comm.md), "The board's vital signs and Orion's own state"):

```bash
orion topic watch orion.host
```

```bash
orion topic read orion.comm
```

`orion.state.<part>` is each part's state, failures and when it tries again: a link that has
lost its device, a peer that is away. Nothing is sampled while nobody watches.

## When something dies

The design's table says what keeps working when each part dies ([design.md](design.md), the
table under "Two programs, five forms"), and `crates/testing/drills` holds every row of it.
In short:

- **Node logic.** Everything but records, actions and workload requests goes on; those are
  refused as "unavailable" until it is back, and then answered as before.
- **The comm core.** Programs on the board keep exchanging topics and frames, which live in
  shared memory. Nothing new connects until it is back; then everything connects again.
- **An executor.** What it ran goes with it, and is started again when an executor is back.
- **Both services.** Workloads keep running and are taken back by name, not started twice.
  A change made in the half second before node logic died may be lost
  (`records.save_after_ms`).

A misbehaving client, peer, provider or link is cut off without the others noticing. Nothing
backs off for good: a part that fails tries again, at most 5 to 10 s apart, and access paths
never wait.

## Updating Orion

Replace `/usr/bin/orion` and restart both services: workloads keep running through it and are
taken back, and programs on the board connect again by themselves.

## What is kept

- `/var/lib/orion`: the comm core's identity, trust store and SSH host key.
- `/var/lib/orion/node`: node logic's records (what should run, resources, actions), files
  sent to the board, and the audit log.

Losing the first gives the board a new identity, which the other nodes must trust again.
Losing the second loses what should run.
