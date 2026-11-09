# TODO

Where the `next` rebuild stands. The design is [`docs/design.md`](docs/design.md).

## Now

- [ ] Assemble the PR list: briefs under `docs/prs/`, a gate script and a status script, in the form
      the Lemnos `next` branch uses, sized so one session can take one.

## Planned work, in groups

The PR list breaks these down.

1. Wire and client.
2. Comm core: connections, the one check, rings, buffer topics, hello, discovery, time.
3. Node logic: records, peer sync, workloads, actions, files.
4. Gateways and the browser path: WebSocket, NT4, the TypeScript client.
5. Access and recovery: SSH, sftp, the recovery shell, the recovery build.
6. Microcontroller side, alongside the others, with its size gate.

## Pinned

Direction is agreed. The detail comes back later.

- Board management (update, slots, reboot, identity, USB). It goes with Lemnos as drivers that add
  their actions, and Orion only routes them. Orion's part is built regardless: the open action
  list, the check, progress, and delivering files. See "Pinned" in the design.

## Open

- What wakes a graph. Every topic gives a subscriber something it can sleep on, so any answer works.
- Pinning a graph to a core. A workload's placement has room for a CPU set.
