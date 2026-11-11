# Documentation

## Using Orion

- [getting-started.md](getting-started.md): build `orion`, run it, and talk to it from the
  command line and from a program.
- [client.md](client.md): the client, `orion`, and every role a program can have: topics,
  frames, records, actions, files, executors, providers, workloads; from a web page, from C,
  and the command line.
- [settings.md](settings.md): every setting of both services.
- [operating.md](operating.md): running a board: systemd, getting in, what to watch, what
  happens when something dies.
- [`../packaging/README.md`](../packaging/README.md): the systemd units and where each file
  installs.

## How it works

- [comm.md](comm.md): the comm core, every way into the board: the server, the topic broker,
  the carriers, SSH, recovery, links and devices.
- [node.md](node.md): node logic: records, workloads, actions, files and resources.
- [wire.md](wire.md): the one wire every carrier speaks.
- [ring.md](ring.md): the topic ring in shared memory.

## Why it is this way

- [design.md](design.md): the design, and [architecture.html](architecture.html), the same
  with drawings.
- [prs/](prs/): what each PR built, what proved it, and where it differs from its brief.
