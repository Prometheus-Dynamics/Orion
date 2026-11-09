# Working on the Orion `next` branch

`next` is a clean-slate rebuild of Orion. It started empty on purpose. `dev` is the frozen reference,
and nothing here changes it. Read [`docs/design.md`](docs/design.md) first: it is the design
everything here builds.

The work will be a pre-planned list of PRs into `next`. That list has not been assembled yet.
[`TODO.md`](TODO.md) says where things stand.

## Reading dev

`git fetch origin dev`, then `git show origin/dev:<path>`. Bring across only what the design calls
for, on purpose, and rework it to fit. Never check out another agent's checkout.

## Rules

- Orion is orchestration and interconnect. It does not know what a camera, a pose or a graph is,
  how to speak to hardware, or how to install anything on a particular board.
- No library needs another one in order to work. A bridge to Styx, Daedalus or Lemnos is an optional
  feature, and Orion's core builds without them.
- One wire. Every carrier uses `wire/`: stable kind numbers, append-only bodies, one revision agreed
  at hello, recorded bytes in tests. The protocol is version 1.
- Every request passes the one check. A handler can only be called with a `Checked<T>`, and only
  the check can make one. Registering an action requires a policy. With security off the check
  costs nothing.
- `comm` and `node` never import each other. They share `wire`.
- Fast data does not go through a daemon. Topics are shared-memory rings that Orion sets up once.
  A publish takes no reply and no timer. With nobody watching, a publish is a memory write.
- Steady-state paths allocate nothing and copy nothing they don't have to.
- Failure rule: never give up, backoff capped at 5 to 10 s, no default auto-off, an easy manual off,
  auto-off only as a configured policy, backoff state visible. Access paths are never backed off.
- One bad connection, peer, provider or link never takes the rest down.
- `unsafe` only in `ring/`, `capi/` and where passing file descriptors requires it, with a `SAFETY`
  comment on each block.
- Everything is behind features. Lean is the default. A recovery build has no `node/`.
- Breaking other repositories is fine. They are not built here and they catch up afterward.
- Hardware you can't reach is marked `untested on board`. Don't hold work back for a board run.
- Timings count only when measured on the CM5.
- Never claim something works without having run it. Report failures with their output.
- Never push tags. Never force-push `next` or `dev`. Don't change `dev`.
- Don't estimate how long anything will take. Describe size by scope and risk.
- Product names appear only in the planning files (this file, `TODO.md`, `docs/design.md`,
  `docs/architecture.html`).
- Builds and scratch go on NVMe or SSD, never the Github HDD.
