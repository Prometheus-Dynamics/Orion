# Working on the Orion `next` branch

`next` is a clean-slate rebuild of Orion. It started empty on purpose. `dev` is the frozen reference,
and nothing here changes it. Read [`docs/design.md`](docs/design.md) first: it is the design
everything here builds.

The work is a pre-planned list of PRs into `next`.

## If you are picking this up

1. Read [`docs/design.md`](docs/design.md).
2. Run `python3 scripts/pr-status.py` to see what's merged, open, ready and blocked.
3. Take a PR marked ready and read its brief at `docs/prs/<id>.md`. The brief is the whole
   assignment: what it needs, what it creates, which files on `dev` to read, what to build and what
   proves it.

[`TODO.md`](TODO.md) is the full list.

## Working a PR

- Base branch `next`, working branch `pr/<id>` (for example `pr/A3`), one PR into `next`.
- Read `dev` as a reference: `git fetch origin dev`, then `git show origin/dev:<path>`. Bring in only
  what the brief lists, and rework it to fit. Never check out another agent's checkout.
- A reading list naming another repository means exactly that: `Styx` is
  `Prometheus-Dynamics/Styx`, `Daedalus` is `Prometheus-Dynamics/Daedalus`, `HeliOS` is its own
  repository. Those are references to read and are not added as dependencies unless the brief says.
- Stay inside the paths your PR creates. Don't edit the root `Cargo.toml`, `TODO.md`, or a changelog.
  `Cargo.lock` is ignored. Keeping to this is what lets many sessions run at once without conflicts.
- Add a crate by creating its directory under `crates/<folder>/`. Name it `orion-<directory>`.
  Inherit shared dependencies with `workspace = true`, and declare any others in your own crate.
- If your PR needs a shared interface changed, stop and say so in the PR description. The interfaces
  in the design change only through A-series or B-series PRs.
- A PR merges when `scripts/gate.sh` passes. Paste the proof named in the brief into the PR
  description.

## Rules

- Orion is orchestration and interconnect. It does not know what a camera, a pose or a graph is,
  how to speak to hardware, or how to install anything on a particular board.
- No library needs another one in order to work. A bridge to Styx, Daedalus or Lemnos is an optional
  feature, and Orion's core builds without them.
- One wire. Every carrier uses `crates/core/wire`: stable kind numbers, append-only bodies, one revision agreed
  at hello, recorded bytes in tests. The protocol is version 1.
- Every request passes the one check. A handler can only be called with a `Checked<T>`, and only
  the check can make one. Registering an action requires a policy. With security off the check
  costs nothing.
- `crates/comm/*` and `crates/node/*` never depend on each other. They share `crates/core/*`.
- Fast data does not go through a daemon. Topics are shared-memory rings that Orion sets up once.
  A publish takes no reply and no timer. With nobody watching, a publish is a memory write.
- Steady-state paths allocate nothing and copy nothing they don't have to.
- Failure rule: never give up, backoff capped at 5 to 10 s, no default auto-off, an easy manual off,
  auto-off only as a configured policy, backoff state visible. Access paths are never backed off.
- One bad connection, peer, provider or link never takes the rest down.
- `unsafe` only in `crates/core/ring*`, `crates/core/fdpass`, `crates/core/measure`, `crates/client/capi`
  and `crates/device/*` ports, with a `SAFETY` comment on each block.
- Everything is behind features. Lean is the default. A recovery build has no node logic.
- Breaking other repositories is fine. They are not built here and they catch up afterward.
- Hardware you can't reach is marked `untested on board`. Don't hold work back for a board run.
- Timings count only when measured on the CM5.
- Never claim something works without having run it. Report failures with their output.
- Never push tags. Never force-push `next` or `dev`. Don't change `dev`.
- Don't estimate how long anything will take. Describe size by scope and risk.
- Product names appear only in the planning files (this file, `TODO.md`, `docs/design.md`,
  `docs/architecture.html`, `docs/prs/`). I9 removes them.
- Builds and scratch go on NVMe or SSD, never the Github HDD.

## Board runs

These need a real board, so they run in a local session on the user's machine after a PR merges.
That session takes `/run/lock/board-gpu.lock`, uses the CM5, and reports CM5 timings only.
