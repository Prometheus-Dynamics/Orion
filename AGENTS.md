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
- A reading list naming another repository means that repository, under the same organization.
  Those are references to read and are not added as dependencies unless the brief says.
- Stay inside the paths your PR creates. Don't edit the root `Cargo.toml`, `TODO.md`, or a changelog.
  Keeping to this is what lets many sessions run at once without conflicts.
- Add a crate by creating its directory under `crates/<folder>/`. Name it `orion-<directory>`.
  Inherit shared dependencies with `workspace = true`, and declare any others in your own crate.
- If your PR needs a shared interface changed, stop and say so in the PR description. The interfaces
  in the design change only through A-series or B-series PRs.
- A PR merges when `scripts/gate.sh` passes on the merged tree. Paste the proof named in the
  brief into the PR description.
- Push with `scripts/push.sh REMOTE BRANCH`, never `git push`. A gate that passes stamps the
  exact tree it checked, and the script refuses any commit to `next` whose tree has no stamp:
  whoever lands runs the gate on the tree they will commit, and changes nothing until it ends.
  A working branch `pr/<id>` pushes without a stamp.
- `scripts/check-changed.sh` runs the gate's own format, clippy, tests and feature matrix for the
  crates you changed and every crate that depends on them. `scripts/prune-target.sh --delete`,
  run right after a gate passes, removes what the gate's debug builds no longer use.

## Workers and landing

1. The landing gate is the proof. A worker does not run the full gate before pushing `pr/<id>`.
   It runs `scripts/check-changed.sh`, the fail-before proof for its change (the test goes red
   with the change undone or mutated, green with it), and fmt and clippy on what it touched.
   Nothing lands on a red gate.
2. Repeat runs are set by risk, and the count is stated in the PR: many runs only for a change
   to timing or concurrency; three for a join of branches that each passed, and for an ordinary
   change.
3. A red landing gate is read and given a named cause before anything else runs. Never rerun
   in hope. A flake is fixed by its cause and recorded in `docs/flakes.md`.
4. Concurrent heavy jobs (a gate, a check, a cross build) are capped to the cores they are
   given. One gate at a time on a machine.
5. A worker starts warm: it reuses a kept worktree and its target directory for its next item
   once the previous item has landed, or seeds a new one with `scripts/seed-target.sh`.
6. A brief names the exact base commit and the files in scope, and is small enough to finish in
   one context. Longer work hands off through a written state file.
7. Board runs go one runner per board, in parallel with software work.
- A crate with two or more integration-test files builds them as one binary: `autotests = false`,
  and `tests/all.rs` names each file as a module (a file that needs features has its `cfg` there).
  Add a new test file to `all.rs`; the gate fails a test file that nothing builds. Tests in one
  crate share a binary, so under plain `cargo test` they run as threads of one process: a test may
  not rely on being alone in its process (the global allocator, the environment, the current
  directory). nextest, which the gate uses, still runs each test in a process of its own. A test
  that runs its binary again names itself with `module_path!()`, and a nextest filter names a test
  by its whole name, `<file>::<test>`; the gate fails a filter that matches no test.
- A test's files go in an `orion_scratch::Scratch` (`crates/testing/scratch`), never straight
  under `std::env::temp_dir()`. It makes `orion-<name>-<pid>-<n>`, a new one each time, and
  removes it with everything in it when dropped, whether the test passed or panicked. Keep it
  for as long as anything uses what is in it: bind it to a name, or hold it in the struct
  that uses it, as its last field. A child process that writes temporary files of its own (a
  browser) gets its `TMPDIR` inside the scratch.
- Each gate step carries, after its label, the slowest time it has taken in a gate that passed,
  here or in CI; its bound is three times that, at least a minute. Past it the step and all it
  started are ended and the gate fails, naming it. A step that ends with a process of its own
  still running fails too, and so does one that leaves anything in its TMPDIR: each step gets
  an empty one of its own. A step written without its number does not run. On a slower machine,
  `GATE_STEP_BOUND_SCALE=2` (a whole number) doubles every bound. A new step gets its slowest
  green time; a step that grows past a third of its bound has that number raised in the change
  that made it slower, with the reason.
- Start a new worktree's target directory with `scripts/seed-target.sh FROM TO`, from another
  worktree's: a copy, never a share, and on btrfs a clone that costs no space.

## Rules

- Orion is orchestration and interconnect. It does not know what a camera, a pose or a graph is,
  how to speak to hardware, or how to install anything on a particular board.
- No library needs another one in order to work. A bridge to another library is an optional
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
- `unsafe` only in `crates/core/measure`, the CRC
  instructions in `crates/core/wire/src/crc.rs`, `crates/client/capi` and `crates/device/*` ports, with
  a `SAFETY` comment on each block.
- Everything is behind features. Lean is the default. A recovery build has no node logic.
- Breaking other repositories is fine. They are not built here and they catch up afterward.
- Hardware you can't reach is marked `untested on board`. Don't hold work back for a board run.
- Timings count only when measured on the test board.
- Never claim something works without having run it. Report failures with their output.
- Never push tags. Never force-push `next` or `dev`. Don't change `dev`.
- Don't estimate how long anything will take. Describe size by scope and risk.
- No product names: other libraries and products go by their role (the camera library, the
  device library, the product). The NT4 crates keep the protocol's own vocabulary, and
  `scripts/check-boundaries.sh` holds the list.
- Builds and scratch go on NVMe or SSD, never the Github HDD.

## Board runs

These need a real board, so they run in a local session on the user's machine after a PR merges.
That session takes `/run/lock/board-gpu.lock`, uses the test board, and reports board timings only.
