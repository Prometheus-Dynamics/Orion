# J8b: The shm-* pins at the device library's bded77a0

**Status:** done

**Group:** J. Cross-repo

**Wave:** 18

**Needs:** C37

Read `AGENTS.md` and `docs/design.md` first.

## Changes

`Cargo.toml`, `Cargo.lock`, `examples/mcu-template/Cargo.lock`,
`examples/stm32g4-device/Cargo.lock`, `TODO.md`

## Build

The device library's P2s (ad5bf930) gives the shared layouts a value: the ring and the marks page are layout 2.
An attach that finds another layout refuses with `RingError::Layout`, which names both values,
instead of reading it wrong. Every consumer moves to bded77a0, which contains it. Orion moves as
its own change, with nothing else in it, so the rev can go to the other consumers at once.

## Built

- The eight `shm-*` workspace dependencies (seven in use) at `bded77a096ce48cf00d686ceb8f3a41286ea871e`. The
  two example workspaces' lock files follow for `shm-sample-time`.
- From `git log d1a8939a..bded77a0 -- shm`, P2s is the only change under `shm/`:
  - `shm_ring::LAYOUT = 2` and `shm_ring_holds::LAYOUT = 2`;
  - a new variant `RingError::Layout { what, found, reads }`. `RingError` was not
    `#[non_exhaustive]` and gains a variant, but Orion never matches its variants. The broker
    logs it and answers `Internal`, and the client wraps it whole in `TopicError::Ring`. So
    nothing in Orion changes to build.
  - A ring or marks page written by a build before ad5bf930 is refused, not misread. Orion makes
    both from the same crates it reads with, so an Orion board is one layout throughout.
- Orion holds no copy of the shm crates. They are git dependencies at one rev, and `cargo tree`
  shows each once (below).

## Proved by

- The gate, unchanged (exit 0): every ring, marks and buffers test in the workspace runs against
  layout 2.
- `cargo tree --workspace --all-features -e normal,build,dev`, each `shm-*` line once, one source:

```text
shm-fdpass v2.0.1-dev (the device library, rev=bded77a096ce48cf00d686ceb8f3a41286ea871e#bded77a0)
shm-ring-buffers v2.0.1-dev (the device library, rev=bded77a096ce48cf00d686ceb8f3a41286ea871e#bded77a0)
shm-ring-doorbell v2.0.1-dev (the device library, rev=bded77a096ce48cf00d686ceb8f3a41286ea871e#bded77a0)
shm-ring-holds v2.0.1-dev (the device library, rev=bded77a096ce48cf00d686ceb8f3a41286ea871e#bded77a0)
shm-ring-time v2.0.1-dev (the device library, rev=bded77a096ce48cf00d686ceb8f3a41286ea871e#bded77a0)
shm-ring v2.0.1-dev (the device library, rev=bded77a096ce48cf00d686ceb8f3a41286ea871e#bded77a0)
shm-sample-time v2.0.1-dev (the device library, rev=bded77a096ce48cf00d686ceb8f3a41286ea871e#bded77a0)
```

  `shm-ring-latest` is a workspace dependency no crate uses, so it is not in the tree.

## Different from the brief

- Mapping `RingError::Layout` to a refusal code with words (code 21, which names `which`,
  `found` and `expected`) is J9's, as ruled. Here it still reaches a program as `Internal`, with
  the words in the comm core's log.
