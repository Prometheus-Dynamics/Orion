# The lock revision

The revision of Orion `next` that the projects around it build against: the commit that adds this
page. It follows the last one everyone held, `455a956e` (J7). Below are what an older consumer
must change (the break list), the levels, and what is known not to be done yet.

Wire revision: `INTERFACE_REVISION` 1, unchanged. Additive level: `INTERFACE_LEVEL` 2. The
shared-memory crates are pinned at the device library's `bded77a0` (layout 2).

## Break list, from `455a956e`

**Wire.** Every change is appended or new: the revision stays 1, and a side at level 0 still
talks to this one.

- `Hello` and `Welcome` gain the appended additive level. A node that sends none is at level 0.
- `Reject` gains appended `node_revision` and `peer_revision`, and `RejectReason::RevisionMismatch`
  (5). A `Hello` of another revision is turned away with both numbers.
- New kinds: `SHELL_*` (`0x80`-`0x87`), `LINK_SIGN_*` and `LINK_SEALED` (`0x90`-`0x94`),
  `ACTION_CALLER_GONE` (`0x38`), `NODE_QUIET` (`0xA0`, comm core to node logic, in the new `Node`
  range `0xA0`-`0xAF`).
- Appended fields: `Run.detached` and `Run.on_behalf`, `Claim.session`, `Cancel.reason`,
  `Declare.on_subscribe`.
- New error codes, each told only to a session at level 2 or above:

  | Code | Error | A session below level 2 is told |
  | --- | --- | --- |
  | 20 | `restoring` | `not found` |
  | 21 | `layout` | `malformed` |
  | 22 | `lease lost` | `busy` |

**The hello document.**

- The node owns the facts `state` and `recovery_why`. A program can no longer publish them: a
  program that declares the topic `hello.state` or `hello.recovery_why` is refused `denied`, with
  words.
- `state` is `running`, `recovery` or `flasher` (`flasher` from `hello.state = flasher` in the
  board's settings). `updating` is gone.
- The answer is signed whenever the asker gives a nonce, or is off the board, with security on or
  off.

**The Rust client (`orion-connect`, `orion`).**

- `ClientError` is `#[non_exhaustive]`: a `match` on it outside `orion-connect` needs a `_` arm.
- `ClientError::Refused(Error)` is `ClientError::Refused { error, detail }`. `detail` is the
  node's words verbatim, or the code's own name when it sent none. It is never empty.

  ```rust
  // before
  Err(ClientError::Refused(Error::Busy)) => wait(),
  // after
  Err(ClientError::Refused { error: Error::Busy, detail }) => { log(&detail); wait() }
  Err(ClientError::Refused { error, .. }) => fail(error),
  _ => ...,
  ```

- New `ClientError` cases:
  - `OtherKey { asked, answered }`: the node proved another key than the one pinned;
  - `Revision { node, client }`: another wire revision;
  - `Level { feature, needs, node }`: a feature asked of a node below its level. Nothing is
    sent.
- `Client::refusal(seq)` returns `Option<ClientError>` (a `Refused` with its words), not
  `Option<Error>`.
- `Welcomed` is `#[non_exhaustive]`: it is read, not built. It gains `level`, `signed` (whether
  the session's frames are signed) and `node_key` (the key the node proved: set at connect on a
  signed session).
- `Session::node_key()`, `Client::node_key()` and `Orion::node_key()` return the node's key on
  any session. On a plain session they ask once with a fresh nonce, check the signed answer, and
  keep the key. This is the call for pinning a node on first connection.

**Service and carrier implementations (`orion-server`).**

- `Connection::local()` and `Service::quiet(session, quiet_ms, sessions)` have no default body.
  Every implementation says what it does.

**Node logic's rules.**

- A lease held by a program on the board that has gone quiet for `leases.quiet_ms` (10 s) can be
  taken by another claimant. The old holder is told `lease lost` with who took it.
- A lease is freed only by the session that wrote it, a caller whose principal is its holder,
  or root. Anyone else is `denied`, with who holds it.
- `orion_resources::Resources::check` takes a `Caller`, not a session number.

**The comm core.**

- A program on the board's socket is never dropped for being quiet.
- `peer.listen` that cannot be listened on no longer means recovery: it is tried again every 5,
  then 10 seconds, and the rest of the comm core runs. `peer.interface` keeps it to one
  interface.

**Shared memory.** Every publisher that offers its ring to the broker is built against the device
library at or after `ad5bf930` (layout 2). An older one is refused `layout` (21), with words that
say to rebuild it.

**Microcontrollers (`orion-device-actions`, `orion-device-core`).** `Heard::Run` gains `session`,
`Heard::Cancel` gains `reason`, and there is a new case `Heard::CallerGone { session }`. A
`match` that names `Cancel { call }` needs `Cancel { call, .. }`, and a `match` without a `_` arm
needs `CallerGone`. A device turns away a host of another wire revision as `RevisionMismatch`.
The C header (`orion_mcu.h`) is unchanged.

**The web client.** Its API is unchanged. It turns away a board of another wire revision, with
both numbers.

**Unchanged.** The C library (`orion.h`).

## Levels

| Level | Addition | What an older node does |
| --- | --- | --- |
| 1 | `SubscribeFromLatest`: `Declare.on_subscribe = Latest` | Starts every subscriber from now |
| 1 | `Shell`: `SHELL_OPEN` | Does not answer |
| 1 | `SignedLink`: `LINK_SIGN_HELLO` on a serial or USB link | Does not answer: the client then says hello plainly to read the level |
| 1 | `DetachedRun`: `Run.detached` | Runs it with no deadline and no limit, and never tells its program the caller went |
| 1 | `RunOnBehalf`: `Run.on_behalf`, from a node | Takes every caller behind the link as one |
| 2 | The error `restoring` (20) | Told `not found` instead |
| 2 | The error `layout` (21) | Told `malformed` instead |
| 2 | The error `lease lost` (22) | Told `busy` instead |

A client asks for a level-1 feature only of a node at level 1 or above. Below it, the call fails
with `ClientError::Level` and sends nothing. `docs/wire.md` is the reference.

## Known limits

- With security on, a lease holder is not yet bound to a principal (C32).
- Only the network way in tries again when it cannot open. The WebSocket, SSH and NT4 ways in
  still mean recovery when their address cannot be listened on.
- Every build takes the shared-memory crates from the device library's repository
  (`shm-sample-time`, and on Linux `shm-fdpass`, for the smallest client). At run time the client
  needs nothing of the device library: no daemon running, no library loaded. Those crates make
  system calls only on descriptors the client hands them, and to read the clock.
- armv7 and riscv64 board files build and pass the size gate, and have not run on a board.

## Builds

The gate checks, at this revision:

- Linux x86_64: every crate, every feature set;
- the client on Windows (`x86_64-pc-windows-msvc`, `x86_64-pc-windows-gnu`) and macOS
  (`x86_64-apple-darwin`, `aarch64-apple-darwin`), clippy with tests;
- the static board files for aarch64, armv7 and riscv64 (musl, with zig 0.15.2), held to their
  sizes.
