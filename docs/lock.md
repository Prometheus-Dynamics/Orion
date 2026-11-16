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

- The error codes 18 (`no deadline`) and 19 (`deadline too long`) are new since `455a956e` too.
  They are reachable only through level-1 `DetachedRun` (`Run.detached`): a node answers them
  only to a detached run, which a client asks only of a node at level 1 or above.

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

- With security on, a lease written through another node is held by that node (`peer:<name>`),
  not by the operator behind it, as with `Run.on_behalf`.
- Every build takes the shared-memory crates from the device library's repository
  (`shm-sample-time`, and on Linux `shm-fdpass`, for the smallest client). At run time the client
  needs nothing of the device library: no daemon running, no library loaded. Those crates make
  system calls only on descriptors the client hands them, and to read the clock.
- armv7 and riscv64 board files build and pass the size gate, and have not run on a board.

## Since the lock revision

Changes on `next` after the lock revision. Each is additive unless it is marked **Break**, with
what to change.

- **J11.** New `ClientError` cases (it is `#[non_exhaustive]`):
  - `Layout { what, found, reads, level }`: shared memory from the comm core in another layout.
    Its words say which side is older and what to update. A client of this revision on a comm
    core of `455a956e` gets it for every topic, where it got `Protocol("a ring that cannot be
    mapped")`;
  - `Memory { what, why }`: any other shared-memory error, with its words.

  Also new: `orion_topics::from_ring`, `Cx::level()`. The C library returns `ORION_E_REFUSED` for
  `Layout`.
- **C40.** The WebSocket, SSH and NT4 ways in try again as the network way in does (5 s, then
  10 s), when their address is taken, not on the board yet, needs a capability, or is on an
  interface not there yet. Settings that cannot work still mean recovery.
  - `orion_server::Retry`, `may_clear` and `open_retrying` are new; `orion_tcp::Retry` is the
    same type.
  - `orion_ws::WsListener::opening` is new.
  - `CommSettings::peer_retry` is now `listen_retry`, for every way in.
- **C32a.** The security switch is the board's record, and trust follows it.
  - **Break: `security` is no longer a setting.** Both services read the switch from
    `/etc/board/security` (`on` or `off`, then `generation <n>`). No file is off; a file that
    cannot be read or says anything else is on, with words naming it. A `security` line in
    `orion.conf` (or `ORION_SECURITY`) is ignored, with a warning. Migration: a board that had
    `security = on` writes `/etc/board/security` with `on` and `generation 1`, and drops the
    line.
  - `orion_config::Config` gains `security_view` (the record as read) and
    `from_source_with_view`; `orion_config::SecurityView` and `SECURITY_VIEW` are new.
    `orion_config::Warning` gains `retired` and is `#[non_exhaustive]` (**Break** only for code
    that builds one: it is read, not built), and `Source::retire` is new.
  - With security on, a lease's holder is the principal of the caller that wrote it; what the
    caller put there is kept in `LeaseRecord.label`, an appended field. **Break** for Rust code
    that builds a `LeaseRecord`: it names `label` (`""` for none). On the wire the field is
    appended: an older reader ignores it.
  - At most 32 operators wait for an administrator. One more that asks is refused `busy`, with
    words; nobody waiting is dropped. `orion_keys::MAX_PENDING` and `KeysError::Waiting` are new.
    **Break:** `KeysError` is `#[non_exhaustive]`, so a `match` on it needs a `_` arm.
  - A flash may leave one key in `operator.seed` in `keys.dir`. The comm core enrolls it with
    everything and removes the file; a seed that is not one key stops the start, with words
    (`KeysError::Seed`). `orion_keys::SEED_FILE` is new.
  - The hello document and the time stay readable by a key the board does not know, with
    security on.
- **C42.** Recovery takes the switch from the board's record too, when the settings cannot be
  read (it was on whatever the record said). `RecoverWith` gains `security_view` (**Break** for
  code that builds one by its fields: add `security_view: orion_config::SECURITY_VIEW.into()`),
  and `RecoveryComm::start` takes the switch.

- **C43.** The hello document says the board's security switch, from its record: the node's own
  facts `security` (`on`, `off`, `damaged`) and `security_generation`, signed with the rest. An
  older node lacks them. `orion::node::Node::security` returns `orion::node::Security`
  (`#[non_exhaustive]`): `NotTold` from an older node. A program may no longer publish
  `hello.security` or `hello.security_generation` (refused `denied`, as `hello.state` is).
  `orion_hello::Hello::with_security` is new, and `RecoveryComm::start` takes the record as
  read.

- **C44.** `ActionResult` gains an appended `code: u32`: the action's own status code, for a
  caller to branch on. Zero is none, and is what an older node or program gives; the words stay
  in `detail`. `orion_actions::Finished` gains `code` (**Break** for code that builds a
  `Finished` by its fields: add `code: 0`), and `Registration::finish_with_code` is new
  (`finish` gives zero). Node logic passes the code on to the caller, on attach, and across
  nodes. The microcontroller library and the C library do not set it (zero); the web client
  does not read it yet.
- **C49.** `orion_server::Dropped::WriteStalled` gains `kind` and `waiting` (**Break** for code
  that builds or matches it by its fields: add them, or match with `..`), and
  `Dropped::WriteFailed { kind, error }` is new. A write that fails is no longer called a
  stall.
- **C47.** The code reaches the other clients. Additive:
  - `orion_device_actions::Actions::finish_with_code` sends it. `finish` sends the result as
    before, with no code, which reads as zero.
  - The MCU template's C API gains `orion_finish_code`.
  - The web client's `Result` gains `code`, zero from a board that did not send one.
  - The C library has no actions, so there is nothing for it to read or set.
- **C52, C52b.** The comm core runs as `orion-comm` (902) with only `CAP_SYS_TIME` and
  `CAP_NET_BIND_SERVICE`. Logins are started by the login helper (`orion login-helper`,
  `orion-login.socket`), root with no bounding set, so a root login is all of root. An image
  with the units must have the `orion-comm` account. The BusyBox scripts are unchanged: the
  comm core there is root and starts logins itself.
- **C53, C53b.** Run handles. `Cancel`, `Progress`, `ActionResult` and `Attach` gain appended
  `instance` and `run` (and `ActionResult` gains `action`), with zeros for a bare one: code
  that builds them by their fields adds them (**Break** for struct literals). New:
  `ACTION_STARTED`, `RunHandle`, `Error::RunGone` (level 3), `Feature::RunHandle`,
  `Actions::attach_handle`, and `Reply::Within(Duration)` (`ORION_REPLY_WITHIN` in C).
- **C54.** `Claim` gains appended `detached`, and `Request::Run` gains `detached` (**Break** for
  a match on `Request::Run` without `..`). A run node logic ends at its deadline is `TimedOut`
  with code 110 (`orion_msg_actions::DEADLINE_CODE`).
- **C55.** `ClientError::PathTooLong { path, len }` (not lost), `orion_connect::SOCKET_PATH_MAX`
  and `socket_path`, `orion_unix::fits`, `Board::try_start_with`. `ORION_E_ARGUMENT` in C.
- **C56.** The microcontroller side catches up:
  - `orion_device_actions::Heard::Run` gains `detached` (**Break** for a match without `..`);
  - the MCU template's C API gains `ORION_EVENT_CALLER_GONE` and `orion_caller_gone`,
    `orion_run_detail` (session, deadline, detached), and `orion_cancel_reason` with
    `ORION_CANCEL_*`, where `ORION_CANCEL_DEADLINE` is a run its caller was told timed out
    with code 110.
- **C57.** A hello on a link always begins a session: one inside an open session on a link
  ends it and begins the next, and an ended session's late frames never reach the next.
  `RejectReason::NoSession` (6) answers a frame where a hello was due (**Add**; an open enum).
  The device library takes `NoSession` mid-session as gone and says hello at once.

  A device is not a caller, so run handles are not its to use. The C library has no actions.

## Builds

The gate checks, at this revision:

- Linux x86_64: every crate, every feature set;
- the client on Windows (`x86_64-pc-windows-msvc`, `x86_64-pc-windows-gnu`) and macOS
  (`x86_64-apple-darwin`, `aarch64-apple-darwin`), clippy with tests;
- the static board files for aarch64, armv7 and riscv64 (musl, with zig 0.15.2), held to their
  sizes.
