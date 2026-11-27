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
- **C58.** A USB link's reader that is let go hands what it reads to the stream that took over.
  Nothing a consumer calls changes.
- **C59.** Over a link that is not signed, the client says hello again on a capped backoff until
  it is welcomed (250 ms doubling to 2 s, within `options.timeout`). A welcome mid-session
  becomes the session, and a `Reject` mid-session is `ClientError::Lost`.
  `Session::reconnect_over_with(open, keep_trying)` and `FIRST_AGAIN`/`MOST_BETWEEN` are
  added (**Add**). `reconnect_over` lets the old halves go before saying hello.
- **C60.** The stub board's action router is node logic's own action list
  (`orion-action-list`) behind the door, not a stand-in: it answers as a board does, so a
  detached run with no deadline is refused (`NoDeadline`) there too.
  `Board::restart_node_logic` and `Board::node_logic_instance` are added (**Add**;
  `orion-stubs` is a dev dependency, not a consumer's). `Router::default` becomes
  `Router::new(node)`. Nothing on the wire changes.
- **J12.** The `shm-*` pins move to `e9224a53`. `NextBatch::Batch` is renamed `NextBatch::Frame`,
  and `NextBatch::NoBuffers` is new (**Break** for a matcher of `NextBatch`, which `orion-buffers`
  re-exports). `BufferPublisher::publish_empty`, `holders_of` and `cut_off`, and
  `AdoptedBuffers::cut_off` are added, and `Torn` and `NoBuffers` are re-exported (**Add**).
  `Held::read`/`Held::current` are the checked reads; `Held::bytes` is unchecked. The wire and the
  layouts are unchanged.
- **C61.** `Session::connect`, `Orion::connect`/`connect_with`, `orion_connect` and the browser's
  `Orion.connect` keep trying until welcomed or refused (**Break** in behaviour: a caller that
  relied on one try uses `connect_once` / `orion_connect_once` / `Orion.connectOnce`).
  `Options::patience` (`Patience::{Forever, Within, Once}`), `connect_once`, `connect_watching`,
  `ClientError::may_retry`, `orion_connect_once`, `TurnedAway` and `CONNECT_AGAIN_MS` are added
  (**Add**; a struct literal of `Options` without `..Default` gains a field). `reconnect` now
  tries again on `Rejected(Busy | NoSession)`.
- **C62.** Tests and drills only: nothing a consumer calls changes. `docs/flakes.md` is added.
- **C63.** `Describing::topic_with` is added (**Add**): a resource's topic declared with its own
  `expiry_ms` and `on_subscribe`. `Describing::topic` is unchanged.
- **D16.** Files from a board (**Add**, level 4, `INTERFACE_LEVEL` 4): kinds `FILE_OFFER` (0x27)
  to `FILE_OFFERS_LIST` (0x2E), `FETCH_WINDOW`, `OfferedBy`; `Files::offer`/`fetch`/
  `start_fetch`/`offers`, `Offer`, `Asked`, `Gift`, `Giving`, `Fetching`, `Offered`;
  `Feature::FileOffers` (a `match` on `Feature` without `_` gains an arm); the comm core's
  `orion-file-offers` service (feature `offers`, on by default); `Sessions::outbox_bytes`.
- **D16b.** `orion_files::index` (`Index`, `Entry`, `NotAnIndex`, `FIRST_LINE`),
  `Fetching::within` and `Fetching::finish`, `orion file fetch` and `orion file offers` (**Add**).
  The stub board offers files as the comm core does.
- **C30.** The comm core offers `orion/` itself, running and in recovery: `orion/index`, `log`,
  `settings`, `last-good.carriers`, `state`, `sessions`, `version`. A program offering `orion/`
  or a prefix under it is refused as `busy` (it was free before). `FileOffersList` lists it
  first, `OfferedBy::CommCore`. New setting `log.window_bytes`. In Rust,
  `orion_file_offers::{OwnFiles, OwnName, SNAPSHOT_MS}` and `FileOffersService::with_own`,
  `orion_config::may_be_secret` and `Source::said`, `orion_log::{window, window_bytes,
  set_window_bytes, WINDOW_BYTES}`, and `CommSettings::{said, log_window_bytes}` (**Add**).
- **C31.** Files the settings name (`offers.file`, `offers.user`): each prefix served while no
  program offers it, listed `OfferedBy::Configured`; a program offering it takes it over. A
  new helper mode, `offers-command`. In Rust, `FileOffersService::with_configured`, and
  `CommSettings::{offers_file, offers_user}` (**Add**). `OwnFiles::read` now takes a `Ready`
  and gives a `Reading` (`Now`, `Later`, `Not`), and `FileOffersService::with_own` takes the
  `Comm` (**Break**: an `OwnFiles` returns `Reading::Now(bytes)` for `Some(bytes)` and
  `Reading::Not(why)` for `None`; pass the comm core to `with_own`).
- **C64.** A subscriber handed a buffer pool page of another layout gets `ClientError::Layout`
  ("a buffer pool", both numbers), as for a ring. It was already mapped; now proved. The pool
  page table in `docs/ring.md` has the layout word.
- **C65.** Tests only: nothing a consumer calls changes. The shell test's open flake is fixed by
  its cause (`docs/flakes.md`).
- **C32b.** Orion's precheck of the security switch (**Add**, level 5, `INTERFACE_LEVEL` 5): a
  new concept, `Security` (`0xB0` to `0xBF`), with kinds `SECURITY_PRECHECK` (0xB0) and
  `SECURITY_PRECHECKED` (0xB1); `SecurityTo`, `PrecheckLine`; `Orion::security_precheck`,
  `node::SecurityTo`; `Feature::SecurityPrecheck` (a `match` on `Feature` or on `Concept`
  without `_` gains an arm). The comm core answers it running and in recovery.
- **J13.** The `shm-*` pins move to `d4942290` (the device library's FL1 merge). Nothing under the
  device library's `shm/` changed since `e9224a53`, so nothing a consumer calls changes.
- **C68.** Attaching to an ended detached run no longer takes its end: every attach while it is
  kept (ten minutes, the newest 256) gets the same end. After that it is refused `not found`
  with words beginning `expired:`. `orion_action_list::MAX_EXPIRED` is added (**Add**). A caller
  that relied on a second attach being `not found` now gets the end again (**Break** in
  behaviour only).
- **C71.** A signed hello on a link skips a sealed frame that comes before the board's answer: one
  left by the last signed session on the link. Before, it failed with `the board does not take a
  signed session on a link`. Nothing a caller calls changes.
  A file of `offers.file` made by a command whose output ends is waited for until its
  `timeout` before the command is ended; before, it was ended at once, and could be told killed.
  One that closes its output and runs past its `timeout` is refused `did not finish in N ms`.
- **I11.** The login helper's client reads a refusal the helper sent before reading its
  question, where before it could fail with `send failed: Broken pipe`. Nothing a caller calls
  changes.
- **I12.** `scripts/gate.sh` runs its steps that have target directories of their own beside
  clippy and the tests. Nothing a caller calls changes.
- **I13.** The gate's debug and test builds are incremental; the size gate, the device size and
  fuzz pin `CARGO_INCREMENTAL=0`. The web tests' browsers run in software only. Nothing a caller
  calls changes.
- **I14.** New `scripts/check-changed.sh`, a subset of the gate for the inner loop, and
  `scripts/prune-target.sh`. Nothing a caller calls changes.
- **I15.** New `scripts/seed-target.sh`, to start a worktree's target directory as a copy of
  another's. Nothing a caller calls changes.
- **I16.** Tests that check nothing listens on a port hold the port while they check. Nothing a
  caller calls changes.
- **I17.** CI runs the tests with cargo-nextest, as the gate does where it is installed, and lets
  the sandbox test's user namespace hold capabilities. The test skips, and says so, where a
  namespace holds none. Nothing a caller calls changes.
- **I18.** The tests that take longest start first, and SHA-256 is optimized in debug and test
  builds. Nothing a caller calls changes.
- **I19.** A test that pushes a file again reads it again for each try. Nothing a caller calls
  changes.
- **I20.** `scripts/gate.sh --part <part>` runs one part of the gate. CI runs the five parts at
  once, each on a runner of its own, and keys its caches on the root manifest. Nothing a caller
  calls changes.
- **I21.** The gate runs its feature matrix, other-systems and device lanes at a lower priority
  than the size gate and the main steps. Nothing a caller calls changes.
- **I22.** `scripts/push.sh` pushes a working branch `pr/<id>` without gate stamps. Every other
  branch still needs them. Nothing a caller calls changes.
- **I23.** A test board (`orion-stubs`) listens on a port below the kernel's ephemeral range when
  none is named. Nothing a caller calls changes.
- **I24.** The size gate reuses a measurement whose content key it has recorded, and measures
  everything at least daily, with `--measure`, and in CI. Nothing a caller calls changes.
- **I25.** A crate with two or more integration-test files builds them as one binary
  (`tests/all.rs`); its tests are named `<file>::<test>`. The gate fails a nextest filter that
  matches no test and a test file nothing builds. Nothing a caller calls changes.
- **I26.** The size key fetches what the lockfile names when it is not fetched yet, and a key
  that cannot be made stops the size gate by its cause. Nothing a caller calls changes.
- **I27.** Each gate step runs within a bound (`scripts/bounded.py`), and fails if it passes it
  or leaves a process running. The BusyBox supervisor ends its wait between starts when it is
  stopped, instead of leaving it running. Nothing a caller calls changes.
- **I28.** The size gate's target directory is absolute, so its path check reads the build it
  made when `CARGO_TARGET_DIR` is unset (CI); when the bytes differ it reports where. Nothing a
  caller calls changes.
- **I29.** The armv7 and riscv64 size builds run before a release only
  (`scripts/size-gate.sh --release`, docs/release.md); x86_64 and aarch64 stay on every change.
  Nothing a caller calls changes.
- **I30.** A new outcome, `Indeterminate` (6). A run node logic stops itself, at its deadline
  or when node logic stops, waits up to `actions.confirm_ms` (10 s) for its program's word and
  ends as the program says; with none it ends `Indeterminate`, where it used to end `TimedOut`
  or `Cancelled` at once. A program that reports `Cancelled` after a deadline still gives
  `TimedOut`. Node logic refuses new runs while stopping, and its stop waits that bound at most.
  `orionctl` exits 3 for `Indeterminate`. An older peer reads 6 as `Unknown(6)`.
- **I31.** A new file state, `Aborted` (5), sent only by a program giving a file:
  `Gift::abort(why)`, and a gift dropped before it is over. The fetch ends at once, `Aborted`, with
  "the program that offers it stopped giving it: <why>". A file asked and dropped unanswered is
  refused. An older client reads 5 as `Unknown(5)` and still ends the fetch as not given.
- **I32.** A fetch is no longer bounded by the session's reply rule. A new `Step::Stalled
  { received, size, quiet }` is said after `NO_PROGRESS` (5 s) without a chunk, and again each
  time as long passes, and the fetch waits on. A `match` on `Step` needs the new arm.
  `Fetching::give_up_after(quiet)`, off by default, ends it `Rejected`, "gave up: no progress for
  <quiet>, the limit set for this fetch". `orionctl file fetch` prints each stall and takes
  `--give-up MS`.
- **I33.** The step bounds self-test waits for each step to run before it times or stops it, and
  `bounded.py` names a leftover read mid-`exec` by its short name. Nothing a caller calls changes.
- **I34.** A gift counts its client's silence from the client's last word, only while the client
  owes an answer. A program that pauses between steps past its session's timeout no longer ends
  its gift `TimedOut`. Nothing a caller calls changes.
- **I35.** Tests keep their files in `orion_scratch::Scratch`, removed when the test ends, passed
  or panicked. `orion_stubs::Board::start` removes the directory it made when the board is
  dropped; a board started in a caller's directory leaves it. Nothing a program calls changes.
- **I36.** Each gate step runs with an empty TMPDIR of its own and fails (125) if it leaves
  anything there, naming each entry. A step written without its slowest time does not run
  (exit 2). `GATE_STEP_BOUND_SCALE` multiplies every bound. A step past its bound says
  `<step>: TIMED OUT after N s (bound N s)`. Nothing a program calls changes.
- **I37.** A files-pull test waits for the fetch it started last before it ends. Nothing a
  program calls changes.
- **I38.** The gate's doctests run beside nextest, their output after it. The same tests run.
  Nothing a program calls changes.
- **I39.** The gate checks the no_std crates in three groups per target, not one call each;
  `scripts/same-features.py` fails the gate if a group ever builds a crate with features its own
  call would not. Nothing a program calls changes.
- **I40.** The seven `shm-*` crates are pinned to the device library's `f8652608`, through the
  `.git` URL. No other package changes. Nothing a program calls changes.
- **I41.** `scripts/same-features.py` runs `cargo tree` without colour, so CI's
  `CARGO_TERM_COLOR=always` no longer makes repeated packages look different. Nothing a program
  calls changes.
- **I42.** Each comm core keeps its own window on the log: two in one process each offer
  `orion/log` within their own `log.window_bytes`. In Rust, `orion_log::Window` (`new`, `bytes`,
  `read`) (**Add**); `orion_log::{window, window_bytes, set_window_bytes}` (**Remove**). No caller
  outside Orion used them. Nothing on the wire changes.
- **I43.** A gift whose file cannot be read ends its client's fetch at once, `Aborted`, with
  "the program that offers it stopped giving it: it could not be read: <the error>". It no
  longer waits out the silence limit. `Gift::step` still returns the read's error. Nothing a
  program calls changes, and nothing new is on the wire.
- **I46.** The seven `shm-*` crates are pinned to the device library's `c8f1b3d2`, through the
  `.git` URL. No other package changes, and no shared-memory code changes. Nothing a program
  calls changes.
- **I47.** The seven `shm-*` crates are pinned to the device library's `5ee2f64a`, through the
  `.git` URL. No other package changes, and no shared-memory code changes. Nothing a program
  calls changes.
- **I44.** The gate also runs the comm core's tests with the features each shipped binary
  builds it with (`orion-bin`, `orion-recovery-bin`), and those binaries' tests alone, beside the
  `--all-features` run. The comm core's tests hold under each. Nothing a program calls changes.
- **I45.** The gate's build lanes skip an item that passed before on exactly its inputs
  (`scripts/lane-reuse.py`), and say what each lane did in a `Gate-Lanes:` line the commit
  carries. `scripts/gate.sh --drop LANE:WHY`, `scripts/lanes-behind.sh` and
  `scripts/release-check.sh` are new. CI runs every lane in full. Nothing a program calls changes.
- **I48.** The seven `shm-*` crates are pinned to the device library's `c65d234f`, through the
  `.git` URL. No other package changes, and no shared-memory code changes. Nothing a program
  calls changes.
- **I49.** A gate lane or the feature matrix that selects nothing fails instead of passing,
  and a change to `scripts/size-gate.sh` measures every size again. Nothing a program calls
  changes.
- **I51.** The seven `shm-*` crates are pinned to the device library's `0d37cc55`, through the
  `.git` URL. No other package changes, and no shared-memory code changes. Nothing a program
  calls changes.
- **I53.** A client that says something and closes while the comm core is writing to it is heard:
  what it sent before it closed is handled before its session ends, where it was dropped when the
  write found it gone first. Nothing on the wire changes, and nothing a program calls.
- **I54.** `orion_measure::sched::without_system_calls`'s child leaves no core dump when it is
  killed for a system call. Nothing else changes.
- **I50.** Kept runs (**Add**, level 6, `INTERFACE_LEVEL` 6): `Register` gains an appended
  `keep_ms: u32` and `Claim` an appended `kept: bool` (older bytes are a prefix of the new);
  `Actions::register_kept`, `Registration::refused`, `Request::Run { kept }` (a struct pattern
  without `..` gains a field), `Feature::KeptRuns` (a `match` on `Feature` without `_` gains an
  arm), `orion_action_list::MAX_KEEP_MS`. A struct literal of `Register` or `Claim` gains a
  field. `Actions::restore` keeps the runs handed to it while it restores. No shared-memory
  layout changes.
- **I52.** Tests only: a fetch whose giving program dies, or whose comm core goes, ends within a
  second, each held by a test. Nothing a program calls changes.
- **I56.** `Describing::action_kept(name, description, policy, keep)` and
  `Orion::register_action_kept(decl, policy, keep)`: kept runs (I50) for a provider's resource and
  through the facade. Additive; nothing on the wire changes.
- **I57.** A run's deadline and its program's bound (`CONFIRM_MS` or the node's) count from the
  end of the millisecond they were set in, so `Actions::next_deadline` and the tick that acts on
  one are 1 ms later than before. Nothing on the wire changes, and nothing a program calls.
- **I55.** `scripts/bounded.py` takes `--cpu` and `--stall` bounds beside its wall bound and
  says which a step passed: slow, stalled or starved. The gate ends a step whose processes use
  no CPU for a minute as stalled, and each step says its CPU seconds. Every wall bound is as it
  was. Nothing built changes.
- **I55b.** Each gate step's number is `WALL:CPU`; past three times its CPU number (at least
  60 CPU seconds) a step is ended as slow. A step without both numbers does not run. Every wall
  bound is as it was. Nothing built changes.
- **I58.** The seven `shm-*` crates are pinned to the device library's `7b4fdb25`, through the
  `.git` URL. No other package changes, and no shared-memory code that Orion builds changes.
  Nothing a program calls changes.
- **I59.** A hello fact may be up to 1024 bytes, where it was 256; a longer one is still left
  out, and the log now says so once for each fact. `recovery_why` is cut to 1024 bytes. Nothing
  on the wire changes.
- **I60.** A session on a serial or USB link is never sent a frame over 8192 bytes (8160
  signed), whatever its hello says it takes; an answer too large is refused `buffer_too_small`.
  A hello too large says its size and the limit. `orion_server::Connection` gains `max_frame`.
  Nothing on the wire changes.
- **I61.** The sample executor runs each command in a process group of its own, and stops and
  kills it as that group, so a child its shell started ends with it. Nothing a program calls
  changes.
- **I62.** A session on a CAN bus is never sent a frame over 4096 bytes, whatever its hello
  says it takes; an answer too large is refused, and a hello says its size. Nothing on the wire
  changes.
- **I63.** The drills' board ports and `forward.rs`'s `-L` port are held, bound and not
  listening, until their program listens there. Tests only; nothing built changes.
- **I64.** Tidy: I2 is marked dropped in `TODO.md`, and `docs/comm.md` says the CAN test over
  `vcan0` is not run by the gate. Nothing built changes.
- **I65.** `scripts/pr-status.py` reads every brief again (both header forms, ids like `C35a`,
  dropped PRs), and the gate runs it `--offline`, failing on a brief it cannot read. Nothing
  built changes.
- **I66.** The seven `shm-*` crates are pinned to the device library's `a6251ecf`, through the
  `.git` URL. No other package changes, and no shared-memory code changes. Nothing a program
  calls changes.
- **I67.** `docs/client.md` says how long a push waits for the board: each answer within the
  bound from the board's last change while sending, and the commit's check within twelve
  `Options::timeout`s under the default, so a long commit needs `Reply::Alive` or `Within`.
  Docs only; nothing a program calls changes.
- **I68.** `orion-test-board` has `TestBoard::hold_commits(prefix, hold)`: a receiver on the
  board that answers each commit `hold` late while its session stays pumped, for testing a
  pushing client's `Reply`. Test support only; nothing a shipped program calls changes.
- **I69.** A push keeps six chunks on their way, not eight, so its window fits in a session's
  outbox at the default; `orion_wire::OUTBOX_BYTES` (**Add**) is that default, and the files
  client checks the window against it at compile time. The comm core warns, naming the session,
  when node logic is dropped because a request to it did not fit. `orion-test-board` has
  `TestBoard::hold_action(name, target) -> HeldAction`: an action whose runs are never answered.
  Nothing on the wire changes.
- **I70.** A request a sender hears the refusal of (a push's three kinds, `FilePull`, `Put`,
  `Get`, `ActionRun`, `ActionAttach`, `PeerStatus`, `PeerForgive`) is refused `busy` when node
  logic's outbox has no room, and node logic is not dropped for it; `Delivery::Refuse`
  (**Add**) in `orion-server`. Node logic keeps chunks to a receiver within its outbox
  (`Sinks::new(outbox_bytes)`, **Change**: it took nothing; `Sinks::default()` for the
  default; `NodeSettings::outbox_bytes`, **Add**). A push turned away as busy goes back and
  sends again. `ClientError::is_busy`, `orion::Error::is_busy` (**Add**); `Workloads::wait`
  returns the put's refusal (**Change**: it never did). `orion-node-app` feature `slow` (tests
  only). Nothing on the wire changes; level 6.
- **I71.** `Describing::publish` fails with `Unpublished { error,
  describing }` and `Resource::withdraw` with `Unwithdrawn { error, resource }` (**Change**:
  both were `ClientError`; each turns into one with `From`, and into `orion::Error`): the
  description or the resource comes back, to try again. A withdraw tried again does not
  withdraw its topics twice. Nothing on the wire changes.
- **I73.** The seven `shm-*` crates are pinned to the device library's `bbb3c420`, through the
  `.git` URL. No other package changes, and no shared-memory code changes. Nothing a program
  calls changes.
- **I72.** `peer.interface` and `ssh.interface` take a list, one listener kept to each by name,
  and a listener follows its interface when it is made again. `SshConfig::interface` and
  `RecoveryConfig::interface` are `interfaces: Vec<String>`; `TcpCarrier::listen` takes a
  `Retry`; `orion_ssh::Listeners` is new and `listen_when_there` is gone. Settings only on the
  wire's side: no frame changes, and the level stays 6.
- **I74.** A test of a program that goes quiet mid-fetch lets it go on once the client has said
  it is quiet, not after a time counted from the program's first sight of progress. Tests only;
  nothing built changes.
- **I75.** `orion-node-app` feature `slow` (tests only) gains `slow::unanswered_gets(node) ->
  Arc<Unanswered>` (`hold`, `release`, `asked`): record gets node logic leaves unanswered.
  Nothing in a shipped build changes.
- **I76.** `files.root.<name> = <directory> rw|ro` (**Add**): node logic offers `roots/` and
  receives pushes to it, ahead of receivers and its store, and registers `files.list`,
  `files.remove`, `files.mkdir` and `files.rename` (text arguments and output, in
  `docs/node.md`). `orion-file-roots` is new. `Source::prefixed`, `FilesConfig::own`,
  `FilesConfig::hidden`, `Files::begin_as`, `NodeSettings::roots`,
  `orion_test_board::Options::granted` and `Granted` (**Add**). The store places a file beneath
  itself with `openat2` (**Change**: a link with an absolute target back inside is now refused
  too). No new wire kinds; level 6.
- **I77.** `mdns` and `mdns.interface` (**Add**): the comm core announces the board by name over
  mDNS, `orion-mdns` (new, feature `announce` in the default set, not in recovery), with
  `_orion._tcp`, `_ssh._tcp` and `_sftp-ssh._tcp`. The `_orion._tcp` TXT record keeps layout 2
  and adds `level`, `state` and `security` by name (**Add**; a layout-2 reader reads it).
  `CommCore::mdns_name` and `MdnsDiscoveryBackend::browse_only` (**Add**); with the responder
  built in, discovery browses only. `orion/state` gains an `mdns:` line when `_orion._tcp` is
  withheld. No wire changes; level 6.
- **I78.** Several `files.sink(prefix)` on one session each get the files under their own
  prefix (**Change**: before, the first was handed the files of all of them). `Client::add` says
  once on standard error when a part is added for kinds another already has. No API change, no
  wire changes; level 6.
- **I79.** `FileName` (**Change**): any file's name is carried, escaped
  (`orion_types::Escaped`, `orion_types::unescape`, **Add**): text outside ASCII as it is, `%XX`
  for an ASCII byte outside `A-Z a-z 0-9 . _ - : /`, `%`, a control or a byte that is not
  UTF-8; only that one form is a `FileName`; at most 255 bytes, was 127. `files.list` lines give
  the escaped name, then what it shows as when that differs; `long` begins a line out of reach;
  `unnamed` is no longer given; a page is at most 16 KB (**Change**).
  `orion_executors::Board::has` takes the name as text, and `Workload::files` holds it as
  `Box<str>` (**Change**). No wire kinds change; level 6. An older board refuses an escaped or
  longer name as `InvalidName`.
- **I80.** `Orion::offer_time` and `Orion::times_asked`, `orion node offer-time` (**Add**):
  the client answers the board's `TimeRequest` after an offer. `orion.time`'s `source` for an
  operator is `operator <name>`, and `(key <8 hex>…)` (**Change**: was `operator`). An
  operator refused for want of a grant is told `denied: needs the grant <grant>` (**Change**:
  was `denied`; the code is the same). No wire changes; level 6.
- **I81.** The hello fact `clock` (`synced`, `unset`) and `Node::clock` (**Add**); the broker
  reserves `hello.clock`, so a program declaring it is refused (**Change**). The announcement's
  `clock=` key, announced again within 2 s of a change; the record is at most 478 bytes, was 465, and
  stays layout 2 (**Add**). `orion/state` gains a `clock:` line (**Add**). `time.rtc`
  (**Add**). `Clock::unsync` on the time sync's clock trait, `TimeSync::view` and `TimeView`
  (**Add**); `orion_mdns::run` takes a `Retell` and `OrionTxt` a `clock` (**Change**).
  Disciplining, the kernel is told unsynchronized at start, after 8 unanswered polls, and on
  stopping (**Change**). No wire kinds change; level 6.
- **I85.** A test only. No interface changes.
- **I86.** A directory Orion makes in a root or the store has every permission the umask
  leaves (**Change**: was `0755` less the umask). `ClientError::is_held`, `orion::Error::is_held`
  (**Add**). `is_busy` is true only for a `Busy` refusal with no words of its own, and both are
  no longer `const` (**Change**: was true for every `Busy`). No wire changes; level 6.
- **I88.** The seven `shm-*` crates are pinned to the device library's `6c8568d0`. No other
  package changes, and no shared-memory code changes. Nothing a program calls changes; level 6.
  The size limit of `orion` for a board is 5,550,000, was 5,510,000, for the clock row.
- **I87.** `files.receivers` (**Add**): a `FileBegin` under a listed prefix with no receiver,
  and a chunk or commit on its way to one that went, is refused `Unavailable` with words. A
  push whose `FileBegin` is refused `Unavailable` asks again for up to twelve timeouts, then
  ends `Unavailable`, `nothing received <prefix> within <bound>` (**Change**: was handed to
  the program at once, from a board whose node logic was away). `Sinks::with_receivers`,
  `Sinks::take_said`, `NodeSettings::files_receivers` (**Add**). No wire changes; level 6.
- **I82.** Once synchronized, a step forward of over an hour needs a second source that agrees
  or an operator's clock (**Change**: two samples from one source were enough). `orion.time`'s
  `last_action` may be `waiting for a second source` (**Add**). A lease with an end that this
  node wrote is written again, its end moved, when the wall clock is stepped (**Change**).
  `orion_resources::shift_leases`, `Sync::clock_stepped`, `Store::wrote` (**Add**). No wire
  changes; level 6.
- **I83.** `gateway` and `nt4` in `time.servers`, and `time.interface` (**Add**).
  `TimeConfig::serve_interfaces`, `serve_retry` and `nt4_host`; `GATEWAY_SOURCE`, `NT4_SOURCE`
  (**Add**). A host named `gateway` or `nt4` can no longer be written bare in `time.servers`
  (**Change**: write it with its port). No wire changes; level 6.
- **I89.** The seven `shm-*` crates are pinned to the device library's `16a5c7d1`. No other
  package changes, and no shared-memory code changes. Nothing a program calls changes; level 6.
- **I90.** The same, to `e199fcb9`. Nothing a program calls changes; level 6.
- **I84.** `orion.time` gains `nt4_offset_us` and `nt4_rtt_us` (**Add**). The NT4 client
  gateway stamps a value with its sample's time on the server's clock (**Change**: was when it
  was sent). Orion's NT4 server keeps the time a client gives a value (**Change**: it stamped
  each with its own clock on arrival). `Publisher::set_at`, `ClientGateway::start_telling`,
  `FarClock` in both crates, `TimeSync::far_clock`, `TimeStatus::nt4` (**Add**); `Far::set`
  takes the sample's time (**Change**). No Orion wire changes; level 6.
- **I91.** The hello fact `settings_unread` and `Node::settings_unread` (**Add**); the broker
  reserves `hello.settings_unread` (**Change**). `orion/state` gains `settings not read: ...`
  (**Add**). `CommSettings::unread` and `CommSettings::told_unread` (**Add**). A setting
  nothing reads is logged as an error (**Change**: was a warning). A lease that cannot be read
  is refused `Busy` with words (**Change**: was bare). The gate refuses a target directory
  another worktree built in (`scripts/target-owner.sh`). No wire changes; level 6.
- **I92.** `orion/state` always ends `panics contained: N` (**Add**). `Sessions::panics`
  (**Add**). No wire changes; level 6.
- **I93.** The gate only. `orion-recovery` for a board is held to its size limit before a
  release, not on every change. No interface changes.
- **I97.** Writes based on a version (**Add**, level 7, `INTERFACE_LEVEL` 7): `WorkloadPut`
  and `WorkloadRemove` gain an appended `based_on`, `WorkloadStatus` an appended `stamp`; a
  stamp on an entry of a client's `Put`, ignored before, is now what the write is based on
  (**Change**). `Error::Stale` (24, level 7; `already exists` below it). `Feature::BasedOn`,
  `ClientError::is_stale`, `Workloads::put_based_on` and `remove_based_on`, `Tracked::stamp`
  (**Add**). A `remove` of a workload that is not there is refused to `wait` at once
  (**Change**: it ran out its time). Strict on one board; last write wins across boards.
- **I99.** Tests and the CI job only. No interface changes.
- **I98.** `packaging/systemd/orion-node.service` no longer sets `RestrictSUIDSGID=yes`
  (**Break** for an image that copies the unit: take the new one, or files in a root fail under
  systemd). Where `openat2` is refused, node logic's refusals name it and the hello carries the
  fact `files` (**Add**). `orion_file_store::probe`, `NO_OPENAT2` (**Add**). No wire changes;
  level 7.
- **I96.** A document only, `docs/from-dev.md`. No interface changes.
- **I94.** A lint only: `clippy::unwrap_used` is denied outside tests in the comm core's and
  node logic's crates. No interface changes.
- **I101.** A test only. No interface changes.
- **I100.** The boards heard on the local network (**Add**): the comm core's own file
  `orion/boards` and the topic `orion.boards` (`listed`, `changes`); `Orion::boards()`,
  `orion::heard::{HeardBoards, HeardBoard, Trust}`, `orion node boards`;
  `orion_mdns::{Browser, Boards, Found, Known, ASK, AGAIN, KEEP, MOST, MOST_ADDRESSES,
  MOST_TXT}`, `Responder::own`; `orion_mdns::run` takes the boards to keep (**Break** for a
  caller of it: one more argument, `None` for none). `OwnFiles::live`, default false (**Add**);
  `orion_files::Target` (**Add**); `SelfStatus::counts`, `Counts` (**Add**). The responder
  asks one question on each interface it announces on. Link-local IPv6 is no longer announced in
  a build that also has `mdns-sd` (**Change**: it was, there only). No wire changes; level 7.
- **I102.** The `shm-*` pins move to `214c0ca7`. Nothing a program calls changes; level 7.
- **I104.** A lease with no end is never taken before its holder was quiet `leases.quiet_ms`; it
  could be up to about 2 ms early (**Change**). The `quiet_ms` a service is told is rounded down
  (**Change**: it could read 1 ms long). No interface changes; no wire changes; level 7.
- **I109.** The gate and the push guard compare code keys (every file but the notes); new gate
  parts `clippy` and `shipped`; `Gate-Lanes:` names both. No interface changes; no wire changes;
  level 7.
- **I110.** The tests, clippy, the shipped builds and the short fuzz run are reused by content
  key; `Gate-Lanes:` names `tests` and `fuzz`. No interface changes; no wire changes; level 7.
- **I111.** 49 libraries are `[lib] test = false` (no unit tests; their doctests run as
  before). No interface changes; no wire changes; level 7.
- **I112.** The gate's device size builds run in parallel. No interface changes; no wire
  changes; level 7.
- **I113.** The gate's lanes start before its checks that build nothing. No interface changes;
  no wire changes; level 7.
- **I114.** The gate runs at nice 10 throughout (`scripts/work-env.sh`). No interface changes;
  no wire changes; level 7.
- **I115.** The tests' keys hold the files outside the crates only for packages that may reach
  them. No interface changes; no wire changes; level 7.
- **I116.** Each gate wrapper keeps its command's status by hand, not by `set -e`: a red
  command fails its step and its lane wherever it is called, a red lane step ends its lane, and
  nothing is recorded or stamped as passed after one. `scripts/gate-self-test.sh` proves it
  on planted failures, called as the gate calls them and where `-e` is ignored.
  No interface changes; no wire changes; level 7.
- **I117.** The whole gate leaves the size gate's builds to CI when a size key is new
  (`not-run(ci-only:N-of-M-size-keys-new)`), and runs it here when every key is measured.
  `scripts/release-check.sh` takes that not-run only where CI's size gate passed at that
  commit. Heavy work refuses a cgroup whose CPU weight is not 100, or that is idle. No interface
  changes; no wire changes; level 7.
- **I118.** `scripts/size-key.py` lists the files once, and the size gate's self-test takes its
  keys all at once: 6 to 10 s, from 40 to 180. The feature matrix keeps 6 shards, measured
  against 3 (`scripts/matrix-ab.sh`). No interface changes; no wire changes; level 7.
- **I119.** The lane keys' self-test runs beside the tests (`beside` in `scripts/gate.sh`), on
  CI's main runner too, where it ran alone for 58 s on every push. No interface changes; no wire
  changes; level 7.
- **I120.** I119 taken back: beside the tests on CI's 4-core runner, the lane keys' self-test
  only shared its cores (main step 185 s against 187 to 198). The self-test runs before the
  tests again. No interface changes; no wire changes; level 7.

  A device is not a caller, so run handles are not its to use. The C library has no actions.

## Builds

The gate checks, at this revision:

- Linux x86_64: every crate, every feature set;
- the client on Windows (`x86_64-pc-windows-msvc`, `x86_64-pc-windows-gnu`) and macOS
  (`x86_64-apple-darwin`, `aarch64-apple-darwin`), clippy with tests;
- the static board files for aarch64, armv7 and riscv64 (musl, with zig 0.15.2), held to their
  sizes.
