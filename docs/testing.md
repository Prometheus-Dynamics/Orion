# Testing a program against a board

A program that uses Orion is tested against a board. It does not need a real one: the
`orion-test-board` crate runs the real comm core and the real node logic in the test's own
process, on a socket in a directory of its own. It is Linux only, like the board.

```toml
[dev-dependencies]
orion-test-board = { git = "<Orion's repository>", branch = "next" }
```

```rust
use orion_test_board::TestBoard;

#[test]
fn my_program_publishes_its_reading() {
    let board = TestBoard::start("reading");
    let orion = board.connect("my-program").expect("connect");
    // ... the program under test, given board.address() ...
}
```

Dropping the board stops node logic, then the comm core, and removes the directory. `name`
names that directory, so tests running at the same time keep apart.

## What it is

Nothing is stood in for. A client is answered, and refused, by the same code as on a board:

- the comm core as `orion comm` assembles it (`CommCore::start`), with its broker, hello,
  keys, status topics and outbound path;
- node logic as `orion node` runs it (`Node::load`), with records, leases, actions, files and
  workloads, saving to the board's directory;
- one `orion.conf` text read by both, as on a board.

## Options

`TestBoard::start_with(name, Options { .. })`:

| Option | Default | What it does |
| --- | --- | --- |
| `name` | `board` | The board's node name |
| `security` | `NoRecord` | The board's security record: `NoRecord` (off), `Off`, `On`, `Damaged` (on, and the hello document says so). With `On`, a network client with no key the board trusts is refused, as on a board |
| `network` | off | The network and WebSocket ways in, on 127.0.0.1 at free ports: `network()`, `websocket()` |
| `heartbeat_ms` | the board's | `comm.heartbeat_ms`. Set it long on a loaded machine, so a slow moment is not a lost session |
| `node` | on | Node logic, or the comm core alone |
| `operator` | none | A public key the board trusts with everything from the start, seeded as a flash seeds one (`operator.seed`, see [comm.md](comm.md)) |
| `conf` | empty | More `orion.conf` lines (see [settings.md](settings.md)). A key set here replaces the board's own line for it |

## Taking parts away

- `stop_node()` stops node logic as when its service stops. What is node logic's (records,
  resources, actions, files, workloads) is refused `unavailable` by the comm core, and the
  comm core goes on. `start_node()` brings it back with what it saved, and returns once it
  answers.
- `start_in_recovery(name, options, why)` starts recovery, the way the comm core does when it
  cannot start. Its hello document says `recovery` and `why`, and there is no node logic.
- Settings that parse but stop the comm core from starting (a directory that cannot be made,
  for one) start recovery instead, as on a board. `state()` says `Recovery(why)`.
- Settings that do not parse panic the test: recovery would then serve the board's usual
  socket, not the test's.

## A receiver that answers late

`board.hold_commits(prefix, hold)` starts a program on the board that receives every file whose
destination starts with `prefix`, as board management does with a boot slot, and answers each
commit `hold` after it was asked, committed or rejected by the file's digest. It pumps its session
all the while, so the comm core and node logic go on answering the pushing client, pings and all:
it is a receiver whose device work at the commit takes `hold`. It needs node logic, and stops when
the returned `HeldCommits` is dropped; `answered()` counts the commits it has answered.

Use it to test what a pushing client does with a late commit (see "How long a push waits for the
board" in [client.md](client.md)). With a short `Options::timeout` the test stays short:

```rust
let held = board.hold_commits("slots/", Duration::from_secs(6));
let orion = Orion::connect_with(&board.address(), "updater", orion::Options {
    timeout: Duration::from_millis(400),
    ..orion::Options::default()
})?;
orion.set_reply(Reply::Alive { ceiling: Some(Duration::from_secs(30)) });
let done = orion.files().push("slots/b.img", &mut source, |_, _| {})?;
assert_eq!(done.state, FileState::Committed);
```

Under `Reply::Timeout` the same push ends with a timed-out `ClientError::Io` after twelve
timeouts, 4.8 s here.

## An action that is never answered

`board.hold_action(name, target)` starts a program on the board that registers the action
`name` on `target` and is handed every run of it, but answers none: no progress and no end. It
is a device whose call never comes back. It pumps its session all the while, so the comm core
and node logic go on answering the caller, pings and all, and the run stays open until the
caller's own bound ends it. `asked()` counts the runs it has been handed. It needs node logic.
When the returned `HeldAction` is dropped its program goes, and node logic ends each run it left
open as failed, "the program that runs it went away".

Use it to test how a program bounds a call that hangs, for example a device call made while a
commit is held:

```rust
let target = Target::Node("board".parse()?);
let held = board.hold_action("arm.call", target.clone());
let mut run = orion.run(&ActionCall { name: "arm.call", target, args: b"", deadline_ms: 0, detached: false })?;
assert!(run.finish(Duration::from_secs(2)).is_err()); // timed out: nothing answered
assert_eq!(held.asked(), 1);
```

## And `orion-stubs`

`orion-stubs` is Orion's own: a comm core put together from its parts, with small stand-ins
for node logic. It stays for what the test board cannot do:

- node logic's own tests, which may not depend on the comm core (`scripts/check-boundaries.sh`:
  comm and node never depend on each other), use it with no stand-ins (`for_node`);
- `orion_stubs::older` stands in for a node of an older revision, to test what a newer client
  does with one;
- the client's tests use the stand-ins to answer what a real board would need a setup for.

The stub and the node share the action list. Actions are not a stand-in:
`orion_stubs::router::Router` passes every action request to node logic's own list
(`orion-action-list`), so a run has a real handle, a handle from before a restart
(`Board::restart_node_logic`) is told `RunGone`, and every refusal is the node's. A test fails
if anything else in the workspace answers action runs by rules of its own
(`crates/testing/stubs/tests/one_list.rs`).

A consumer uses the test board.
