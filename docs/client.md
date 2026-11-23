# The client

What a program uses to talk to a comm core. One client, with the same calls wherever the comm
core is.

## One crate

A program depends on `orion` and nothing else of Orion's. `Orion::connect(address, name)` gives
one handle; what it can do is behind cargo features so a program carries only what it uses.

| Feature | What it gives |
| --- | --- |
| `topics` (default) | `declare`, `adopt`, `subscribe` |
| `records` (default) | `records()`: put, get, watch |
| `actions` (default) | `run`, `attach`, `register_action` |
| `buffers` | `declare_buffers`, `adopt_buffers`, `subscribe_buffers` |
| `files` | `files()`: push, pull |
| `workloads` | `workloads()`: say what should run, hear where it is |
| `executor` | `executor()`: run workloads for Orion |
| `provider` | `provider()`, `resources(prefix)` |
| `shell` | `shell(account, command, terminal)`: a shell on the board over the session |
| `tokio` | wait inside a runtime |

`examples/roles.rs` in that crate is every role in one file.

**Starting before the comm core.** `Orion::connect` keeps trying until the comm core welcomes
the program or refuses it, so a program may start before the comm core, or while it restarts,
and needs no loop of its own. Tries come every 250 ms, as for any way in, and each waits up to
`Options::timeout` for the welcome. `Options::patience` says how long:

| `Patience` | Keeps trying |
| --- | --- |
| `Forever` (default) | until welcomed or refused |
| `Within(d)` | for up to `d`, then the last failure |
| `Once` | one try: `connect_once`, for a tool that says so to a person |

`connect_watching(address, name, options, keep_trying)` tells `keep_trying` how it goes before
each wait (`Status`: tries failed, when the next is) and stops when it says `false`: a monitor
shows "connecting" from it and never stacks a loop of its own on top. Over a link
(`Session::over`, `over_link`), the patience is how long a hello, or a signed link's handshake,
is said again on the same halves.

A refusal is an answer and ends the trying at once (`ClientError::may_retry` is `false`):

- turned away at hello for the program's roles, its wire revision, or by the check
  (`Rejected(RoleNotOffered | RevisionMismatch | VersionMismatch | Denied)`, `Revision`);
- on the network, a node that is not the one pinned (`OtherKey`) or that refuses this client or
  needs a key it does not have (`Denied`);
- a socket path too long (`PathTooLong`), a frame too large (`TooLarge`), memory or a layout it
  cannot read (`Memory`, `Layout`), something that is not the wire (`Protocol`), and a refusal
  of a request (`Refused`).

Everything else is tried again: nothing at the address, nobody answering, a connection that
broke (`Io`, `Lost`), and a comm core with no room for another session or no session for this
one (`Rejected(Busy | NoSession)`). In C, `orion_connect` keeps trying and `orion_connect_once`
is the one try; in a browser, `Orion.connect` keeps trying and `Orion.connectOnce` is the one try.
A device says hello until a host answers, and after a refusal says it again a while later.

**When the comm core goes away.** A call fails with an error for which `is_lost` is true.
`orion.reconnect(..)` connects again and says everything again: topics are declared with their
rings handed back, then actions are registered, then records are put again and subscriptions and
watches are asked for from where they were. Actions come before records, so a resource said
again names actions that are already back. If an action is refused (another program took its
name while the comm core was away), `reconnect` fails and the resource is not said again. On the board a ring belongs to its publisher and its subscribers, so values kept
going from one to the other while the comm core was away, and it takes the same ring back when
it returns. Nothing that was flowing stops.

**A socket path too long.** A Unix socket's path is at most 107 bytes (`SOCKET_PATH_MAX`:
`sun_path` is 108 with its NUL). A longer one is `ClientError::PathTooLong { path, len }`,
said before anything is tried, and never `is_lost`: connecting again cannot help, so a loop
that tries again while it is lost stops at once. In C it is `ORION_E_ARGUMENT`.
`orion_connect::socket_path` checks a path the same way. Node logic refuses to start with one.

**When another program is not back yet.** After the node or its comm core restarts, a resource
whose provider has not said it again has the state `Restoring`, never `Ready`. Wait for `Ready`
(a watch tells both changes) and then call it. A run sent in the gap anyway is refused
`Error::Restoring`: ask again shortly. After two seconds what has not come back is `Lost`, and
its actions are `NotFound`. A node below level 2 says `NotFound` throughout, and a client built
before level 2 is told `NotFound` by any node.

**When the board is busy.** The comm core passes some requests on to node logic. When node logic
is behind, and its outbox in the comm core has no room for one of these, the comm core turns
that request away as `busy` (`Refused { error: Busy }`; `is_busy()` is true) and node logic goes
on. Busy is never a final no: the same request asked again later may be taken. Which requests
it can come back for, and what this client does with it:

| Request | Called through | On busy |
|---|---|---|
| `FileBegin`, `FileChunk`, `FileCommit` | `files.push`, `start_push` | asked again by the push itself within its own bound ("Files") |
| `FilePull` | `files.pull` | handed to the program |
| `Put` | `records.put`, a provider's `Resource::write` | handed to the program; the record is not written |
| `Get` | `records.get` | handed to the program |
| `ActionRun`, `ActionAttach` | `orion.run`, `attach`, `attach_handle` | the run's first read (`poll`, `wait`, `finish`) hands it to the program; the run never started |
| `PeerStatus`, `PeerForgive` | `node().peers`, `orion.forgive` | handed to the program |

A program that is handed busy waits a moment and makes the call again, on the same session: an
error for which `is_busy()` is true never has `is_lost()` true, so a program that reconnects on
`is_lost()` keeps its session through busy. A run that got busy was never started, and must be
run again: its `Run` says busy once and nothing after. A provider's `set_state` that got busy is
not written and is called again. A provider's `publish` that fails gives its description back
(`Unpublished { error, describing }`), and `describing.publish(..)` is called again. A
`withdraw` that fails gives the resource back (`Unwithdrawn { error, resource }`), and
`resource.withdraw()` again finishes it: topics already withdrawn are not withdrawn again, and
the record is written gone. Both turn into the `ClientError` for `?`. Every other request
node logic answers is never turned away for want of room: a standing one (a watch, an action
registered, a receiver, an executor's offer) or one nothing waits for an answer to (an action's
progress and result, a cancel, a receiver's status, a workload said, an executor's report). For
those the comm core still ends node logic's session as one that was not reading, and node logic,
back, is passed the standing ones again and has the rest said again (`NodeBack`), as after any
restart of it. In C the busy refusal is `ORION_E_REFUSED`, with "busy" in `orion_last_error()`,
and `orion` on the command line exits 1 with "refused: busy". Neither tells it from a final
refusal by its code. A client older than this is handed the same refusal, with no `is_busy` to
ask.

The sections below are the crates the handle is made of.

## Connecting

`orion-connect` gives a `Session`.

| Address | Written | What it is |
| --- | --- | --- |
| Local | `/run/orion/orion.sock` or `unix:<path>` | the socket of the comm core on this board. Descriptors travel here and nowhere else. |
| Network | `orion+tcp://host[:port]`, with `#<key in hex>` to accept only that node | a comm core on the network. Port 9200 unless told. |
| Serial | `serial:<path>[@baud]`, with `#<key in hex>` to accept only that board | a comm core at the other end of a serial line, 115200 unless told. |

`Session::over_link` takes a link that someone else opened, as one descriptor.
`Session::over(read, write, expect, name, options)` takes one the program opened itself in two
halves, a USB interface it claimed say: a `LinkRead` and a `LinkWrite`. The same frames go over
it as over a serial port, signed with a key in `options`, and `expect` is then the key the board
must prove. `reconnect_over(read, write)` says hello again over new halves, as the same program.

A link that is not signed loses frames with nothing said, so over one the hello is said again
until the welcome comes or `options.timeout` passes: again after 250 ms (`FIRST_AGAIN`), the wait
doubling to 2 s (`MOST_BETWEEN`). The board takes a hello inside a session on a link as a start
over, so this is always safe; a welcome to an earlier hello that arrives after the session
began is followed by the welcome to the later one, and the session takes that as what it now
is. `reconnect_over_with(open, keep_trying)` is the way back that never gives up: it opens new
halves with `open` and says hello over them, again and again at the pace of `reconnect`, until
it is welcomed or `keep_trying` says stop. A program whose link comes and goes calls that.

`ClientError::Lost` also comes from a `Reject { NoSession }` in the middle of a session: the
comm core has no session for this one any more (a link let go, a restart). Say hello again.

The read half runs on a thread of its own; the session writes on the caller's thread. The
session never waits for that thread. When the session is dropped, or given new halves, it drops
its write half: that is the program's signal to make the read return with an error (cancel the
transfer, close the handle), which ends the thread. What a late read brought is thrown away.

**Systems.** The client builds on Linux, Windows and macOS. Off Linux a session reaches a comm
core across the network (`orion+tcp://`) or over a link the program opened (`Session::over`);
the board's own socket and serial addresses are Linux's, and fail elsewhere with an I/O error
of kind `Unsupported`. So
is all that needs the board's socket: buffer topics, adopting a ring, `as_fd`, `next_async`.
Topics work: a subscriber's samples arrive as messages and go to a ring of its own on the heap,
read with the same calls. CI builds the client and runs its tests on Windows and macOS; the
tests that need a comm core run on Linux, where the comm core is.

With a key in its options a client's network session is signed: the node knows who is calling,
the client checks the node holds its key, and nothing can be changed on the way. Without one it
is plain, which a node with security on refuses. On a link (a serial port, a USB cable) the same:
with a key the session is signed and the board knows the client as that key's operator; without
one the client is the link's device, which a board takes with security on as well, for what a
device may do. A signed link's frames are 32 bytes shorter than a plain one's. A node that proves another key than the one the
address is pinned to (`#<key>`) is refused with `ClientError::OtherKey { asked, answered }`: both
keys, so a tool can show the two fingerprints and let a person decide whether the board was
replaced or is not the one they meant.

**Which node, and whether signed.** `Welcomed::signed` says whether the session's frames are
signed. `Welcomed::node_key` is the key the node proved: set at connect on a signed session, so
a tool that connected with no pin learns the key to pin. On a plain session it is `None` until
asked: `Session::node_key()`, `Client::node_key()` or `Orion::node_key()` asks the node once
(`Identify`, with a fresh nonce), checks the signed answer, and keeps the key; an answer not
signed by the key it names is `ClientError::Denied`. `Orion::node()` keeps the key it proves too.
The hello answer is signed whenever the asker gives a nonce, with security on or off.

**Refusals carry the node's words.** `ClientError::Refused { error, detail }`: the code, and the
node's words verbatim ("cam.front is held by workload:detector since ... ms: its holder is
answering"), or the code's own name when it sent none, never empty. `ClientError` and
`Welcomed` are `#[non_exhaustive]`: a `match` on `ClientError` needs a `_` arm, and `Welcomed` is
read, not built.

**A node that is not this client's wire, or not its level.** A node of another wire revision is
turned away at hello with `ClientError::Revision { node, client }`, whichever side says it first.
A node at the same revision names its additive level in `Welcomed::level` (0 if it names none).
A call that needs a later level than the node's (`Feature`: a topic whose subscribers start from
its newest values, a shell, a signed link, a detached run) fails with
`ClientError::Level { feature, needs, node }` and sends nothing; `needs(feature)` asks first.
A request for something the node does not serve at all is refused as `unsupported`. In C both
are `ORION_E_REFUSED`, with the words in `orion_last_error`. See `docs/wire.md` for the table.

**The session is blocking and owns no thread and no timer.** `next(wait)` waits for the next
frame and, while it waits, sends a ping twice a heartbeat and notices when the comm core has said
nothing for as long as it allows (`Welcomed::idle`, three heartbeats unless the comm core sets
`comm.idle_ms`). **A session is only as alive as its program**: no ping is sent while the
program neither waits on the session nor calls into it, and a comm core at the far end of a
network or a link ends a session it has not heard from for the idle time. On the board's own
socket neither side ends the session for silence: the comm core keeps a quiet program and logs
it, and the client keeps a silent comm core, because that socket reports the other end closing
at once. A program with nothing to wait for calls `next`
with a zero wait from its own loop; one that waits on other things too gives `as_fd` to `poll`.
With feature `tokio`, `next_async` and `answer_async` wait inside a runtime instead.

**How long an answer may take** is `Options::reply`, and `set_reply` changes it for every
later wait, the helpers' included, until it is set again (a reconnect keeps it). Set it back
after a request that needed a longer ceiling.

- `Reply::Timeout`, the default: each answer within `Options::timeout`, the same number that
  bounds connecting and the hello. A busy node, or a starved machine, can make a healthy answer
  late.
- `Reply::Within(bound)`: each answer within `bound`, a bound of its own. `Options::timeout`
  then bounds connecting and the hello alone.
- `Reply::Alive { ceiling }`: for as long as the node is heard from. The wait fails with:
  - `ClientError::Lost` when nothing at all has come for the node's idle time;
  - a timed-out `ClientError::Io` at the ceiling, if there is one. The session is still good.

  The window is the node's declared value (`Welcomed::idle`), never a number of the client's.
  The client pings every half heartbeat while it waits, and the node answers each ping, so a
  node that is running is never silent that long. Under `Alive` this holds on the board's own
  socket too.
- `silent_for()` is how long since anything at all came. The same calls are on `Client` and
  `Orion`.
- C: `orion_set_reply` (`ORION_REPLY_TIMEOUT`, `ORION_REPLY_ALIVE` or `ORION_REPLY_WITHIN`) and
  `orion_silent_for_ms`. The web client: `setReply` and
  `silentFor`; it has no bound by default.
- A node that takes the connection and never says hello fails with a timed-out
  `ClientError::Io` (`ORION_E_TIMEOUT` in C), not the kernel's "try again".

**Who the board is, and being let in.** `node()` is who the board says it is, signed by its key
and made for the question, `clock()` how its clock compares, `sync_peers()` and `forgive(peer)`
where its sync with other boards is. `offer_time(stratum)` gives the board this program's clock:
the board asks for it when it polls, and the session answers whenever it is pumped, by any call or
`pump(wait)`, for as long as it lasts (`times_asked()` counts). It needs what `docs/comm.md`,
"Time sync", says: `operator` in the board's `time.servers`, and `clock.set` with security on.
`node().clock()` says whether the board's clock is set, `synced` or `unset`, so a tool can offer
the time when it is not (`orion-ctl node time` says so). `enroll(public_key, name, enrollment_key)` asks the board to
let an operator in: from root on the board for any key, at once; from the operator itself, on a
session signed with that key, at once with the cluster's enrollment key (only a proof made from
it is sent) and `Pending` until approved without. The answer (`node::Enrollment`) has the state,
what the operator may do, and the board's key. `security_precheck(SecurityTo::On | Off)` is
what Orion has against turning the board's security switch that way, as `missing: ...` /
`busy: ...` lines and none when nothing is. It changes nothing, and needs a node at level 5
(`Feature::SecurityPrecheck`).

**There is one way back.** A call that fails with an error for which `is_lost` is true means
the connection broke or the comm core went quiet. `reconnect` connects again to the same
address as the same program, a short fixed time between tries, for as long as the caller
says. A connection to the comm core is a way in, and those are not backed off. An answer of
"no" (a rejected hello, a key that is not welcome) is returned at once: asking again does not
change it. What is an answer is the same as for the first connect (`may_retry`, above).

## One session, many parts

A program has one session and several things going over it: topics, records, actions.
`orion_connect::Client` shares the session. Each part registers a `Handler` for the kinds of
frame that are its own, and whoever is waiting on the session pumps it: every frame that arrives
goes to the part it belongs to, so a part waiting for its answer never loses another part's
frame. A refusal is kept until whoever sent the request asks for it.

**One handle for each part on a session.** A frame goes to the first part that has its kind,
so a second handle for the same part (a second `Files`, `Records` or `Topics` made on one
`Client`) hears nothing. `Client::add` says so once, on standard error, naming the kinds. The
handles `Orion` gives (`orion.files()`, `orion.records()` and the rest) are one each.

A client is one thread's. A program that talks to the comm core from several threads opens a
session in each; sessions are cheap.

## Topics

`orion-topics`. `Topics::declare` gives a `Publisher`, `Topics::subscribe` a `Subscriber`.

| Call | What it does |
| --- | --- |
| `publisher.publish(field, time, payload)` | One sample of a field, belonging to a time |
| `publisher.subscribers()` | How many are listening. With none, a publisher may as well not sample |
| `publisher.with_request(..)` | What the newest subscriber asked for |
| `publisher.withdraw()`, `topics.withdraw(name)` | Ends the topic while the session goes on: subscribers hear the publisher is gone, and it is not declared again after a reconnect. Another program may then declare it |
| `subscriber.next()` | The next sample in the order published, or how many were lost |
| `subscriber.latest(field)` | The newest sample of a field |
| `subscriber.at_or_before(field, time)` | The sample that belongs to a time: hit, lagged or missing |
| `subscriber.wait(for)` | Sleeps until there is something to read |
| `subscriber.status()` | Waiting for a publisher, live, or the publisher is gone |

The calls are the same wherever the comm core is. On the board a topic is a ring in shared
memory: a publish writes it and wakes the subscribers that are asleep, a read reads it, and the
comm core is not in the path. Neither allocates, and a subscriber that is awake costs the
publisher no system call. Off the board the same publisher sends each sample as a message and
the comm core writes it to the ring, and the same subscriber keeps a ring of its own for the
samples that arrive as messages. The program reads a ring either way.

A `TopicDecl` says what a subscriber that is sent samples as messages gets when it starts
(`on_subscribe`). This is a subscriber on another board, or in a browser. `FromNow` gives it
what is published from then on. `Latest` first gives it the newest sample of each field that
has one and has not passed `expiry_ms`, each with its own time. A subscriber that reads the ring
has `subscriber.latest(field)` for that.

`wait` sleeps on the subscriber's doorbell and on the session together, so nothing is polled on
a timer and the session is kept up while the program sleeps.

## Buffer topics

`orion-buffers`, for frames. A frame is too large to copy into a ring, so a buffer topic's
samples only name buffers, and the buffers are shared memory the publisher hands over once.

A publisher declares the topic over a pool of buffers. For each frame it calls `poll` (to take
in subscribers that came or went), `take` (a free buffer, or `Dropped` when there is none: then
that frame is the one to lose), fills it, and `publish`es its number with a time and its own
metadata. Nothing is copied, by it or by anyone after.

A subscriber calls `next`. A frame's `held` is its buffers: take it out to keep the frame, and
drop it to give the buffers back. The publisher sees who holds what and never reuses a held
buffer. A subscriber that holds more than it asked to be allowed, or for longer, is cut off, so
a stuck reader cannot stop a camera.

Read a held frame's bytes through `held.read(index, |bytes| ..)`. It checks, after the read,
that the buffer was not reused while it ran, and gives `Err(Torn)` if it was: drop that value,
it is not the frame's. A buffer is only reused under a reader that was cut off. `held.current`
gives the slice with the same check. `held.bytes` and `held.descriptor` are not checked, and a
slice kept from before the frame was held, or after it was dropped, is memory the publisher may
be writing.

A job that ended with nothing to show (a refusal, a timeout) is published with
`publish_empty(time, meta)`: an entry that names no buffer, so it is never dropped for want of
one. A subscriber gets it as `NextBatch::NoBuffers` (its `seq`, `time` and the publisher's own
`meta`), never as a frame. Its reason, if the metadata says one, is in the publisher's words.

A publisher can also cut a subscriber off by hand: `holders_of(buffer)` says who holds a buffer,
and `cut_off(slot)` makes what that one holds reusable at once, as the limits do. For an adopted
topic, `AdoptedBuffers::cut_off(slot, pool, table)` cuts off only Orion's own subscribers.

**A ring the program made itself.** A program that serves readers of its own, with or without
Orion, makes its ring, pool and hold table with the `shm-*` crates. `adopt` (a value topic) and
`adopt_buffers` (a buffer topic) make that same ring an Orion topic: Orion's subscribers are
handed the same ring and pool, and the program keeps writing with its own writer, calling
`wake` after each publish. Orion keeps its own subscribers' doorbells and `wake` rings them: one
eventfd write for each that is asleep, nothing for one that is awake, and no doorbell events reach
the program. Its own readers' doorbells are its own to ring. An Orion subscriber to the topic gets
the pool as it was offered (`pool()`: each buffer's descriptor and where it is), besides each
frame's `held`. The same declaration again with another ring is refused, `malformed`, if that ring
is not of its shape. For a buffer topic, `poll(pool, table)` puts Orion's subscribers'
marks in the program's table, in the reader slots it lets Orion use (`reader_slots`, a bit
each, and not zero: `adopt_buffers` refuses "any", since the ring has readers Orion did not
place); its own readers keep the rest. `poll` only ever empties a slot it filled itself. If a
subscriber's slot is taken by one of the program's own readers, that reader is left where it is,
the subscriber is not given the pool, `poll` makes the other changes and returns the error
(`TopicError::Ring`). A ring of another shape than the declaration is refused.
A comm core that restarts is handed the same ring and pool again.

`withdraw` on a buffer publisher, adopted or not, ends a buffer topic the same way. A provider's
`resource.withdraw()` withdraws the resource's topics with it.

Buffer topics are between programs on one board: there is no memory to share across a network,
and the calls say so there. The comm core sets the two sides up and then sees no frame.

## Records

`orion-records`. A record is a named piece of state with a stamp from the cluster's clock.

| Call | What it does |
| --- | --- |
| `records.put(entries)` | Writes records, and returns them as stored, each with its stamp. An entry with `deleted` set says the record is gone |
| `records.get(class, prefix)` | Reads once: the records of a class whose keys start with a prefix |
| `records.watch(class, prefix, after)` | Everything newer than a stamp, then each change as it is made |
| `watch.poll()`, `watch.wait(for)` | The next change |
| `watch.newest()` | The stamp to watch from next time |

Watches are pushed. A watcher is sent each change in the order it was made and asks for nothing
again. A program that reads too slowly for too long does not grow without bound: its watch
holds 1,024 changes, then stops taking them, and once the program has read what it holds the
watch asks for everything newer than the last change it was given. No record is missed; one
that changed several times in between is seen at its latest.

## Actions

`orion-actions`. An action does one thing once. Orion ships almost none: the list is open, and
any program that connects adds its own.

| Call | What it does |
| --- | --- |
| `actions.run(call)` | Starts an action and returns at once with a `Run` |
| `run.poll()`, `run.wait(for)` | News of the run: how it is doing (the newest only), or how it ended |
| `run.finish(for)` | Waits for how it ended |
| `run.cancel()` | Asks for it to stop. How it ended still comes |
| `actions.attach(number)` | Comes back to a run that was started detached, by its number |
| `run.handle()` | The run's handle, which names it whatever becomes of this session |
| `actions.attach_handle(handle)` | Comes back to the detached run a handle names |
| `actions.register(decl, policy)` | Adds an action. The policy is required |
| `actions.register_kept(decl, policy, keep)` | The same, and its detached runs are kept for up to `keep` while this program's session is lost |
| `registration.refused(call)` | Whether node logic refused this program's finish of a run, and why |
| `registration.poll()`, `.wait(for)` | The next run handed to this program, a request to stop one, or a caller gone |
| `registration.progress(..)`, `.finish(..)` | Says how a run is doing, and how it ended |
| `registration.finish_with_code(call, outcome, code, ..)` | The same, with the action's own status code |

Registering an action needs a policy, and that policy is what the one check decides every run
by. A run the check refuses never reaches the program that owns the action: the caller is told
`Denied` and the owner hears nothing.

**A code to branch on.** How a run ended (`Finished`) has its outcome, its output, words for
people (`detail`) and the action's own status `code`: a number whose meaning the action defines
and documents (a service's `BUSY`, say), so a caller branches on it, not on the words. Zero is
none given, and is what a node too old to pass one on gives. A program sets it with
`registration.finish_with_code`; `finish` gives zero. A microcontroller sets it with
`Actions::finish_with_code`, or `orion_finish_code` in the C API, and a web page reads it as
`result.code`. The words stay in `detail`. Node logic
passes the code on as the program gave it: to the caller, to whoever attaches to a detached run
later, and back across nodes. Orion's own ends give zero, except a deadline: a run its program
stopped because its deadline passed is `TimedOut` with code 110 (`DEADLINE_CODE`, `ETIMEDOUT`'s
number), so a caller can tell it from a program that gave up by itself. A program that went
gives zero.

When node logic stops a run itself (its deadline passed, or node logic is stopping), the program
is told to stop and the run waits for its word, up to `actions.confirm_ms` (10 s). The program's
word ends the run: a run it had finished a moment before ends as it finished, not as stopped.
A run with no word by then ends `Indeterminate` (6), code 0: it may have been done or not, and
node logic does not guess. An older client reads 6 as an outcome it does not know, which is not
a success either.

A run handed to the program says who asked for it (`caller`), as the audit log names them:
`local:<uid>`, `operator:<key>`, `peer:<node>`, `device:<name>` or `anonymous`. A run forwarded
from another node was asked for by that node. The check has already decided; `caller` is for a
program that answers different callers differently, or keeps its own log.

A request to stop a run says why (`reason`): `Asked` (its caller asked), `CallerGone` (its
caller's session ended first, and it was not detached), `Deadline` (its deadline passed), or
`Stopping` (node logic is stopping). For the last two the caller is told what the program says,
or `Indeterminate` if it says nothing in time. Node logic sets it. What a caller says of why is not kept. A run carried
from another node keeps the reason that node gave. `Unsaid` comes from a host that does not say.

A run also carries `session`, a token for the caller's session that is never reused while the
node runs, also when one identity connects twice, and also for a caller on another node whose
run was carried here. When that session ends, for any cause, the
program is told once with `Request::CallerGone { session }`, whether its runs from it are still
going or not: that is the time to let go of what it holds for that caller. A detached run goes
on until its deadline, and the run says so (`Request::Run { detached }`): a program need not
guess from the deadline whether `CallerGone` ends it. A node too old to say gives `false`. A detached run needs a deadline (`deadline_ms`), no longer than the node
takes (`actions.max_detached_ms`, an hour unless said): it is refused otherwise, as `NoDeadline`
or `DeadlineTooLong`.

**A run's handle.** A caller's numbers start over with each session, so once a session is
lost its number may name another run. Node logic tells each run's handle as it starts, and
`run.handle()` gives it once any `poll` or `wait` has read it. It is `RunHandle { instance, run }`,
kept as 16 bytes (`to_bytes`, `from_bytes`) or as text (`<16 hex>-<16 hex>`, by `Display` and
`FromStr`). Keep it with whatever the run is for.

- `actions.attach_handle(handle)` comes back to that run alone, from any session of the same
  starter. It needs a node at level 3 (`Feature::RunHandle`): below it, nothing is sent.
- `run.cancel()` names the handle once it is known. By number alone, a node stops only a run
  this same session started.
- A handle from before node logic last started is refused as `RunGone`: that run is gone, and
  how it ended is not known on the node.
- **If the session is lost before the handle comes**, how the run went is not known. Find out
  from what the action does, by reading what it changes. Never come back to the number alone:
  by then it may name another run.

`actions.attach(number)` still works for a detached run that no later run of the same starter
took, detached or not. Once one has, the number is refused as `NotFound`, saying to use the handle. A
node too old to give handles gives none, and `run.handle()` is `None`; so is a run carried to
another node.

**Kept runs.** A program whose job outlives a lost session (hardware still moving, say)
registers with `actions.register_kept(decl, policy, keep)`. It needs a node at level 6
(`Feature::KeptRuns`): below it, nothing is sent. When that program's session is lost, each
detached run of that action it was doing is kept for `keep`, at most a minute
(`MAX_KEEP_MS`). It is not ended:

- its caller, attached, is told progress starting `restoring:`, and `restored:` when the program
  is back;
- a cancel asked meanwhile is kept and given to the program when it is back; a deadline that
  passed meanwhile has the program told to stop as soon as it is back;
- the program is back when a session of the same identity (the same kind of caller, user and
  program name) registers the action again: this session through `actions.restore()`, or a new
  one. Each run kept for it comes again as a `Request::Run` with `kept: true`, the same `call`,
  no arguments and the deadline left, before the registration is answered. Go on and finish it
  as usual. A new session can do this only once node logic has seen the old one go: until then
  the action is still the old session's, and registering it is refused as taken;
- a program not back within `keep` has the run end `Indeterminate` (6), code 0, with words
  saying its session was lost and did not come back. A finish it sends after that is refused as
  `RunGone`, naming that ending: `registration.refused(call)` reads it.

While the session is lost, every call fails with an error whose `is_lost()` is true: hold the
jobs, do not cancel them. Node logic stopping ends every kept run `Indeterminate` at once.

A provider asks the same of a resource's action with `describing.action_kept(name,
description, policy, keep)`, and the facade with `orion.register_action_kept(decl, policy,
keep)`. Both need the same level, and fail the same way below it.

A program that is not taking its runs does not have them pile up: 64 wait, and the next is
answered as rejected, busy, at once.

## Files

`orion-files`. A file goes to a board in chunks, is checked against its SHA-256 when all of it
is there, and only then is put where it belongs. The wire names a destination, not a path.

`files.push(destination, source, progress)` sends it and returns how it ended: committed, or
rejected with why. A push resumes: the transfer's number comes from the file's digest and its
destination, so the same file to the same place is the same transfer, and beginning it again is
answered with how many bytes the board already has. After a broken connection, reconnect and
push again; nothing the board has is sent twice. At most six chunks of 32 KiB are on their
way at once, so a push neither fills the connection nor waits for an answer to each chunk. Six
is what fits in a session's outbox at the comm core's default (`comm.outbox_bytes`, 256 KiB)
with a chunk to spare. When the board has no room for a chunk yet (node logic is behind, or
the program that receives the file is), it turns the chunk away as `busy`. The push then goes
back to where the board last said it was and sends from there after a short wait: 20 ms,
doubling to 320 ms, starting again once the board takes something. A `FileBegin` or
`FileCommit` turned away as busy is asked again after 20 ms. None of it renews the push's
bound: a board that stays busy ends the push as one that went quiet would. A client older
than this does not read a chunk's refusal; its push stops moving, ends at its bound, and
carries on from the board's count when pushed again. The
file is hashed before anything is sent; the session is looked after meanwhile, however long
that takes.

How it ended carries the file's SHA-256 (`transfer.digest`). A program that keeps it, with the
size, pushes the same file later with `files.push_with(destination, source, Known { digest,
size }, progress)`, and the file is not read through to hash it again. A wrong `Known` is found
by the board's check, and the push ends rejected. `files.push_file(destination, path, known,
progress)` pushes a file by its path. It looks at the file's size and change time when it begins
and again before it commits: a file that changed meanwhile is not committed, and the push fails
saying so.

A push can be stepped. `files.start_push(destination, source, known)` begins it, and
`pushing.step(wait)` moves it on, sending what the window has room for and taking what the board
said. It returns `Waiting` (the board has not said how much it has), `Progress { received, size
}`, or `Done(transfer)`. Between steps the program makes its other calls on the same session,
including other pushes. `push` is `start_push` stepped to the end.

**How long a push waits for the board** follows `Options::reply` (see above), with two clocks:

- *While it begins and sends*, each answer is due within the bound counted from the board's last
  change of what it says about the transfer (in practice, its count of bytes received). Under
  the default `Reply::Timeout` that is `Options::timeout`, 5 s. The same status sent again does
  not restart it, so a receiver that stops answering chunks for that long ends the push with a
  timed-out `ClientError::Io`.
- *Once every byte is there*, the push waits for the board's check from when it sent the commit,
  and nothing the board says meanwhile but `Committed` or `Rejected` moves that start. Under
  `Reply::Timeout` the wait is twelve `Options::timeout`s, 60 s by default; past it the push ends
  with a timed-out `ClientError::Io` on its own. Under `Reply::Within(bound)` it is `bound`, and
  under `Reply::Alive { ceiling }` it lasts as long as the node is heard from, up to `ceiling`.

A destination whose receiver does long work at the commit, such as writing a device image, needs
`set_reply(Reply::Alive { ceiling })` or `Reply::Within(bound)` with a ceiling or bound above
that work's longest time, set before the push. The receiver only has to go on pumping its session
meanwhile.

`files.pull(..)` asks the board to fetch the file itself from a URL, check it and put it in
place. Only the request and the board's reports cross the connection.

`files.sink(prefix)` is the other end, for a program on the board that puts files where they
belong itself, as board management does with a boot slot. It becomes the receiver of every
file whose destination starts with the prefix, and the node keeps none of it. `sink.next(wait)`
hands it a `Begin` (answer with how many bytes of that transfer it already has), then each
`Chunk`, then `Commit`; it answers each with `sink.status(transfer, received, state, detail)`.
Its answers pace the sender, so it is never handed more than a few chunks ahead of what it has
answered for.

One session can hold several prefixes: each `files.sink(prefix)` gives a receiver of its own,
and each is handed only the files under its prefix (the longest, where two match), as the node
chooses. A prefix another session holds is refused as `already exists`.

**Files from a board.** A program on the board offers the files under a prefix, and a client
anywhere fetches one by name; the comm core hands each fetch to the program offering the longest
prefix of the name, and knows nothing of what the file holds. Both need a node at level 4
(`Feature::FileOffers`).

- `files.offer(prefix)` offers every name under `prefix` (one or more parts of a name, ending in
  `/`) for as long as the session lives. A prefix another program offers is refused as `busy`,
  naming that program; the refusal is counted where the list of offers says it, so a clash is
  seen. `offer.next(wait)` hands the program what was asked for (`asked.name()`, `from()`, how
  much the client has already, and `caller()`, who asked as the audit log names them). It
  answers `asked.refuse(why)`, in its own words, or `asked.give(source)`: the source is read
  through once for its digest (`give_known(source, Known)` skips that), and the returned gift is
  stepped (`gift.step(wait)`, or `gift.finish()`) until the client has it all or says why not.
  The client's answers pace it: at most eight chunks on their way. A gift waits on its client
  only while the client owes an answer, and counts the client's quiet from its last word: a
  program busy elsewhere between steps, however long, is not a client gone quiet. `gift.abort(why)` stops
  giving it, and the client's fetch ends at once, `Aborted`, with "the program that offers it
  stopped giving it: <why>", rather than waiting out the session's silence limit. A gift dropped
  before it is over is aborted the same way ("it was dropped before it was given"), and so is
  one whose file cannot be read ("it could not be read: <the error>"): the step that read it
  returns that error. A file asked and dropped unanswered is refused, "the program that offers
  it did not give it".
  `orionctl file fetch` prints the words and exits 1.
- `files.fetch(name, into, have, progress)` fetches it into `into`, which holds its first `have`
  bytes already (zero for none): a fetch that broke off goes on from what is here. The digest is
  checked over all of it at the end: committed, or rejected with why (the file is shorter than
  what is here, or what is here is not the file). `files.start_fetch(name, into, have)` begins one
  to be stepped, as a push is. A name nothing offers is refused as `not found`, "nothing offers
  <name>"; a program that goes mid-fetch ends it rejected, "the program that offers it went".
- A fetch never gives up on a program that is there. When the program has given nothing for
  `NO_PROGRESS` (5 s; `fetching.stalled_after(quiet)` to change it), a step says
  `Step::Stalled { received, size, quiet }`, again each time as long passes, and the fetch keeps
  waiting. The comm core says if the program goes, and a lost session ends it.
  `fetching.give_up_after(quiet)` sets a limit, off by default: that much quiet at a stretch
  ends the fetch not taken, "gave up: no progress for <quiet>, the limit set for this fetch", and
  the program is told the same. `orionctl file fetch` prints `NAME: no progress for N s, R of S
  bytes here; still waiting` each time, and `--give-up MS` sets the limit.
- `files.offers()` lists every prefix offered (`Offered { prefix, by, name, principal, since_ms
  }`) and how many offers were refused for a clash.

**The index of a prefix.** Each prefix offered has an index, `<prefix>index`, so a client can say
how much a fetch will be, and warn of what may hold a user's data, before it fetches anything.
Orion owns its format, and every program writes it alike:

```text
orion-index 1
total 1048576
logs/kernel 262144 no-user-data
logs/journal 786432 user-data
```

The first line is the format and its version; `total` bounds the whole prefix in bytes; then
one line a file: its whole name, the most bytes it can be, and `user-data` if it may hold a
user's data or `no-user-data` if not. `orion_files::index::Index` writes one (`write`) and reads
one (`parse`, which says which line is wrong and why, and `bound(name)`). A fetch held to a
file's bound, `files.start_fetch(..).within(bound)`, does not take a file said to be larger, and
writes nothing of it.

**The comm core's own, `orion/`.** Every board offers `orion/` itself, running and in recovery:
its log, its settings with secrets left out, its state and why, its sessions, its version, and
the record of its last good start, listed in `orion/index` (`docs/comm.md`). `offers()` lists it
`OfferedBy::CommCore`. No program may offer it.

**Files the board's settings name.** A board may serve prefixes from its settings
(`offers.file`: files and commands), while no program offers them; `offers()` lists such a
prefix `OfferedBy::Configured`, and as the program's while a program offers it. A client
fetches them as any other.

**The image's roots, `roots/`.** A board whose image names roots (`files.root.<name>` in its
settings) has node logic offer `roots/`: `roots/<name>/<path>` is fetched as any offered file,
and `roots/<name>/index` is the root's index. A push to `roots/<name>/<path>` lands there, in
place only once whole and checked, unless the root is read-only. Listing, removing, making a
directory and renaming are node logic's actions, `files.list`, `files.remove`, `files.mkdir`
and `files.rename`, run with `orion.run` with text arguments. `offers()` lists `roots/` as node
logic's. The names, grants, words and what the actions give are in `docs/node.md`, "The image's
roots". A file manager on a PC builds its tree from `files.list`, and a backup fetches each file
of a root's index.

**Names are carried escaped.** A file on a board may have any name. A client sends and is given
it in one form, `orion::types::Escaped` of its bytes: letters, digits, `. _ - : /` and text
outside ASCII as they are, `%XX` for anything else (`My Report.txt` is `My%20Report.txt`, `100%`
is `100%25`). A listing line's fourth field is that form, the one to ask for, and when it is not
how the name shows, the rest of the line is what it shows as. `orion::types::unescape` gives the
bytes back. A name is at most 255 bytes in this form, its directories and `roots/<root>/`
with it. A path longer than that cannot be reached through Orion: its listing line begins
`long`, by name, so a file manager can show it greyed and say why. It can be reached, or renamed
shorter, over SSH or SFTP. A board before this change refuses an escaped or longer name as
`InvalidName`.

## The executor role

`orion-executor`. An executor is any program that connects and says "I run this kind of
workload". Orion decides what should run; the executor starts and stops what it is told and
reports what is true. It keeps no record of its own of what should be running.

A program implements `Executor`:

| Method | What it does |
| --- | --- |
| `kinds()` | The kinds of workload it runs |
| `start(name, spec)` | Starts one. `start_on` also gives the kind and the CPUs it may use |
| `stop(name)`, `kill(name)` | Asks one to stop; ends one now |
| `inventory()` | Everything it has and where each is. A workload not here is gone |

and gives it to `Serving`. `start` says what it runs and everything it has; `turn` does what
Orion asked since the last turn and then reports every workload whose state changed. The
executor never has to remember to report: what it says it has is compared with what Orion was
last told. When the connection has been made again, `start` sends the whole inventory once
more, which is how Orion takes back runs that carried on without it, by name. When Orion itself
has started again it asks (`ExecutorAsk`), and the next turn answers with the whole inventory.

**An example to read.** `crates/testing/sample-executor` is the smallest working executor: a
workload's spec is a shell command, which it runs as a child process in a process group of its
own. `stop` is `SIGTERM` and `kill` is `SIGKILL`, each to that group, so what the shell started
ends with it. The inventory is what is true of the children: running, stopping,
failed if one exited with an error, and no longer listed once one has ended. Its `run` is the
program around an executor: connect, offer, take turns, and when the connection breaks connect
again and offer what is still running. It is also a program,
`orion-sample-executor [--socket PATH] [--kind KIND]...`.

## The provider role

`orion-provider`. A resource is a thing that exists: a camera, a fan. A provider is the
program that speaks for it.

```text
provider.resource("fan.left", "fan")      the kind is the provider's own word
    .label("mounted", "left intake")
    .topic("speed", fields, ..)            declares fan.left.speed, returns its publisher
    .action("set", description, policy)    registers fan.left.set
    .publish(Ready)                        writes the record: now it can be found
```

`.topic` declares a topic whose values never go stale and whose subscribers sent samples as
messages start from now. `.topic_with(&TopicDecl { .. })` declares it as the `TopicDecl` says,
the name taken under the resource's: a value that changes rarely, a mode say, is declared with
`OnSubscribe::Latest` and an `expiry_ms`, so a dashboard on another board is given it when it
starts instead of waiting for it to change.

What is written down is enough for a program that has never heard of the kind. The record
names the resource's topics and actions; a topic's declaration names its fields, types and
units; an action has a description and a policy. `resources(records, prefix)` is the reading
side: a dashboard, a tool or another board lists what is there, reads it and drives it with no
code for that kind of device.

## Workloads

`orion::workloads`. Orion holds what should be running and tracks every run. `put` says a
workload should exist: a name, a kind (which picks the executor), a spec only the executor
reads, and what to do when it ends. `remove` says it should stop existing; its record stays
until the run is really gone, unless forced. `status` and `wait` give where it is: pending,
starting, running, stopping, gone, or failed or lost.

`put_needing(workload, files)` is `put` for a workload that needs files: each a destination on
the board and the SHA-256 of what must be there. Orion starts it only when every one is in
place, and until then says which it waits for.

## From a web page

`web/client` is the client for a browser: one TypeScript file with no dependencies, built to
`web/client/dist/orion.js` (checked in, so a page needs no build step). It talks to the comm
core's WebSocket listener (`ws.listen`, see `docs/comm.md`).

```js
import { Orion, FieldType } from "./orion.js";

const orion = await Orion.connect("ws://board.local:8080/orion", "dashboard");

const level = await orion.subscribe("tank.level", (sample) => {
  // sample.field, sample.time (bigint, nanoseconds), sample.payload (Uint8Array)
});

const clicks = await orion.declare({
  name: "page.clicks",
  fields: [{ name: "count", type: FieldType.Int }],
  history: 16,
  maxPayload: 8,
});
clicks.publish(0, BigInt(Date.now()) * 1000000n, payload);

const result = await orion.run({ name: "pump.start" }, (step) => show(step.percent));
```

A request the board refuses throws `Refused`, with the board's code in `code`. Records, files
and workloads are not in the web client. After changing `src/orion.ts`, run
`web/client/build.sh`; a test fails if `dist/` is not the build of the source.

### Video in a page

`web/video-example` shows two video topics on plain `http://`: JPEG pictures in an `<img>`,
and H.264 as fragmented MP4 through Media Source. Orion carries the bytes and one bit,
"others depend on this one", which the publisher sets on a key frame; on a slow link the
board drops for that page alone and next sends a key frame. See its README.

## From C

`orion-capi` builds `liborion_c.a` and `liborion_c.so` (SONAME `liborion_c.so.1`), and
`crates/client/capi/include/orion.h` is the header. It covers topics: connect, declare and
publish, subscribe, wait and read.

```c
orion_check_version(ORION_ABI_MAJOR, ORION_ABI_MINOR);
Orion *orion;
orion_connect(NULL, "my-tool", 2000, &orion);          /* NULL: the board's own socket */
OrionSubscriber *level;
orion_subscribe(orion, "tank.level", &level);
OrionSample sample; uint8_t bytes[8];
while (orion_wait(level, 1000) >= 0)
    while (orion_next(level, &sample, bytes, sizeof bytes) == 1) { /* one sample */ }
```

Every call returns `ORION_OK`, a positive count where the header says so, or a negative
status; `orion_last_error()` says why in words. Nothing allocates memory the caller frees but
the handles. No thread is started. A handle belongs to the thread that made its session, and
must not be used after it is closed.

The version is a major and a minor. Something added raises the minor; anything that would
break a caller raises the major, and with it the library's name.

## The command line

`orion-ctl` is the client as a tool for a person. Its verbs follow the six things Orion is
made of, and `--address` takes any address the client does, so what it does to a board on the
bench it does to one across the network.

```text
orion [--address A] [--name N] [--timeout MS] [--key FILE] <concept> <verb> ...

node      show                      who the board is, checked against its signature
node      peers                     the nodes it knows
node      time                      how its clock compares with this computer's
node      offer-time [--stratum N] [--for MS]
                                    gives the board this computer's clock, while it runs
resource  list [PREFIX]             the things that exist
resource  show NAME                 one of them: its topics, its actions, its labels
topic     read TOPIC                the newest value of each field
topic     watch TOPIC [--count N] [--for MS]
topic     publish TOPIC NAME:TYPE=VALUE... [--hold MS]
action    run NAME [--node N | --resource R] [--args TEXT]
                   [--deadline MS [--detached]] [--for MS]
action    attach CALL [--for MS]
workload  put NAME --kind K [--spec TEXT | --spec-file PATH] [--node N]
                   [--restart always|on-failure|never] [--for MS]
workload  remove NAME [--force] [--for MS]
file      push DESTINATION PATH
file      pull DESTINATION URL --size BYTES --sha256 HEX [--for MS]
file      fetch NAME PATH [--bound BYTES] [--give-up MS]   a file offered on the board; goes on from what PATH holds
file      offers                    what is offered on the board, and by whom
operator  ask --as NAME [--enrollment-key-file PATH]   this operator's key (--key) asks to be let in
operator  enroll KEY --as NAME                         root on the board lets a key in
```

`--key FILE` is the operator's key, made there the first time: a network session is signed with
it. `operator ask` prints where the operator is (`enrolled`, or `pending` until someone on the
board approves it), its own key, and the board's key with its fingerprint, to compare and to pin
the address to ([getting-started.md](getting-started.md), "The first connection to a board with
security on"). It exits 0 only when the operator is enrolled.

It exits 0 when the thing was done, 1 when it was not (the board refused, the action failed,
the time ran out), 3 when an action ended `Indeterminate` (it is not known whether it was done;
it says so, and to look at what the action changes), and 2 when the command was not understood, which it finds out before it
asks any board. A value is shown by its field's declared type and unit, so the tool reads
topics it has never heard of. The whole tool is one function, `orion_ctl::run`, so it can be a
mode of the one binary.

