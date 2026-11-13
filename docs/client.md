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

**When the comm core goes away.** A call fails with an error for which `is_lost` is true.
`orion.reconnect(..)` connects again and says everything again: topics are declared with their
rings handed back, then actions are registered, then records are put again and subscriptions and
watches are asked for from where they were. Actions come before records, so a resource said
again names actions that are already back. If an action is refused (another program took its
name while the comm core was away), `reconnect` fails and the resource is not said again. On the board a ring belongs to its publisher and its subscribers, so values kept
going from one to the other while the comm core was away, and it takes the same ring back when
it returns. Nothing that was flowing stops.

The sections below are the crates the handle is made of.

## Connecting

`orion-connect` gives a `Session`.

| Address | Written | What it is |
| --- | --- | --- |
| Local | `/run/orion/orion.sock` or `unix:<path>` | the socket of the comm core on this board. Descriptors travel here and nowhere else. |
| Network | `orion+tcp://host[:port]`, with `#<key in hex>` to accept only that node | a comm core on the network. Port 9200 unless told. |
| Serial | `serial:<path>[@baud]`, with `#<key in hex>` to accept only that board | a comm core at the other end of a serial line, 115200 unless told. |

`Session::over_link` takes a link that someone else opened.

With a key in its options a client's network session is signed: the node knows who is calling,
the client checks the node holds its key, and nothing can be changed on the way. Without one it
is plain, which a node with security on refuses. On a link (a serial port, a USB cable) the same:
with a key the session is signed and the board knows the client as that key's operator; without
one the client is the link's device, which a board takes with security on as well, for what a
device may do. A signed link's frames are 32 bytes shorter than a plain one's. A node that proves another key than the one the
address is pinned to (`#<key>`) is refused with `ClientError::OtherKey { asked, answered }`: both
keys, so a tool can show the two fingerprints and let a person decide whether the board was
replaced or is not the one they meant.

**The session is blocking and owns no thread and no timer.** `next(wait)` waits for the next
frame and, while it waits, answers the comm core's pings, sends its own, and notices when the
comm core has said nothing for as long as it allows (`Welcomed::idle`, three heartbeats unless
the comm core sets `comm.idle_ms`). A program with nothing to wait for calls it
with a zero wait from its own loop; one that waits on other things too gives `as_fd` to `poll`.
With feature `tokio`, `next_async` waits inside a runtime instead.

**Who the board is, and being let in.** `node()` is who the board says it is, signed by its key
and made for the question, `clock()` how its clock compares, `sync_peers()` and `forgive(peer)`
where its sync with other boards is. `enroll(public_key, name, enrollment_key)` asks the board to
let an operator in: from root on the board for any key, at once; from the operator itself, on a
session signed with that key, at once with the cluster's enrollment key (only a proof made from
it is sent) and `Pending` until approved without. The answer (`node::Enrollment`) has the state,
what the operator may do, and the board's key.

**There is one way back.** A call that fails with an error for which `is_lost` is true means
the connection broke or the comm core went quiet. `reconnect` connects again to the same
address as the same program, a short fixed time between tries, for as long as the caller
says. A connection to the comm core is a way in, and those are not backed off. An answer of
"no" (a rejected hello, a key that is not welcome) is returned at once: asking again does not
change it.

## One session, many parts

A program has one session and several things going over it: topics, records, actions.
`orion_connect::Client` shares the session. Each part registers a `Handler` for the kinds of
frame that are its own, and whoever is waiting on the session pumps it: every frame that arrives
goes to the part it belongs to, so a part waiting for its answer never loses another part's
frame. A refusal is kept until whoever sent the request asks for it.

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

Read a held frame's bytes through `held.bytes(..)` each time. A slice kept from before the
frame was held, or after it was dropped, is memory the publisher may be writing.

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
| `actions.attach(number)` | Comes back to a run that was started detached |
| `actions.register(decl, policy)` | Adds an action. The policy is required |
| `registration.poll()`, `.wait(for)` | The next run handed to this program, a request to stop one, or a caller gone |
| `registration.progress(..)`, `.finish(..)` | Says how a run is doing, and how it ended |

Registering an action needs a policy, and that policy is what the one check decides every run
by. A run the check refuses never reaches the program that owns the action: the caller is told
`Denied` and the owner hears nothing.

A run handed to the program says who asked for it (`caller`), as the audit log names them:
`local:<uid>`, `operator:<key>`, `peer:<node>`, `device:<name>` or `anonymous`. A run forwarded
from another node was asked for by that node. The check has already decided; `caller` is for a
program that answers different callers differently, or keeps its own log.

A request to stop a run says why (`reason`): `Asked` (its caller asked), `CallerGone` (its
caller's session ended first, and it was not detached), `Deadline` (its deadline passed; the
caller has already been told it timed out), or `Stopping` (node logic is stopping; the caller is
told it was cancelled). Node logic sets it. What a caller says of why is not kept. A run carried
from another node keeps the reason that node gave. `Unsaid` comes from a host that does not say.

A run also carries `session`, a token for the caller's session that is never reused while the
node runs, also when one identity connects twice, and also for a caller on another node whose
run was carried here. When that session ends, for any cause, the
program is told once with `Request::CallerGone { session }`, whether its runs from it are still
going or not: that is the time to let go of what it holds for that caller. A detached run goes
on until its deadline. A detached run needs a deadline (`deadline_ms`), no longer than the node
takes (`actions.max_detached_ms`, an hour unless said): it is refused otherwise, as `NoDeadline`
or `DeadlineTooLong`.

A program that is not taking its runs does not have them pile up: 64 wait, and the next is
answered as rejected, busy, at once.

## Files

`orion-files`. A file goes to a board in chunks, is checked against its SHA-256 when all of it
is there, and only then is put where it belongs. The wire names a destination, not a path.

`files.push(destination, source, progress)` sends it and returns how it ended: committed, or
rejected with why. A push resumes: the transfer's number comes from the file's digest and its
destination, so the same file to the same place is the same transfer, and beginning it again is
answered with how many bytes the board already has. After a broken connection, reconnect and
push again; nothing the board has is sent twice. At most eight chunks of 32 KiB are on their
way at once, so a push neither fills the connection nor waits for an answer to each chunk. The
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

`files.pull(..)` asks the board to fetch the file itself from a URL, check it and put it in
place. Only the request and the board's reports cross the connection.

`files.sink(prefix)` is the other end, for a program on the board that puts files where they
belong itself, as board management does with a boot slot. It becomes the receiver of every
file whose destination starts with the prefix, and the node keeps none of it. `sink.next(wait)`
hands it a `Begin` (answer with how many bytes of that transfer it already has), then each
`Chunk`, then `Commit`; it answers each with `sink.status(transfer, received, state, detail)`.
Its answers pace the sender, so it is never handed more than a few chunks ahead of what it has
answered for.

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
workload's spec is a shell command, which it runs as a child process. `stop` is `SIGTERM`,
`kill` is `SIGKILL`, and the inventory is what is true of the children: running, stopping,
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
operator  ask --as NAME [--enrollment-key-file PATH]   this operator's key (--key) asks to be let in
operator  enroll KEY --as NAME                         root on the board lets a key in
```

`--key FILE` is the operator's key, made there the first time: a network session is signed with
it. `operator ask` prints where the operator is (`enrolled`, or `pending` until someone on the
board approves it), its own key, and the board's key with its fingerprint, to compare and to pin
the address to ([getting-started.md](getting-started.md), "The first connection to a board with
security on"). It exits 0 only when the operator is enrolled.

It exits 0 when the thing was done, 1 when it was not (the board refused, the action failed,
the time ran out), and 2 when the command was not understood, which it finds out before it
asks any board. A value is shown by its field's declared type and unit, so the tool reads
topics it has never heard of. The whole tool is one function, `orion_ctl::run`, so it can be a
mode of the one binary.

