# The client

What a program uses to talk to a comm core. One client, with the same calls wherever the comm
core is.

## One crate

A program depends on `orion` and nothing else of Orion's. `Orion::connect(address, name)` gives
one handle; what it can do is behind cargo features so a program carries only what it uses.

| Feature | What it gives |
| --- | --- |
| `topics` (default) | `declare`, `subscribe` |
| `records` (default) | `records()`: put, get, watch |
| `actions` (default) | `run`, `attach`, `register_action` |
| `buffers` | `declare_buffers`, `subscribe_buffers` |
| `files` | `files()`: push, pull |
| `workloads` | `workloads()`: say what should run, hear where it is |
| `executor` | `executor()`: run workloads for Orion |
| `provider` | `provider()`, `resources(prefix)` |
| `tokio` | wait inside a runtime |

`examples/roles.rs` in that crate is every role in one file.

**When the comm core goes away.** A call fails with an error for which `is_lost` is true.
`orion.reconnect(..)` connects again and says everything again: topics are declared with their
rings handed back, subscriptions and watches are asked for from where they were, actions are
registered. On the board a ring belongs to its publisher and its subscribers, so values kept
going from one to the other while the comm core was away, and it takes the same ring back when
it returns. Nothing that was flowing stops.

The sections below are the crates the handle is made of.

## Connecting

`orion-connect` gives a `Session`.

| Address | Written | What it is |
| --- | --- | --- |
| Local | `/run/orion/orion.sock` or `unix:<path>` | the socket of the comm core on this board. Descriptors travel here and nowhere else. |
| Network | `orion+tcp://host[:port]`, with `#<key in hex>` to accept only that node | a comm core on the network. Port 9200 unless told. |
| Serial | `serial:<path>[@baud]` | a comm core at the other end of a serial line, 115200 unless told. |

`Session::over_link` takes a link that someone else opened.

With a key in its options a client's network session is signed: the node knows who is calling,
the client checks the node holds its key, and nothing can be changed on the way. Without one it
is plain, which a node with security on refuses.

**The session is blocking and owns no thread and no timer.** `next(wait)` waits for the next
frame and, while it waits, answers the comm core's pings, sends its own, and notices when the
comm core has said nothing for three heartbeats. A program with nothing to wait for calls it
with a zero wait from its own loop; one that waits on other things too gives `as_fd` to `poll`.
With feature `tokio`, `next_async` waits inside a runtime instead.

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
| `registration.poll()`, `.wait(for)` | The next run handed to this program, or a request to stop one |
| `registration.progress(..)`, `.finish(..)` | Says how a run is doing, and how it ended |

Registering an action needs a policy, and that policy is what the one check decides every run
by. A run the check refuses never reaches the program that owns the action: the caller is told
`Denied` and the owner hears nothing.

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
way at once, so a push neither fills the connection nor waits for an answer to each chunk.

`files.pull(..)` asks the board to fetch the file itself from a URL, check it and put it in
place. Only the request and the board's reports cross the connection.

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
more, which is how Orion takes back runs that carried on without it, by name.

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

