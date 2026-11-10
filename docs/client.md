# The client

What a program uses to talk to a comm core. One client, with the same calls wherever the comm
core is.

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

