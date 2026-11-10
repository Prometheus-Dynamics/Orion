# The comm core

The always-up program: every way in and out of a board. This page describes it as it is built,
crate by crate.

## The server

`crates/comm/server` (`orion-server`). Everything a connection goes through before a service sees
it.

**Carriers** bring connections. A connection says who is on the other end (socket credentials, a
verified key, a link's device, or nothing), which roles it may ask for, whether descriptors can
travel on it, and moves whole frames. The server knows nothing else about it.

**Sessions.** A connection becomes a session with `Hello` and `Welcome`. A session has:

- a bounded outbox (`comm.outbox_bytes`, 256 KiB): what waits to be sent to it;
- a deadline to say hello (`comm.hello_ms`, 5 s);
- a deadline to be heard from: three heartbeats (`comm.heartbeat_ms`, 2 s);
- a deadline for each frame to be taken (`comm.write_ms`, 5 s).

There can be `comm.max_sessions` (256) at once. One more is told `Busy` and let go.

**Services** answer kinds. A service is three functions that return at once:

- `asks` reads a frame and says what it asks for and who may ask it. That is all the check is told.
- `handle` is called with a `Checked<Incoming>`. Only the server can make one, after the check, so
  no service can be reached around it.
- `closed` is told when a session is gone.

A frame of a kind no service answers is ignored, so an older comm core can talk to a newer client.

**What a session that misbehaves does to the others: nothing.**

| It | What happens |
| --- | --- |
| Floods requests | Its reader is one task taking turns: it yields every 32 frames |
| Does not read its answers | Its outbox fills. An answer that cannot be queued closes it |
| Does not read a stream | Frames marked droppable are dropped and counted. It is closed at its write deadline |
| Goes quiet | Closed after three heartbeats |
| Never says hello | Closed at the hello deadline |
| Sends a frame that makes a service panic | That session is closed. The service keeps serving |
| Sends a bad frame, a second hello, another protocol version | Closed, with a `Reject` where one applies |

Everything is on one thread. Nothing allocates per request once a session is open: the read
buffer, the outbox and the timers belong to the session.

`orion_server::mem` is a carrier in memory, for tests of every service.

## The same-board carrier

`crates/comm/unix` (`orion-unix`). One Unix socket (`socket`, `/run/orion/orion.sock`). Every
program on the board connects once and keeps the connection.

- A frame is `[u32 length][frame]` in one `sendmsg`, with descriptors attached to it. A frame must
  fit the socket's buffer, which the default 64 KiB limit does.
- Who is connecting comes from the kernel (`SO_PEERCRED`): a user id and a group id. The socket's
  permission bits decide who can connect at all.
- Any program may be a client, a provider or an executor. Only root and the configured node users
  may be the node logic.
- Binding looks at what is at the path first. A socket nobody answers is replaced. A socket
  somebody answers is another comm core, and is left to it. Anything that is not a socket is never
  touched.
- A send the socket takes only part of is finished as plain bytes, so a reader that is a little
  behind still gets every frame whole.

## The topic broker

`crates/comm/broker` (`orion-broker`). A topic is a ring in shared memory (`docs/ring.md`). The
broker does set-up and nothing else: after it, a publish is a memory write and a wake between two
programs, and the broker handles no request until someone connects, subscribes or leaves.

| Request | What the broker does |
| --- | --- |
| `Declare` | Makes the ring from the declaration. Answers `Opened` with the write end |
| `Subscribe`, with the subscriber's doorbell attached | Sends the topic's declaration, then `Opened` with a read end. Passes the doorbell (`Bell`) and the subscriber's request (`Demand`) to the publisher. Sets the count in the ring |
| `Subscribe` before the topic exists | Answers `TopicState: Waiting`. The declaration and `Opened` follow when someone declares it |
| `Unsubscribe`, or the subscriber's connection closing | Tells the publisher to drop that doorbell, and the new count |
| `Open` | A read end without subscribing, for a look. The write end again, for the publisher only |

- **Start order does not matter.** A subscriber that comes first waits and is given the ring when
  the topic is declared.
- **A publisher that goes away** leaves its ring. Subscribers are told `PublisherGone`. The same
  declaration again carries on in the same ring (`Live`), so subscribers do nothing. A different
  declaration under the same name replaces the ring, and subscribers are sent the new one under
  the same handle. A second program may not declare a topic that has a publisher.
- **A comm core that restarts** has no rings, but its publishers kept theirs and kept publishing. A
  publisher declares again with its ring's write end attached and the broker adopts it, checking it
  against the declaration. Nothing that was flowing stops.
- **One cap** (`topics.max_bytes`, 16 MiB) bounds all ring memory, and `topics.max` (1024) the
  number of topics. A declaration over either is refused. A topic nobody publishes or subscribes
  to is removed, and its memory with it.
- **Who may**: by default programs on the board, other nodes and devices declare; programs on the
  board, other nodes and operators subscribe.
- **A program that cannot map shared memory** is answered `shared_memory: false` and still counts
  as a subscriber. `orion-broker-proxy` carries its samples.
