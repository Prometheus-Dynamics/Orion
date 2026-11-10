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

## Publishing on behalf

`crates/comm/broker-proxy` (`orion-broker-proxy`). A program on another board, in a browser or on a
microcontroller cannot map a ring. It declares like anyone else, is answered
`shared_memory: false`, and sends `Sample` or `SampleBatch` messages. The comm core writes them to
the topic's ring and rings the subscribers that are asleep, as a local publisher would. Every
subscriber on the board reads a ring, whoever the publisher is.

- Only a topic's own publisher may send its samples, and only one that was not given the ring. A
  ring has one writer.
- A sample takes no answer. One that cannot be written (a field or size outside the declaration)
  is refused, so a mistake is heard.
- What such a publisher gets back is `Demand`: the subscriber count and the newest subscriber's
  request, on every change. It can stop sampling when nobody listens.
- Nothing allocates per sample. The writer and the doorbells for a topic are rebuilt only when its
  ring, publisher or subscribers change.

## Buffer topics in the broker

`crates/comm/broker-buffers` (`orion-broker-buffers`). A frame topic is an ordinary topic whose
entries name buffers (`docs/ring.md`). Set-up adds the buffers and the hold marks:

1. The publisher declares the topic and offers its pool once (`BufferOffer`: the pool page and the
   buffers' descriptors). The broker keeps the offer.
2. A subscriber subscribes with its doorbell and its page of hold marks attached. It is sent the
   ring as for any topic.
3. The broker gives the subscriber a reader slot and sends its marks page to the publisher
   (`Holds`).
4. The publisher adds the page to its table and answers `Holds`.
5. Only then is the subscriber told its slot and handed the pool.

So a subscriber never reads a frame the publisher is not yet protecting. A subscriber that sends no
marks holds nothing and is handed the pool at once.

- A subscriber that comes late gets the current pool. One that comes before the camera gets it
  when it is offered.
- A replaced pool is a new offer with a higher generation, handed to every subscriber. An older
  generation is refused.
- A subscriber that leaves is taken out of the publisher's table (`Holds`, not present).
- Only a topic's publisher may offer its pool.

The broker maps no buffer and sees no frame. A comm core that restarts interrupts no frames: the
ring, the pool and the marks belong to the publisher and its subscribers, who offer the same things
again when they reconnect.

## Keys, enrollment, operators

`crates/comm/keys` (`orion-keys`). Who is trusted.

| | What it is | Where it is kept |
| --- | --- | --- |
| The node's identity | An ed25519 key, made on the first start | `<state.dir>/node-identity.ed25519`, 32 bytes, owner only |
| Peers | Other nodes, each pinned to one key | `<state.dir>/trust` |
| Operators | People's tools: a key, and what it may do | `<state.dir>/trust` |
| Revoked keys | Never trusted again | `<state.dir>/trust` |

`trust` is text, one entry per line, written beside and renamed over. Lines this build does not
know are kept, so an older node never destroys what a newer one stored. A key or store that is
there but unreadable stops the start; it is never replaced, because that would change who the board
is.

**A connection is known by its key**, not by what it says: the peer pinned to that key, else an
operator, and nobody at all if the key is revoked.

**What an operator may do** is its grants: read, a list of actions (`name`, `prefix*`, `*`), or
everything. The one check asks the trust store for every operator request. An operator trusted
with everything may also write and log in.

**How a key becomes trusted:**

| How | Result |
| --- | --- |
| An administrator on the board enrolls it (`OperatorEnroll` from root on the local socket) | Enrolled, whatever it was before |
| An operator proves it knows `enrollment.key` (a MAC over the node's name and its own key) | Enrolled |
| An operator asks with no proof | Pending, for an administrator |
| A node proves it knows `enrollment.key` (the three-step handshake) | Pinned as a peer |

The handshake is dev's: each side proves it knows the key with a MAC and that it holds its identity
with a signature, both bound to the role, both names, both keys and two fresh nonces. The key never
crosses the wire. A handshake is used up by its first confirm, right or wrong.

**A refused operator stays refused.** A revoked key, or a pending one that was turned down, is not
enrolled again by asking or by proof. Only an administrator brings it back. A peer's name stays
pinned to its first key: another key under the same name is refused.

**With security off** none of this is consulted. The check admits everyone, a tool with no key is
answered as an operator that may do everything, and nothing is recorded.

Settings: `cluster` (`default`), `enrollment.key` (none: only an administrator enrolls),
`operators.read` (`true`), `operators.actions` (none).

## The network carrier

`crates/comm/tcp` (`orion-tcp`). `orion+tcp`: how nodes reach each other and how people's tools
reach a node. The same frames as every carrier, with a length in front.

**A signed session**, with security on:

```text
C -> S  "ORTC" mode version                      mode 0: plain, 1: signed
S -> C  status                                   0: go on, 1: this node needs a signed session
C -> S  eph_C nonce_C key_C                      32 bytes each
S -> C  eph_S nonce_S key_S sig_S
C -> S  sig_C
S -> C  status                                   0: accepted, 2: this key is not welcome
```

Each side signs both sides' fresh values with its identity key, so a handshake cannot be played
back. The ephemeral exchange gives the two a secret, and every frame after carries a 16-byte MAC
under it, over the frame and its place in the order. A frame changed, added, dropped or reordered
in flight fails, and the session ends. The session is not encrypted: it can be read, and it cannot
be forged.

- The server knows the other end by the key it proved: the peer pinned to that key, else an
  operator (granted whatever the trust store says, which for a new key is nothing). A revoked key
  does not get past the handshake.
- A client that knows which key to expect refuses any other, so a node cannot be impersonated to
  its peers.
- Each handshake is its own task with its own deadline (`comm.hello_ms`). A connection that says
  nothing, or something else, holds up nobody.

**With security off** a plain session is accepted: no keys, no MAC, and the other end is nobody in
particular. A node with security on refuses a plain session and says so, so a tool can report "this
node needs a key".

**One connection each way.** A node dials each peer and keeps that connection for its own requests;
the peer dials back for its own. The dialer connects, hands the connection over, and connects again
when it ends: every 250 ms for as long as the program runs, never slower however long the peer has
been gone. Its state (running, retrying, failures) is reported on every change.

Descriptors do not cross a network. A subscriber over this carrier is answered
`shared_memory: false`.

## The hello document

`crates/comm/hello` (`orion-hello`). Anyone who connects may ask who the board is (`Identify`, with
a nonce) and is answered (`Identity`):

| Field | From |
| --- | --- |
| Node name, cluster, public key, interface revision | Orion |
| Clock, milliseconds since 1970 | Orion |
| Facts about the board: name, type, value | Other programs |
| The asker's nonce | The question |
| Signature | The node's key, over a fixed label and everything before this field |

- **Signed.** `orion_hello::verify` checks that the document is signed by the key it names and answers the
  nonce that was asked. Any byte changed fails it, and so does the same document offered for
  another question. Whether that key is the board the asker meant to reach is the asker's to
  decide: a tool that knows the board checks the key, one that does not learns it here.
- **Orion invents no board facts.** A program that knows one publishes it as a topic named
  `hello.<fact>`; the document carries the newest value of each, in name order, up to 64 facts of
  256 bytes. When that program goes, its facts go.
- **Anyone may ask**, a key that has been granted nothing included.
- **Always answers.** It is in the comm core and needs nothing else running.
- Someone on the board who gives no nonce is not checking a signature and gets the document
  unsigned. Over the network it is always signed.

## The board's vital signs and Orion's own state

There is no metrics endpoint. What a dashboard or a tool wants to know about the board is a topic
(`orion-self-status`), subscribed to like any other:

| Topic | Fields |
| --- | --- |
| `orion.host` | `uptime_s`, `mem_total_kb`, `mem_available_kb`, `cpu_percent`, `temperature_c` |
| `orion.comm` | `sessions`, `opened`, `rejected`, `requests`, `refused`, `unknown`, `dropped`, `panics`, `topics`, `ring_bytes` |
| `orion.state.<part>` | `state` (0 starting, 1 running, 2 retrying, 3 off), `failures`, `restarts`, `retry_at_ms` |

Every field is eight bytes, an integer or a float, little-endian.

Nothing is sampled while nobody watches. `orion.host` and `orion.comm` are read only while they
have a subscriber: the first gets a reading at once, then one every interval, and only the fields
that changed are published. When the last subscriber leaves, the reading stops and no timer is
left running. A part's state is published by the part when it changes, which costs nothing while
nothing does; that is how a link's backoff is visible.

These are topics the comm core publishes itself. The broker keeps them when nobody subscribes,
and refuses a client that declares over one or opens one to write.

