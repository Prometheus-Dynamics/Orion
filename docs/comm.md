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
- a heartbeat (`comm.heartbeat_ms`, 2 s), and a deadline to be heard from (`comm.idle_ms`; zero,
  the default, is three heartbeats, otherwise at least two). `Welcome` carries both, and a client
  gives up on a comm core it has not heard from for as long. A slow link, or a test on a loaded
  machine, sets them longer;
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
| Goes quiet | Closed at `comm.idle_ms` (three heartbeats) |
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
| `TopicWithdraw` | From the topic's publisher only (anyone else is `not found`): ends its topic while its session goes on. Subscribers are told `PublisherGone`, as when the session ends, and the publisher is answered with the same `TopicState` |

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

## Samples for subscribers without a ring

A subscriber on another board, or in a browser, cannot map a ring. It subscribes like anyone
else, is told `shared_memory: false`, and is sent each sample as a `Sample` message.
`orion-outbound` sends them. For each such subscription it keeps a reader on the topic's ring,
inside the comm core, from the moment the subscription is made. The publisher writes its ring
and does nothing different.

- **Each subscriber has its own byte limit**, `outbound.max_bytes` (128 KiB). A sample is sent
  only if what already waits to be sent to that session, plus the sample, fits. Otherwise it
  is dropped for that subscriber and counted.
- **After a drop, skip forward to the next independent sample.** A publisher marks a sample
  "others depend on this one": a key frame. Once a subscriber has lost a sample of a field
  that uses the mark, nothing of that field is sent to it until the next marked sample. A
  field that never uses the mark has only independent samples, and the next one is sent.
- **A slow reader hurts nobody else.** Nothing waits: the publisher is never held up, the
  ring is never held, and each other subscriber is sent everything it has room for. A
  subscriber is never closed for being slow at taking samples. One that falls so far behind
  that the ring overwrites what it had not been sent has lost those, like any reader.
- **Woken, not polled.** Publishers ring one doorbell for all such subscribers, as they would
  ring a subscriber's own, and only when the sender is asleep.
- Subscriptions take turns of 256 samples, so one busy topic does not starve another.

## The WebSocket carrier

A browser cannot open a socket of its own. `orion-ws` lets it reach the comm core over a
WebSocket and speak the same frames as everyone else: each Orion frame is one binary message.
Nothing above the carrier knows the difference. A browser is a subscriber without a ring and a
publisher on behalf, like a program on another board.

- **No HTTP framework.** A WebSocket starts as one HTTP request. It is read and answered here:
  the upgrade and nothing else. An ordinary page request is answered `426`, anything that is
  not a request `400`.
- **One listener, several protocols.** A connection names the subprotocol it speaks; the comm
  core's is `orion.v1`. The listener hands each connection to whoever asked for its
  subprotocol, so another WebSocket protocol, as NT4 is, shares the port. A subprotocol nobody
  takes is `404`.
- **Who a browser is.** Nobody the comm core knows: anonymous. It may do what the policies let
  anyone do. With `ws.origins` set, a page from another origin is `403`, so that a page on
  another site cannot use a visitor's browser to reach the board. A connection that names no
  origin is not from a browser and is taken.
- **For a server with a port of its own.** `accept` upgrades a connection the caller
  accepted, with the server's order of subprotocols deciding; `connect_on` is the client's
  side, offering several. `WsRead::next` is a read that may be dropped half way and carries
  on, for a loop that also waits on timers and a queue; it holds the message itself, up to a
  length the caller gives. The NT4 crate (`crates/gateways/nt4`) is built on these.
- **A short queue in the kernel.** `ws.send_buffer` caps what the kernel holds for a
  connection. Left alone the kernel grows that to megabytes, which the board can no longer
  drop: `outbound.max_bytes` would only begin to count after seconds of video were already
  waiting. With the cap, a page on a slow link is behind by the cap and the outbound limit
  and no more.
- **Bounded.** The request must come within the hello time and fit 8 KiB. A message longer
  than the largest frame is refused by its length and not read. A client that does not mask,
  sends text where a frame belongs, or sends frames out of order is dropped.
- A message may come in pieces; pings are answered between them; a goodbye is answered.

## The NT4 gateway

Programs that speak NetworkTables 4 reach topics through `orion-nt4-gateway`, an NT4 server
inside the comm core (`crates/gateways/nt4-gateway`, on the NT4 crate `crates/gateways/nt4`).
It is off unless `nt4.listen` is set. `nt4.listen = ws` puts it on the WebSocket listener
(`ws.listen`) instead of a port of its own: connections that ask for an NT4 subprotocol go to
the gateway, the rest stay Orion's.

- **The mapping is configuration.** Each entry is `topic:field=type@name`: one field of one
  topic here, the NT4 type string that is announced, and the NT4 topic's name, which runs to
  the end of the entry. Nothing in the code knows what a name means.

  ```
  nt4.listen  = 0.0.0.0:5810
  nt4.publish = tank.level:litres=double@/Tank/level, drive.pose:pose=struct:Pose2d@/Drive/pose
  nt4.accept  = panel.setpoint:value=double@/Panel/setpoint
  ```

- **To NT4: latest value, at a period.** Every `nt4.period_ms` the gateway looks at each
  field in `nt4.publish` and, if it has a newer sample, sets the NT4 topic to it. NT4
  carries the latest value, so a sample replaced within a period is never sent. The gateway
  reads the ring like any other reader; a publisher pays nothing for it. The NT4 topic is
  announced with the field's first sample.
- **Types.** `Bool` is `boolean`; `Int` is `int` (or `double`, `float`); `Float` is `double`
  or `float`; `Text` is `string` or `json`; `Bytes` and `Structured` are `raw` and every NT4
  type that is bytes (`struct:…`, `proto:…`); `FloatArray` is `double[]` or `float[]`;
  `IntArray` is `int[]`. A field mapped to a type it is not is never announced, and that is
  logged once. `boolean[]` and `string[]` have no like here.
- **A value that expires is withdrawn.** For a topic declared with an expiry, the NT4 topic
  is deleted once its newest sample has stood longer than the expiry, and announced again
  with the next sample: NT4 clients do not go on reading a value nobody stands behind. Age
  is counted from when the gateway read the sample. A topic without an expiry keeps its
  last value in NT4.
- **From NT4.** For each topic named in `nt4.accept` the comm core declares a topic of its
  own, with one field for each mapped name, and publishes what NT4 clients write to those
  names. Programs here subscribe to it like any other topic. A value of another type than
  the mapping says, or longer than `nt4.max_payload`, is dropped and that is logged once.
  A topic cannot both go to NT4 and come from it, and an NT4 name is mapped once.

### As a client of a server somewhere else

Some machines run an NT4 server of their own that their programs talk to. A board beside one
joins it as one more client: `orion-nt4-client-gateway`, on when `nt4.server` is set.

```
nt4.server  = 10.0.0.2
nt4.name    = coprocessor
nt4.publish = tank.level:litres=double@/Tank/level
nt4.accept  = machine.state:enabled=boolean@/Machine/enabled
```

The mapping and its rules are the same. `nt4.publish` fields are published on the server;
`nt4.accept` names topics of the server, whoever writes them there, and their values are
published here. The client reconnects on its own: what the board publishes is sent again,
each topic's last value with it, and the accepted topics are subscribed to again. A value
that expires stops being published there until its next sample. `nt4.server` and
`nt4.listen` together are refused: a board is a server or a client of one.

A topic that comes from NT4 holds state, not a stream: a program that subscribes after the
value was published reads it with `latest`.

## Topics of other nodes

A program subscribes to `<node>/<topic>` exactly as to a topic of its own board: it is handed
a ring and reads it. The ring is a local copy, and `orion-topic-peers` fills it.

- **Only while someone asks.** When the first program here subscribes to a topic of another
  node, this comm core connects to that node's comm core and subscribes there, as any
  subscriber without a ring does. More programs asking for the same topic share that one
  subscription. When the last one leaves, it unsubscribes; when nothing of a node is asked
  for, the connection to it is closed.
- **A local copy of the ring.** The other node sends the topic's declaration, then each
  sample. The declaration makes a ring of the same shape here and the waiting subscribers
  are handed it. Each sample is written into it and sleeping subscribers are rung. From there
  on they are same-board subscribers, with history and "the value at or before".
- **Before the topic exists** a subscriber waits, as it would on its own board, and is handed
  the ring when someone on the other node declares the topic.
- **Sample times in this board's time.** A sample's time is on its publisher's clock. The link
  compares the two clocks with the request the time service answers, when it connects and
  every ten seconds, and adds this clock less that one to each sample's time. A difference
  smaller than half the round trip, or than a fifth of a millisecond, is not one the
  comparison can see, and times are left as they are: that is the case for boards kept
  together by time sync.
- **A link that breaks** tells the subscribers here that the publisher is gone and keeps
  their rings, then tries again every 200 ms for as long as someone asks. When it is back and
  the declaration is the same, they are told the topic is live again and go on in the ring
  they have. The same happens when only the publisher on the other node goes and comes back.
- **What is sent, and what is dropped for a slow link**, is the other node's outbound part's
  business. To it, this comm core is one more subscriber without a ring.
- **Where a node is** comes from `peer.nodes`, `name=address:port` for each; the key it must
  hold is the one it is pinned to in the trust store, if it is. A node nobody knows the place
  of leaves its subscribers waiting.

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
- An offer may keep reader slots back (`reader_slots`, a bit for each of the 64; zero for none
  kept): a publisher that serves readers of its own, outside Orion, keeps their slots. A
  subscriber already in a slot the offer keeps back is moved: the publisher is told the old slot
  is gone and given its marks under a new one. One with no slot left is warned about and gets no
  pool.
- A publisher may bring its own ring to a declaration: the broker checks it against the
  declaration and uses it, as it does for a publisher coming back after a restart.

The broker maps no buffer and sees no frame. A comm core that restarts interrupts no frames: the
ring, the pool and the marks belong to the publisher and its subscribers, who offer the same things
again when they reconnect.

## Keys, enrollment, operators

`crates/comm/keys` (`orion-keys`). Who is trusted.

| | What it is | Where it is kept |
| --- | --- | --- |
| The node's identity | An ed25519 key, made on the first start | `<keys.dir>/node-identity.ed25519`, 32 bytes, owner and group |
| Peers | Other nodes, each pinned to one key | `<keys.dir>/trust` |
| Operators | People's tools: a key, and what it may do | `<keys.dir>/trust` |
| Revoked keys | Never trusted again | `<keys.dir>/trust` |

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

## The SSH way in

`crates/comm/ssh` (`orion-ssh`). The comm core answers SSH itself, on `ssh.listen`; a person's
ordinary `ssh` connects to it. It is off unless `ssh.listen` or `ssh.interface` is set.
`ssh.interface` keeps it to one network interface whatever its address (`SO_BINDTODEVICE`), on
`ssh.listen`'s port (22 if not set); an interface that is not there yet, such as a USB gadget's
network that comes up after the comm core, is waited for, and SSH opens on it when it comes.
Recovery keeps to the same interface when the settings were read.

- **Logins go through the one check.** The client proves it holds a key. The trust store says
  whose key it is, and the check decides whether that operator may log in: an operator
  trusted with everything may. With security off anyone is let in, with a key or without, as
  everywhere else. There are no passwords and no `authorized_keys`: an operator's key is its
  SSH key, and the trust store is the one place that says who may do what.
- **A shell as the user who logged in.** The name the client gives is an account of the
  board's, read from `/etc/passwd` and `/etc/group`. Each session runs in a helper process:
  the comm core's own program started again with `ssh-session` (or `ssh.helper`). The helper
  takes the terminal as its own, becomes the account with all its groups, checks that it has
  and that it cannot go back, and then becomes the account's shell. A comm core that runs as
  root can let in any account; one that does not can only let in the account it runs as.
- **What a session is.** A login shell or a command, on a terminal if the client asks for
  one, with its window size, input, output, errors apart, exit code, and the signal that
  ended it as a shell would report it. Its environment is `HOME`, `USER`, `LOGNAME`, `SHELL`,
  `PATH`, `TERM` and `SSH_CONNECTION` and nothing the client sends. When the client goes,
  the session is ended.
- **sftp and scp.** `crates/comm/ssh-sftp` (`orion-ssh-sftp`) is the `sftp` subsystem, which
  both `sftp` and `scp` speak. It is a helper process too: the session helper becomes the
  user and then starts the comm core's program with `ssh-sftp`, which serves that one client
  on its input and output. Every file is opened, made, renamed and removed by a process that
  is the user, so what the user may not do the kernel refuses, and a fault ends one transfer.
  A board built without feature `ssh-sftp` refuses the subsystem.
- **Connections from the board.** `crates/comm/ssh-forward` (`orion-ssh-forward`). Someone
  who is logged in may ask for a connection from the board to a service the board reaches,
  on the board or on a network only it is on: `ssh -L` and `ssh -W`. The board connects,
  giving up after ten seconds, and carries the bytes on a channel until either end closes.
  Whoever may log in may reach what the board reaches, as from a shell. `ssh.forward = false`
  refuses them. Nothing listens on the board for anyone: there is no `-R`.
- **Read without rights.** `crates/comm/ssh-sandbox` (`orion-ssh-sandbox`), on unless
  `ssh.sandbox = false`. Each connection gets a child process, the comm core's program
  started with `ssh-connection`, and the child reads everything the client sends. Before it
  reads a byte it becomes `ssh.sandbox_user` (`nobody`) with no groups, checks it cannot go
  back, and gives up gaining rights by any means. It has no trust store. What needs rights
  it asks the comm core for on a socket: whether a key may log in, a session, a connection
  from the board. The comm core decides with the check, answers a session or a connection
  only after it has itself let that connection log in, lets a connection log in once, and
  holds it to the login time. A child that crashes or is taken over ends one connection:
  its sessions are ended with it and nothing else notices. The child does hold the host
  key; taking one over lets someone pretend to be the board to a client, not get in.
- **Small before the login.** One family of key exchange (`mlkem768x25519-sha256`,
  `curve25519-sha256`), two ciphers (`chacha20-poly1305`, `aes256-gcm`), public keys only.
  At most `ssh.max_unauthenticated` connections may be waiting to log in, and each has
  `ssh.login_ms`. A refusal takes half a second.
- **Always a key.** The host key is ed25519, in OpenSSH's format, at `ssh.host_key`
  (`<state.dir>/ssh_host_ed25519_key`), made on the first start. A file there that is not a
  key is moved aside (`.corrupt-<time>`) and another made, and that is logged: clients will
  see the key change. A key that cannot be written is used for the run. Nothing about the
  disk closes the way in.

## Recovery

`crates/comm/recovery` (`orion-recovery`). A board is repaired by logging in to it, so a comm
core that cannot start as it was told to does not end: it opens the recovery way in and stays
up (`run_or_recover`). That happens when the settings cannot be read, and when the comm core
cannot start as they say: a state directory that cannot be made or written to (tried before
anything is kept in it), or one on the root file system with `state.require_mount` on, a trust store
or identity that cannot be read, an address that cannot be listened on.

- **Nothing from the state directory.** Who may log in is `recovery.keys`
  (`/etc/orion/recovery_keys`): OpenSSH public keys, one a line, in the image. It is read
  again every couple of seconds, so a key written from the console works without a restart.
  A line that is not a plain ed25519 key lets nobody in. The trust store is not opened.
- **Nothing from a config that could not be read.** Then nothing in it is believed: security
  is on, the addresses are port 22 and then 2222, the keys file is the usual one. If the
  settings were read, their SSH address is tried first and their security switch holds: a
  board that says security is off is as open in recovery as it always is.
- **Never wider.** With security on and no recovery keys, nobody gets in, and that is logged.
  Settings that were read and name `ssh.interface` keep every address recovery tries to that
  interface.
- **Nothing written.** The host key is the board's if it can be read, and one for the run if
  not. A session needs no home directory, and an account whose shell is gone gets `/bin/sh`.
  If the account files themselves are gone and the comm core is root, `root` is still let in.
- **Never backed off.** If no address can be listened on, each is tried again every second,
  for ever, at the same pace.
- **To the service manager** recovery is a comm core that is up, with `recovery: <why>` as
  its status: it is not restarted into the same failure and it is not given up on.

sftp and scp work in recovery, so a log can be copied off a broken board and a fixed file
copied on.

## The helper programs

The comm core starts helper programs: a session that becomes the user (`ssh-session`), the
sftp subsystem (`ssh-sftp`), the child that reads an SSH connection (`ssh-connection`). Each
is the comm core's own program started again with that word. `orion_comm_app::helper` is where
those words are known: a binary that holds the comm core calls it first thing and exits with
what it returns, if it returns anything. `orion-comm-helper` is a program that is only that,
for images that keep the helpers apart (`ssh.helper`).

## One binary: `orion`

`crates/bins/orion` builds `orion`, the file a board installs. The comm core, node logic and
the command line are its modes, so the two services are two runs of one file and the board
holds one copy of the code they share.

```
orion comm [settings file]     the comm core
orion node [settings file]     node logic
orion [ctl] <concept> <verb>   the command line (docs/client.md)
```

- Both services read the same file (`/etc/orion/orion.conf` unless one is named) and the
  environment. A key that neither reads is logged by both; one that only the other reads is
  not.
- `orion node show`, `peers`, `time`, `sync` and `forgive` are the command line's. Anything that is not a mode
  is the command line too, so `orion topic read imu` needs no `ctl`.
- Linked as `orionctl`, `orion-comm` or `orion-node`, it is that mode.
- The comm core here is the whole one, recovery included, and the file is its helper programs.
  It serves serial and CAN links (`links.serial`, `links.can`) as well as its default features.
- Node logic needs to be let in: run it as root, or name its user in `socket.node_uids`.

## The comm core alone: `orion-recovery`

`crates/bins/orion-recovery` builds `orion-recovery`: the comm core as one file, for an
initramfs, a flasher image, or any board that needs only the way in. It is the comm core with
the network carrier, SSH with sftp, forwarding and the sandbox, and recovery; nothing of node
logic. Built for a board (`aarch64-unknown-linux-musl`, profile `service`) it is linked
statically: the image needs this file and a shell.

```
orion-recovery [settings file]
```

- Settings come from the file (`/etc/orion/orion.conf`) and the environment. With no file
  there it listens for SSH on port 22 and is otherwise a comm core with nothing set.
- It ends in recovery instead of ending, as above. `ORION_RECOVERY_KEYS` and
  `ORION_RECOVERY_LISTEN` name the keys file and the addresses for that, and hold even when
  the file is what is broken.
- It is its own helper programs.
- It logs to standard error, one line an event; `ORION_LOG` says how much (`info`).

**Size gate.** `scripts/size-gate.sh`, part of the gate, builds it as it ships and fails if
it has grown past a limit set a tenth above its last measured size: 2,634,088 bytes for a
board, static. A change that needs the bytes raises the limit in the same commit and says why.

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

## Time sync

The comm core can keep the board's clock right by itself (`orion-time`), so an image needs no NTP
daemon.

**Sources**, in rank order, in `time.servers`:

| Written | Is |
| --- | --- |
| `host[:port]` | an NTP server (SNTP, RFC 4330) |
| `peer://host[:port]` | another Orion node serving its clock |
| `tcp://host:port` | the same, asked over TCP, so time crosses TCP-only paths |
| `operator` | the clock of a connected operator who offers it |

**Modes**, in `time.sync`: `report` measures the offset and publishes it, never touching the
clock, so it is safe next to another daemon. `discipline` also adjusts the clock; it needs
`CAP_SYS_TIME`, and only reports while `systemd-timesyncd` runs.

**Rules.** Offsets up to 0.4 s are slewed through the kernel, which also marks the clock
synchronized. A larger offset is stepped, but only once two samples agree within 0.1 s. After the
first synchronization the clock is never stepped backwards. Answers that took over 1 s are not
used. Polling starts at 2 s, then runs from `time.poll_min_ms` (64 s) doubling to
`time.poll_max_ms` (1024 s) while the offset stays under 50 ms. The last synchronized time is
saved in the state directory; at start a clock behind it is stepped forward to it.

**Wrong sources** are passed over for the next in rank: any source whose time is before this
build existed, and a fleet peer over a day behind or over 400 days past the last synchronized
time.

**Serving.** `time.serve = addr:port` serves the clock as SNTP over UDP and TCP. The answer says
how good the clock is: its source's stratum plus one when this board synchronized it, stratum 3
when another daemon did, stratum 10 with `time.serve_authority`, and "unsynchronized" otherwise,
which clients ignore. Over a session anyone may send `TimeRequest` and gets the board's clock
back in `TimeReply`.

**An operator's clock.** The operator sends `TimeOffer` with its stratum. Offering the time is
the same power as setting it, so this needs the grant `clock.set`. The board answers `TimeTurn`
and from then on sends `TimeRequest` on that session when it polls; the operator answers
`TimeReply`. The board measures the round trip on its own monotonic clock. When the operator's
session ends, the next source in rank is used.

**State** is the topic `orion.time`, published when it changes: `mode`, `synced`, `source`,
`offset_ms`, `delay_ms`, `stratum`, `last_action`, `error`, and `monotonic_to_wall_ns`: added to
a sample time on this board, which is on `CLOCK_MONOTONIC`, it gives the wall time. It is
published again when it has moved by more than a millisecond. `TimeRequest` says which clock to
answer with: the wall clock for time sync, the monotonic one for a topic link. `time.synced_file`
(`/run/orion/time-synced`) exists while the board disciplines the clock and it is synchronized.

## Links

A link is one more carrier of the one wire (`orion-link-serial`). Frames are COBS-encoded and
end in `0x00`, so a receiver finds the next frame after any noise. What is on the other end says
hello like any client, with the role `device`, and the server knows it as the device named after
the link.

A serial link is written `name=path@baud` (`uart0=/dev/ttyAMA0@115200`): 8 data bits, no parity,
one stop bit, no flow control.

**A link never gives up.** A port that can't be opened, or that fails, is opened again 250 ms
later, for as long as the program runs. A link is a way into the machine, and ways in are not
backed off. Its state (running, or retrying and when) is reported as it changes.

**A silent link costs nothing.** The server gets a connection only when a whole valid frame has
arrived. Until then one task waits for the port to become readable: no session, no timer, no
allocation. A device that stops talking loses its session to the heartbeat deadline, not the
port.

**A link that crosses a network** (feature `sealed`). A person's computer with the device on one
of its ports can carry the port's bytes to the board over TCP. The handshake is signed by the
board's key and the operator's; the operator must be enrolled and granted the action
`link.attach`. After it, every byte is in a sealed record (ChaCha20-Poly1305, a key per
direction, counter nonces, key rotation every 2^20 records or 10 minutes), so nobody between the
two can read the device's traffic or put a frame into it. The board takes one such device at a
time; handshakes run side by side, so a caller that says nothing holds nobody up.

**A CAN bus** (`orion-link-can`) carries the same frames cut into segments, one per CAN frame: 7
bytes each on classic CAN, up to 63 on CAN FD. A bus has many devices. Each has an address, and
its two identifiers are the bus's base pair plus that address, so lower addresses win
arbitration. Each device is its own session, known as `<bus>.<address>`; one that stops talking
loses its session and the others are not touched. A whole frame that a session is not reading
is dropped and counted: one slow session does not hold the bus. The socket receives only the
identifiers of the bus's devices, and is opened again every 250 ms after it fails.

## Devices as resources

A device on a link declares topics with the broker and registers actions with node logic, as any
program does. What it cannot afford is to be a provider as well, so the device gateway
(`orion-device-gateway`, feature `devices`) is one for it. It wraps the broker and the relay and
notes what a session with the role `device` declared or registered once the service took it.
Then, from a session of its own in memory, started with the first device, it writes one resource per device: named after the
device's hello, of kind `device`, listing its topics and its actions (`<resource>.<action>`).
A client finds it, reads it and drives it like any other resource.

- **A device that goes is `Lost`, not gone.** Its record stays; when it comes back and declares
  again it is `Ready`, with what it declares now.
- A record is written when it changed, never on a timer. One node logic did not take (it is
  restarting) is written again a second later, until it is.
- Devices may register actions, answer runs and take files: the relay's routes for those admit
  devices as well as programs on the board.

## Discovery

A node can find the others of its cluster on the local network (`orion-discovery`, mDNS/DNS-SD
behind feature `mdns`). It announces the service `_orion._tcp` with this TXT record:

| Key | Value |
| --- | --- |
| `v` | TXT layout version, `2` |
| `id` | the node's name (the instance name is only a label) |
| `pk` | its ed25519 public key, 64 hex characters |
| `wv` | wire protocol version |
| `rev` | highest interface revision |
| `cl` | cluster name |
| `tcp` | where it takes `orion+tcp` connections |

Addresses come from the announcement; IPv4 is tried before IPv6, and link-local IPv6 addresses
are left out because they need a scope.

**Discovery only finds peers. It never makes one trusted.** Anyone on the network can announce
anything. Announcements from other clusters, malformed ones and the node's own are ignored. A
peer of this cluster is listed with one of these states, asked of the keys each time:

| State | Meaning |
| --- | --- |
| `discovered` | Seen. Not trusted, not connected to. |
| `enrolled` | The key it announces is the key held for its name. |
| `revoked` | Its key was revoked here. Never enrolled again by itself. |
| `key mismatch` | It announces another key than the one held for its name. Not followed. |
| `incompatible` | It speaks another wire version. |

A peer becomes enrolled when a person pins its key after comparing the fingerprint, or, when the
cluster has a shared enrollment key, when both nodes prove they hold it. With that key set, a
node enrolls with every discovered peer by itself over `orion+tcp`, requiring the peer to hold
the key it announced, and tries again from 5 s doubling to 5 minutes when it fails.

A peer stays listed until it says goodbye or its announcement is not refreshed within the
time-to-live (120 s). That only drops it from the list: its key stays held. The rest of the comm
core is told when an enrolled peer is present, has moved to other addresses, or has gone, which
is what keeping a connection to it needs.

**A USB link** (`orion-link-usb`) is the same byte stream over handles the comm core is given.
The comm core never opens hardware: the program that owns the board's hardware finds the device
or sets up the gadget function, opens it, and sends `LinkOffer` on the board's socket with the
open descriptors attached. Only a program on the board may. A handle is a stream (one descriptor
that can be waited on), a pair of blocking endpoints (a thread sits in each, so nothing is
polled), or, with feature `bulk`, a USB device with the interface and bulk endpoints to use. A
link with no handle opens nothing and waits. When a new handle is offered for a link that has
one, the device was unplugged and is back: the old stream ends and the link goes on over the new.

## Passing requests to node logic

Records, and later workloads, actions and files, are node logic's to answer, and node logic is
another process. There is no second protocol for it: `orion-relay` is a service like the
others.

- Node logic connects on the board's socket with the node role, which the socket offers to
  root and to `socket.node_uids`, and sends `NodeAttach` with the kinds it answers.
- A request of one of those kinds goes through the one check here, at the door, like every
  other. If it passes it is sent to node logic inside `NodeRequest`, which names the session,
  the request's `seq` and the caller. Node logic never sees a request that was refused.
- Node logic answers with `NodeAnswer`: a session, a kind and a payload. The relay sends that
  frame to that session as it is. A session that is gone is not node logic's failure.
- When a session ends that node logic was passed a request from, it is told with `NodeGone`.
- When node logic attaches after one before it, every session that one was passed a request
  from, and every session refused while it was away, is sent `NodeBack`: node logic knows
  nothing of it now. The client writes the program's resources again on it.
- With no node logic attached, or for a kind it did not name, the request is refused with
  `unavailable`. Nothing else in the comm core notices that node logic is away.
- A request that stands, a watch or a registered action, is kept and passed again to the node
  logic that comes back. So a program that was watching goes on being told, and one that
  added an action still has it, without asking again. A watcher may be told a version it
  already has: the same record again. A session may have 1024 standing; one more is refused
  as `busy`. One that node logic refuses does not stand.
- When node logic dies with requests it was passed and had not answered, a read, a write, a
  run under way, each caller is refused with `unavailable` then, and does not wait for ever.
- One node logic at a time: when another attaches, the one before is closed.

Which kind asks for what, and who may ask, is a table of routes in the relay.

| Kinds | Who may ask |
| --- | --- |
| `Put`, `Get`, `Watch` | programs on the board, operators with the grant, peers |
| `FileBegin`, `FileChunk`, `FileCommit`, `FilePull` | programs on the board, operators with the grant, peers |
| `FileSink`, `ExecutorOffer` | programs on the board. It stands |
| `FileStatus` | programs on the board; node logic takes it only from the receiver of that transfer |
| `WorkloadPut`, `WorkloadRemove` | programs on the board, operators with the grant, peers |
| `ExecutorReport`, `ExecutorInventory` | programs on the board; node logic takes them only from an executor |
| `Register` (an action) | programs on the board |
| `Run` (an action) | whoever the action was registered for; for one not registered here, programs on the board, operators with the grant, peers |
| `Progress`, `ActionResult` | programs on the board; node logic takes them only from the program that has the run |
| `Cancel`, `Attach` | programs on the board, operators, peers; node logic takes them only from the run's caller, or whoever started it |

**Actions and the one check.** The list of actions is node logic's, but who may run one is
decided here. A `Register` stands, so the relay holds it, and it carries the action's policy.
A `Run` names an action and a target; the relay finds the standing `Register` with the same
name and target and gives its policy to the check. So node logic is only ever handed a run of
one of this node's actions that was allowed, and registering an action without a policy is not
something the wire can say. With no such registration the action may be another node's: the
run is let through for anyone this node knows, and node logic carries it to the node that has
the action, where that node's door decides by the policy it was registered with, or answers
`not found`.

## The comm core as a program

`orion-comm-app` puts the parts together. It needs nothing else on the board: no node logic, no
other program. Its settings, with the file and the environment they come from, are in
[`docs/settings.md`](settings.md).

Under systemd (`Type=notify`) the comm core sends `READY=1` once every way in is up, with a
status line saying what it serves, `WATCHDOG=1` every half of the watchdog timeout, and
`STOPPING=1` when asked to stop. The "alive" message is sent from the comm core's own thread:
one that no longer turns stops saying so, and is restarted.

A part that cannot start stops the start, and the error names the part: the socket another
comm core holds, an address that is taken, a bus whose identifiers cannot work. A link with
nothing on it is not such a failure: it starts, keeps trying, and says so in
`orion.state.link.<name>`.

