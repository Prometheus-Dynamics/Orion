# Node logic

The half of Orion that decides: what should exist, what should run, who may do what. It is the
comm core's first client and connects to it like any other program. If it dies, links, topics
and sessions stay up, and it comes back as a client.

## The record store

`orion-store`. A record is a named piece of state: a class, a key, and bytes the store keeps
without reading. Every node keeps a full copy of the cluster's **desired** records, and they
converge without a leader.

Each version of a record carries a stamp from the writing node's hybrid logical clock:
`(physical milliseconds, a counter, a tag of the node's name)`, compared in that order.

| What happens | The rule |
| --- | --- |
| A local write | Stamped by this node's clock, which is always past every stamp the node has issued or accepted, even if the wall clock steps back |
| A version from a peer | Replaces the one here if its stamp is greater |
| Equal stamps | A delete beats a write, then the larger value wins. Only a restored backup or two names with one tag can cause it, so the rule only has to be the same everywhere |
| A delete | Is a version: a tombstone, so a node that missed it cannot bring the record back |
| An old tombstone | Collected after 7 days. A delete older than that with nothing here to delete is ignored |
| A stamp over 5 minutes ahead of this node's clock | Refused. One wrong clock must not write versions nobody can override |
| A restart | The clock starts past every stamp in what was kept |

The merge is a join over a total order, so the result is the same whatever order versions
arrive in and however often they are sent again.

**Observed** records are what a node sees right now. Each node owns its own slice and sends the
whole of it; a peer's slice replaces its last, nothing is merged across nodes, and a slice goes
when its node does.

The store is plain data with no lock and no I/O. Node logic owns it from one task.

## Keeping records across a restart

`orion-persist`. The desired records are kept in one snapshot file in the state directory,
`records.v1.snap`, with the snapshot before it as `records.v1.snap.prev`.

**A save replaces the snapshot in one step.** The new one is written beside the old and synced,
and only then renamed into place, the old becoming `.prev`; then the directory is synced. Power
can go at any moment: what is found afterwards is the old snapshot whole or the new one whole.
A save that reported success is what is found.

**A file that cannot be read never stops start-up.** A snapshot that is cut short, fails its
checksum or is not a snapshot is renamed out of the way (`…unreadable-N`, kept for whoever
wants to look) and the one before it is used. If that is no good either, the node starts with
nothing and its peers send it what it is missing.

Each record in the file is a wire record with its length in front, so the wire's append-only
rule holds here too. A snapshot of another format has another name and is neither read nor
touched. Observed records are not kept: a node that has just started sees nothing yet.

## Node logic as a program

`orion-node-app` is node logic assembled. It is the comm core's first client: it connects to
the board's socket with the node role, says which kinds it answers, and from then on is passed
those requests by the comm core, already checked (see "Passing requests to node logic" in
`docs/comm.md`). It answers through the comm core. It has no socket of its own and no door.
Being a client like any other, it publishes what it has to show as topics.

Its settings are in [`docs/settings.md`](settings.md).


- **Records.** `Put` writes each entry with a stamp from this node's clock (an entry with
  `deleted` set deletes, and is skipped if there is nothing to delete) and is answered with
  the records as stored. `Get` is answered with the live records under the prefix, in as many
  messages as it takes, about 24 KiB each. `Watch` is sent what is newer than `after` and then
  every change; a second watch of the same class and prefix from a session replaces the first.
- **Saving.** A change is saved `records.save_after_ms` later, with whatever else changed
  meanwhile. The write is done by a helper thread that exists only while it writes; node logic
  goes on answering. Asked to stop, it saves what it has not yet and returns.
- **When it dies** nothing else does: the comm core refuses record requests with `unavailable`
  and serves everything else. Started again, node logic loads the snapshot, so stamps go on
  from where they were, connects, and is passed the watches programs still have.
- **When the comm core goes** node logic keeps its store and tries the socket again, 20 ms
  apart at first and at most a second. Watches are forgotten: they belonged to sessions that
  ended with the comm core, and their programs ask again when they reconnect.

- **Resources.** A write of a resource or a lease is held to the rules of resources (below)
  before the store takes it, and a write is all or nothing: one entry refused refuses them
  all.
- **Files.** Node logic takes the files sent to this node (below).
- **Actions.** Node logic keeps the open list (below) and publishes what happens to runs on
  the topic `orion.actions`.

One thread, with the store and the lists as plain data owned by it.

## The workload tracker

`orion-workloads` holds what should be running and where each run is. It is plain data with no
clock, socket or executor: it is told what happened and when, and hands back commands for
executors (`Start`, `Stop`, `Kill`) and changes of state for whoever is told.

| State | Meaning |
| --- | --- |
| Pending | should run and nothing runs it: no executor offers its kind, or it ended well and waits to be started again |
| Starting | an executor was told to start it |
| Running | the executor says it runs |
| Stopping | the executor was asked to stop it, or says it is stopping |
| Gone | it ended and is not started again; the record stays until it is removed |
| Failed | it failed; with a restart due it is started again then, otherwise it stays |
| Lost | its executor went away, so nobody knows |

- **Stop, then kill.** A run that was asked to stop and has not after the grace time
  (10 s) is told to end, once. If it survives that too it stays `Stopping`, with its record,
  for as long as its executor does not report it gone. Orion does not pretend.
- **Force remove says what it left behind.** The last status of a workload removed by force
  says whether a run was left, in which state, and whether there was an executor to tell to
  end it; the audit log has the same.
- **A record stays until its run is reported gone.** Removing a workload that runs asks for a
  stop and keeps the record, Stopping, however long that takes. If its executor is away the
  record waits, Lost, for the executor to come back and say. Removing with force drops the
  record at once and has the run killed; nothing else drops a record early. A workload with no
  run is dropped as soon as it is removed.
- **Restart.** A run that ends is started again by its policy: always, only if it failed, or
  never. The wait is 1 s, then doubles each time to 60 s, and it never gives up unless
  `max_attempts` is set. A run that stayed up 10 s starts the count again. A clean end under
  "always" waits too, so a program that exits at once does not spin.
- **Executors.** A workload is started when an executor for its kind is here. When one comes,
  its inventory is taken first: a run it has is adopted by name and not started twice; a run
  Orion thought it had and it does not is started again at once, whatever the policy, and
  that is not counted as a failure; a run it has that Orion does not want is returned to be
  stopped. When an executor goes, its runs are Lost. A wait for a restart is not cut short by
  an executor coming.
- **Put again.** The same content changes nothing, also for a run that ended: to run it again,
  remove it and put it. Other content stops the run and starts it with the new when it is
  gone. A put for a workload that is being removed keeps it: it is started again when the old
  run is gone.

## Peer sync

`orion-sync` carries desired records between nodes until they agree, and tells each node what
the others see. There is no leader. Any two nodes that can talk exchange what the other lacks,
and the store's merge gives the same result whatever arrives, in whatever order, however often.
Like the store it is plain data: it is handed messages and a place to put its own.

Two kinds, both already on the wire:

| Message | Meaning |
| --- | --- |
| `PeerSync` with records | data: versions to merge. Asks for nothing |
| `PeerSync` with no records | a summary: the sender's newest stamp and a digest of all it has. Data is always followed by one |
| `PeerObserved` | the sender's whole observed slice, replacing the one before |

What a node does with a peer's summary:

1. The digests are equal: they agree. It answers with its own summary if the peer has not
   heard this digest from it.
2. It has versions newer than the peer's newest stamp, which the peer cannot have: it sends
   them.
3. The peer's newest stamp is newer than its own: it sends its summary, once for this pair of
   digests, and the peer answers by rule 2.
4. Otherwise the difference is in the past, as after a partition: it sends everything, once
   for this pair of digests and at most every `full_interval_ms` (10 s). Held back, it is
   sent when due if they still differ.

- **Event-driven.** Nothing is sent on a timer. A change is sent to every connected peer as it
  is made. A version taken from one peer is passed on to the others and not back, so a node
  between two that never meet carries everything across.
- **At rest nothing is said.** Two nodes that reconnect unchanged exchange a summary each and
  what they observe.
- **Observed.** A node tells each peer its whole observed slice when they connect and whenever
  it changes. Slices are not passed on: each is told by its own node. When the connection
  goes, the peer's slice is forgotten, since nobody knows what it sees now. A slice is one
  message; one that does not fit is not sent, and that is logged.
- **Clock skew.** A version stamped further ahead than the store allows is refused and counted.
  The two then differ until time catches up; everything is sent once for it and not again.
- **Stale peers.** A peer last agreed with longer ago than deletes are remembered (seven days)
  would bring back what was deleted while it was away. It is not synced with in either
  direction, and shows as `Stale`, until it is forgiven: `PeerForgive`, from a program on the
  board or an operator with write access to records, or `orion node forgive PEER`. Each node
  refuses the other, so both have to forgive. `PeerStatus` (`orion node sync`) says where
  sync is with each peer and when the two last agreed. Two nodes that stayed
  connected and in agreement are never stale. When peers last agreed is given back by
  `agreed_at` to be kept across a restart.
- **Whose message.** The connection says who the peer is. A message that names another node is
  refused.

### On connections

- **Two ways to a peer.** Node logic keeps a session to every other node its node records give
  an address for (`orion-peer-links`), made as this node with its key, and only to a node
  that proves it holds the key the trust file pins for it (`orion-dial`, as for a forwarded
  run). A peer that connected here is heard through the comm core, which passes `PeerSync`
  and `PeerObserved` on from peers and from nobody else. Frames for a peer go on this node's
  own session to it when that is up, and otherwise back on the session the peer made: one of
  the two being able to reach the other is enough.
- **Up and down.** A peer is up while either way is. When neither is, its observed slice is
  forgotten. When the other node's node logic was away and is back, it is told where this
  node is again.
- **A helper a node.** Each session is held by a thread that wakes node logic through a socket
  pair when something came, and otherwise sleeps a second at a time for the heartbeat. A
  node it cannot reach is tried again a second and then up to thirty apart.
- **Kept.** When each peer was last agreed with is written to `sync-agreed` in the state
  directory when it changes, and read back at start.
- **Who to sync with** is the node records: put one for a node, with its address, on any node,
  and every node that hears of it connects. A single board has none and starts no thread.

## The open list of actions

`orion-action-list` is the list of actions and the runs under way. Orion ships almost no
actions; any program on the board adds its own.

- **Adding.** `Register` names the action, what it runs against (this node, or a resource) and
  who may run it. It is answered with `Registered`. A name is taken once for a target; the
  same name for another target is another action. The same registration again from the same
  program is the same action. An action for another node cannot be added here.
- **Running.** `Run` names an action and a target. The program that added it is handed a
  `Claim`. A run has two numbers: the caller's `call`, unique on its session, and the list's
  own, which is the one in `Claim`. Each side only ever sees its own. `Claim.caller` names who
  asked, as the audit log does. `Progress` (which may be
  dropped if the caller is not reading) and `ActionResult` from the program go to the caller
  under the caller's number, its `code` (the action's own status code, appended) as the program
  gave it. `Cancel` from the caller reaches the program; the run ends when
  the program says how.
- **Who may run it** is not decided here. The policy the action was registered with is what
  the check is told at the door, so only a run that passed arrives.
- **When an end goes away.** A caller that goes has its run cancelled, unless the run is
  detached: that one carries on. A program that goes takes its actions off the list and its
  runs end as `Failed`.
- **A caller's session ends.** `Claim` names the session that asked by a token that is never
  reused while node logic runs, so two sessions of one identity are told apart. When that
  session ends, for any cause, each program that was given a run from it is sent
  `ACTION_CALLER_GONE` with the token, once, whether its runs are still going or not. A detached
  run goes on until its deadline, or until a later session of the same identity attaches and
  cancels it. Orion says only that the session ended: what a program holds for that caller is
  its own business. A run carried here by another node names, in `on_behalf`, that node's
  token for the session that asked it, so each session behind one peer link is a caller of its
  own here with a token of its own. When that session ends there, the node that carried the
  run sends `ACTION_CALLER_GONE` with its token, and the program here is told as above, while
  the link between the nodes lives on; when the link ends, every caller behind it does. Only a
  peer's `on_behalf` is read, and only a peer may say a caller ended. Each node passes an end
  on to where it carried runs, so a run carried over several nodes is told at the end of them.
- **Detached runs.** A run started detached carries on when its caller's session ends, with
  its deadline and under its program as before. It must have a deadline, so that it ends
  even if nobody comes back: without one it is refused as `a detached run needs a deadline`,
  and one longer than `actions.max_detached_ms` (an hour unless said) as `deadline is longer
  than allowed`. Whoever started it comes back with `Attach`, from any session: it is sent
  the run's handle (`ACTION_STARTED`), the latest progress at once and then what follows. How
  a detached run ended is kept for ten minutes (256 runs at most, the oldest goes first) and
  given to its starter each time it asks, the same each time: coming back does not use it up.
  That is so even if a caller seemed to be attached when it ended, because a session can end a
  moment before node logic hears of it. Once it is no longer kept, coming back is refused
  `not found` with words that begin `expired:`, and a run that never was is `not found` with no
  such words. The last 1,024 runs that expired are remembered for that. "Whoever
  started it" is the same user on the board, the same operator key or the same peer, as the
  comm core says; for anyone else the run is `not found`.
- **Run handles.** Every run started here is told to its caller with `ACTION_STARTED`: its
  number, its handle and its action. The handle is `instance`, drawn at random each time node
  logic starts, and `run`, node logic's own number for the run. Progress and results carry it
  too. It is kept as 16 bytes (each half little-endian) or written `<16 hex>-<16 hex>`. A
  caller's numbers start over with each of its sessions, so after a session is lost a number
  can name another run; a handle never does.
  - `Attach` and `Cancel` that name a handle reach that run alone, for its starter. A handle
    from before node logic last started is refused as `run gone` (23); a session below level 3
    is told `not found`.
  - A number alone, as an older client sends it: a cancel reaches only a run this same session
    started under it. An attach reaches the starter's run under that number that no later run
    took. Any run started under a number, detached or not, takes it: the starter's earlier runs
    under it, going or kept, are reached by their handles only, and an attach by the number
    alone is refused saying so. A number alone never gives back an older run's answer.
  - A run carried to another node has no handle here: its caller follows and cancels it by
    number.
  - If a session is lost between starting a run and hearing its handle, how the run went is
    not known. Find out from what the action does (read what it changes), never by coming back
    to the number alone, which may name another run by then.
- **Deadlines, and stopping.** A run with a deadline that passes, and every run when node logic
  stops, is told to stop, and its program has `actions.confirm_ms` (10 s) to say how it ended.
  What it says ends the run; a run it stopped at its deadline ends as `TimedOut`, with code 110
  (`DEADLINE_CODE`), and one it had finished ends as it finished. With no word by then the run
  ends `Indeterminate`, code 0: node logic never says a run was cancelled or timed out when the
  program may have finished it. While stopping, no new run is taken (`unavailable`), and node
  logic stops once every run has ended, by the bound at the latest. What a program says after
  its run ended is ignored. Node logic's other ends (caller or program gone) have code 0.
- **A program is told a run is detached.** The run handed to it carries `detached`, so it
  knows that the caller's session ending does not end the run.
- **Only the program that has a run speaks for it.** A result from anyone else is `denied`.
- **Runs as a topic.** `orion.actions` has two fields. `event` is a line for each thing that
  happens to a run: `<run> <action> <node|resource>:<target> <percent> <outcome or -> <message>`,
  with 255 for a percent that is not known. `running` is how many runs are under way.
- There is room for 4096 runs at once; one more is refused as `busy`.

A run for another node, or for a resource another node has, is carried there (below).

## Actions on other nodes

An action lives where the program that registered it is connected. `orion-action-forward`
carries a run to the node that has it.

- **Where to.** A run that names another node goes to that node. A run that names a resource
  goes to the node in the resource's record. The node is reached at the address in its node
  record. With no such record the action is `not found`.
- **How.** Node logic connects to the other node's comm core over the network, with this
  node's key, and runs the action the way any caller would. No kind was added for it.
- **Only to the node pinned.** The key the trust file pins for the other node (`peer NAME KEY`,
  in `keys.dir`) is asked for when connecting, and a node that does not prove it holds that
  key is refused, `denied`. With security on, a node the trust file does not name is `denied`
  too. The file is read on each connect, so a peer trusted or revoked since is seen.
- **The check runs where the action runs.** This node's door lets a run of an action it does
  not have through to node logic for anyone it knows; it has no policy for it. The other
  node's door sees a peer and decides by the policy the action was registered with there. An
  action that is to be run from other nodes admits peers. A refusal there, `denied` or
  `not found`, is the caller's answer here.
- **To the caller it is one run.** Progress and the result come back under its own number.
  Cancel reaches the program. A caller that goes has the run cancelled there, unless it was
  detached.
- **When the other node goes** with a run under way, the run ends as `Failed`, "the node that
  runs it went away". While it cannot be reached a run is refused as `unavailable` within
  three seconds. When it is back, runs go there again with nothing to be done here.
- **A helper a node.** Connecting and waiting on a network session would hold node logic up,
  so each other node has a thread, started with the first run for it. Its session is dropped
  after 30 seconds with nothing running. At most 1024 runs are carried at once.

This node does not check the other node's key against its trust store, which is the comm
core's; the session is signed with this node's key and the other node checks that.


## Files a node receives

`orion-file-store` takes files sent to the node, from any connection the check lets write
files.

- **In chunks.** `FileBegin` names the transfer, the destination, the size and the SHA-256
  digest. `FileChunk` carries bytes at an offset. Each is answered with `FileStatus` saying how
  many bytes are here in order; that answer is what paces the sender. Only the next bytes are
  taken: a chunk ahead of or behind where the file is changes nothing.
- **Resumable.** Bytes are appended to a part file in `.incoming` under `files.dir`, with a
  note of what the transfer is. A sender that was interrupted begins again with the same
  number, destination, size and digest, from any session, and is told where to go on. That
  holds across a restart of node logic and of the board. The same number for another file
  starts over.
- **Checked.** `FileCommit` with every byte here is answered with `Verifying`. The file is read
  back and hashed; if the digest matches it is synced and renamed to its destination, and the
  committer is sent `Committed`. If not, the part is deleted and the answer is `Rejected`.
  Nothing is ever at a destination that did not match its digest, and what was there before
  stays until the rename.
- **A helper on demand.** Appending a chunk is done at once. Hashing may take seconds, so each
  commit has a thread of its own that ends with it; node logic goes on answering.
- **Where.** A destination is a path under `files.dir`, never with `..`. One that a link on the
  way would take out of the store, one that is a directory, and the store's own `.incoming`
  are rejected.
- **What is in place is known.** The store notes the digest and size of each file it puts in
  place, in `.placed` beside the files, so it can say whether a destination holds a given
  file without reading it, also after a restart. A file that got to a destination another
  way is not known to be anything.
- At most 64 transfers are under way at once; one more is `busy`. A file larger than
  `files.max_size` is rejected at `FileBegin`.

A file can also be fetched by the node itself (below).

## Files that go straight to a receiver

Some files are not the node's to keep. An image for a boot slot is written to the slot by the
program that manages the board, and a small board cannot hold it once in the node's store and
again in the slot. `orion-files-sink` is how such a program takes them.

- **Asking.** A program on the board sends `FileSink` with a prefix and is answered
  `FileSinkReady`. It is now the receiver of every file whose destination starts with that
  prefix. A prefix has one receiver; of two prefixes a destination starts with, the longer
  one's receiver has it. The request stands: the program is still the receiver after node
  logic restarts.
- **Passing.** A `FileBegin` for such a destination, each `FileChunk` and the `FileCommit`
  are sent on to the receiver as they came, and the receiver's `FileStatus` answers are sent
  back to the sender as they came. The receiver is the receiver in every sense: it says how
  many bytes it has, which paces the sender and is where a resumed push goes on from; it
  checks the digest; it says committed or rejected.
- **Nothing is kept.** The node stores none of the file and holds a chunk only for the call
  that brought it. Memory does not grow with the file.
- **Within the receiver's outbox.** What has been sent to a receiver and not yet answered,
  across every transfer to it, stays within its session's outbox in the comm core, less one
  chunk's room. Node logic takes that figure from `comm.outbox_bytes`, and never above its
  default, in case only one of the two restarted since it changed. A chunk past it is
  refused to its sender as `busy`, and so is a chunk after one that was turned away, so what
  the receiver is sent stays in order. A sender that goes back sends again chunks that are on
  their way already; those are not sent twice.
- **A sender that comes back** in a new session and begins the same transfer again is the one
  told from then on.
- **A receiver that goes** takes its prefixes with it. Whoever was sending to it is told the
  transfer is rejected, "the program that receives it went away". The destination is then a
  place in the node's own store again, unless the image says otherwise:
- **Prefixes the image says are receivers'** (`files.receivers = slots/, devices/`) are never
  the store's. A file begun under one while no receiver has asked for it, because the receiver
  is still starting or has gone, is refused `unavailable`, in words: `nothing receives
  slots/b.img yet: slots/ is a receiver's`. A chunk or commit still on its way to a receiver
  that went is refused the same way: `the program that received it went away: begin it again`.
  The sender asks again, and the file goes to the receiver when it is there. Without the
  setting, a file that arrives before its receiver is stored by the node and answered as
  committed, and the receiver never sees it. A prefix is one or more parts of a name ending in
  `/`; anything else in the setting is an error at start.
- At most 64 prefixes and 256 transfers passing through at once; one more is `busy`.
- **A receiver with little room**, a microcontroller on a serial line, names the largest
  chunk it takes in `FileSink` (`max_chunk`; 0 for any). It is then sent one frame at a
  time: each waits here until the one before is answered, and a chunk larger than it takes
  is cut into pieces that wait their turn. What waits is what the sender has in flight, at
  most 1 MiB of a transfer; more is `busy`. A sender that begins again, having heard nothing,
  is passed on at once and what waited is dropped: the receiver says where to go on from.

## Sending a kept file to a receiver: `files.send`

Node logic has actions of its own: `files.send`, and the actions on the image's roots (next
section). `files.send` on the node, with `FileSend { file, to }`
as its arguments, sends the file the node keeps at `file` to the destination `to`, which a
receiver asked for. It is how an image put on the board once reaches a device's firmware
slot, and as many devices as it is run for.

- **As a client.** Node logic registers the action like any program, so the relay checks
  each run against its policy at the door: programs on the board, and operators with a
  grant. It is handed the claim, and sends the file's `FileBegin`, chunks and `FileCommit`
  through the receivers' path above, as any sender's. Its own chunks are no larger than the
  receiver takes, and it sends the next once the last is answered, so nothing waits.
- **Ends** when the receiver says committed (succeeded) or rejected (failed, with the
  receiver's reason). Rejected at once: a file the node does not keep, a destination no
  receiver asked for, a file already being sent there.
- **Resumes.** The transfer's number comes from the digest and the destination, as a client's
  push numbers it, so a run tried again, or a push of the same file, goes on where the
  receiver is. Sent again once it is there, the receiver says so and the run succeeds.
- **Retries.** With no answer for 2 seconds the transfer is begun again, and the receiver says
  where it is. After 5 such tries the run fails, "the receiver stopped answering".
- **Cancelled** runs stop sending; the receiver keeps what it has, to resume.

## The image's roots: `roots/<name>/`

The image lets clients reach directories of its choosing, its roots, and nothing else. Each is
one line of the settings, `files.root.<name> = <directory> rw` (or `ro`); the environment's
`ORION_FILES_ROOT_<NAME>` says the same. A name is 1 to 32 of `a-z`, `0-9`, `-` and `_`. A file
in a root is `roots/<name>/<path>`, in the namespace that push and fetch already use, and is
reached the same way with security on or off: the switch decides who, the roots decide where.
`orion-file-roots`.

- **Where.** Every open, listing, removal and rename goes through `openat2` with
  `RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS` from the root's directory, opened again for each.
  A `..`, an absolute link or a magic link that leads out is refused by the kernel: "outside
  the root". A link that stays inside is followed. The root's directory itself is the image's
  to make, with the ownership it wants: readable by node logic's account, and writable for
  `rw`.
- **Whose what Orion makes is.** A directory Orion makes (`files.mkdir`, or on the way to a
  pushed file) is made as a file is: with every permission the process's umask leaves. So one
  rule, the unit's umask, decides both. In a root an application shares with Orion by group
  (the root setgid and `2770`, the unit's `UMask=0007`), a pushed file is `0660` and a
  directory `2770`, the group's: the application can make files in a directory Orion made, and
  Orion in one the application made the same way.
- **Fetch.** Node logic offers `roots/` as a program offers a prefix (`FileOffer`), and gives
  each file as a program does. A helper thread reads the file once for its digest; then up to
  8 chunks of up to 32 KiB are on their way, paced by the client's `FileTaken`. A slow client,
  or a node logic slow to read, holds the file back, and nothing is dropped. A file that grows
  while it is given is given as it was when it was read for its digest; one that shrinks is
  aborted, saying so. At most 64 files are given at once. A client that owes an answer and
  says nothing for a minute is let go. A fetch goes on from what the client has (`have`).
- **The index.** `roots/<name>/index` is the root's index, in the format of
  `orion_files::index`: every file under the root, its size as its bound, each `user-data`.
  It is in the order the directories are read, a directory's own files before those under it.
  Links, pipes and devices are not in it, nor the root's `.orion`. Files whose names Orion
  does not carry (below) and files more than 32 directories down are left out, and node logic
  warns how many. A root of more than 65,536 files is refused an index: list it instead. A
  file at the top of a root named `index` is the index's name, so it cannot be fetched.
  A file that grows after the index was made is larger than its bound: a fetch held to the
  bound refuses it, and the index fetched again has the new size.
- **Push.** Node logic receives `roots/<name>/<path>` itself, ahead of any program's receiver
  and of the node's own store. The bytes go to a part file in the root's own `.orion`
  directory, on the root's file system, and only when every byte is there and the digest
  matches is it renamed into place, with the directories on the way made beneath the root.
  A push that breaks off never replaces the file it was for. Its part survives node logic
  restarting, and pushing the same file again goes on from it. A root marked `ro` refuses
  every push, whoever asks: "the root is read-only".
- **The actions** (their arguments are text, words split by spaces; what `files.list` gives is
  text too):

  | Action | Arguments | Does |
  |---|---|---|
  | `files.list` | none | the roots: `root rw <name>` a line |
  | `files.list` | `roots/<root>/<dir> [<after>]` | up to 128 entries of `<dir>` in at most 16 KB, by name, after `<after>` if given: `<kind> <size> <mtime> <name>` a line, `kind` one of `file`, `dir`, `link`, `other`, the size of a file only, `mtime` in seconds since 1970, `name` as it is carried (escaped) and, when that is not how it shows, the rest of the line what it shows as. A line begins `long` for an entry whose name, with its directory's, is over 255 bytes carried. Then `more` if there are more |
  | `files.remove` | `roots/<root>/<path>` | removes a file, a link (not what it leads to) or an empty directory |
  | `files.mkdir` | `roots/<root>/<path>` | makes one directory, in one that is there |
  | `files.rename` | `roots/<root>/<from> roots/<root>/<to> [replace]` | renames within one root; what is at `<to>` is replaced only with `replace` |

  A run that cannot be done ends `Rejected` (its arguments, a read-only root, another root)
  or `Failed` (what the file system said), with the words in its detail.
- **Grants.** A fetch needs `read` (`What::Read("files")`); a push needs everything
  (`What::Write("files")`); each action needs its own grant (`files.list`, or `files.*` for
  all four). The actions admit programs on the board and operators. With security off,
  everyone may do everything. A run on a root is audited with its arguments.
- **Names.** Any file's name is carried, escaped: letters, digits, `. _ - : /` and text
  outside ASCII as they are, and `%XX` (its byte, in uppercase hex) for anything else: a
  space, a quote, `%` itself, a control, a byte that is not UTF-8. `My Report.txt` is
  `My%20Report.txt`, `Отчёт.txt` is itself, `100%` is `100%25`. Only that one form names a
  file: a name escaped where it need not be, or in lowercase hex, is `not a name Orion
  carries`. The limit is 255 bytes of the escaped name in all (`roots/<root>/` counts), with
  no empty part, `.` or `..`. An entry over it is listed as `long` and cannot be fetched,
  pushed or changed through Orion: over SSH or SFTP it can be reached, or renamed shorter. The
  root's own `.orion` is never listed, and no name may be under it.

**The words**, as a client is told them: `no such root: <name>`, `the root is read-only`,
`not found`, `outside the root`, `not a file`, `not a directory`, `there already`, `the
directory is not empty`, `the file's permissions do not allow it`, `the root's file system is
read-only`, `the root's file system is full`, `.orion is the root's own`, `<name> is not a name
Orion carries`, `a root is not a file: its index is roots/<name>/index`, `a root is not changed:
only what is in it`, `a file is renamed only within its root`, `busy: 64 files are being given`.
A push placed through a link that leads out is `the destination is outside the store`.

**A restore that breaks off.** A whole root is restored by pushing each file and then removing
what the backup does not have (comparing `files.list` with it). Each file is whole: the old one
or the new one, never a mix. A restore broken off halfway leaves the root partly restored: some
files new, the rest old, files the backup does not have not yet removed, and the push under way
as a part in `.orion`, out of sight. Nothing on the board marks the root as being restored.
The client that restores knows how far it got, and says so ("<root>: 140 of 312 files restored;
restore again to finish"); restoring again pushes every file again (a file already there is
sent again whole) and resumes the one that was under way. A staged restore that makes the root
change all at once is proposed, not built, in `docs/prs/I76.md`.

## The audit log

`orion-audit` writes who did what. Node logic records every request it is passed that changes
something, and so passed the check: a record written or deleted, an action registered, run or
cancelled, a file begun or committed. Reads, chunks and words of progress are not recorded.

A line is one JSON object:

```json
{"time_ms":1710000000000,"node":"left","who":"operator:9f3c…","did":"run","what":"reboot node:left","result":"ok"}
```

`who` is `local:<uid>`, `operator:<key>`, `peer:<node>`, `device:<name>` or `anonymous`, as the
comm core named the caller. `result` is `ok`, or why node logic refused.

- **Capped.** When the next line would take the file past `audit.max_bytes`, the file becomes
  `audit.log.1`, `.1` becomes `.2`, up to `audit.keep`, and the oldest is deleted. A file is
  rotated between lines, never in one.
- **Its own helper.** Recording formats the line and puts it in a queue of 1024. It never
  touches the disk and never waits. One thread writes, everything that is waiting in one
  write. If more is asked than the disk takes, entries are dropped and counted, and the log
  says so itself in a line `"who":"orion","did":"dropped"` when there is room again.
- What is waiting is written when node logic stops.

Not in the log: a request the check refused at the door. It never reaches node logic; the comm
core counts it.

## Resources

A resource is a thing that exists: a camera, a fan, a motor controller. Orion does not know
what kinds there are. A provider, any program on the board, writes a record for each thing it
offers: name, kind, node, state, and what it offers, which is its labels (tags), its topics and
its actions by full name. With that a program that has never heard of the kind lists it, reads
its topics and runs its actions. The records are the store's and reach every node by peer sync.
`orion-resources` is the rules.

- **One provider.** The session that wrote a resource is the one that may write it again or
  take it away. Anyone else is `denied`.
- **Where its provider is.** A program writes resources for its own node. A record that is not
  a resource record, or sits under another name than its own, is `malformed`.
- **Lost, not gone.** When its provider's session ends, a resource stays listed with state
  `Lost` and everything else as its provider said. When the provider is back and writes it
  again, it is what the provider says.
- **Bindings are leases.** Whoever is to use a resource alone holds it: a lease record under
  the resource's name, with the holder and when the lease ends (zero for "until released").
  A resource has one holder at a time; another is `busy`. Only a resource that is `Ready` can
  be taken; one that is not is `unavailable`. A holder keeps what it holds while the resource
  is lost, and may renew. A lease that has ended holds nothing. A resource that is taken away
  takes its lease with it. Node logic binds with `bind` and `release`; a program binds by
  writing the lease record, under the same rules.
- **A lease lasts as long as it was given, whatever the clock does.** A lease that ends at a
  time ends on the wall clock, and a board with no clock battery gets the time hours after it
  starts: the clock steps forward by years. Node logic sees the step (the wall clock less the
  monotonic one changed by 200 ms or more, looked at before every request and every turn) and
  moves the end of each such lease this node wrote by as much, written as a new version. A
  lease another node wrote is that node's to move. Peer sync's times (when a peer was last
  agreed with, when everything may be sent again) move the same way, so a step makes no peer
  stale.
- **With security on, a lease is its writer's.** Its holder is the principal of the caller that
  wrote it (as the audit log names it), whatever the caller put there; what it put there is kept
  as the lease's `label`, for people. With security off the holder is what the writer says.
- **Only its holder frees a lease.** Deleting a lease record is allowed to the session that
  wrote it, a caller whose principal (as the audit log names it, `local:1000` say) is the
  lease's holder, and root. Anyone else is `denied`, with "<resource> is held by <holder>: only
  its holder or root frees it". A lease that has ended holds nothing, and anyone may clear it.
  A holder whose session ended (or whom a restart of node logic forgot) writes its lease again,
  which renews it, and then frees it.
- **A lease that cannot be read is still said to be held:** `<resource> is held: its lease
  cannot be read`, so a caller that waits out a bare `busy` does not wait this out.
- **A refusal as busy says who.** It names the holder and since when (the lease's stamp), and
  why the lease could not be taken: its holder is answering, has not been quiet long enough,
  has a run under way, or its lease ends on its own.
- **A lease with no end can be contested.** It is never freed by time alone. But when the
  program holding it is quiet (on the board's socket, past the idle time, so the comm core
  keeps its session and tells node logic: `NodeQuiet`) for `leases.quiet_ms` (ten seconds unless
  said), another claimant's claim succeeds. Work in flight is not quiet: while a run of one of
  the resource's actions that the holder's session asked for is under way, the lease is not
  taken. With no claimant nothing is lost, however long the holder is quiet. When the old holder
  speaks again, whatever it asks under the lease while the new holder keeps it (writing or
  freeing the lease, running one of the resource's actions) is refused `lease lost` (22), with
  "lease lost while quiet: taken by <holder>". Once the new holder lets go, the old one may
  claim it like anyone. A session from anywhere but the board's socket ends when it goes quiet,
  as before, so its lease is never contested this way.

**After a restart.** Who provides what is not kept: when node logic starts again, or the comm
core does, node logic knows no provider. The comm core tells every session that node logic
had been serving, and every one that asked while it was away, that it is back (`NodeBack`),
and a program's client writes its resources again then, and after it reconnects. Two seconds
after node logic connects, a resource of this node that nobody has written again is `Lost`.
A provider that died while node logic was away is noticed that way.

**Restoring.** In those two seconds nothing a record says of this node's resources is known to
be true, and the records say so:

- When node logic connects to a comm core, every resource of this node that is not `Lost`
  is written as `Restoring`. It becomes what its provider writes when the provider is back, and
  `Lost` if it is not back in the two seconds. A watcher sees both edges, and so do the other
  nodes. A program that waits for `Ready` and then runs is therefore right: `Ready` is only
  ever said by a provider that is there.
- A run of an action that was registered before the restart, and is not again yet, is refused
  `restoring` (20, "not back yet after a restart: ask again"), not `not found`. So is a read of
  exactly one resource that was provided before and has no record. This is the backstop for a
  caller that did not look at the record first. After the two seconds, and for any name that
  was not served before, the answer is `not found` as always.
- What "before" means is kept in `<state dir>/served`: the actions and resources programs
  served, at most 1024 names. Node logic looks at what is served once a second, and writes
  the file from a helper thread only when it changed, so nothing on the path of a request
  waits for a disk. A name stays on the list for a minute after its program left, so a node
  being shut down, its programs first, still knows what it served. When only the comm core
  restarted, node logic also counts what it was serving the moment before.
- **First boot.** With no list (a fresh flash, a wiped state directory) nothing is restoring
  and a lookup that misses is `not found` at once. The node served nothing before, so there
  is nothing for it to be coming back to, and a caller is not made to wait two seconds for a
  name no program ever offered. A list that cannot be read is the same case and is logged.
  It never stops the node from starting.
- **Topic ids.** A topic's id is given out by the comm core and does not survive its restart.
  A request on an id the new comm core never gave out (an unsubscribe, a withdraw), in its
  first two seconds, is refused `restoring` with the words "topic ids do not survive a restart:
  this one is from before it. A client that reconnects declares and subscribes again, under
  new ids". The Rust client does exactly that in `Topics::restore`, and a program that uses it
  never holds a stale id.
- `restoring` is told to sessions at level 2 or above. An older client is told `not found`,
  as before (see `docs/wire.md`).

## Placement

`orion-placement` decides which node runs a workload, without a leader. Every node holds the
same records and works out the same answer; only the node the answer names acts, by saying
"this one is mine". It is handed a view of the cluster and what a workload needs, and says what
the local node should do. It talks to nothing.

- **Eligible.** A node can run a workload if it is live, takes new work, has an executor for the
  workload's kind, has every label the workload asks for (a label with no value asks only that
  it is there), and, if the workload is to sit beside a resource, is the node that resource is
  on. A node's labels are what it reports of itself with what its record says over it, key by
  key.
- **The choice** among eligible nodes is rendezvous hashing: the highest score of (workload,
  node), where the score is FNV-1a over the workload's name, `0xff` and the node's name,
  finished with the SplitMix64 mixer; equal scores go to the smaller name. Nodes that see the
  same view choose the same node. Removing a node only moves the workloads it had; adding one
  only moves the workloads it wins. The hash is the one `dev` used and is pinned by a test.
- **Only the chosen node writes**, and only for itself. A node never assigns work to another,
  which it may not be able to reach.
- **A workload that runs stays.** While its node is eligible it does not move, whoever would
  win it now. When its node stops being eligible, the node that would take it waits a grace
  period (10 s) first. If the old node is back within it, nothing moves and the wait starts
  over next time.
- **A workload told a node** (`node` in `WorkloadPut`) runs there and nowhere else, eligible or
  not, gone or not.
- **What a workload needs of resources** is written in its labels: `orion.resource=<name>` for
  a resource it must hold, wherever that is, and `orion.colocate=<name>` for one whose node it
  must run on. These two keys are not node labels.
- **Resources across nodes.** A workload holds a resource under the name `<node>/<workload>`.
  Among equal resources it is given one it already holds, or else the ready, free one on a
  live node with the highest score of (workload, resource), so workloads spread and every
  node agrees. The node a resource is on arbitrates it: a lease whose workload is no longer
  assigned to the node the lease names is dropped.

### On connections

`orion-executors` runs it (E20) from the store, whenever a node, workload or resource record
changes, anyone's, or an executor comes or goes, or a grace period ends:

- **Who is there.** A node that observes its own node record is live: a peer's slice is
  forgotten when neither way to it is up (E19). That record lists the kinds its executors run,
  under `orion.kind`, and what it reports of itself; the node's record in the store says labels
  over those. `orion.schedulable=false` keeps new work off a node.
- **Whose it is.** The node placement chooses says so by observing the workload. If two did
  for a moment, one that can still run it wins over one that cannot, and of those the higher
  score: the other lets it go.
- **Started or let go.** A workload that is this node's is put in the tracker and starts when
  its executor and files are there. One that another node now runs is let go here, and its
  record stays. A record that is gone, from any node, ends its run.
- **Nobody can run it**: every node keeps it `Pending`, saying why, until one can.
- **A workload that names a node** is that node's, as before.
- `workloads.placement_grace_ms` (10000) is the grace period.

Liveness is each node's own judgement and is not shared. Two nodes may disagree for a moment;
that is safe because a node only ever takes work for itself, after the grace period, and two
"mine" for one workload are one record that the store's merge settles.

## Files a node fetches itself

Instead of being sent a file, a node can be told where to get it: `FilePull` with a URL, the
destination, the size and the digest. `orion-files-pull` fetches it. Nothing of the file
crosses the connection that asked.

- **Into the same part file** a pushed file would use, so what is fetched is checked against
  its digest and put in place exactly as a pushed file is, and never before.
- **It goes on where it was.** If part of the file is already here the server is asked for the
  rest with `Range`. A server that honours it answers `206` and the bytes are appended. One
  that does not answers `200` with the whole file, and the part is started over. A fetch that
  breaks off answers `Rejected` with why and how many bytes are here, and keeps them: asking
  again with the same number goes on from there.
- **Reports.** Whoever asked is sent `Receiving` with the byte count about four times a
  second, then `Verifying`, then `Committed` or `Rejected`. Asking again while it runs makes
  the asker the one who is told.
- **A helper for each fetch**, which ends with it. At most four at once.
- **Bounded.** A server that sends more than the file's size is cut off there. Connecting may
  take 10 s, and the server may be silent for 30 s in the middle of a file.
- **URLs** are `http://` and `https://`. Anything else is rejected.

Fetching is the cargo feature `pull` of `orion-node-app`, on by default. It brings in the one
HTTP client in Orion (`ureq`) and, for `https`, the one use of rustls; a build without the
feature has neither, and refuses `FilePull` as `unavailable`.

## Workloads and executors

`orion-executors` is the tracker on the wire. It takes `WorkloadPut` and `WorkloadRemove` from
programs, tells executors to start, stop and kill, takes what they report, and tells whoever
asked about a workload where it is.

- **What should run is a record**, of class `Workload`, under the workload's name, holding the
  put as it came. So it is saved, reaches the other nodes, and is known when node logic starts
  again. It goes when the workload is really gone. A workload is not written with a plain
  `Put`; that is `denied`.
- **Pending until an executor offers the kind.** An executor sends `ExecutorOffer` and then
  `ExecutorInventory`. Only after the inventory is anything started on it; what it already
  has is adopted by name, and what it has that Orion does not want is stopped.
- **If Orion restarts, runs carry on.** The offer stands in the comm core, so a node logic that
  starts again is passed it, and answers every offer with `ExecutorAsk`: say everything you
  have. The runs are adopted from the answer and nothing is started twice.
- **If an executor goes** its runs are `Lost`. When one for the kind is back with nothing, they
  are started again at once.
- **A kind has one executor.** A second program offering it is refused, `already exists`. Only
  the executor of a workload's kind may report on it.
- **Who is told.** Every session that put or removed a workload is sent each `WorkloadStatus`
  of it until the workload or the session is gone (16 sessions a workload). Putting a workload
  again as it is changes nothing and is answered with where it is.
- **Files before start.** A workload may name files it needs, each a destination and the
  SHA-256 of what must be there. It is started only when every one is in the node's store
  with that digest. Until then it is `Pending` and says which it waits for, "waiting for file
  models/detector.bin", or the first and how many more. When the last one is put in place
  the workload starts by itself. A file under the right name with other contents is not the
  file. A file that goes away does not stop what runs; it holds the next start.
- **A workload that names another node** is kept as a record and not run here; if it ran here
  it is stopped. Choosing a node for one that names none is placement's, wired in E18; until
  then such a workload runs on the node it was put on.

## Node logic, assembled

`orion-assembly` is the entry point: `run_node(path)` reads the settings file, if there is one,
and the environment, puts every part above together, and runs until `SIGTERM` or `SIGINT`,
saving what it has not yet when it is told to stop. It is what the `node` mode of the binary
calls. The parts are records, resources, actions (with detached runs and runs on other nodes),
files (received, handed to receivers, fetched), workloads and the audit log, in one process
with one thread.

The settings file is the comm core's too, so each program meets keys that are the other's.
Node logic reports only a key under one of its own names (`records.`, `files.`, `audit.`,
`workloads.`) that nobody read: that one is a typo.

Cargo feature `pull`, on by default, is fetching files from a URL. A build without it has no
HTTP client and no rustls.

Peer sync (E19) and placement (E20) are assembled.
