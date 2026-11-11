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
  direction, and shows as `Stale`, until `forgive` is called for it. Two nodes that stayed
  connected and in agreement are never stale. When peers last agreed is given back by
  `agreed_at` to be kept across a restart.
- **Whose message.** The connection says who the peer is. A message that names another node is
  refused.

## The open list of actions

`orion-action-list` is the list of actions and the runs under way. Orion ships almost no
actions; any program on the board adds its own.

- **Adding.** `Register` names the action, what it runs against (this node, or a resource) and
  who may run it. It is answered with `Registered`. A name is taken once for a target; the
  same name for another target is another action. The same registration again from the same
  program is the same action. An action for another node cannot be added here.
- **Running.** `Run` names an action and a target. The program that added it is handed a
  `Claim`. A run has two numbers: the caller's `call`, unique on its session, and the list's
  own, which is the one in `Claim`. Each side only ever sees its own. `Progress` (which may be
  dropped if the caller is not reading) and `ActionResult` from the program go to the caller
  under the caller's number. `Cancel` from the caller reaches the program; the run ends when
  the program says how.
- **Who may run it** is not decided here. The policy the action was registered with is what
  the check is told at the door, so only a run that passed arrives.
- **When an end goes away.** A caller that goes has its run cancelled, unless the run is
  detached: that one carries on. A program that goes takes its actions off the list and its
  runs end as `Failed`.
- **Detached runs.** A run started detached carries on when its caller's session ends, with
  its deadline and under its program as before. Whoever started it comes back with `Attach`
  and the run's number, from any session: it is sent the latest progress at once and then
  what follows, and can cancel. How a detached run ended is kept for ten minutes (256 runs
  at most, the oldest goes first) and given to its starter when it asks, once; that is so
  even if a caller seemed to be attached when it ended, because a session can end a moment
  before node logic hears of it. "Whoever
  started it" is the same user on the board, the same operator key or the same peer, as the
  comm core says; for anyone else the run is `not found`. A caller's numbers start over with each
  of its sessions, so a new run under a number replaces what was kept under it, and of two
  such runs under way the newer is the one come back to.
- **Deadlines.** A run with a deadline that passes ends as `TimedOut` and the program is told
  to stop. What the program says of it afterwards is ignored.
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
- **A sender that comes back** in a new session and begins the same transfer again is the one
  told from then on.
- **A receiver that goes** takes its prefixes with it. Whoever was sending to it is told the
  transfer is rejected, "the program that receives it went away". The destination is then a
  place in the node's own store again.
- At most 64 prefixes and 256 transfers passing through at once; one more is `busy`.
- **A receiver with little room**, a microcontroller on a serial line, names the largest
  chunk it takes in `FileSink` (`max_chunk`; 0 for any). It is then sent one frame at a
  time: each waits here until the one before is answered, and a chunk larger than it takes
  is cut into pieces that wait their turn. What waits is what the sender has in flight, at
  most 1 MiB of a transfer; more is `busy`. A sender that begins again, having heard nothing,
  is passed on at once and what waited is dropped: the receiver says where to go on from.

## Sending a kept file to a receiver: `files.send`

Node logic has one action of its own. `files.send` on the node, with `FileSend { file, to }`
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
  again, it is what the provider says. If the comm core goes, every resource provided
  through it is lost the same way.
- **Bindings are leases.** Whoever is to use a resource alone holds it: a lease record under
  the resource's name, with the holder and when the lease ends (zero for "until released").
  A resource has one holder at a time; another is `busy`. Only a resource that is `Ready` can
  be taken; one that is not is `unavailable`. A holder keeps what it holds while the resource
  is lost, and may renew. A lease that has ended holds nothing. A resource that is taken away
  takes its lease with it. Node logic binds with `bind` and `release`; a program binds by
  writing the lease record, under the same rules.

After node logic restarts it no longer knows which session provides what, until each provider
writes its resource again. A provider that dies in that time is not noticed.

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

Not assembled yet, because they need connections between nodes that the comm core does not
make yet: peer sync (`orion-sync`) and placement (`orion-placement`). Both are built and
proved on their own. Until they are wired, a node's records stay on that node and a workload
runs on the node it was put on, or the one it names.
