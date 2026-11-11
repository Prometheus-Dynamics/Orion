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

| Setting | Default | What it is |
| --- | --- | --- |
| `node.name`, `socket`, `state.dir` | as the comm core | the same file is read by both |
| `records.save_after_ms` | 500 | how long after a change the records are saved |
| `records.max_drift_ms` | 300000 | a stamp from a peer further ahead than this is refused |

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

One thread, with the store as plain data owned by it.

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
