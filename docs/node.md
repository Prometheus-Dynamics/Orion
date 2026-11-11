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
