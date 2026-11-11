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
