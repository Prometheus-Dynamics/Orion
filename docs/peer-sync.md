# Peer Sync

Every `orion-node` keeps a full copy of the cluster's **desired state** (nodes, artifacts,
workloads, resources, providers, executors, leases). Peer sync is the leaderless anti-entropy
protocol that makes those copies converge. This document describes the transports it runs over,
how concurrent writes are merged, how deletes are tracked, and what changed in control protocol
v3.

Observed state (what is actually running) is not merged like desired state: every node owns its
own observed slice and pushes it to its peers (see "Observed state" below).

## Behaviour before control protocol v3

This section records how merging worked up to protocol v2, because it explains the HeliOS
"bytewise max" report and why the model changed.

- Each node had one desired-state `Revision`: a counter that every `put_*`/`remove_*` incremented.
  Peers compared revisions to decide who was "ahead".
- **Remote revision higher**: the node sent its per-object content hashes and the peer answered
  with a `MutationBatch` that overwrote every object whose hash differed and removed every object
  the peer did not have. A node that had made more (unrelated) writes therefore overwrote the
  other node's concurrent writes in the same section, and objects created only on the "behind"
  node were deleted.
- **Local revision higher**: the node replayed its mutation history since the peer's revision, or
  pushed a full-section batch. History replay matched only the revision *number*, so it was also
  applied to a peer that had diverged at the same revision.
- **Equal revision, different fingerprint**: the nodes exchanged snapshots and merged them with
  `merge_desired_cluster_state`: union of keys, and for a key present on both sides with different
  content, the record whose **rkyv encoding is lexicographically larger** won ("bytewise max").
  The merged state then *replaced* the state on both nodes. Only workloads had tombstones
  (`workload_tombstones`, revision-stamped and compared against the other node's whole-state
  revision); deleted nodes, artifacts, resources, providers, executors and leases came back from
  any peer that still had them.

So "bytewise max" was accurate for the equal-revision path, but the more common path was "the node
with the higher write counter wins whole sections", and deletes were not durable. Neither rule is
related to when a write happened, and the outcome could depend on sync order.

## Model since control protocol v3

### Hybrid logical clock

Each node runs a hybrid logical clock (`orion_core::HybridLogicalClock`). A timestamp
(`orion_core::HlcTimestamp`) is

```text
(physical_ms: u64, logical: u32, node: u64)
```

ordered lexicographically. `physical_ms` is wall-clock milliseconds since the Unix epoch,
`logical` breaks ties inside one millisecond, and `node` is a stable 64-bit FNV-1a hash of the
writing node's id (`orion_core::hlc_node_tag`), so two nodes never produce equal timestamps.
If two different node ids hash to the same tag and also write at the same `(physical, logical)`,
the merge falls back to the content rule below, which is still deterministic.

- **Local write**: `now()` returns `(max(last.physical, wall), logical', node)` where `logical'`
  is `last.logical + 1` when the physical part did not advance and `0` otherwise. Timestamps from
  one node are strictly increasing even if the wall clock steps backwards.
- **Receive**: every accepted remote timestamp is folded into the clock (`observe`), so the next
  local write is ordered after everything this node has seen.
- **Restart**: the clock is not persisted separately. On startup it is seeded from the largest
  timestamp in the persisted desired state (live objects and tombstones), so a node whose real-time
  clock was reset still orders new writes after its own earlier ones.

### Per-object versions

`DesiredClusterState` carries two maps with the same shape as the object sections
(`DesiredObjectStamps`):

- `stamps`: the HLC timestamp of the last write of every live object.
- `tombstones`: the HLC timestamp of the delete of every deleted object.

An object key is live or tombstoned, never both. The old `workload_tombstones` field is gone;
every section has tombstones now.

`MutationBatch` gained `stamps: Vec<HlcTimestamp>`. It is either empty (an unstamped batch from a
local client such as `orionctl`, which the node stamps when it commits) or exactly one timestamp
per mutation (a batch from a peer, or a batch in the node's own mutation history).

### Merge rule (last writer wins per object)

For one object key, a candidate version `(stamp, Put(record) | Remove)` replaces the current
version if and only if:

1. there is no current version (neither live nor tombstoned), or
2. `stamp > current_stamp`, or
3. `stamp == current_stamp` and the candidate wins the tie: a `Remove` beats a `Put`, and between
   two different records the one whose rkyv encoding is larger wins. Equal timestamps with
   different content can only come from a node-tag collision or a node whose state directory was
   restored from a backup, so this rule only has to be deterministic.

This is a join over a total order, so the result is the same whatever order versions arrive in,
however often they are re-sent, and whether they come as mutation batches, summaries plus diffs,
or whole snapshots. Every node computes the same winner without coordination. The rule is
implemented once, in `DesiredClusterState::apply_stamped` (`orion-control-plane`), and every
desired-state write in `orion-node` goes through it.

Consequences worth knowing:

- Concurrent writes to the **same** object: the later HLC timestamp wins; the other write is
  dropped everywhere (counted as `stale_remote_writes_ignored`). "Later" is HLC order: a write
  made after a node has seen another write is always ordered after it, but two writes on
  different nodes that have not synced in between are ordered by their wall clocks, and within
  the same millisecond (or within clock skew) by node tag, not by real time.
- Concurrent writes to **different** objects never interfere, even inside one section.
- Delete versus concurrent update: whichever has the later timestamp wins. An update made after
  the delete (in HLC order) brings the object back; an update made before it is discarded.
- Writes are whole-object: two nodes that change different fields of the same workload
  concurrently do not get a field-level merge.
- Nodes still re-assert the provider and executor records of integrations registered in-process
  on them: if a merge replaced such a record, the node writes its own record back with a new
  timestamp.

### Revision and mutation history

`Revision` keeps its meaning as a **node-local commit sequence number**: it increases by exactly
one for every object version a node applies (local or remote). It orders the node's mutation
history, drives state watchers (`WatchState`), and names persisted checkpoints. It is not a
cluster-wide version: two converged nodes usually have different revisions, and revisions are no
longer compared between peers. Convergence is judged by the desired-state fingerprint, which
hashes the records, their stamps and the tombstones of every section.

The mutation history stores the batches a node actually applied, with their stamps. Replaying the
history on top of its baseline (startup, persistence) reproduces the state exactly. History is no
longer shipped to peers, because history positions are not comparable between nodes.

### Clock skew

`ORION_NODE_HLC_MAX_DRIFT_MS` (default `300000`, five minutes) bounds how far ahead of the local
wall clock a remote timestamp may be. A remote object version whose `physical_ms` is more than
that ahead is **rejected**: it is not applied and not folded into the local clock, the rest of the
batch is still applied, and `clock_skew_rejections` in the observability snapshot (and the
`orion_node_desired_merge_clock_skew_rejections_total` metric) increase, with a warning log naming
the peer.

Rejecting instead of clamping keeps the merge deterministic: clamping would give the same write
different timestamps on different nodes. A rejected version becomes acceptable once real time
catches up with it, so a node whose clock is a little fast only delays its writes. A node whose
clock is far ahead (for example a device that booted with a bad RTC) has all its writes rejected
by its peers, which is visible on the peers' counters; fix its clock, or raise the limit if the
skew is expected.

Orion does not discipline clocks. Run NTP, chrony or PTP on nodes that share desired state.

### Tombstones and garbage collection

A delete leaves a tombstone so that peers that still have the object (or receive an older write
later) learn about the delete. Tombstones are collected after
`ORION_NODE_TOMBSTONE_RETENTION_MS` (default `604800000`, seven days) measured on the HLC
physical clock. Collection runs at the start of every peer sync round and on reconcile passes. It
does not change the revision or the mutation history.

Remote tombstones that are already past retention are not stored and not pulled or pushed, so
two nodes whose clocks disagree slightly about expiry do not hand the same tombstone back and
forth. An expired tombstone still deletes an older live copy if one exists.

The retention window is a real bound: **a node that has been disconnected for longer than the
retention window can resurrect objects that were deleted while it was away**, because the
tombstones are gone everywhere else. Reset such a node's state directory (or let it rejoin with an
empty desired state) instead of letting it sync its stale copy back. `tombstones` and
`tombstones_collected` are reported in the observability snapshot.

## Sync protocol

Peer sync is driven by the node that runs `sync_peer` (the *initiator*, every
`ORION_NODE_RECONCILE_MS`). One round is bidirectional, so a node that only makes outbound
connections still converges. The engine (`crates/node/src/app/peer_sync*.rs`) is independent of
the transport; it talks to a peer through the `PeerSyncTransport` trait (send one signed control
request, receive one response), which the HTTP and TCP transports implement.

1. `Hello` exchange. Both sides report revisions, the overall desired fingerprint and one
   fingerprint per section. Equal overall fingerprints end the round.
2. `SyncSummaryRequest` for the sections whose fingerprints differ. The response is a
   `DesiredStateSummary`: per object a content hash and its stamp, plus the section's tombstones.
3. The initiator compares the summary with its own and splits the differing keys into
   - *push*: keys where the local version wins the merge rule, and
   - *pull*: keys where the remote version wins or that only the peer has.
4. Push: a stamped `Mutations` batch with the local versions of the push keys. The peer applies it
   with the merge rule.
5. Pull: a `SyncDiffRequest` listing the pull keys. The peer answers with a stamped `Mutations`
   batch, which the initiator applies with the merge rule.
6. Observed push: the initiator sends its own observed slice when it changed (see below).

A converged round is a single `Hello` exchange (plus the occasional observed push). Expired
tombstones are collected at the start of every round, so two nodes that both hold an expired
tombstone do not keep exchanging it.

Because the merge is idempotent and commutative, a round that fails half way, runs concurrently
with the peer's own round, or races with new local writes cannot corrupt state; the next round
picks up what is still different. Rounds are idempotent, so the `orion+tcp` client retries a
request once on a fresh connection when its cached connection was closed.

Older request shapes are still served for embedders and tests: a `SyncRequest` whose fingerprint
matches is answered `Accepted`; one with a summary gets the versions the responder has that win
against the summary; one without a summary gets a full `Snapshot` (its `desired_revision` is
ignored, since revisions are node-local). A pushed `Snapshot` is merged per object, never adopted
wholesale. An unstamped `Mutations` batch from a peer is treated like a local client write: its
`base_revision` must equal the receiver's revision and the receiver stamps it.

A pulled version that the receiving node rejects in validation (for example a running workload
assigned to the receiver that its executors refuse) fails the round, as before; the round is
retried with backoff and the failure is visible in the peer's `last_error`.

### Observed state

Observed state has a single writer per record: the node that observes it. Each node's *observed
slice* is its own `NodeRecord` (health and clock facts), the workloads assigned to it, and the
resources and leases of its own providers and executors that are part of the desired state. At the
end of a sync round the initiator pushes its slice as an authenticated `ObservedUpdate` when the
slice changed since the last successful push to that peer, and at least every 30 seconds (so a
peer that restarted catches up). The receiver replaces exactly that origin's slice: records the
origin owns but no longer reports are pruned, records of other nodes are never touched, and the
receiver keeps its own observed revision (it advances by one when the slice changed) and ignores
the sender's applied revision. Pushes are best effort; a failed push does not fail the desired-state
round and is retried on the next one. Observed slices are not relayed: a node learns another
node's observed facts only from that node.

## Transports

`ORION_NODE_PEERS` entries choose the transport by URL scheme:

| Scheme | Cargo feature | Listener |
| --- | --- | --- |
| `http://host:port`, `https://host:port` | `transport-http` (default) | `ORION_NODE_HTTP_ADDR` |
| `orion+tcp://host:port` | `peer-tcp` | `ORION_NODE_PEER_ADDR` |

Both kinds can be mixed in one cluster and in one node's peer list, as long as the node is built
with both features. A peer URL whose feature is not compiled in fails startup with an error that
names the feature.

### `orion+tcp` (feature `peer-tcp`)

The `peer-tcp` feature adds a plain TCP transport that needs only tokio, so it works in the
IPC-only build:

```sh
cargo build -p orion-node --release --no-default-features --features peer-tcp
ORION_NODE_ID=node-a ORION_NODE_PEER_ADDR=0.0.0.0:9200 \
  ORION_NODE_PEERS='node-b=orion+tcp://10.0.0.2:9200|<node-b-public-key-hex>' orion-node
```

Wire format (one long-lived connection per peer, requests are sequential on a connection):

```text
frame    = [b'O' b'C'][CONTROL_PROTOCOL_VERSION u16 LE][payload_len u32 LE][payload]
request  = [kind u8][rkyv archive]        (same body as the HTTP codec: control,
                                           observed update, or authenticated peer request)
response = [status u8][sig_len u8][signature][key_len u8][public key][body]
           status 0: body is an rkyv HttpResponsePayload
           status 1: body is a UTF-8 error message
```

The frame header is the same fixed preamble as local IPC stream frames
(`orion_transport_ipc::ControlFrameReadState`), so a version skew is reported as a typed
`ProtocolMismatch` before anything is decoded: the server answers a skewed frame with a
payload-free frame carrying its own version and closes the connection. Payloads are limited by
`ORION_NODE_TRANSPORT_MAX_PAYLOAD_BYTES`, connections by
`ORION_NODE_TRANSPORT_MAX_CONCURRENT_CONNECTIONS`, and each read or write by
`ORION_NODE_TRANSPORT_IO_TIMEOUT_MS`; idle server connections are closed after a minute and the
client reconnects (and retries the request once, which is safe because every peer request is
idempotent under the merge rule).

### Threat model and why `orion+tcp` has no TLS

Peer requests are signed with the sending node's ed25519 key, the same as over HTTP
(`AuthenticatedPeerRequest`): the signature covers the node id, public key, a per-node monotonic
nonce and the payload, and the receiver rejects unknown keys (with `ORION_NODE_PEER_AUTH=required`
or configured keys), revoked peers and replayed nonces. Over `orion+tcp` the **responses are
signed too**: the responder signs a domain-separated message containing its node id and public
key, the exact request bytes (which include the request nonce and signature), the status and the
body. The initiator verifies the signature against the key it has configured or pinned for that
node id. In `required` mode an unsigned or unverifiable response is an error; in `optional` mode
an unsigned response is accepted, a signed one must verify.

That gives, for both directions:

- **authenticity and integrity**: an on-path attacker cannot forge or alter requests or
  responses, so it cannot inject desired state into either node;
- **replay protection**: requests carry nonces, responses are bound to the request they answer;
- **no confidentiality**: desired state (workload specs, configs, resource endpoints) crosses the
  network in clear text and can be read by anyone on the path;
- **no protection against dropping or delaying** traffic (a denial of service), as with any
  transport.

TLS is therefore not required for integrity, and the `peer-tcp` feature deliberately does not
pull in rustls, which is what keeps it usable in the small IPC-only build. Use `orion+tcp` on a
trusted or already-encrypted network (a private VLAN, WireGuard, IPsec), or use `https://` peers
(feature `transport-http`) when desired state must stay confidential on an untrusted network.
With `ORION_NODE_PEER_AUTH=disabled` nothing is signed in either direction; do not use that
outside tests.

Trust is enrollment-based on every transport: a peer is trusted because its public key is
configured in `ORION_NODE_PEERS`, enrolled with `orionctl`, enrolled with a shared enrollment key,
or (in `optional` mode only) pinned on first contact. Discovering a peer never makes it trusted;
see [discovery.md](discovery.md) for mDNS discovery, the enrollment handshake and its threat
model.

### Size and dependency cost

Measured on x86_64 Linux with the workspace release profile (`opt-level = "z"`, fat LTO, one
codegen unit, stripped). Crates are the unique packages in `cargo tree -e normal` for
`orion-node`, including the workspace's own `orion-*` crates.

| `orion-node` build | Binary size | Crates (non-Orion) |
| --- | --- | --- |
| `--no-default-features` (IPC only) | 2.65 MiB (2,782,520 B) | 74 (64) |
| `--no-default-features --features peer-tcp` | 2.88 MiB (3,019,744 B) | 74 (64) |
| `--no-default-features --features transport-http` | 5.05 MiB (5,295,056 B) | 151 (141) |
| default (`transport-http`, `peer-tcp`, `transport-tcp`, `transport-quic`) | 5.74 MiB (6,014,048 B) | 166 (154) |

`peer-tcp` adds about 230 KiB and no crates to the IPC-only build; clustering over HTTP instead
costs about 2.4 MiB and 77 more crates (axum, hyper, reqwest, rustls and their dependencies).

## Upgrading from protocol v2

The changes above alter archived types (`DesiredClusterState`, `DesiredStateSummary`,
`MutationBatch`, `NodeObservabilitySnapshot`; `PeerHello` is unchanged), so they ship in control
protocol v3 together with the clock facts and status lane changes of the same release. v2 peers,
`orionctl` builds and client libraries are refused with a `ProtocolMismatch` error. Upgrade every
node of a cluster, `orionctl`, and providers or executors that embed `orion-client` together.

Persisted state directories are migrated on first start:

- The snapshot format version goes from 3 to 4. A node that finds a format-3 manifest decodes the
  old desired snapshot, mutation history and history baseline with the v2 layouts, replays the
  history, converts the result and rewrites all desired-state files in format 4 before it serves
  anything. Every migrated object and every migrated workload tombstone gets the stamp
  `(0, 0, local node tag)`, so any write made after the upgrade wins over pre-upgrade state, and
  pre-upgrade conflicts between nodes resolve deterministically by node tag and content. The old
  files are kept in `<state dir>/legacy-format-3/`; an interrupted migration restarts from there.
- The observed snapshot is reset to an empty state (keeping its revision): observed records are
  rebuilt from what providers, executors and peers report. The applied snapshot, the trust store,
  the node identity, maintenance state and artifacts are unchanged.
- Format-3 node records are read with their old layout (no `clock`); the clock facts are
  reported again by the running node.
- If the old files cannot be decoded (for example a state directory written by a different
  pre-release, or desired state that still uses the removed
  `ResourceOwnershipMode::ExclusiveOwnerPublishesDerived`), startup fails with an error naming the
  state directory and the files to move aside; nothing is overwritten. Moving `snapshot-*.rkyv`
  and `mutation-history*.rkyv` out of the state directory starts the node with an empty desired
  state that it then pulls from its peers.
- A state directory written by this version cannot be read by an older `orion-node`.

## Hooks for placement, cross-node binding and discovery

- **Placement** (`orion-cluster`): placement decisions are ordinary desired-state writes, so two
  nodes that run a deterministic leaderless placement and write the same assignment converge
  without flapping (identical content, the later stamp wins), and differing decisions resolve by
  the merge rule. `DesiredClusterState::version_of` exposes the stamp, including the writer's node
  tag, for "who decided" diagnostics.
- **Cross-node binding**: `PeerSyncTransport` is a generic signed request/response channel over
  `ControlMessage`; lease and binding RPCs between nodes can use the same transports and the same
  authentication without a new listener.
- **Discovery**: implemented by the `discovery-mdns` feature ([discovery.md](discovery.md)).
  An enrolled peer goes through `NodeApp::enroll_peer` with its pinned key, exactly like a peer
  enrolled with `orionctl peers enroll --base-url`; `PeerTransportKind::from_base_url` maps the
  advertised URL to its transport. Responses over `orion+tcp` are verified against enrolled keys,
  so an mDNS announcement alone can never inject state, and discovery requires
  `ORION_NODE_PEER_AUTH=required` so unenrolled peers are never pinned on first contact.
- **Observed facts and the status lane**: each node's observed slice already travels to its peers
  at the end of every round (`push_observed_slice` in `crates/node/src/app/peer_observed.rs`).
  The volatile status lane is local-only; replicating it would follow the same per-origin pattern
  (origin pushes its own entries, receivers replace that origin's set) on the same transport.
