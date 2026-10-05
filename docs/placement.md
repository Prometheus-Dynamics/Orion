# Placement and Cross-Node Binding

Orion places workloads without a leader, and lets a workload on one node bind a resource owned by
another node. Both are built on the same pieces: desired state merged per object with the hybrid
logical clock (HLC, last writer wins, see [peer-sync.md](peer-sync.md)), each node's observed
slice pushed to its peers, and a local judgement of which peers are alive. Orion stays generic:
labels, selectors and resources are opaque strings to it.

## Node labels

A node declares its labels with `ORION_NODE_LABELS`, a comma-separated list of `key=value` or bare
`key` terms:

```sh
ORION_NODE_LABELS='zone=north,camera=front,gpu'
```

The labels are normalized (trimmed, one value per key, sorted) and published in the node's own
observed `NodeRecord`, which reaches its peers in the observed slice. A node record in the desired
state (written with `orionctl apply`) may also carry labels; for the same key the desired label
wins. A node's observed record also reports `schedulable = false` while the node is cordoned,
draining, in maintenance or isolated, so other nodes stop choosing it.

## Placement constraints

`WorkloadRecord::placement` is `Option<WorkloadPlacement>`:

| Field | Meaning |
| --- | --- |
| `None` | Manual placement (the behaviour before placement existed): only an explicit `assigned_node_id` runs the workload. |
| `node_selector: Vec<LabelRequirement>` | Every term must match: `LabelRequirement::equals("zone", "north")` (label equality) or `LabelRequirement::exists("gpu")` (label existence). |
| `colocate_with_resource: Option<ResourceId>` | Run on the node that hosts this resource: its provider's node, or the node of the executor that realizes it. |
| empty `WorkloadPlacement::any()` | Any eligible node. |
| `decision: Option<PlacementDecision>` | Written by the placement engine (node and reason); not set by users. |

```rust
WorkloadRecord::builder("workload.detector", "graph.exec.v1", "artifact.detector")
    .desired_state(DesiredState::Running)
    .placement(
        WorkloadPlacement::any()
            .require_label(LabelRequirement::equals("zone", "north"))
            .colocate_with("resource.camera.front"),
    )
    .build();
```

A node is **eligible** for a workload when it is live (see "Liveness"), schedulable, has an
executor registered for the workload's runtime type, matches every selector term, and hosts the
co-location resource if there is one. Resource requirements do not restrict eligibility, because
cross-node binding can satisfy them from another node.

### Explicit assignment stays authoritative

If `assigned_node_id` is set and is not the node recorded in `placement.decision` (or there is no
decision), the assignment is **explicit**: placement never moves it, even when the node is gone or
does not match the constraints. To hand an explicitly assigned workload back to placement, clear
`assigned_node_id`. To pin a workload, set `assigned_node_id` and clear `placement.decision` (or
set `placement` to `None`).

## Deterministic, leaderless choice

`orion-cluster` (no_std) owns the algorithm; `ClusterCoordinator` runs it on every node.

1. Each node builds a `ClusterView` from its converged desired state (executors, node records,
   providers), the observed slices its peers pushed (labels, schedulability, resource hosts), and
   its liveness judgement.
2. `choose_node` filters the eligible nodes and picks the one with the highest **rendezvous
   (highest random weight) score** `rendezvous_score(workload_id, node_id)`: 64-bit FNV-1a over
   `workload_id`, `0xff`, `node_id`, finalized with the SplitMix64 mixer. Equal scores go to the
   smaller node id. The hash is fixed across releases and platforms.
3. Nodes that see the same view choose the same node. Removing a node only moves the workloads it
   had won; adding a node only attracts workloads it wins (the property test in
   `crates/cluster/src/placement_tests.rs` checks both over random node sets).

### Who writes the assignment

Only the chosen node writes, and only an assignment **to itself**: it sets `assigned_node_id` to
its own id and `placement.decision = { node_id: self, reason }` (`Placed` or
`Failover { from }`) as an ordinary local desired-state write. The write is stamped with the HLC
and replicated by peer sync like any other. If two nodes briefly disagree (different liveness
views) and both write, the merge keeps the later write everywhere, and the other node, seeing the
workload assigned to an eligible node, does nothing further. A node never writes an assignment for
a node it may not be able to reach.

### Hysteresis

- A workload whose assignee is still eligible **never moves**, even if another node (for example
  one that just joined) would win the hash now.
- When the assignee becomes gone or ineligible (unreachable, unschedulable, labels changed, the
  co-location resource moved), each node notes when it first saw that. The node that would take
  the workload over moves it only after that has lasted **`ORION_NODE_PLACEMENT_GRACE_MS`**
  (default 10000). If the assignee comes back within the grace period, nothing moves.
- Unassigned workloads are placed immediately (no grace).

The previous assignee stops its copy as soon as it learns the new assignment (its runtime stops
local workloads that are no longer assigned to it). Between the failover write and that moment a
partitioned previous assignee may still be running the workload; Orion does not fence it.

## Liveness

A peer is **live** while this node has heard from it within **`ORION_NODE_LIVENESS_TIMEOUT_MS`**
(default 5000): a successful outbound sync round (the peer's signed `Hello` answer), or any
authenticated request the peer sent (its own sync rounds, its observed-slice pushes). The local
node is always live. Nodes this node never heard from are not live, so they are never chosen by
it.

Liveness is a local judgement and is not replicated: two nodes can disagree for a moment. That is
safe because placement only acts after the grace period, only for the acting node itself, and
conflicting assignments converge by the HLC merge. Placement and failover therefore need each node
to sync with the nodes it could take over from (a full mesh, or at least every node peered with
every node that runs placement-managed workloads); observed slices are not relayed. Liveness
timeouts are only re-evaluated on reconcile passes, so failover takes up to grace plus
`ORION_NODE_RECONCILE_BACKSTOP_MS` on an idle node. `NodeApp::unreachable_peers()` lists the
peers this node considers gone.

## Cross-node binding

Before this release the runtime planner only bound resources of the local node
(`ResourceBinding::node_id` was always local). Now a requirement that no local resource satisfies
is resolved against other nodes' resources:

1. **Resolve.** After the runtime planned local workloads, each unsatisfied requirement
   (`ReconcileReport::unsatisfied`) is matched against every resource this node knows of (observed
   slices of its peers, desired records): same type, matching ownership mode and capabilities,
   `Available`, owned by another **live** node. Candidates are ranked by rendezvous score of the
   workload id over resource ids, so equal resources spread across workloads deterministically.
   Every node now pushes all resources of its providers and executors in its observed slice (not
   only those also in the desired state), so peers can resolve them.
2. **Authorize.** A candidate must have room under its ownership mode: `Exclusive` admits one
   consumer, `SharedLimited { max_consumers }` that many, `SharedRead` any number. The count is
   the lease's current holders plus the owner's own local bindings of the resource (from the
   owner's observed slice).
3. **Lease.** The consumer node adds `LeaseHolder { node_id, workload_id }` to the resource's
   `LeaseRecord::holders` in the desired state (holders are sorted; `holder_node_id` and
   `holder_workload_id` mirror the first one). Because the lease is one HLC-merged record per
   resource, concurrent leases resolve deterministically: exactly one version survives
   everywhere, and a node whose holder was dropped sees a full lease on its next pass. The
   **owner** node arbitrates its resources: it drops holders whose workload no longer runs on the
   holder's node, and holders beyond capacity (its own local bindings count first, remaining
   holders keep their sorted order). The owner's local planner counts remote holders against
   capacity, so it never hands a leased exclusive resource to a local workload.
4. **Expose.** The planner binds the leased resource with
   `ResourceBinding { resource_id, node_id: owner, remote: Some(RemoteBinding { endpoints,
   available }) }`. In-process executors get it in `ExecutorCommand::Start`; IPC executors get the
   workload record with the remote binding appended from `QueryExecutorWorkloads` /
   `WatchExecutorWorkloads` (`orion_client::AssignedWorkload::remote_bindings`,
   `BoundResource::is_remote`, `BoundResource::is_available`, `BoundResource::endpoint::<T>()`).
   **Bytes flow over the resource's own endpoints**; Orion does not proxy data (its generic data
   plane, `RemoteBinding` frames over TCP/QUIC, is not on this path).
5. **Provider view.** `QueryProviderLeases` / `WatchProviderLeases` on the owner node return
   leases on the provider's resources, including the ones held by remote workloads
   (`LeaseRecord::is_held_by`).

### Owner disappears

- When the owner stops being live, the binding stays but is marked unavailable
  (`RemoteBinding::available = false`); the executor gets a new `Start` (in-process) or watch
  event with the updated binding. The lease is kept.
- If the owner comes back within `ORION_NODE_PLACEMENT_GRACE_MS`, the binding becomes available
  again, with the same lease.
- If it stays gone longer, the consumer releases its holder and resolves the requirement again
  (another node's matching resource, if any). A workload that ran with a remote binding it no
  longer holds a lease for (released, or lost to a competing holder) is stopped, and started again
  once it is bound again.
- A released or stopped workload's holder is removed by the consumer; holders of deleted,
  stopped or reassigned workloads are also removed by the owner.

## Surfaces

- `orionctl get workload(s)`: `assignment=` (`explicit`, `placed`, `failover-from:<node>`,
  `pending`, `manual`), `placement=` (`any`, `selector=...;colocate=...`, `-`) and `bound=` (the
  bindings as `<resource>@<node>`, `*` marks a remote binding, `*!` an unavailable one).
- `orionctl describe workload`: `assignment:`, `placement:` and one line per binding with node,
  endpoints and availability.
- `orionctl get nodes` shows each node's labels.
- `NodeApp::cluster_view()` and `orion::cluster::ClusterCoordinator::status` give the view and
  status programmatically.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `ORION_NODE_LABELS` | empty | This node's labels (`k=v,k2=v2,flag`). |
| `ORION_NODE_LIVENESS_TIMEOUT_MS` | `5000` | A peer not heard from for this long is gone. |
| `ORION_NODE_PLACEMENT_GRACE_MS` | `10000` | How long an assignee or binding owner must stay gone or ineligible before the workload or binding moves. |

## Limits

- Placement does not balance load or consider resource capacity; it spreads workloads by hash.
- Whole-object writes: an assignment write and a concurrent user edit of the same workload are
  merged by last writer wins (see peer-sync.md); the placement engine re-places on its next pass
  if the user's version wins.
- Liveness needs direct peering (observed slices and liveness are not relayed).
- No fencing: a partitioned former assignee may run a workload until it hears of the new
  assignment.
- `LeaseRecord::holders` travels in the rkyv control protocol, peer sync and persistence, but is
  skipped by serde so the postcard MCU link wire stays unchanged: link devices (and JSON/YAML
  output of `orionctl`) see only the first holder in `holder_node_id` / `holder_workload_id`.
