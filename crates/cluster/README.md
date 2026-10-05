# orion-cluster

Cluster coordination and membership primitives for Orion (`no_std` + `alloc`, default `std`
feature):

- `ClusterView`, `eligibility`, `choose_node`: deterministic, leaderless workload placement
  (rendezvous hashing over eligible nodes) from converged state.
- `ClusterCoordinator`: which placement-managed workloads the local node should assign to itself,
  with grace-period hysteresis.
- `leases`: resolving, authorizing and arbitrating cross-node resource leases.
- Membership and admission helpers.

See [`docs/placement.md`](../../docs/placement.md).
