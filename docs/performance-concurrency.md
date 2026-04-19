# Performance And Concurrency Review

This document records the current release audit for `orion-node` hot paths, blocking behavior,
and peer-sync concurrency defaults.

## Locking Model

`orion-node` intentionally uses `std::sync::RwLock` for the in-memory registries and persisted
runtime state in [`crates/orion-node/src/app/state_access.rs`](../crates/orion-node/src/app/state_access.rs).

Audit conclusions:

- These locks guard synchronous, in-process data structures such as peer registries, client
  registries, provider/executor registries, desired-state caches, and the persisted runtime store.
- The accessor layer is synchronous and returns standard lock guards, which keeps the code obvious
  about when a lock is held.
- The reviewed hot paths avoid holding those guards across `.await` points. Async code generally
  clones or snapshots what it needs before awaiting, rather than parking with a live lock guard.
- The remaining `std::sync::Mutex` usage in runtime code is limited to `JoinHandle` ownership for
  shutdown (`ReconcileLoopHandle`, persistence worker, auth-state worker, observability worker)
  rather than shared mutable state in request or reconcile hot paths.

Because of that structure, replacing these locks with async locks would add overhead and make the
call graph less explicit without addressing a currently observed deadlock or contention issue.

## Lock Ordering Expectations

The release audit now treats the following as the canonical lock ordering for `orion-node`:

1. runtime store
2. mutation history
3. mutation history baseline
4. desired-state metadata and summary caches
5. peer registry / peer client registry
6. local client registry
7. observability

Practical rules derived from the current code:

- Desired-state transactions in
  [`crates/orion-node/src/app/desired_state.rs`](../crates/orion-node/src/app/desired_state.rs)
  take `store -> mutation_history -> mutation_history_baseline` together and should finish their
  in-memory mutation before touching persistence or watcher fanout.
- Desired-state cache invalidation happens after those transaction locks are released. Cache locks
  should not be held while mutating the store or history vectors.
- Provider and executor trait callbacks must stay outside the runtime-store write lock. The
  reconcile path now snapshots providers/executors first and only then mutates the shared store.
- Client stream delivery should stay split into two phases: mutate the client registry, drop that
  lock, then perform `try_send(...)` on the stream sender. The current
  `prepare_client_stream_flush -> execute_client_stream_flush -> finalize_client_stream_flush`
  split exists specifically to enforce that.
- Observability updates should remain leaf operations. They may follow store/peer/client updates,
  but they should not wrap those locks or call back into mutation/reconcile paths.
- Worker-thread coordination and audit-log writes are intentionally outside the in-memory registry
  lock graph. They can block, but they should do so only after shared state has been released.

These rules are not just style guidance. They are what keeps the current `std::sync` lock model
safe on both sync and async call paths without needing async-aware locking primitives.

## Remaining Contention

The current release candidate does not show an obvious deadlock in audited paths, but there are
still a few sections worth treating as the main contention budget:

- Desired-state persistence still serializes the full encoded bundle after every meaningful desired
  revision change. The persistence worker now exposes queue wait time, which should be watched in
  soak runs.
- Watcher fanout no longer rescans workloads/leases once per client, but it still does
  per-revision recomputation once per distinct watched executor/provider. Large watch-heavy loads
  should still be benchmarked.
- Status/observability snapshots take several read locks in sequence (`store`, `peers`, `clients`,
  `observability`). That is acceptable for admin surfaces, but they should stay read-only and
  avoid growing extra derived work.
- Local control-plane state updates still do `store` mutation, persistence, and then reconcile in
  sequence. That is correct, but it remains one of the higher-latency synchronous paths in the
  node.
- Mutation-history normalization and persisted-state capture hold the store/history/baseline locks
  long enough to clone and encode state. If release perf work finds contention there, that is the
  next place to consider more incremental persistence formats.

## Persistence Saturation Guardrails

The persistence worker request flow was re-audited after the helper-thread handoff change.

Current release stance:

- Runtime threads no longer block directly on persistence worker queue send/reply wait paths.
- The remaining sync persistence entrypoints are compatibility edges for startup/admin callers,
  while request-serving code should prefer async persistence.
- Saturated persistence queues must still expose backpressure clearly in observability metrics.

Synthetic regression coverage now exists in
[`crates/orion-node/src/tests/state/observability.rs`](../crates/orion-node/src/tests/state/observability.rs)
with a queue capacity of `1`, injected persist delay of `25ms`, and `8` concurrent artifact writes.

Current regression bounds for that scenario:

- total completion time should stay below `1500ms`
- max recorded persistence worker enqueue wait should stay below `1000ms`
- worker enqueue wait counters must become non-zero so backpressure is visible instead of silent

In the focused run on April 19, 2026, that saturation test completed in about `410ms`, well within
the current regression bound. These numbers are not a universal production SLA; they are a release
guardrail for catching obvious regressions in worker-queue behavior.

## Audit Queue Guardrails

The audit path does not currently expose enqueue-wait counters because the release overload policy
is intentionally binary:

- `block`: operators explicitly accept that audit appends may stall the caller
- `drop_newest`: the caller should not stall behind slow audit I/O

For the non-blocking `drop_newest` path, release guardrails are defined by the direct audit-sink
regression in
[`crates/orion-node/src/app/observability.rs`](../crates/orion-node/src/app/observability.rs):

- queue capacity `1`
- injected audit append delay `25ms`
- second enqueue should still complete in under `10ms`
- dropped-record accounting must increase when the queue is saturated

That is a narrow release regression check for the chosen overload mode, not a generic production
latency guarantee.

## Runtime Snapshot Collection

Release decision:

- Provider and executor snapshot collection remains synchronous for this release.
- The supported contract is that `provider_record`, `snapshot`, `validate_resource_claim`,
  `executor_record`, `validate_workload`, and `apply_command` stay cheap and local rather than
  performing filesystem or network I/O.
- The current code now documents that contract directly in `orion-runtime`, and reconcile collects
  provider/executor snapshots before mutating the runtime store.

This is a deliberate tradeoff. Async collection is still a plausible future direction if
integrations grow more expensive, but adopting it now would widen the runtime surface area and add
complexity before there is evidence that synchronous snapshot reads are the release bottleneck.

## Reconcile Serialization Audit

`crates/orion-node/src/app/reconcile.rs` was re-reviewed specifically for unnecessary
serialization.

Current conclusions:

- `collect_runtime_state()` does serialize provider and executor snapshot collection, but it does
  so before the runtime-store mutation phase. That means a slow integration can lengthen reconcile
  time, but it does not hold the shared runtime-store write path while doing so.
- `apply_reconcile_report_async()` also serializes `executor.apply_command(...)` calls in reconcile
  order. That is intentional for this release because command ordering is clearer, executor traits
  are still synchronous, and command application is expected to be a cheap local callback rather
  than blocking I/O.
- The release risk is therefore latency inflation from a misbehaving integration, not lock
  inversion or hidden async blocking inside the store-mutation phase.

Accepted release boundary:

- keep snapshot collection synchronous and serialized
- keep command application synchronous and serialized
- rely on the documented integration contract that these callbacks remain cheap and side-effect
  local

If future integrations need network or filesystem work inside `snapshot()` or `apply_command()`,
that should trigger a real runtime-surface redesign rather than ad hoc offload inside reconcile.

## Dynamic Dispatch Boundaries

The remaining dynamic dispatch in the release surface was reviewed as part of the same audit.

Boundary uses that remain intentional:

- provider and executor integration registries in `orion-node`
- transport handler traits for HTTP, IPC, TCP, and QUIC server boundaries
- control middleware composition in `orion-service`
- authorization lookup seams that allow `NodeSecurity` policy to stay decoupled from app state

These are subsystem boundaries where pluggability and cloneable composition matter more than
inlining.

For `orion-service` specifically, the current release decision is explicit: it is an ergonomic
control-plane middleware surface, not a candidate for hot-path zero-cost specialization. The crate
docs and README now treat that as the supported boundary rather than leaving it as an implicit
tradeoff.

The release cleanup removed an unnecessary internal boxed-future layer from
[`crates/orion-node/src/managed_transport.rs`](../crates/orion-node/src/managed_transport.rs), so
the remaining dynamic dispatch is concentrated at those boundaries rather than in internal startup
plumbing.

## Blocking Behavior

Blocking work is intentionally routed through
[`crates/orion-node/src/blocking.rs`](../crates/orion-node/src/blocking.rs).

Reviewed call sites fall into three categories:

- filesystem and certificate operations
  Examples: trust-store persistence, TLS bootstrap file generation, state persistence
- synchronous control-plane execution
  Examples: local IPC and HTTP handlers calling the synchronous control pipeline
- worker-thread coordination
  Examples: persistence/auth-state worker queue send and reply wait paths

Current request-serving classification after the service-adapter narrowing:

- required blocking I/O
  - trust-store persistence and trust-root writes triggered by local peer-admin operations
  - explicit storage durability helpers in
    [`crates/orion-node/src/storage_io.rs`](../crates/orion-node/src/storage_io.rs)
  - audit-log appends when operators explicitly choose `ORION_NODE_AUDIT_LOG_OVERLOAD_POLICY=block`
- avoidable blocking
  - none remain at the HTTP/IPC adapter boundary after the blanket request wrapping was removed
- should become async over time
  - local control operations that still perform synchronous desired-state persistence and then
    reconcile inline (`ProviderState`, `ExecutorState`, local snapshot/mutation adoption)
  - security-mutating local admin paths that still persist trust state synchronously even though
    the adapter no longer blocks the runtime thread while waiting

Release changes from this audit:

- Sync persistence and auth worker requests now hand off queue send/reply wait work to a helper
  OS thread when called from within a Tokio runtime, rather than blocking the runtime thread with
  `block_in_place(...)`.
- HTTP and IPC control adapters no longer wrap every request in blocking offload. They now route
  only the known mutating operations that persist state, touch trust storage, or rotate TLS
  material through helper-thread execution, while read-only/query operations stay inline.
- Some blocking paths are intentionally still synchronous because they are startup/admin code, not
  hot-path request handling:
  - HTTP TLS bootstrap and rotation in
    [`crates/orion-node/src/app/tls_bootstrap.rs`](../crates/orion-node/src/app/tls_bootstrap.rs)
  - server TLS certificate/key file loading in
    [`crates/orion-transport-http/src/tls.rs`](../crates/orion-transport-http/src/tls.rs)
  - durable storage read/write helpers in
    [`crates/orion-node/src/storage_io.rs`](../crates/orion-node/src/storage_io.rs)

This helper should stay scoped to short, bounded synchronous work. It should not become a catch-all
for long-running CPU-heavy tasks.

## Peer Sync Parallelism

Peer sync concurrency currently lives in
[`crates/orion-node/src/app/peer_sync_parallel.rs`](../crates/orion-node/src/app/peer_sync_parallel.rs)
and is operator-configurable through `ORION_NODE_PEER_SYNC_MODE` and
`ORION_NODE_PEER_SYNC_MAX_IN_FLIGHT`.

Current behavior:

- `0` peers => no work
- `1-2` peers => use the requested parallelism directly
- small clusters => cap to the small-cluster runtime tuning value
- larger clusters => cap to the large-cluster runtime tuning value
- spawn staggering only activates once the peer count crosses the no-stagger threshold

This behavior is covered by the unit tests in
[`crates/orion-node/src/tests/sync/peer_sync/parallel.rs`](../crates/orion-node/src/tests/sync/peer_sync/parallel.rs).

The policy is intentionally conservative for public release. The remaining open work is empirical:
recording perf-harness results for different node counts and confirming whether these defaults are
still the right tradeoff under real release-candidate workloads.

The current recorded baseline is in
[`docs/perf-baselines-2026-04-19.md`](perf-baselines-2026-04-19.md). In that run:

- average sync wave time was about `259ms` at `3` nodes
- average sync wave time was about `367ms` at `5` nodes
- average sync wave time was about `474ms` at `7` nodes
- parallel only modestly beat serial at `3` and `5` nodes, and lost at `7` nodes in that local
  Docker run

Release guardrails derived from that run:

- `3`-node docker peer-sync average should stay below `325ms`
- `5`-node docker peer-sync average should stay below `450ms`
- `7`-node docker peer-sync average should stay below `550ms`

Those thresholds are intended to catch obvious regressions in peer-sync iteration duration while
preserving headroom for machine-to-machine variance. They also reinforce the current design choice
to keep larger-cluster peer-sync defaults conservative and operator-tunable.
