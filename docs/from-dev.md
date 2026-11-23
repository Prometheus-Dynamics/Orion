# Porting a program from `dev` to `next`

For a program built against Orion's `dev` branch: the control-plane client (`LocalNodeRuntime`,
`LocalControlPlaneClient`, the provider and executor services, `ActionCaller`), the records
with typed config values, status entries and streams, and the NT4 crate. It says, group by
group, what became of each thing and what the program does instead.

Each line is marked:

- **exists**: `next` has it, under this name.
- **yours**: `next` does not have it and does not mean to; the program does it itself, and
  this says how.
- **gap**: `next` has nothing for it yet. Each gap is listed again at the end.

Nothing here was run against a ported program. It is written from `next`'s code and docs and
from a reading of one `dev` program's calls. Where a thing was not checked, it says so.

## The shape that changed

On `dev` a program talked to a control plane: it registered a provider or executor identity,
published whole snapshots of its resources, read a `StateSnapshot` with a desired and an
observed side, and wrote desired state as mutations against a revision.

On `next` there is no snapshot and no control-plane API. There are six things, and everything
is one of them ([`design.md`](design.md), "Six things"):

| On `next` | What it is | Read in |
| --- | --- | --- |
| A **record** | a named piece of state with a stamp: a node, a resource, a lease, a workload | [`client.md`](client.md), "Records" |
| A **topic** | values that change, read from a ring or as messages | [`client.md`](client.md), "Topics" |
| An **action** | one thing done once, with progress, cancel and a result | [`client.md`](client.md), "Actions" |
| A **file** | bytes pushed to or fetched from a board, checked by digest | [`client.md`](client.md), "Files" |
| A **workload** | what should run, and where each run is | [`client.md`](client.md), "Workloads" |
| A **resource** | a thing that exists, with its topics and actions | [`client.md`](client.md), "The provider role" |

So most of a port is not a rename. It is deciding, for each thing the program kept in a
record's config, which of these it is.

## Crates, features, sockets

| On `dev` | On `next` | |
| --- | --- | --- |
| `orion` with `client`, `macros`, `transport-ipc`; `orion-client` with `ipc` | one crate, `orion`, with a feature for each part: `topics`, `records`, `actions` (default), `files`, `workloads`, `executor`, `provider`, `buffers`, `shell`, `tokio` | exists |
| `orion::core::{NodeId, ResourceId, ProviderId, ExecutorId, WorkloadId, ArtifactId, CapabilityId}` | names are checked text types in `orion::types`: `NodeName`, `ResourceName`, `TopicName`, `FileName`. There is no provider, executor, artifact or capability id | renamed, and four removed |
| `orion::Revision`, `Revision::ZERO` | a record's stamp (`HlcTimestamp`); a watch takes "everything newer than this stamp" | renamed |
| `PeerBaseUrl`, `orion+tcp://host:port` | `Address`: `orion+tcp://host[:port]`, with `#<key in hex>` to accept only that node | exists |
| `PublicKeyHex` | a node's key is 32 bytes in `Node::public_key`, with `Node::fingerprint` for people | renamed |
| Two sockets, `/run/orion/control.sock` and `control-stream.sock` | one, `/run/orion/orion.sock`. Requests and pushed changes share the session | removed |
| `LocalNodeRuntime::new(socket, stream_socket)`, `connect_at` on each client | `Orion::connect(&address, name)`: one handle, and each part hangs off it | renamed |
| `LocalServiceRetryPolicy`, sleep loops on `ClientError::NoMessageAvailable` | `Orion::connect` keeps trying until the comm core welcomes it. A call that fails with `is_lost()` means connect again; `is_busy()` means ask again; `is_held()` means another holds it. There is no "no message available" | removed |
| `#[orion_runtime_type("...")]`, `#[orion_resource_type("...")]` | gone. A workload's kind and a resource's kind are plain text the program chooses | removed |
| `orionctl` | `orion-ctl` ([`client.md`](client.md), "The command line"). Its verbs were not matched one by one for this note | renamed |

The session is blocking and owns no thread. With the `tokio` feature a program waits inside a
runtime. A `dev` program built on async streams of events becomes one that calls `poll` or
`wait` on each part it uses, or pumps the session in a task.

## Registering what a program is

On `dev` a program registered a `ProviderRecord` and an `ExecutorRecord`, each with an id and
the types it offered, through `LocalRuntimePublisher::register_all`.

On `next` there is nothing to register first. A program is what it does on its session.

| On `dev` | On `next` | |
| --- | --- | --- |
| `ProviderRecord`, `ProviderId`, `register_all` | removed. The session that writes a resource is its provider | removed |
| `ExecutorRecord` with a `runtime_type` | `executor().start(..)`: the program implements `Executor`, whose `kinds()` says which kinds of workload it runs | renamed |
| `publish_provider_resources(all of them)`, where leaving one out removed it | each resource is its own record: `provider.resource(name, kind)`, then labels, topics and actions, then `.publish(state)`. One is taken away by name. A resource whose provider's session ends is `Lost`, not gone | changed |
| `publish_executor_snapshot(workloads, derived)` | `Serving::turn`: the executor's `inventory()` is compared with what Orion was last told, and what changed is reported. There is no snapshot to assemble | changed |
| One program that is both | one session can be an executor and a provider: both features on, both parts on the one `Orion` | exists |

`examples/roles.rs` in the `orion` crate is every role in one file, and
`crates/testing/sample-executor` is the smallest working executor.

## Resources

A `dev` `ResourceRecord` carried a great deal: ownership mode, health, availability, lease
state, capabilities, endpoints, labels, and a state with a typed config payload and the last
action's result. A `next` resource record is small on purpose: name, kind, node, state, and
what it offers (labels, topics, actions). What changes goes in a topic.

| On `dev` | On `next` | |
| --- | --- | --- |
| `ProviderResource::new(id, type, provider)` | `provider.resource(name, kind)` | renamed |
| `.label("k=v")` | `.label(key, value)` | exists |
| `health`, `availability` | the resource's state: `Ready`, `Pending`, `Failed`, and `Lost` when its provider is gone | changed |
| `ownership_mode(Exclusive / SharedRead)`, `lease_state` | a lease is a record under the resource's name. One holder at a time; a resource nobody leases is shared. Whether a kind may be shared is the program's rule | yours |
| `ResourceCapability` with a `CapabilityId` and detail | a label, or an action whose presence says it | yours |
| `.endpoint("scheme://addr")`, `CustomEndpointScheme` | a label holding the address. For frames, see buffer topics below | yours |
| `state.config`: readings as `TypedConfigValue`s, republished with the whole record | a topic under the resource: `.topic("values", fields, ..)`, a field for each reading, with its type and unit. Subscribers are told when it changes; nothing is republished whole | changed |
| `state.action_result` on the record | the action's own result, to its caller. If others must see the last result, a topic | yours |
| `DerivedResource` with `realized_by_executor`, `realized_for_workload`, `source_workload` | an ordinary resource the program publishes, with those links as labels. Orion does not know one resource came from a workload | yours |

A value that changes rarely, a mode say, is declared with `OnSubscribe::Latest`, so a
subscriber that starts later is given it at once.

**Frames.** A `dev` program that handed out a Unix-socket endpoint for frames can use a
buffer topic: the frames go by descriptor, with no copy, and a subscriber on another board or
in a browser is sent them as messages ([`client.md`](client.md), "Buffer topics"). Whether to
move is the program's choice; an address in a label still works on the same board.

## Reading state

| On `dev` | On `next` | |
| --- | --- | --- |
| `fetch_state_snapshot()`, `StateSnapshot { desired, observed }` | `records().get(class, prefix)` for each class wanted: `Node`, `Resource`, `Lease`, `Workload`. There is one record for a thing, not a desired and an observed one | changed |
| `ControlPlaneEventStream::subscribe_state(Revision::ZERO)` | `records().watch(class, prefix, after)`: everything newer than a stamp, then each change as it is made. A watch that falls behind catches up by itself and misses no record | renamed |
| `resources` merged from both sides | `orion.resources(prefix)`: lists what is there, each with its topics and actions, with no code for the kind | exists |
| A workload's `observed_state` | `workloads().status(name)` and `.wait(..)`: pending, starting, running, stopping, gone, failed or lost | renamed |
| Leases told to a provider (`LocalProviderEvent::LeasesChanged`) | `records().watch(RecordClass::Lease, prefix, after)` | renamed |
| A single revision for "the state" | none. Each record has its own stamp | **gap** if the program needs one number that moves when anything does |

## Writing what should be

| On `dev` | On `next` | |
| --- | --- | --- |
| `apply_mutations(MutationBatch { base_revision, mutations })` | no batch and no base revision. Each thing is written by its own call | changed |
| `PutWorkload`, `RemoveWorkload` | `workloads().put(..)`, `.remove(name)`; `remove` with force for a run that will not die | renamed |
| `DesiredState::Running / Stopped` on a workload | a workload that should run exists; one that should not is removed. There is no stopped-but-kept workload | **gap** if the program keeps disabled workloads in Orion; otherwise it keeps them itself |
| `PutArtifact`, `RemoveArtifact`, `ArtifactRecord` with a content type, a size and labels | no artifact record. A file a workload needs is a file: pushed with `files().push(..)`, and named, with its SHA-256, in `workloads().put_needing(workload, files)`. Orion starts the workload only when each is in place | changed |
| An artifact's labels (a name, a revision) read back by other programs | the workload's spec, which only its executor reads, or a resource the program publishes | yours |
| `RemoveResource`, `RemoveProvider`, `RemoveExecutor` (cleanup after a node was renamed) | a resource is taken away by its provider, or is `Lost` when the provider is gone. There are no provider or executor records to remove | removed |
| Retrying on `Rejected` because the revision moved | nothing to retry: two writers of one record are settled by its stamp, the later write standing | **gap** if two writers must not overwrite each other unseen |

## Workloads and their config

| On `dev` | On `next` | |
| --- | --- | --- |
| `WorkloadRecord::builder(id, runtime_type, artifact_id)` | a workload has a name, a kind (which picks the executor), a spec, the files it needs, where it may run and what to do when it ends | changed |
| `WorkloadConfig` with dotted `TypedConfigValue` fields, `deserialize_config`, `ConfigDecodeError`, `config_json_value` | the spec is bytes that only the executor reads. Orion does not look inside. The program picks a format, JSON say, and decodes it itself | yours |
| `resource_bindings` on the workload | a lease on each resource the run uses alone, taken by the program; or names in the spec | yours |
| `assigned_node_id`, `.assigned_to(node)` | where it may run is part of the workload, and placement decides ([`node.md`](node.md), "Placement") | changed |
| `LocalExecutorService::subscribe_workloads`, `LocalExecutorEvent::Bootstrap / WorkloadsChanged` | the `Executor` trait: `start(name, spec)`, `stop(name)`, `kill(name)`, `inventory()`. Orion calls; the executor keeps no list of what should run | changed |
| A reconcile tick against the snapshot | none needed. After a reconnect `start` sends the whole inventory, and Orion takes back runs that carried on, by name | removed |

A graph that was sent inline in a workload's config can stay in the spec. One that is large,
or shared, is better as a file the workload needs: it is then checked by digest and is on the
board before the workload starts.

## Actions

| On `dev` | On `next` | |
| --- | --- | --- |
| `ActionCaller::connect_at(..).call(request, timeout)` | `orion.run(&ActionCall { name, target, args, deadline_ms, detached })`, which returns a `Run` at once; `run.finish(wait)` waits for how it ended | renamed |
| `ActionRequest::new(id, ActionTarget::Resource(id), kind)` | an action has a full name, `fan.left.set`: the resource's name and the action's. `target` says which node | changed |
| `request.args` as a map of `TypedConfigValue` | `args` is bytes. By convention, text: the words a person would type | yours |
| `ActionState::{Accepted, Running, Succeeded, Failed, Rejected, TimedOut}`, `is_terminal()` | `run.poll()` gives progress, then `Finished` with an outcome, its output, words, and the action's own status code | renamed |
| `watch_action_requests()`, `ActionReporter::{succeed, fail, reject}` | `actions.register(decl, policy)`, then `registration.poll()` for the next run, `.progress(..)` and `.finish(..)` | renamed |
| An action with no policy | not possible. Registering needs a policy, which is what the one check decides each run by | new |
| A caller that leaves | `detached: true` and `run.handle()`: the run carries on, and `attach_handle` comes back to it | new |

## Status entries and streams

Both are topics on `next`. There is no status lane and no stream catalog.

| On `dev` | On `next` | |
| --- | --- | --- |
| `StatusEntry { subject, key, value }` under `node/<id>` or `resource/<id>` | a field of a topic: `orion.host` for the board, `<resource>.<topic>` for a resource | changed |
| `subscribe_status(StatusQuery::all())` | subscribe to each topic wanted. A program that wants every resource's values lists the resources and subscribes to each one's topics | yours, and a **gap**: there is no subscription to every topic under a prefix |
| A status key that expires | a topic's `expiry_ms`: a value older than that is not handed out | exists |
| `StreamRef`, `StreamCatalog`, `SeriesDecl`, `SeriesKind` | a topic's declaration: its fields, each with a type and a unit | renamed |
| `SampleChunkBuilder`, chunks of samples | `publisher.publish(field, time, bytes)`, a sample at a time, each with its own time | changed |
| `StreamCursor { epoch, seq }`, resubscribing from a cursor, `Backfill::Since / LastMs` | a topic keeps `history` samples in its ring. A reader on the same board reads back through it. A subscriber sent messages starts from now, or from the newest sample with `OnSubscribe::Latest` | **gap** for a remote subscriber that must have the last N seconds, or carry on from where it was after a reconnect |
| `StreamReset` | a topic's publisher gone and back: `subscriber.status()` says waiting, live, or gone | renamed |

## A device library's devices

On `dev`, a device library's bridge wrote each device's readings as status entries under the
device's resource, and a program read them with a status query.

On `next` that bridge publishes each device as a resource, and a program reads and drives it
with nothing written for the kind:

- **To list them:** `orion.resources("<the bridge's prefix>")`. Each record names the
  device's labels, its topics and its actions.
- **To read one:** subscribe to its values topic. A field for each reading, typed, with a
  unit; the newest is given at once to a subscriber that starts later.
- **To call one:** `orion.run` of its action. The bridge's own docs say which actions a
  device has and what their arguments are (a set, a call by method name, power).
- **To push a file to one:** `files().push("<prefix>/<device>/<name>", ..)`. With
  `files.receivers` naming the prefix, a push made before the bridge is up waits for it.

A program that wrapped devices in resources of its own, to add an action Orion could route,
can stop: the bridge's resources take their actions directly.

## The board itself

| On `dev` | On `next` | |
| --- | --- | --- |
| `query_observability()`, `HostMetricsSnapshot` | the topic `orion.host`: `uptime_s`, `mem_total_kb`, `mem_available_kb`, `cpu_percent`, `temperature_c`. Read only while someone subscribes | **gap** for what it lacks: busy per core, load averages, a reading for each temperature sensor |
| `host.*` status keys | the same topic | changed |
| `NodeObservabilitySnapshot`: revisions, ready and configured peers | `orion.comm` (sessions, requests, panics), `orion.state.<part>`, and `sync_peers()` for where sync with each other board is | changed |
| `NodeRecord.clock` | the hello fact `clock` (`Node::clock`: `synced` or `unset`) and the topic `orion.time` | changed |
| A setting the build ignores | the hello fact `settings_unread` and a line in `orion/state` | new |

## Other boards

| On `dev` | On `next` | |
| --- | --- | --- |
| `query_discovery()`, `DiscoverySnapshot { backend, cluster, peers }` | the node records (`records().get(RecordClass::Node, "")`): each board of the cluster with its address and labels. `node()` gives this board's name, cluster and key | changed |
| `DiscoveredPeerRecord.state` (enrolled, discovered, revoked), `key_fingerprint` | the comm core keeps this list ([`comm.md`](comm.md), "Discovery"), with two more states. A call that gives it to a client was not found | **gap** |
| Finding boards from a computer | each board announces `<name>.local` and `_orion._tcp` by mDNS, with its key, cluster, state, security and clock in the record ([`comm.md`](comm.md), "Announced by name") | new |
| Changing a board's cluster by rewriting settings and restarting the service | `cluster` is a setting of both services; they are `orion-comm.service` and `orion-node.service` | changed |
| Relaying TCP to another board's `orion+tcp` port | the port is 9200 unless set. Reaching another board's topics and actions needs no relay: topics of other nodes are subscribed to here, and an action's `target` names the node | changed |

## NT4

The NT4 crate carried over. A program that runs its own client changes little.

| On `dev` | On `next` | |
| --- | --- | --- |
| `Client::start(ClientConfig::new(host, name))`, `client.handle()`, `.split()`, `next_event()` | the same | exists |
| `handle.publish(topic, type)`, `handle.set_properties(..)`, `Publisher::set(value)` | the same | exists |
| `handle.subscribe(names, SubscribeOptions { prefix, .. })`, `TopicEvent::Value` | the same | exists |
| `Server::start(ServerConfig { bind, .. })` in tests | the same. The server now keeps the time a client gives a value, where it stamped each on arrival | exists, changed |
| `handle.server_time_us() - latency` published as a value of its own | `Publisher::set_at(value, Some(time))`: the value itself carries when it was measured, on the server's clock | new |

**A capture time.** On `dev` a program worked out "the server's time now, less my latency" and
published that number as a topic beside the value. On `next` the value's own timestamp is the
capture time:

- A program with its own client calls `set_at(value, Some(capture_us))`, where `capture_us` is
  the server's time at capture (`server_time_us()` at capture, or now less the latency).
- A program that publishes through Orion's topics and lets the board's NT4 gateway carry them
  (`nt4.server`, `nt4.publish`) does nothing: the gateway stamps each value with its sample's
  time on the server's clock.
- A program that wants the number, for a log or another protocol, adds `nt4_offset_us` from
  the topic `orion.time` to a sample time on the board (in microseconds). That topic is there
  only with `time.sync` on. The offset is a shared base, not a date.

Whether to keep a client of its own or use the gateway is the program's choice. The gateway
maps fields of topics to NT4 names by settings and reconnects by itself; a client of its own
is needed for what the mapping does not do, such as struct schemas published by hand.

## Security, for a program on the board

A `dev` program passed nothing to Orion to say who it was, and that is still so. On the
board's socket the comm core knows a program by the account it runs as.

- **With security off**, the check admits everything, from anyone who can reach the board.
- **With security on**, every request passes the one check with the policy of what it asks
  for: an action's policy is the one its owner registered. A program on the board is not an
  operator: it has no grants, and what it may do is what each policy says of a local program.
  Node logic's role is kept to the accounts in `socket.node_uids`.
- A program that registers an action chooses its policy, and so decides whether operators
  need a grant for it.

The rules in full are in [`comm.md`](comm.md), "Keys, enrollment, operators". They were not
read line by line for this note: a port with security on should test each call it makes as
the account it will run as.

## The gaps, together

Things a `dev` program had that `next` has nothing for. Each is small to say and not yet
decided.

1. **A workload that exists but should not run.** `dev` had `DesiredState::Stopped`; `next`
   has a workload or no workload.
2. **Compare before write.** `dev` refused a write made against an old revision; `next`
   settles two writers by stamp, and the earlier write is lost without a word.
3. **One number for "something changed".** `dev` had a revision for the whole state.
4. **History for a remote subscriber.** `dev` streams could backfill the last N milliseconds
   and carry on from a cursor; a `next` subscriber sent messages starts from now or from the
   newest sample.
5. **More of the board's vital signs.** `orion.host` has one CPU figure and one temperature;
   `dev` had per-core, load averages and each sensor.
6. **The discovery list for a client:** which boards are seen, and which are enrolled,
   revoked or mismatched.
7. **A subscription to every topic under a prefix.** A program lists the resources and
   subscribes to each topic, and must do so again when one appears.
