# Actions

An action is a one-shot, named operation on a node, a provider, a resource, or an executor: reboot
a device, restart a unit, blink a locate LED, install an update. Orion carries the request to the
component that owns the target, tracks its lifecycle, and reports the result. Orion implements no
action itself: names are free-form, and handlers are supplied by the embedder (in-process) or by
local provider and executor clients (out of process).

## Wire shapes

`orion-control-plane` (`no_std` + `alloc`), control protocol v4:

```rust
pub enum ActionTarget {
    Node(NodeId),
    Provider(ProviderId),
    Resource(ResourceId),
    Executor(ExecutorId),
}

pub struct ActionRequest {
    pub action_id: String,       // client-chosen, unique among tracked actions
    pub target: ActionTarget,
    pub name: String,            // free-form; see the well-known names below
    pub args: BTreeMap<String, TypedConfigValue>,
    pub deadline_ms: u64,        // relative budget from acceptance; 0 = node default
    pub requested_by: String,    // replaced by the node with the authenticated requester
}

pub enum ActionState {
    Accepted,
    Running { progress: Option<u16> }, // per-mille, 0..=1000
    Succeeded,
    Failed { reason: String },
    Rejected { reason: String },
    TimedOut,
}

pub struct ActionResult {
    pub action_id: String,
    pub target: ActionTarget,
    pub name: String,
    pub state: ActionState,
    pub output: BTreeMap<String, TypedConfigValue>, // small, bounded by the node
    pub handled_by: NodeId,      // the node that owns the target
    pub requested_by: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

pub struct ActionReport {        // a handler's progress or outcome report
    pub action_id: String,
    pub state: ActionState,      // Running, Succeeded, Failed, or Rejected
    pub output: BTreeMap<String, TypedConfigValue>,
}

pub struct ActionQuery {
    pub action_id: Option<String>,
    pub target: Option<ActionTarget>,
}
```

Control messages: `RunAction(Box<ActionRequest>)` and `QueryActions(ActionQuery)` are answered
with `ActionResults(Vec<ActionResult>)`; `WatchActions(ActionQuery)` streams
`ClientEventKind::ActionResults` (newest result per action, coalesced for slow watchers);
`WatchActionRequests(Vec<ActionTarget>)` and `ClaimNodeActions(Vec<String>)` register a handler,
which receives `ClientEventKind::ActionRequest(Box<ActionRequest>)` events and answers with
`ReportActionResult(Box<ActionReport>)`. Between nodes, `RunAction` and `QueryActions` are answered
with `HttpResponsePayload::Actions(Vec<ActionResult>)` (HTTP route `/v1/control/actions`).

The existing `ResourceActionResult` / `ResourceActionStatus` describe the last action applied to a
resource inside `ResourceState` and are part of the frozen MCU link wire, so they are not reused as
the lifecycle record; `ActionResult::as_resource_action_result()` converts a final result for
handlers that also record it on the resource.

## Lifecycle

```text
Accepted ──> Running { progress } ──> Succeeded | Failed { reason }
    │                                       ^
    └──> Rejected { reason }  (no handler, unknown target, refused by the handler)
    └──> TimedOut              (deadline passed before a final report)
```

- The node that receives a `RunAction` validates it, stamps `requested_by` (`local:<client name>`
  for local clients, `local:<name>` for `NodeApp::run_action`, `peer:<node>/<original>` for a
  forwarded request), resolves the deadline (`0` = `ORION_NODE_ACTION_DEFAULT_DEADLINE_MS`, capped
  at `ORION_NODE_ACTION_MAX_DEADLINE_MS`), routes it, and returns the current result.
- `action_id` is client-chosen. Resubmitting the same id with the same target, name and arguments
  returns the existing result (safe retries); a different request with a used id is refused.
- Routing failures are recorded as `Rejected` results (queryable like any other); malformed
  requests and a full registry are errors.
- A final state never changes: late reports (after a timeout, say) are ignored.
- Results are held in memory, bounded by `ORION_NODE_ACTION_MAX_TRACKED`, and dropped
  `ORION_NODE_ACTION_RESULT_TTL_MS` after they finish. Nothing is persisted.

## Routing

The owner of a target is: the node itself for `Node(id)`; the provider record's node for
`Provider`; the executor record's node for `Executor`; and for `Resource`, the node of the
resource's provider (or of the executor that realizes it).

On the owning node:

| Target | Handler |
| --- | --- |
| `Node(local)` | The in-process `ActionHandler` registered for the name with `NodeAppBuilder::with_action_handler`, else the local client that claimed the name with `ClaimNodeActions`. |
| `Provider(id)` | The local client registered as the provider's handler (`WatchActionRequests`). |
| `Executor(id)` | The local client registered as the executor's handler. |
| `Resource(id)` | The handler of the resource's provider, else the handler of the executor that realizes it. |

No handler means `Rejected` with a reason naming the missing handler. Orion registers no handler
by default, so every action is rejected until the embedder or a local client provides one.

**In-process handlers** implement `orion_node::actions::ActionHandler` (any `Fn(ActionRequest,
ActionContext) -> impl Future<Output = ActionOutcome>` closure does). The node runs the future on
its runtime, aborts it at the deadline, and records `ActionOutcome::Succeeded(output)`,
`Failed(reason)` or `Rejected(reason)`; `ActionContext::progress` reports `Running`.

**Out-of-process handlers** are local provider or executor clients (`orion-client`):

- `LocalProviderService::watch_action_requests()` / `LocalExecutorService::watch_action_requests()`
  register the service as the handler of its provider or executor (the provider must be
  registered on this node; the latest registration wins, like provider state).
- `claim_node_actions(["update", "reboot"])` claims node-targeted action names, for example for a
  device-manager daemon. **Conflict rule:** a name that has an in-process handler cannot be claimed
  (in-process handlers own their names), and a name claimed by another connected client is
  refused; the holder may claim again.
- The returned `ActionRequestWatch` yields requests (`next()`) and reports with `progress`,
  `succeed`, `fail`, `reject`, or `report(ActionReport)`; only the client a request was delivered
  to may report on it.
- When the handler's stream disconnects (or its session expires), the node releases its
  registrations and claims and fails the actions still waiting for it with `Failed { reason:
  "handler disconnected" }`. The watch re-registers when it reconnects.

### Across nodes

When the owner is another node, the submitting node forwards the request **once** over the
signed peer transport (`orion+tcp` or HTTP(S), the same `PeerSyncTransport` request/response that
peer sync uses) and then polls the owner with `QueryActions` (100 ms, doubling to 1 s) until the
result is final, mirroring the owner's state and output into its own record. Its local deadline is
the request's deadline plus a 2 s grace, so the owner's own `TimedOut` arrives first. The owner
never forwards again (one hop), so a stale view of who owns a target produces a `Rejected` result
instead of a loop. A failed forward (unreachable peer, refused authentication) is recorded as
`Failed`.

This was chosen over a desired-state intent because actions are imperative and short-lived: an
intent would be persisted, replicated to every node and would need its own garbage collection, and
a node that comes back long after the deadline would still act on it. A direct signed exchange
fails fast and leaves no residue.

The owner answers a peer's `QueryActions` only with actions that peer forwarded.

## Authorization

| Caller | May |
| --- | --- |
| Local client, role `ControlPlane` (`orionctl`, operators) | `RunAction`, `QueryActions`, `WatchActions` |
| Local client, role `Provider` or `Executor` | `WatchActionRequests` (its local providers or executors, matching role), `ClaimNodeActions`, `ReportActionResult` (for actions delivered to it) |
| Peer node | `RunAction`, `QueryActions` and (forwarded) `QueryStatus` only when the request is **authenticated** (ed25519 signed) **and** the peer is **enrolled** (configured, pinned, or enrolled through discovery), whatever `ORION_NODE_PEER_AUTH` says; with `disabled` peer authentication, peers cannot submit actions. |
| Remote operator (`operator:<name>`, [remote-operator.md](remote-operator.md)) | `RunAction` for action names matching its policy (`*`, `prefix*`, exact; the node default is `ORION_NODE_OPERATOR_ACTIONS`, empty by default); `QueryActions` (every action with read access, else its own); `requested_by` is the operator id (`peer:<node>/operator:<name>` on the owner of a forwarded action). Only enrolled operators, over the signed peer transports. |
| Anyone else (`orionctl --http`, unauthenticated or unknown peers, unenrolled operators) | Nothing. |

Local IPC identity checks (`ORION_NODE_LOCAL_AUTH`) apply as for every local message. Use the
signed peer transport's integrity guarantees (see "Threat model" in [peer-sync.md](peer-sync.md)):
`orion+tcp` signs requests and responses but does not encrypt action arguments.

Authorization of a remote operator happens on the node it is connected to; the node that owns a
forwarded action's target authorizes the forwarding peer, as for every forwarded action.

## Actions that restart the node

Action records live in node memory, so an action whose handler reboots the node (or restarts
`orion-node`) cannot report its final result after the reboot, and the record is gone. Handlers
should:

1. report `Succeeded` with output such as `phase = "rebooting"` (or `Running` with progress)
   **before** they reboot, so the requester knows the action reached that point;
2. let the durable outcome come from facts: the node's observed `NodeRecord::host` (`os_version`,
   `image_version`, `boot_id`) is republished after the restart and replicated to peers (see
   [host-facts.md](host-facts.md)); and/or
3. republish the action's status-lane keys after the restart (below).

## Status-lane key convention

Handlers may mirror an action's lifecycle into the volatile status lane, under the action's target
as subject (`ActionTarget::status_subject()`), so watchers of the status lane (`orionctl get
status`, `watch_status`) see it and a handler can republish it after a restart:

| Key | Value |
| --- | --- |
| `action.<action_id>.state` | `String`: `accepted`, `running`, `succeeded`, `failed`, `rejected`, or `timed_out` |
| `action.<action_id>.progress` | `UInt`: per-mille, 0 to 1000 (like `ActionState::Running`) |
| `action.<action_id>.error` | `String`: the failure or rejection reason |

The status lane is node-local, but a `QueryStatus` whose subject another node owns is forwarded
once to that node (from local control-plane clients and remote operators alike; see "Status
queries across nodes" in [remote-operator.md](remote-operator.md)), so watchers can follow an
action's keys on its owner through any node.

`ActionResult::status_entries()` builds these entries and `ActionRequestWatch::publish_action_status`
publishes them. Status-lane ownership applies: a provider publishes for its provider and resources,
an executor for its executor; a client that holds a node action claim may publish `action.*` keys
(only those) for `node/<local node>`. The keys live for their TTL like any status entry.

## Well-known action names

Orion implements none of these; handlers that implement one follow its convention
(`orion_control_plane::action_names`):

| Name | Arguments |
| --- | --- |
| `reboot` | optional `delay_ms` (`UInt`), `reason` (`String`) |
| `restart-unit` | `unit` (`String`, required) |
| `locate` | optional `duration_ms` (`UInt`), `enabled` (`Bool`, `false` stops it) |
| `self-test` | optional `level` (`String`, handler-defined) |
| `update` | `bundle_url` (`String`) or `transfer_id` (`String`) naming the bundle, `sha256` (`String`, hex digest), `size` (`UInt`, bytes). Typically claimed by a device-manager client; reports `Succeeded` with `phase = "rebooting"` before rebooting, and the image version in the host facts is the durable outcome. |

## Limits

Action ids, names, argument and output keys are at most 128 bytes; at most 32 arguments and 32
output entries; string and byte values at most 1024 bytes. Larger output entries are dropped.

| Variable | Default |
| --- | --- |
| `ORION_NODE_ACTION_DEFAULT_DEADLINE_MS` | `30000` |
| `ORION_NODE_ACTION_MAX_DEADLINE_MS` | `600000` |
| `ORION_NODE_ACTION_RESULT_TTL_MS` | `600000` |
| `ORION_NODE_ACTION_MAX_TRACKED` | `256` |

## Surfaces

```text
orionctl action run node/node-a reboot --arg delay_ms=5000 --wait
orionctl action run resource/camera.front locate --arg duration_ms=10000
orionctl action run node/node-b update --arg transfer_id=t-17 --arg sha256=string:ab12... --wait -o json
orionctl get actions [--target resource/camera.front] [--id <action id>] [-o json|yaml|toml]
```

`--arg KEY=VALUE` infers `bool`, `uint`, `int`, then `string`; force a type with
`KEY=TYPE:VALUE` (`bool`, `int`, `uint`, `string`, `hex`). `--wait` polls until the action is final
and exits non-zero unless it succeeded. Both commands use the local socket only.

`orion-client`:

- control plane: `LocalControlPlaneClient::{run_action, query_actions, wait_for_action}`,
  `ControlPlaneEventStream::subscribe_actions`, `ActionWatch::connect_at(..).next()`;
- providers and executors: `watch_action_requests`, `claim_node_actions`, `ActionRequestWatch`.

`orion-node`: `NodeAppBuilder::with_action_handler`, `NodeApp::{run_action, query_actions,
action_handler_names}`, `orion_node::actions::{ActionHandler, ActionContext, ActionOutcome,
ActionFuture}`, `ActionTuning` (`NodeRuntimeTuning::actions`).
