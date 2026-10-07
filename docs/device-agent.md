# Device Agents

A device agent is a small daemon shipped by a device package (not by Orion) that connects to
`orion-node`'s local IPC, claims node-level actions such as `update`, `reboot` and `locate`, runs
them with the package's own tools, and keeps the durable outcome of the last update visible on the
status lane. Atlas's `pd-device-agent` (Atlas `docs/ota.md`) is the first one. This page is the
contract between such an agent, `orion-node` (control protocol v4), and the tools that drive it
(Atlas through the remote operator client, `orionctl`, fleet tools).

Orion implements none of the actions and never touches disks: image bytes come from the URL the
requester names, and the agent's writer verifies them. See [actions.md](actions.md) for the general
action mechanism and [update-recovery.md](update-recovery.md) for the larger update design.

An example agent with a fake updater is in
[`crates/client/examples/device_agent/`](../crates/client/examples/device_agent/agent.rs), and
`crates/node/tests/device_agent.rs` runs it against a real node.

## Connecting

- The agent is an `orion-client` provider (or executor) client on the node's sockets
  (`/run/orion/control.sock`, `/run/orion/control-stream.sock` in the packaged service). It does
  **not** need to register a provider: claims only need a session.
- Access: run it in the node's group (`Group=orion` or `SupplementaryGroups=orion`) with
  `ORION_NODE_LOCAL_AUTH=same-user-or-group`, or list it in `ORION_NODE_LOCAL_AUTH_ALLOW` (see
  "Local IPC access" in [node-env.md](node-env.md)).
- Order it `After=orion-node.service` with `Wants=orion-node.service`, but let it start without
  the node: it retries until the sockets accept (the example uses a 1 s fixed-delay retry policy
  without a limit). Nothing on the device may depend on the agent being connected.

```rust
let service = LocalProviderService::new(runtime, "pd-device-agent",
        ProviderRecord::builder(ProviderId::new("pd-device-agent"), node_id).build())
    .with_retry_policy(LocalServiceRetryPolicy::fixed_delay(Duration::from_secs(1)));
let mut watch = service.claim_node_actions(["update", "reboot", "locate"]).await?;
let reporter = watch.reporter();      // clone into tasks; reports while `next()` waits
loop {
    let request = watch.next().await?; // reconnects and re-claims by itself
    handle(&reporter, request).await;
}
```

## Claiming node actions

`claim_node_actions(names)` sends `ClaimNodeActions(names)` on the client's event stream.

| Rule | Behaviour |
| --- | --- |
| Target | `ActionTarget::Node(<this node>)` requests with a claimed name go to the claimant. Requests for other nodes are forwarded to their owner first (one hop), so remote requesters reach the agent too. |
| Conflicts | A name with an in-process handler (`NodeAppBuilder::with_action_handler`) cannot be claimed. A name held by another connected client is refused; the holder may claim again. |
| Pending | Requests accepted while no claimant was connected are not queued for later: with no handler they are `Rejected` at once. Requests already delivered to the claimant but not reported are redelivered when it claims again on the same connection name. |
| Release | Claims are released when the claimant's stream disconnects or its session expires. `ActionRequestWatch::next` reconnects with the service's retry policy and claims again; `ActionRequestWatch::reconnects()` counts this. |

## Action names and arguments

Constants in `orion_control_plane::{action_names, update_action}`.

| Action | Arguments | Result |
| --- | --- | --- |
| `update` | `image_url` (`String`, required: the agent downloads it), `sha256` (`String`, 64 hex digits, of the file as served), `size` (`UInt`, bytes). `transfer_id` (`String`) is reserved for in-band transfers. | `Succeeded` with output `phase = "rebooting"` and `version_staged` (`String`) once the image is staged **and** the switch to it was issued. `Rejected` for invalid arguments, `Failed { reason }` when staging fails. |
| `reboot` | optional `delay_ms` (`UInt`), `reason` (`String`) | `Succeeded` with `phase = "rebooting"`, reported **before** rebooting. |
| `locate` | optional `duration_ms` (`UInt`), `enabled` (`Bool`, `false` stops it) | `Succeeded` once the LED pattern is running (or stopped). |

`orionctl` passes them with `--arg`:

```text
orionctl action run node/raze-1 update --arg image_url=http://10.0.0.5:8080/raze-2.0.img.xz \
    --arg sha256=string:<64 hex digits> --arg size=734003200 --deadline-ms 900000 --wait
```

### Result semantics: "staged and apply issued"

Action records live in `orion-node`'s memory and do not survive the reboot that finishes an
update. So `update` ends `Succeeded` just before the agent reboots into the trial slot, and that
means only: the image was downloaded, verified, written to the inactive slot, and the trial boot
was issued. It does **not** mean the new image booted or was confirmed. The outcome comes from
durable facts after the reboot:

1. the node's host facts (`NodeRecord::host`: `boot_id` changes, `image_version` / `os_version`
   show the running image; replicated to peers, see [host-facts.md](host-facts.md));
2. the `update.*` status keys the agent republishes after every boot (below):
   `update.state = confirmed` with the new `update.version_active` means success,
   `rolled-back` means the trial boot was not confirmed and the old slot runs again.

If the switch fails after `Succeeded` was reported (for example a `pre-reboot` hook refuses), the
image stays staged and the agent publishes `update.state = staged` with `update.error`.

### Deadlines

The node default deadline is 30 s (`ORION_NODE_ACTION_DEFAULT_DEADLINE_MS`); an `update` that
downloads and writes an image needs minutes. Requesters set `deadline_ms` on the request (Atlas:
the expected transfer time plus margin), capped by `ORION_NODE_ACTION_MAX_DEADLINE_MS` (default
10 minutes; raise it in the image's environment file for slow links). When the deadline passes the
action becomes `TimedOut` and later reports are ignored, but the agent keeps going: the `update.*`
keys still show the real progress and outcome.

### Idempotency

Resubmitting the same `action_id` with the same arguments returns the existing record while the
node remembers it (`ORION_NODE_ACTION_RESULT_TTL_MS`). After a reboot or node restart the record is
gone, so a requester that retries sends a new request; the agent's writer must treat a request for
an image that is already staged (same `sha256`) or already running as done and succeed at once.

## Progress while an action runs

Three channels, all optional for the agent, all used by the example:

| Channel | Content | Who sees it |
| --- | --- | --- |
| `ActionState::Running { progress }` (`reporter.progress(id, Some(per_mille))`) | per mille, 0 to 1000 | `QueryActions`, `WatchActions`, `orionctl get actions`, remote operators (`RemoteOperator::query_action`) |
| `node/<id>` keys `action.<action_id>.state` (`accepted`, `running`, `succeeded`, `failed`, `rejected`, `timed_out`), `action.<action_id>.progress` (`UInt` per mille), `action.<action_id>.error` (`String`) | the action's lifecycle, the [actions.md](actions.md) convention | status-lane queries and watches |
| `node/<id>` key `update.progress` | progress of the updater's current step | same |

## Status keys under the Node subject

A client that holds a node action claim may publish, for the subject `node/<this node>`:

- `action.*` keys (any action id), and
- `<name>.*` keys for every node action `<name>` it claimed: the holder of `update` owns
  `update.*`, the holder of `locate` owns `locate.*`, and so on.

Everything else under `node/<id>` (for example the node's own `host.*` metrics) is refused with an
authorization error. Publish with `ActionReporter::publish_status` /
`ActionRequestWatch::publish_status` and `node_status_entry(key, value)`.

### The `update.*` keys

Published by the holder of `update` (`orion_control_plane::update_action::KEY_*`). Orion stores and
forwards them; it does not interpret them.

| Key | Value |
| --- | --- |
| `update.state` | `String`, the writer's state: `idle`, `staging`, `staged`, `trying`, `confirmed`, `rolled-back` (Atlas's `pd-device-update` vocabulary) |
| `update.version_active` | `String`, version of the running image |
| `update.version_staged` | `String`, version in the staged slot (empty when none) |
| `update.slot_active` | `String`, for example `A` |
| `update.slot_staged` | `String` (empty when none) |
| `update.progress` | `UInt`, per mille, of the current step |
| `update.error` | `String`, the last error (empty when none) |
| `update.boot_id` | `String`, the kernel boot id the agent published them in, so readers can match them against `NodeRecord::host.boot_id` and ignore leftovers |

### Surviving reboots

Status entries live in node memory only, for their TTL, and are not replicated. The keys are
"stable" because the agent keeps them published:

1. after it connects (every boot, and every agent restart), it publishes all `update.*` keys from
   the writer's persistent state (`update status`);
2. it republishes them periodically (the example: every 30 s) with TTL `0`, which means the node
   maximum (`ORION_NODE_STATUS_MAX_TTL_MS`, default 5 minutes), so they never lapse while it runs;
3. the periodic republish also restores them within one interval after `orion-node` restarts
   (the claim is re-established by `next()` first; a publish before that is refused and simply
   retried at the next tick);
4. it publishes them again on every state change.

Readers query them on the node itself, or through any peer: a `QueryStatus` for `node/<id>` is
forwarded once to the owning node (local control-plane clients and remote operators alike):

```text
orionctl get status --subject node/raze-1 --key-prefix update.
```

```rust
// Atlas, through the remote operator client:
let entries = operator.status(StatusQuery::subject(StatusSubject::Node(node_id))
    .with_key_prefix("update.")).await?;
```

## Disconnects

When the agent's stream drops (agent crash, agent restart, the reboot itself, an `orion-node`
restart):

- its claims are released, so new requests for those names are `Rejected` ("no handler") until it
  claims again;
- actions delivered to it and not yet final become `Failed { reason: "handler disconnected" }`
  (an `update` that already reported `Succeeded` before rebooting is unaffected);
- its status entries stay until their TTL runs out; they are not removed on disconnect, so the
  last known `update.*` state remains readable for up to `ORION_NODE_STATUS_MAX_TTL_MS` while the
  device reboots. Compare `update.boot_id` with the node's current `boot_id` to tell them apart
  from the new boot's;
- `orion-node` itself keeps running; nothing else on the device is affected.

## From here to durable update records (milestone U3)

This contract needs no protocol change. Milestone U3 of [update-recovery.md](update-recovery.md)
adds durable, replicated records: an `UpdateIntentRecord` desired-state section (what a node should
run, written by the requester) and an `UpdateStatusRecord` in each node's observed slice (phase,
installed and previous version, last error), plus `orionctl get updates`, requester authorization
(`ORION_NODE_UPDATE_REQUESTERS`) and audit records. It is not a small step: it needs a
control-protocol bump (batched with other layout changes), a new desired section with HLC stamps
and tombstones, the observed-slice fold, and U4's delivery loop to be useful. When it lands:

- the node folds the agent's `update.*` keys into its `UpdateStatusRecord` (or the agent reports
  the phase directly), so the outcome replicates to peers and survives the status-lane TTL;
- U4 delivers intents as `update` actions keyed by `(intent, version, generation)` with
  re-delivery after reboots, so requesters stop sending actions themselves;
- the action names, arguments, result semantics and `update.*` keys above stay valid, so an agent
  written against this page keeps working, and Atlas's Orion capability changes only where it reads
  the outcome.
