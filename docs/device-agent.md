# Device Agents

A device agent is a small daemon shipped by a device package (not by Orion) that connects to
`orion-node`'s local IPC, claims node-level actions such as `update`, `update.cancel`,
`update.rollback`, `reboot` and `locate`, runs
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
let mut watch = service
    .claim_node_actions(["update", "update.cancel", "update.rollback", "reboot", "locate"])
    .await?;
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
| `update` | `image_url` (`String`, required: the agent downloads it), `sha256` (`String`, 64 hex digits, of the file as served), `size` (`UInt`, bytes). `transfer_id` (`String`) is reserved for in-band transfers. | **Asynchronous.** `Succeeded` with output `phase = "staging"` once the download and stage have **started**. `Rejected` for invalid arguments, or while another image (other `sha256`) is staging. A request for the image already staging succeeds the same way (a retry). |
| `update.cancel` | none | Aborts a download or stage in progress, or forgets a staged update: `Succeeded` with `phase = "cancelled"` (`update.state` becomes `cancelled`), or `phase = "idle"` when there was nothing to cancel. |
| `update.rollback` | none | Boots back to the previous confirmed slot: `Succeeded` with `phase = "rebooting"`, reported **before** rebooting. `Rejected` while an update is staging (cancel it first) or when there is no previous confirmed slot. |
| `reboot` | optional `delay_ms` (`UInt`), `reason` (`String`) | `Succeeded` with `phase = "rebooting"`, reported **before** rebooting. |
| `locate` | optional `duration_ms` (`UInt`), `enabled` (`Bool`, `false` stops it) | `Succeeded` once the LED pattern is running (or stopped). |

The agent claims all five (the example does); `update.cancel` and `update.rollback` belong with
`update`, because only the `update` holder knows what is staging.

`orionctl` passes them with `--arg`:

```text
orionctl action run node/raze-1 update --arg image_url=http://10.0.0.5:8080/raze-2.0.img.xz \
    --arg sha256=string:<64 hex digits> --arg size=734003200 --wait
orionctl get status --subject node/raze-1 --key-prefix update.     # follow the outcome
orionctl action run node/raze-1 update.cancel --wait
orionctl action run node/raze-1 update.rollback --wait
```

### Result semantics: `update` is asynchronous

A large image over a slow link can take longer than the longest action deadline the node accepts
(`ORION_NODE_ACTION_MAX_DEADLINE_MS`, 10 minutes by default), and action records live in
`orion-node`'s memory, so they would not survive the reboot that finishes an update anyway. So
`update` ends `Succeeded` with `phase = "staging"` as soon as the agent has started the download
and stage in the background. That means only "started"; it says nothing about the image. The
outcome comes only from durable facts:

1. the `update.*` status keys (below), which the agent publishes on every change and republishes
   after every boot. `update.state` moves through

   ```text
   staging -> staged -> rebooting -> trying -> confirmed      (success)
                                            -> rolled-back    (trial not confirmed)
   staging | staged -> cancelled                              (update.cancel)
   staging -> error                                           (download, checksum or write failed; update.error)
   ```

   `update.version_active` is the running image's version and `update.error` the last error;
2. the node's host facts (`NodeRecord::host`: `boot_id` changes on the reboot, `image_version` /
   `os_version` show the running image; replicated to peers, see [host-facts.md](host-facts.md)).

A requester such as Atlas therefore treats the action result as "accepted for staging", then
follows `update.state` (status-lane watch or periodic `QueryStatus`) until `confirmed`,
`rolled-back`, `cancelled` or `error`, and matches `update.boot_id` against the host facts'
`boot_id` to know the keys describe the current boot.

If the switch fails after staging (for example a `pre-reboot` hook refuses), the image stays staged
and the agent publishes `update.state = staged` with `update.error`.

### Deadlines

All five actions finish quickly (`update` only starts the work), so the node default deadline
(`ORION_NODE_ACTION_DEFAULT_DEADLINE_MS`, 30 s) is enough and requesters need not set
`deadline_ms`. Time limits for the transfer itself belong to the agent's writer and the requester's
watch on `update.state`.

### Idempotency

Resubmitting the same `action_id` with the same arguments returns the existing record while the
node remembers it (`ORION_NODE_ACTION_RESULT_TTL_MS`). After a reboot or node restart the record is
gone, so a requester that retries sends a new request. The agent treats an `update` for the image
that is already staging (same `sha256`) as started, and its writer should treat an image that is
already staged or running as done. `update.cancel` with nothing to cancel succeeds with
`phase = "idle"`.

## Progress while an action runs

The actions themselves are short; the long-running work (the stage) reports through the status
lane:

| Channel | Content | Who sees it |
| --- | --- | --- |
| `node/<id>` keys `update.state` and `update.progress` (`UInt` per mille of the current step) | the stage's progress and outcome | status-lane queries and watches, through any node (`QueryStatus` is forwarded to the owner) |
| `node/<id>` keys `action.<action_id>.state` (`accepted`, `running`, `succeeded`, `failed`, `rejected`, `timed_out`), `action.<action_id>.progress`, `action.<action_id>.error` | each action's lifecycle, the [actions.md](actions.md) convention | same |
| `ActionState::Running { progress }` (`reporter.progress(id, Some(per_mille))`) | optional, for handlers of other long actions | `QueryActions`, `WatchActions`, `orionctl get actions`, remote operators |

## Status keys under the Node subject

A client that holds a node action claim may publish, for the subject `node/<this node>`:

- `action.*` keys (any action id), and
- `<name>.*` keys for every node action `<name>` it claimed: the holder of `update` owns
  `update.*` (which includes `update.cancel.*` and `update.rollback.*`), the holder of `locate`
  owns `locate.*`, and so on. Holding only `update.cancel` grants `update.cancel.*`, not
  `update.state`.

Everything else under `node/<id>` (for example the node's own `host.*` metrics) is refused with an
authorization error. Publish with `ActionReporter::publish_status` /
`ActionRequestWatch::publish_status` and `node_status_entry(key, value)`.

### The `update.*` keys

Published by the holder of `update` (`orion_control_plane::update_action::KEY_*`). Orion stores and
forwards them; it does not interpret them.

| Key | Value |
| --- | --- |
| `update.state` | `String`: `idle`, `staging`, `staged`, `rebooting`, `trying`, `confirmed`, `rolled-back`, `cancelled`, `error` (`update_action::STATE_*`) |
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
  (an `update` has already reported `Succeeded` once staging started; whether the stage survives
  an agent restart is up to the writer, and `update.state` tells);
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
