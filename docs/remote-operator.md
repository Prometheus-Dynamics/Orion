# Remote Operators

A remote operator is a desktop or fleet tool (a hardware manager, a provisioning station, a
dashboard backend) that talks to `orion-node` over the network **without running a node**. It
lists the cluster's nodes with their host facts, reads the status lane and observability, and runs
actions. Orion ships the client as the `remote` feature of `orion-client`
(`orion_client::remote`, also re-exported as `orion::remote` with the facade's `remote` feature).

```text
   desktop tool                     node-a                          node-b
  RemoteOperator  -- orion+tcp -->  operator principal   -- peer -->  owner of the
  (operator:alice)  signed both     (authz per operator)   forward    action target
                    directions       converged state
```

Operators are a **principal kind of their own**, distinct from peer nodes:

- they authenticate like peers (ed25519, signed `AuthenticatedPeerRequest`, fresh nonces, signed
  responses bound to the request), but with an `operator:<name>` principal id whose key lives in
  the node's operator trust store, never in the peer trust store;
- they are **never cluster members**: nodes do not sync with them, keep no state for them in
  desired or observed state, do not count them for liveness or placement, do not list them as
  peers, and refuse every sync message, desired-state write and observed update from them;
- what they may do is decided **per operator** by an `OperatorPolicy`.

## Client API

```toml
orion-client = { version = "1.0", default-features = false, features = ["remote"] }
# optional mDNS browsing: features = ["discovery"]
```

```rust
use orion_client::remote::{
    ActionRequest, ActionTarget, NodeTrust, OperatorIdentity, RemoteOperator, StatusQuery,
};

// Identity: an ed25519 key. The caller owns storage (keyring, a 0600 file, ...).
let identity = OperatorIdentity::generate("alice")?;            // or from_secret_key_bytes
let secret: [u8; 32] = identity.secret_key_bytes();              // store this
println!("{} {}", identity.operator_id(), identity.fingerprint()); // operator:alice sha256:...

let operator = RemoteOperator::connect("orion+tcp://10.0.0.2:9200", identity, NodeTrust::FirstUse).await?;
if !operator.is_enrolled() {
    operator.enroll_with_key(b"<ORION_NODE_ENROLLMENT_KEY>").await?; // or: administrator approval
}
let nodes = operator.nodes().await?;                    // NodeRecord incl. host + clock facts
let node_b = operator.node(&"node-b".into()).await?;
let status = operator.status(StatusQuery::default()).await?;
let observability = operator.observability().await?;
let result = operator
    .run_action(ActionRequest::new("reboot-42", ActionTarget::Node("node-b".into()), "reboot"))
    .await?;                                            // node-a forwards to node-b
let done = operator.wait_for_action("reboot-42", std::time::Duration::from_secs(60)).await?;
```

| Item | Purpose |
| --- | --- |
| `OperatorIdentity::{generate, from_secret_key_bytes, secret_key_bytes, operator_id, principal, public_key, public_key_hex, fingerprint}` | ed25519 identity; the fingerprint has the same `sha256:<32 hex>` format as discovery and `orionctl`. |
| `NodeTrust::{FirstUse, Key([u8; 32]), Node { node_id, public_key }}` | Which node key responses must verify against. |
| `RemoteOperator::{connect, connect_with}` | Opens the session (`OperatorHello`); succeeds for unenrolled operators so they can enroll. |
| `RemoteOperatorConfig { io_timeout, max_payload_bytes, poll_interval }` | Defaults: 5 s, 8 MiB, 500 ms. |
| `node_id`, `node_public_key`, `node_fingerprint`, `welcome`, `is_enrolled`, `hello` | Who the node is and what the operator may do there (`OperatorWelcome`). |
| `enroll_with_key(&[u8])` | Shared-key enrollment (below). |
| `state_snapshot`, `nodes`, `node(id)` | Converged cluster state of the connected node; node records merged like `orionctl get nodes`. |
| `status(StatusQuery)`, `watch_status(StatusQuery)` | Status-lane entries; queries for a subject another node owns are forwarded to it (below). |
| `run_action`, `call_action(request, timeout)`, `action(id)` / `query_action(id)`, `query_actions`, `wait_for_action(id, timeout)`, `watch_actions(query)` | Actions ([actions.md](actions.md)); `call_action` waits for the result without polling. |
| `observability()` | `NodeObservabilitySnapshot` of the connected node. |
| `discovery::browse_nodes(duration, cluster)` (feature `discovery`) | mDNS browse of `_orion._tcp`; returns `DiscoveredNode { advertisement, key_fingerprint, urls, compatible }`. |
| `RemoteError` | `Transport`, `ProtocolMismatch`, `NodeAuthentication`, `Rejected` (signed refusal; `is_not_enrolled()`), `Enrollment`, `ActionTimeout`, ... |

**Watches poll.** `orion+tcp` is request/response only, so `watch_status` and `watch_actions`
re-query every `poll_interval` (500 ms by default, `with_interval` changes it, minimum 50 ms) and
yield when something changed: `RemoteStatusWatch::next` returns the full matching set when it
differs from the previous poll, `RemoteActionWatch::next` returns the results that are new or
changed. `wait_for_action` polls the same way. Each poll is one signed request. `call_action` does
not poll: it sends `RunAction` with `wait_ms`, and the node answers once the action is final (see
"Waiting for the result" in [actions.md](actions.md)).

A transport adapter for a consumer with a `nodes() / status(query) / run_action(request) /
query_action(id)` interface maps one to one onto `RemoteOperator` (each wrapped in
`Result<_, RemoteError>`).

**Cross-node.** The connected node answers with its own converged view: `nodes()` lists every
node it syncs with (their observed records, host and clock facts replicate with each node's
observed slice). An action whose target another node owns is forwarded once over the signed peer
transport; that node sees `requested_by = peer:<connected node>/operator:<name>`. Observability
is per node: connect to a node to read its own.

### Status queries across nodes

The status lane is volatile and **node-local**: entries live only on the node they were published
on (host metrics on the node itself, provider and action keys on the node that hosts the provider).
So a `QueryStatus` whose subject another node owns is **forwarded once** over the signed peer
transport, the same way as a cross-node action:

| Subject | Owner |
| --- | --- |
| `node/<id>` | that node |
| `provider/<id>`, `executor/<id>` | the node of the provider or executor record |
| `resource/<id>` | the node of the resource's provider (or of the executor that realizes it) |
| `workload/<id>` | the node the workload is assigned to |

The owner answers from its own lane and never forwards again; peers' `QueryStatus` needs an
authenticated, enrolled peer. A query without a subject, or for a subject the connected node owns,
is answered locally. A subject whose owner is unknown in the connected node's converged state, or
whose owner is not a configured peer of it, is also answered locally, which normally yields **no
entries** (not an error). The same routing applies to `QueryStatus` from local control-plane
clients (`orionctl get status --subject node/<other node>`). Operators need read access.
`watch_status` polls through the same path. Forwarding needs the multi-threaded runtime that
`orion-node` uses; an embedder that serves requests on a current-thread runtime gets an error for
remote subjects.

### How fresh node records are

Each node publishes its own `NodeRecord` (health, clock and host facts) in its observed slice.
Host facts are sampled immediately when the node starts, then every
`ORION_NODE_HOST_FACTS_REFRESH_MS`; the record is republished only when an identity fact changes
(for example `boot_id` or `image_version` after a reboot into a new image). Observed slices reach
other nodes only through **direct** peers: at the end of each sync round (every
`ORION_NODE_RECONCILE_MS`) a node pushes its own slice to the peer it synced with when it changed,
and at least every 30 seconds. Slices are **not relayed**, so a node only knows the records of nodes
it peers with directly.

What an operator sees right after a node reboots:

- connected to the rebooted node: its own record (new `boot_id`, image version) is fresh as soon as
  the node serves requests;
- connected to another node that peers directly with it: the new record arrives within about one
  sync round after the rebooted node is back (its first round pushes the changed slice), and
  until then the old record is shown;
- connected to a node that does not peer with it directly: the record does not arrive there at
  all; connect to a node that peers with it.

Status-lane keys of the rebooted node are gone with its memory until its handlers republish them
(see "Actions that restart the node" in [actions.md](actions.md)).

`examples/remote_operator.rs` in `crates/client` enrolls, lists nodes with host facts, runs an
action and waits for it.

## Node identity

Every response carries the node's ed25519 signature over its node id, its key, the exact request
bytes (which include the operator's nonce and signature), the status and the body. The client
verifies it against the key chosen with `NodeTrust`:

- `FirstUse` pins whatever key answers the first hello. Compare `node_fingerprint()` with the
  `local_fingerprint` that `orionctl get operators` (or `get discovered-peers`) prints on the node,
  or enroll with the shared key, which authenticates the node as well. Store the key and use
  `NodeTrust::Node` afterwards.
- `Key` / `Node` refuse any other key (an impostor, or a node whose state directory was reset).

The node must sign responses, so `ORION_NODE_PEER_AUTH` must not be `disabled`.

## Enrollment

Two ways, both ending with the operator's key pinned in `trusted-operators.json` (state
directory) together with its policy:

### Administrator approval

The operator connects once; its validly signed hello is recorded as **pending** (in memory, at
most 32 entries, listed for 10 minutes). On the node:

```text
$ orionctl get operators
operators local_fingerprint=sha256:3f9a... enrollment_key=false default_actions=-
operator id=operator:alice state=pending fingerprint=sha256:91c2... method=- read=false actions=- last_seen_ms=...

$ orionctl operators enroll operator:alice --fingerprint sha256:91c2... --action locate --action 'self-*'
operators enroll accepted: operator:alice is enrolled
```

Compare the fingerprint with the one the operator's tool shows (`OperatorIdentity::fingerprint`)
over a channel you trust. Without `--fingerprint` the command shows the pending key and asks;
`--yes` skips the question (lab setups); `--public-key <hex>` enrolls a key that never connected.
`--no-read` withholds read access, `--action PATTERN` (repeatable) sets the action patterns,
`--no-actions` allows none; without either the node default applies.

### Shared enrollment key

When the node runs discovery with an enrollment key (`ORION_NODE_DISCOVERY=mdns`,
`ORION_NODE_ENROLLMENT_KEY`, see [discovery.md](discovery.md)), an operator that knows the key
enrolls itself with `enroll_with_key`. It runs the same handshake as nodes, with `role =
operator` in `EnrollmentHello` and bound into the transcript (so a node proof is never an operator
proof and vice versa), no initiator URL, and the same nonce, HMAC and signature rules. Operators
enrolled this way get read access and the node default action patterns. The handshake refuses
removed operators and operators enrolled with another key (it never overrides an administrator).

### Removal

```sh
orionctl operators remove operator:alice
```

revokes the key (persisted): every request of that operator, including its hello, is refused, and
the shared key does not enroll it again. `orionctl operators enroll` lifts the revocation.
Enrollments and removals go to the audit log (`operator_enrolled`, `operator_removed`).

## Authorization

| Principal | May |
| --- | --- |
| Enrolled operator, `read = true` | `OperatorHello`; `QueryStateSnapshot` (node records with host and clock facts, desired state), `QueryStatus`, `QueryObservability`, `QueryActions` (all tracked actions). |
| Enrolled operator, `read = false` | `OperatorHello`; `QueryActions` (only its own actions) if it may run any action. |
| Enrolled operator, any | `RunAction` for names matching its action patterns. |
| Unenrolled operator (valid signature) | `OperatorHello` only (recorded as pending). |
| Revoked operator | Nothing. |

Everything else (`Hello` / sync, `Snapshot` and `Mutations` pushes, observed updates, local-only
administration, `WatchActions`) is refused for operators. Action patterns are `*` (every name),
`prefix*`, or an exact name. The node default is `ORION_NODE_OPERATOR_ACTIONS` (comma-separated,
empty by default, which makes operators read-only unless their policy names actions). A policy
stored with the enrollment (`--action` / `--no-actions`) replaces the default for that operator.

Node ids starting with `operator:` are reserved: nodes refuse to start with one, and peers with
such an id cannot be configured or enrolled.

## Threat model

- **An enrolled operator can do exactly what its policy allows, on every node it is enrolled on
  and, through forwarding, on the targets its actions name.** A `reboot` or `update` pattern is a
  remote reboot or update right: grant the narrowest patterns that do the job, and `read` only to
  tools that need it. Authorization happens on the node the operator is connected to; the owning
  node trusts its enrolled peer's forward (one hop, as for every forwarded action).
- **Operator keys live on desktops.** Treat the secret key like an SSH key: store it in the OS
  keyring or a file only the user can read, one identity per person or tool, never shared. A
  stolen key acts with that operator's policy until the operator is removed from every node.
- **Revocation** is per node: `orionctl operators remove` on each node that enrolled the operator
  (operator trust is not replicated). Rotating the shared enrollment key stops new self-enrollment
  but does not revoke operators that already enrolled.
- **The shared enrollment key** is a cluster credential: a holder can enroll as any operator name
  that is not enrolled or removed yet, with read access and the default action patterns (and as a
  node, see [discovery.md](discovery.md)). Keep `ORION_NODE_OPERATOR_ACTIONS` narrow on nodes that
  accept self-enrollment.
- **Integrity, not confidentiality**: requests and responses are signed and bound together, nonces
  stop replays (the node keeps the last 256 nonces per principal; the client uses random 64-bit
  nonces), and an on-path attacker can neither forge nor alter them. Traffic is not encrypted:
  node records, host facts and action arguments are readable on the path. Use a trusted network or
  a VPN (see [peer-sync.md](peer-sync.md#threat-model-and-why-oriontcp-has-no-tls)).
- **Pending entries** come from anyone who can reach the listener and sign with some key. They
  grant nothing, are bounded (32, 10 minutes), and approving one requires comparing the
  fingerprint.
- **Denial of service**: as for peers, anyone who reaches the listener can use connection slots.

## Wire

Control protocol v4 (unreleased additions in the same release as host facts and actions):
`ControlMessage::{OperatorHello, QueryOperators, Operators, EnrollOperator, RemoveOperator}`,
`HttpResponsePayload::{OperatorWelcome, Status}`, the `/v1/control/operator` HTTP route (for
`OperatorHello` and `QueryStatus` over a peer surface), `OperatorId`, `OperatorPolicy`,
`OperatorEnrollment`, `OperatorRecord`, `OperatorsSnapshot`, `OperatorWelcome`,
`OperatorTrustState`, `OperatorEnrollmentMethod`, and `EnrollmentHello::role` (`EnrollmentRole`;
the enrollment handshake version is 2). Operators reuse `AuthenticatedPeerRequest` unchanged:
the principal kind is the `operator:` prefix of `PeerRequestAuth::node_id`.

The `orion+tcp` frame payloads, request/response signing, the enrollment proofs and the
`_orion._tcp` TXT layout are implemented once in `orion-auth` (`peer_tcp`, `crypto`,
`enrollment`, `discovery` modules) and the connection handling in
`orion_transport_ipc::ControlTcpClient`; `orion-node` and the client share them.

## Platform support

The remote operator client builds and runs on Linux, macOS and Windows. Build it with
`default-features = false`: the default `ipc` feature is the local Unix-socket client and stays
Unix-only (it needs Unix sockets, peer credentials and fd passing).

- `orion-transport-ipc` exposes its platform-neutral part on every target: the control frame
  codec (preamble, `ControlFrameReadState`, `write_control_*` / `read_control_*`), the in-memory
  transport and `ControlTcpClient`. The Unix socket client/server, fd frames and the fd
  latest-value channel are `#[cfg(unix)]`. The wire format is the same on every platform.
- `discovery` uses `mdns-sd`, which supports Windows, macOS and Linux (it picks interfaces itself
  and needs UDP 5353 to be allowed by the host firewall).
- CI lints `remote` and `discovery` for `x86_64-pc-windows-gnu` (library, unit tests and
  `examples/remote_operator.rs`) and `x86_64-apple-darwin` (library) on Linux, builds the example
  and runs the client unit tests on `windows-latest` and `macos-latest`, and runs the
  `orion-transport-ipc` unit tests (framing and an `orion+tcp` loopback exchange) on Windows.
  Tests that start an `orion-node` stay Linux-only.
- Linking a Windows binary from Linux needs a MinGW toolchain (`x86_64-w64-mingw32-gcc`); native
  builds on Windows use MSVC as usual. Cross-checking for macOS from Linux works for the library
  and the example; the `orion-client` test and example targets on Unix also build the HTTP dev
  dependency (ring), which needs a macOS SDK.
- Verified downstream (2026-10-05): a consumer workspace depending on `orion-client` with
  `features = ["remote"]` built, passed clippy and linked natively with MSVC on GitHub's
  `windows-latest` runner, and passed fully on `macos-latest`.

## Footprint

`orion-client` with only `remote` pulls `orion-auth` (`crypto`, `enrollment`), the protocol layer
of `orion-transport-http` (no reqwest, axum or hyper), `orion-transport-ipc` (frame code),
`ed25519-dalek`, `hmac`, `sha2`, `getrandom` and tokio; no rustls, no mDNS and no node internals.
Measured on x86_64 Linux with the workspace release profile (`opt-level = "z"`, fat LTO, one
codegen unit, stripped); crates are the unique packages of `cargo tree -e normal`:

| Build | Crates (Orion) | Binary |
| --- | --- | --- |
| `orion-client --no-default-features --features ipc` (local IPC client, for comparison) | 41 (7) | - |
| `orion-client --no-default-features --features remote` | 61 (9) | - |
| `orion-client --no-default-features --features discovery` (adds `mdns-sd`) | 70 (9) | - |
| tokio-only baseline binary (current-thread runtime, one TCP connect) | - | 0.44 MiB (465,408 B) |
| `examples/remote_operator.rs` (connect, enroll, list nodes, run and wait for an action) | - | 1.00 MiB (1,043,232 B) |

So the remote operator client adds about 565 KiB to a tokio binary. Against the IPC-only client
it adds `orion-auth`, `orion-transport-http` (protocol layer only), `ed25519-dalek`,
`curve25519-dalek`, `sha2`, `hmac`, `digest`, `rand_core` / `getrandom`, `subtle`, `zeroize` and
their small helpers (20 crates).

## Limits and gaps

- Watches poll; there is no server push over `orion+tcp`.
- Operator trust is per node; there is no cluster-wide operator directory.
- Policies restrict action names, not targets; read access is all or nothing.
- Status forwarding is one hop and only to configured peers of the connected node; node records
  are not relayed beyond direct peers.
