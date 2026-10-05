# Control Protocol Compatibility

Orion's control protocol, both local IPC (`orionctl`, `orion-client`, providers and executors
talking to `orion-node`) and HTTP (`orionctl --http` and node-to-node peers), carries **rkyv**
archives. rkyv archives are layout-exact: adding, removing, reordering, or resizing a field in any
protocol type (for example the `resource_usage` section of `NodeObservabilitySnapshot`) makes
archives from other builds undecodable.

Orion therefore does not support mixed versions on the control protocol. Instead of letting a
version skew show up as a confusing rkyv validation error, every exchange carries an explicit wire
version that is checked **before** any archived payload is decoded.

## The version

`orion_core::CONTROL_PROTOCOL_VERSION` (`crates/core/src/protocol.rs`) is the single source of
truth. It is a `u16` that changes whenever the archived layout of a protocol type changes.

| Version | Change |
|---|---|
| 1 | Implicit, unversioned layout before the preamble existed |
| 2 | `NodeObservabilitySnapshot::resource_usage`; version preamble and header added |
| 3 | `NodeRecord::clock` and `NodeObservabilitySnapshot::clock` (`NodeClockFacts`, `ClockSourceKind`); `ResourceOwnershipMode::ExclusiveOwnerPublishesDerived` removed; volatile status lane: `PublishStatus`, `QueryStatus`, `WatchStatus`, `Status` control messages, `ClientEventKind::Status`, and the `observed_persistence` / `status_lane` resource-usage sections; per-object hybrid-logical-clock versions in desired state (`DesiredClusterState::stamps`/`tombstones` replace `workload_tombstones`, `DesiredStateSummary::stamps`/`tombstones`, `MutationBatch::stamps`), `NodeObservabilitySnapshot::desired_merge`; the `orion+tcp` peer transport uses the same preamble (see `docs/peer-sync.md`); peer discovery and enrollment: `QueryDiscovery`, `Discovery`, `EnrollDiscoveredPeer`, `RemovePeer`, `EnrollmentHello`, `EnrollmentChallenge`, `EnrollmentConfirm` control messages, `HttpResponsePayload::EnrollmentChallenge`, `NodeObservabilitySnapshot::discovery` (see `docs/discovery.md`) |

## Local IPC

Every local control message starts with a fixed 4-byte preamble that is not rkyv-encoded, so its
layout never changes between releases:

```text
[b'O' b'C'][CONTROL_PROTOCOL_VERSION u16 LE]
```

- Unary socket (`UnixControlClient` / `UnixControlServer`): `[preamble][archive]` until EOF, in
  both directions.
- Stream socket (`UnixControlStreamClient`, `read_control_frame` / `write_control_frame`): each
  frame is `[preamble][payload_len u32 LE][archive]`. The `ClientHello` / `ClientWelcome`
  handshake is the first frame in each direction, so the version is exchanged there before any
  session state is created.

The reader checks the magic, then the version, and only then decodes the archive. When the version
differs, the node replies with a payload-free message that carries only its own preamble and then
closes the connection, so both sides report the same typed error:

- `IpcTransportError::ProtocolMismatch { local, remote }` in `orion-transport-ipc`
- `ClientError::ProtocolMismatch { local, remote }` in `orion-client`

`orion-node` logs a warning and counts the rejection in `ipc_malformed_input_count`. Bytes without
the magic are still reported as `DecodeFailed`, as before.

## HTTP

HTTP bodies are rkyv too. `HttpClient` sends `x-orion-control-protocol: <version>` on every
request, and `HttpServer` stamps the same header on every response, including health, readiness,
metrics, and errors.

- The server rejects control `POST`s that have a different version with `409 Conflict`, and those
  with a missing or invalid header with `400 Bad Request`, before decoding the body.
- The client compares the response header before decoding the body. A `200` without the header is
  a `DecodeResponse` error because it did not come from a compatible `orion-node`.
- Both sides report `HttpTransportError::ProtocolMismatch { local, remote }`. Peer sync records it
  as a protocol failure.

## What a mismatch looks like

`orionctl` against a node from another release (local socket):

```text
incompatible Orion control protocol: this client speaks v2 but orion-node speaks v3; upgrade
orionctl/client libraries and orion-node together (they must come from the same Orion release)
```

`orionctl --http`:

```text
incompatible Orion control protocol: orionctl speaks v2 but the orion-node at http://node:9100
speaks v3; upgrade orionctl and orion-node together (they must come from the same Orion release)
```

Peer nodes, and other users of `HttpClient` or `IpcTransportError` directly:

```text
Orion control protocol mismatch: this side speaks v2, the remote side speaks v3; upgrade orionctl,
client libraries, and orion-node together so they share the same protocol version
```

The fix is always the same: deploy `orionctl`, the client libraries embedded in providers and
executors, and `orion-node` from the same Orion release.

## Changing protocol types

`crates/orion/tests/control_protocol_layout.rs` fingerprints the archived `size_of`/`align_of` of
every control-protocol type and compares the result with
`orion_core::CONTROL_PROTOCOL_LAYOUT_FINGERPRINT`. When you change a protocol type, the test fails
with:

> bump CONTROL_PROTOCOL_VERSION and update the fingerprint

To make the change:

1. Increment `CONTROL_PROTOCOL_VERSION` and add a row to the table above.
2. Set `CONTROL_PROTOCOL_LAYOUT_FINGERPRINT` to the value printed by the failing test.
3. Mention the bump in `CHANGELOG.md`.

The fingerprint catches added, removed, or resized fields and variants. A pure reorder, or a swap
between field types of the same size, keeps the fingerprint unchanged but still breaks the wire.
Those changes need a manual version bump. When you add a new protocol type, also add it to the
test's type list.

The fd latest-value channel (`UnixFdLatestServer`) has its own fixed binary header with an
independent version byte, and it is not covered by this version.
