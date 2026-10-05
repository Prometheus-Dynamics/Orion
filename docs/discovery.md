# Peer Discovery and Enrollment

`orion-node` can find the other nodes of its cluster on the local network with mDNS/DNS-SD
(cargo feature `discovery-mdns`, `ORION_NODE_DISCOVERY=mdns`). Discovery only **finds** peers. It
never makes a peer trusted: a discovered peer is not synced with and its requests are refused
until it is **enrolled**, by an operator or with a shared enrollment key. Enrolled peers are
ordinary sync peers (see [peer-sync.md](peer-sync.md)): their ed25519 key is pinned in the trust
store and every request and response is signed and verified as for peers listed in
`ORION_NODE_PEERS`.

```text
             mDNS (_orion._tcp)              enrollment               peer sync
  node-a  <---------------------->  node-b    ---------->  trusted    ---------->  converged
          discovered, not trusted             operator or             signed requests
                                               shared key              and responses
```

## Enabling it

```sh
cargo build -p orion-node --release --no-default-features --features peer-tcp,discovery-mdns
# or on top of the default features: --features discovery-mdns

ORION_NODE_ID=node-a \
ORION_NODE_PEER_AUTH=required \
ORION_NODE_PEER_ADDR=0.0.0.0:9200 \
ORION_NODE_DISCOVERY=mdns \
ORION_NODE_CLUSTER=lab \
ORION_NODE_STATE_DIR=/var/lib/orion \
  orion-node
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `ORION_NODE_DISCOVERY` | `off` | `mdns` advertises this node and browses for others. |
| `ORION_NODE_CLUSTER` | `default` | Cluster name (1-63 of `[A-Za-z0-9._-]`). Only peers that advertise the same name are considered. |
| `ORION_NODE_DISCOVERY_TTL_MS` | `120000` | How long a discovered peer stays listed without a fresh announcement. |
| `ORION_NODE_DISCOVERY_INTERFACES` | all | Comma-separated interface names (`eth0`) or addresses to run mDNS on. |
| `ORION_NODE_ENROLLMENT_KEY` | unset | Shared enrollment key (at least 32 bytes, for example `openssl rand -hex 32`). Enables automatic mutual enrollment. |
| `ORION_NODE_ENROLLMENT_KEY_FILE` | unset | File holding the key (surrounding whitespace ignored). Mutually exclusive with the variable. |

Requirements, checked at startup:

- `ORION_NODE_PEER_AUTH=required`. In `optional` mode a node pins any peer's key on first
  contact, which would trust every discovered peer; discovery refuses to run with it.
- `ORION_NODE_PEER_ADDR`: the `orion+tcp` listener is what is advertised and what automatic
  enrollment uses. When it is bound to a specific address only that address is announced,
  otherwise every interface address. A non-loopback HTTP listener is advertised as well.
- A build without `discovery-mdns` fails startup when `ORION_NODE_DISCOVERY=mdns` or an
  enrollment key is set.

Set `ORION_NODE_STATE_DIR` so enrollments survive restarts (`discovered-peers.json`, next to the
trust store). Peers enrolled earlier are restored at startup, before anything is discovered.

## What is advertised

Service type `_orion._tcp.local.`, instance name = node id, SRV port = the `orion+tcp` port. The
TXT record:

| Key | Value |
| --- | --- |
| `v` | TXT layout version, `1` |
| `id` | Node id (authoritative; the instance name is only a label) |
| `pk` | ed25519 public key, 64 hex characters |
| `cp` | Control protocol version (`CONTROL_PROTOCOL_VERSION`, 3) |
| `cl` | Cluster name |
| `tcp` | `orion+tcp` listener port |
| `http` / `https` | HTTP peer listener port, when advertised |

Peer URLs are built from the A/AAAA addresses and these ports (`orion+tcp://` first, IPv4 before
IPv6; link-local IPv6 addresses are skipped because they need a scope).

**The full public key is advertised, not just a fingerprint.** A public key is not secret, it fits
easily in a TXT entry, and advertising it lets an operator pin exactly the key whose fingerprint
they checked, without a second, equally unauthenticated round trip to fetch the key. An
advertisement is never trusted by itself either way: an attacker can advertise anything, so what
matters is that enrollment binds the key the operator verified (operator path) or the key that
proved knowledge of the enrollment key (shared-key path).

Announcements from other clusters, malformed records and the node's own announcement are ignored
(counted as `announcements_ignored`). A peer that speaks another control protocol version is listed
as `incompatible` and cannot be enrolled.

## The discovered set

Every node keeps the peers it has seen with first-seen, last-seen and expiry times. An entry
expires when it is not refreshed within `ORION_NODE_DISCOVERY_TTL_MS`, or immediately when the peer
sends an mDNS goodbye (clean shutdown). Expiry only drops the peer from the discovered list: an
enrolled peer stays trusted and registered for sync. The trust state of each entry is derived from
the trust store:

| State | Meaning |
| --- | --- |
| `discovered` | Seen, not trusted, not synced with. |
| `enrolled` | The advertised key is the pinned key and the peer is registered for sync. |
| `revoked` | Removed by an operator; never enrolled again automatically. |
| `key_mismatch` | Advertises a different key than the one pinned for that node id (a re-keyed node, or an impostor). Not followed. |
| `incompatible` | Other control protocol version. |

```sh
$ orionctl get discovered-peers --socket /run/orion/control.sock
discovery backend=mdns cluster=lab local_fingerprint=sha256:3f9a... enrollment_key=false discovered=1 enrolled=0 ...
peer node=node-b state=discovered fingerprint=sha256:91c2... protocol=v3 urls=orion+tcp://10.0.0.2:9200 last_seen_ms_ago=812 expires_in_ms=119188 last_enrollment_error=-
```

`-o json|yaml|toml` prints the full `DiscoverySnapshot` including the public keys.

When an enrolled peer that was enrolled through discovery shows up at a new address (DHCP), its
sync URL follows the announcement. This is safe because every response is verified against the
pinned key: a spoofed address can make sync fail, never inject state. Peers from `ORION_NODE_PEERS`
keep their configured URL.

## Enrolling peers

There are three ways for a peer to become trusted. All of them end in the same place: the key is
pinned in the trust store, the peer is registered for sync (`NodeApp::enroll_peer`), and the
enrollment is persisted.

### Operator approval

```sh
# on node-a
orionctl peers enroll node-b --socket /run/orion/control.sock
peer node-b cluster=lab urls=orion+tcp://10.0.0.2:9200
  key fingerprint sha256:91c2...
  public key      7c1e...
Compare the fingerprint with `orionctl get discovered-peers` on node-b (local fingerprint). Trust this key? [y/N] y
peers enroll accepted: node-b is trusted and registered for sync
```

Compare the fingerprint with the `local_fingerprint` that `orionctl get discovered-peers` prints
**on node-b**, over a channel you trust (a console, SSH). `--fingerprint sha256:...` does the
comparison non-interactively; `--yes` skips it (only for lab setups). The node pins exactly the key
with that fingerprint and refuses if the advertisement changed in between. Trust is directional:
enroll node-a on node-b as well, otherwise node-b refuses node-a's requests.

`orionctl peers enroll --node-id ... --base-url ... [--public-key ...]` (the pre-existing form)
still enrolls a peer that was not discovered. `orionctl peer ...` is an alias of `orionctl peers`.

### Shared enrollment key

When all nodes of a cluster have the same `ORION_NODE_ENROLLMENT_KEY`, they enroll each other
automatically: each node runs the handshake below with every discovered peer of its cluster that is
not enrolled yet, over `orion+tcp`. Failures are retried with exponential backoff (5 s doubling to
5 minutes) and shown as `last_enrollment_error`.

```text
I -> R  EnrollmentHello     { version, role, cluster, I, pk_I, n_I, url_I, R }
R -> I  EnrollmentChallenge { R, pk_R, n_R, HMAC(K, "responder\0" || T), Sig_R("responder\0" || T) }
I -> R  EnrollmentConfirm   { I, R, n_R, HMAC(K, "initiator\0" || T), Sig_I("initiator\0" || T) }
R -> I  Accepted            R has pinned pk_I and registered I at url_I; I then pins pk_R

T = "orion-enroll-v1" || version || role || cluster || I || pk_I || n_I || url_I || R || pk_R || n_R
    (every field length-prefixed; n_I and n_R are 32 random bytes; K is the enrollment key;
     version is ENROLLMENT_PROTOCOL_VERSION = 2; role is 0 for nodes, 1 for remote operators)
```

- The enrollment key never crosses the wire; the HMAC-SHA256 proofs show that each side knows it.
- The ed25519 signatures show that each side holds the private key behind the key that is pinned,
  so a key holder cannot enroll somebody else's public key.
- Both proofs cover both node ids, both keys, the initiator's URL and both nonces. The initiator
  also checks that the responder's key is the one it advertises.
- `n_R` is single-use: the responder keeps at most 64 outstanding challenges for 30 seconds and
  forgets each one when its confirmation arrives, so a captured confirmation cannot be replayed.
- The handshake messages are the only peer requests a node answers without a known peer key. They
  are refused when no enrollment key is configured, when the initiator was removed by an operator,
  and when another key is already pinned for that node id (shared-key enrollment never overrides an
  operator decision or re-keys a peer).

`url_I` is the initiator's `orion+tcp` port on the address the responder sees the connection come
from, so the responder can sync back without having discovered the initiator itself.

**Remote operators** use the same handshake with `role = operator`, an `operator:<name>` initiator
id and no URL ([remote-operator.md](remote-operator.md)). The role is bound into the transcript,
so a node's proof never enrolls an operator and an operator's proof never enrolls a node. An
operator enrolled this way is pinned in the operator trust store (`trusted-operators.json`), not
the peer trust store: it is never synced with, gets read access and the node's default action
patterns (`ORION_NODE_OPERATOR_ACTIONS`), and shared-key enrollment never overrides an
administrator's removal or an operator enrolled with another key. A holder of the enrollment key
can therefore also enroll operators; keep the default action patterns narrow.

The handshake is served on both peer transports (the `/v1/control/enroll` HTTP route exists too),
but nodes only *initiate* it over `orion+tcp`.

### Static configuration

`ORION_NODE_PEERS` entries with a key segment (`node-b=orion+tcp://10.0.0.2:9200|<key hex>`) work
exactly as before and do not need discovery.

## Removing peers

```sh
orionctl peers remove node-b --socket /run/orion/control.sock
```

revokes node-b's key (persisted in the trust store, so it survives restarts and node-b's requests
are refused), stops syncing with it, and forgets its discovery enrollment. Discovery keeps listing
it as `revoked`, and the shared enrollment key does not enroll it again. Enrolling it with
`orionctl peers enroll node-b` is an explicit operator decision that lifts the revocation. A peer
that comes from `ORION_NODE_PEERS` is registered again at the next start (and then refused because
it is revoked); remove it from the variable as well. The difference to `orionctl peers revoke` is
that `remove` also unregisters the peer from sync and forgets its enrollment.

## Threat model

mDNS is unauthenticated: anyone on the link can send or spoof announcements and goodbyes.
Discovery is therefore treated as a hint, never as trust.

- **Spoofed announcements** can add bogus entries to the discovered list (capped at 1024 entries),
  flip an unenrolled entry's advertised key, or make an entry disappear. They cannot make a node
  trust anything: the operator path pins the key whose fingerprint was compared out of band, and
  the shared-key path pins only keys whose owners proved knowledge of the enrollment key and of
  their private key. For an enrolled peer a spoofed announcement can at most redirect sync to a
  wrong address, which fails signature verification (denial of service only).
- **A holder of the enrollment key can join the cluster** as any node id that is not enrolled,
  removed or pinned yet, and then reads and writes desired state like any peer. Treat the key like
  a cluster credential: distribute it only to nodes that may join, keep it out of images, and
  rotate it (change it on all nodes) when a holder leaves; rotating does not revoke nodes that
  already enrolled, remove those with `orionctl peers remove`.
- **Offline guessing**: an attacker who records a handshake, or answers one by pretending to be a
  peer, obtains an HMAC over known data and can test guesses of the key offline. The key must
  therefore be random, not a passphrase; nodes refuse keys shorter than 32 bytes.
- **Online guessing** costs one handshake per guess and each failure is counted
  (`orion_node_enrollment_failures_total`).
- **Denial of service**: anyone can make a node issue challenges (bounded to 64 outstanding, the
  oldest are dropped), which can delay a legitimate shared-key enrollment until the retry.
  Discovery adds no new way to make a node accept state.
- **Confidentiality** is unchanged from [peer-sync.md](peer-sync.md#threat-model-and-why-oriontcp-has-no-tls):
  `orion+tcp` traffic, including the handshake, is signed but not encrypted.
- Discovery does not authenticate the cluster name. Different clusters on one link are separated
  by their keys (operator-pinned or enrollment key), not by the name.

## Observability

`NodeObservabilitySnapshot::discovery` (`orionctl get observability -o json`) and Prometheus:

| Metric | Type | Meaning |
| --- | --- | --- |
| `orion_node_discovered_peers` | gauge | Peers of the local cluster in the discovered set. |
| `orion_node_discovered_enrolled_peers` | gauge | Discovered peers that are enrolled. |
| `orion_node_discovery_announcements_total` | counter | Valid announcements of the local cluster received. |
| `orion_node_discovery_announcements_ignored_total` | counter | Other cluster, malformed, or the node's own. |
| `orion_node_discovery_peers_expired_total` | counter | Entries dropped after TTL expiry or a goodbye. |
| `orion_node_enrollment_attempts_total` | counter | Operator approvals plus outbound and inbound shared-key handshakes. |
| `orion_node_enrollment_successes_total` | counter | Completed enrollments. |
| `orion_node_enrollment_failures_total` | counter | Failed enrollments (wrong key, refused, unreachable, stale fingerprint, ...). |

The metrics are only exported when discovery runs. Operator enrollments and removals are written to
the audit log (`peer_enrolled`, `peer_removed`).

## Implementation notes

- **Backend**: the [`mdns-sd`](https://crates.io/crates/mdns-sd) crate (pure Rust, maintained,
  RFC 6762/6763 probing, known-answer handling, cache expiry, IPv4 and IPv6, multiple interfaces)
  with default features off. It runs its own daemon thread and needs no async runtime. A
  hand-rolled responder/querier over UDP 5353 would add no crates but would have to reimplement
  name probing and conflict handling, cache refresh, goodbyes and multi-interface/IPv6 handling to
  interoperate with Avahi and Bonjour; the dependency was the better trade. Cost, measured on
  x86_64 Linux with the release profile (`opt-level = "z"`, fat LTO, stripped):

  | `orion-node` build | Binary size | Crates (non-Orion) |
  | --- | --- | --- |
  | `--no-default-features --features peer-tcp` | 2.92 MiB (3,064,424 B) | 74 (64) |
  | `--no-default-features --features peer-tcp,discovery-mdns` | 3.44 MiB (3,606,256 B) | 84 (74) |

  The feature adds about 530 KiB: `mdns-sd` and its dependencies, the discovery runtime, the
  enrollment handshake and HMAC-SHA256.

  `discovery-mdns` adds `mdns-sd`, `flume`, `spin`, `lock_api`, `scopeguard`, `fastrand`,
  `if-addrs`, `socket-pktinfo`, `log` and `hmac` (`sha2`, `socket2` and `mio` were already in the
  tree).
- The backend is pluggable (`orion_node::discovery::DiscoveryBackend`).
  `MemoryDiscoveryBus` simulates a multicast segment in memory; the tests use it, and embedders can
  feed peers found by other means into it. `NodeApp::start_discovery(config, backend, endpoints)`
  starts the runtime; `orion-node` starts it with `MdnsDiscoveryBackend` from the environment.
- Live peers are re-reported by restarting the mDNS browse every 30 seconds, which returns the
  daemon's cached instances and re-queries the network; records that expire in the mDNS cache are
  reported as withdrawn.
- The TXT layout and parser (`orion_auth::discovery`), the handshake proofs
  (`orion_auth::enrollment`) and the fingerprint (`orion_auth::crypto::key_fingerprint`) live in
  `orion-auth`, shared with the remote operator client, which can browse `_orion._tcp` with its
  `discovery` feature.
- Control messages (control protocol v3): `QueryDiscovery` / `Discovery`, `EnrollDiscoveredPeer`,
  `RemovePeer` on the local socket, and `EnrollmentHello` / `EnrollmentChallenge` /
  `EnrollmentConfirm` between peers (`HttpResponsePayload::EnrollmentChallenge`). `orion-client`
  has `LocalControlPlaneClient::{query_discovery, enroll_discovered_peer, remove_peer}`.
