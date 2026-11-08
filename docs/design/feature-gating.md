# Feature gating: what each crate's defaults pull in

The rule:
- Each crate's default features are the minimum a typical consumer needs.
- These are opt-in: transports (HTTP and TLS, TCP, QUIC), cluster and peer sync, auth backends,
  YAML and TOML, NT4, the CLI, and debug tooling.
- Links between workspace crates use `default-features = false` and enable only what they use.

This note is the audit against that rule (2026-11-07): the state of every crate, the cuts made,
the sizes before and after, and a second round of cuts.

## Sizes

x86_64 release builds with the workspace profile (fat LTO, `opt-level = "z"`, stripped):

| Binary and build | Before | After | Change |
| --- | ---: | ---: | ---: |
| `orion-node`, default build | 6,425,336 | 3,349,848 | −3,075,488 (−48%) |
| `orion-node --features full` (the old default set) | 6,425,336 | 6,052,528 | −372,808 (−5.8%) |
| `orion-node --features peer-tcp,link-gateway` | | 4,493,344 | |
| `orionctl`, default build | 4,372,032 | 2,146,912 | −2,225,120 (−51%) |
| `orionctl --features full` (the old default set) | 4,372,032 | 4,339,384 | −32,648 |

Sizes are in bytes. Two kinds of saving are in this table:
- **The default build** shrinks because transports and formats are now opt-in. Anyone who wants
  the old binary adds `--features full`.
- **The full build** shrinks because the node's log filter no longer links a regex engine. Every
  build of `orion-node` gets that saving, the minimal ones included.

## Cuts made

| Cut | Where | Who opts in |
| --- | --- | --- |
| `orion-node` default `[]` (was `transport-http, transport-tcp, transport-quic, peer-tcp`); new `full` enables all four | `crates/node/Cargo.toml` | the perf, soak and docker CI workflows and the docker images (`--features full`); `orionctl`'s tests (its dev-dependency asks for `transport-http`); the board image already lists its features |
| `orionctl` default `[]` (was `http, yaml, toml`); new `full` enables all three | `crates/orionctl/Cargo.toml` | the docker images (`--features full`) |
| Log filter: `tracing-subscriber`'s `Targets` instead of `EnvFilter`, and the `env-filter` feature dropped | workspace `Cargo.toml`, `crates/node/src/main.rs` | nobody. `RUST_LOG` keeps its `level` and `target=level` directives; span and field filters go (`docs/logging.md`) |
| `orion-transport-common` default `[]` (was `tls`) | `crates/transport-common/Cargo.toml` | nobody in the workspace: every consumer already asks for `tls` |
| `orion-transport-http` default `[]` (was `transport`) | `crates/transport-http/Cargo.toml` | nobody in the workspace: every consumer already asks for `client`, `server` or `transport` |

## The crates today

| Crate | Default | Opt-in features (what each pulls in) | Notes |
| --- | --- | --- | --- |
| orion-core, orion-control-plane, orion-data-plane, orion-cluster, orion-runtime | `std` | — | `no_std` without defaults; workspace links already have defaults off |
| orion-auth | `std` | `crypto` (ed25519-dalek, sha2), `enrollment` (+hmac), `link-channel` (+chacha20poly1305, curve25519-dalek) | compliant |
| orion-link | `stats` | `device`, `actions`, `streams`, `xfer`, `alloc` (postcard, serde, the record crates), `std`, `crc-table`, `crc-extern`, `embedded-io`, `embedded-io-async`, `embedded-can` | compliant; the MCU tier is tracked by `scripts/mcu-size.sh` and `scripts/device-size.sh` |
| orion-transport-common | `[]` | `tls` (rustls with ring, rustls-pemfile) | changed |
| orion-transport-http | `[]` (protocol types) | `client` (httparse, rustls, tokio-rustls, native roots), `server` (axum, hyper, tokio-rustls), `transport` (both) | changed |
| orion-transport-ipc | — | — | tokio with what IPC needs |
| orion-transport-tcp | `[]` (plaintext) | `tls` (rustls, tokio-rustls, orion-transport-common/tls) | changed |
| orion-transport-quic | — | `insecure-self-signed` (rcgen) | quinn and rustls are the transport itself |
| orion-client | `ipc` (+orion-runtime) | `remote` (ed25519, auth enrollment, HTTP protocol types, tracing), `discovery` (+mdns-sd) | changed |
| orion-nt4 | `client`, `server` | — | linked by no workspace binary; both halves need the same dependencies |
| orion (facade) | the model crates | every transport, `client`, `remote`, `service`, `macros`, `cluster` | compliant |
| orion-node | `[]` (IPC only) | `transport-http`, `peer-tcp`, `transport-tcp`, `transport-quic`, `full`, `link-gateway`, `discovery-mdns`, `systemd-notify`, `alloc-mimalloc`, `alloc-jemalloc` | changed |
| orionctl | `[]` (IPC + JSON) | `http`, `yaml`, `toml`, `full` | changed. clap is the CLI itself |
| orion-capi, liborion, liborion-rs | —, —, `static` | `shared` | compliant |

No crate uses a feature in `cfg` that it does not declare. The declared features all have users,
apart from umbrellas (`transport`, `full`).

## Second round (2026-11-07)

The four cuts that were left are made:

| Cut | Where | Who opts in |
| --- | --- | --- |
| `orion-transport-tcp`: rustls and tokio-rustls behind a new `tls` feature; without it the crate carries plaintext frames only (`TcpClientTlsConfig`, `TcpServerTlsConfig`, `TcpServerClientAuth`, `serve_tls*`, `with_tls` are `tls` items) | `crates/transport-tcp` | the facade's new `transport-tcp-tls` (`transport-tcp` alone is plaintext); `orion-node`, which asks for `transport-tcp-tls` and, with its `transport-tcp` feature, `orion-transport-tcp/tls` |
| `orion-client`: `orion-runtime` only with `ipc`, `tracing` only with `remote`, tokio `fs` only in dev-dependencies | `crates/client/Cargo.toml` | nobody: the modules that use them are already behind those features |
| `orionctl` on a current-thread runtime; `orion-client`, `orion-transport-ipc` and `orionctl` ask tokio for `rt`, not `rt-multi-thread` (the IPC benchmark asks for it as a dev-dependency) | `crates/orionctl/src/main.rs`, three `Cargo.toml` | nobody. `orionctl`, in its default and IPC-only builds, no longer links tokio's multi-thread scheduler |
| `default-features = false` on the workspace links to `orion-node`, `orion-nt4` and `liborion-rs` | workspace `Cargo.toml` | `orion-node`'s tests ask for `liborion-rs/static`; `orion-node`'s defaults were already `[]` and nothing in the workspace links `orion-nt4` |

Sizes after the second round, same build: `orionctl` default 2,145,776 (from 2,146,912),
`orionctl --no-default-features` 2,069,808, `orion-node` default 3,336,264 (from 3,349,848). The
binaries barely change because the linker already dropped the unused code. What the cuts remove
is dependencies: a plaintext `orion-transport-tcp` consumer no longer builds or links rustls and
ring, and a remote-only `orion-client` consumer no longer builds `orion-runtime`.

The binaries of the hardware layer and the fleet tool depend on these crates from another
repository, so their before and after sizes are measured there.
