# orionctl

`orionctl` is the operator CLI for Orion.

The command surface is organized around operator intent instead of transport details:

- `get`: inspect node health and control-plane state
- `watch`: stream state changes
- `apply`: create or update desired state
- `delete`: remove desired state
- `peers`: manage trusted Orion peers

Local admin workflows default to IPC. Remote read workflows use `--http`.

Structured file/config support:

- `orionctl apply workload --spec` accepts `.json`, `.yaml`/`.yml`, and `.toml`
- `--spec-format json|yaml|toml` overrides extension inference
- structured output supports `-o json`, `-o yaml`, and `-o toml`
  - TOML output is wrapped under a top-level `value` key so list-shaped responses remain valid TOML

Cargo features (all on by default, so a plain build behaves as before):

- `http`: `--http` remote targets, including `get health` and `get readiness`. It links only the
  client half of `orion-transport-http` (reqwest over rustls), not the axum/hyper server.
- `yaml`: `-o yaml` and YAML workload specs.
- `toml`: `-o toml` and TOML workload specs.

JSON and summary output and local IPC are always available. `cargo build -p orionctl
--no-default-features` gives an IPC-only CLI with JSON output; add back `--features yaml,toml` or
`--features http` as needed. Asking a build for something it was compiled without (for example
`--http` or `-o yaml`) fails with an error that names the missing feature. Release-profile sizes
(x86_64, stripped):

| Build | Size | Crates (total / runtime) |
| --- | --- | --- |
| default (`http`, `yaml`, `toml`) | 4.38 MiB | 139 / 111 |
| `--no-default-features --features yaml,toml` | 2.65 MiB | 68 / 45 |
| `--no-default-features` (IPC + JSON) | 1.80 MiB | 57 / 34 |

Workload apply supports both direct flags and full specs:

- Typed config flags:
  `orionctl apply workload --workload-id workload.demo --runtime-type graph.exec.v1 --artifact-id artifact.demo --config-schema graph.workload.config.v1 --config-string graph.kind=inline --config-string graph.inline='{"nodes":[]}'`
- Full JSON spec:
  `orionctl apply workload --spec workload.json`
