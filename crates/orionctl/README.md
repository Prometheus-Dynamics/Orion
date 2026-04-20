# orionctl

`orionctl` is the operator CLI for Orion.

The command surface is organized around operator intent instead of transport details:

- `get`: inspect node health and control-plane state
- `watch`: stream state changes
- `apply`: create or update desired state
- `delete`: remove desired state
- `peers`: manage trusted Orion peers

Local admin workflows default to IPC. Remote read workflows use `--http`.

Workload apply supports both direct flags and full specs:

- Typed config flags:
  `orionctl apply workload --workload-id workload.demo --runtime-type graph.exec.v1 --artifact-id artifact.demo --config-schema graph.workload.config.v1 --config-string graph.kind=inline --config-string graph.inline='{"nodes":[]}'`
- Full JSON spec:
  `orionctl apply workload --spec workload.json`
