# orionctl

`orionctl` is the operator CLI for Orion.

The command surface is organized around operator intent instead of transport details:

- `get`: inspect node health and control-plane state
- `watch`: stream state changes
- `apply`: create or update desired state
- `delete`: remove desired state
- `peers`: manage trusted Orion peers

Local admin workflows default to IPC. Remote read workflows use `--http`.
