# Packaging

How Orion runs on a board: two systemd services from one binary, a settings file and a user.
`scripts/packaging-check.sh` checks every file here, and the gate runs it.

How Orion is built and put into an image is not here: whoever owns an image's build
describes that, installing the files below where the table says.

| File | Installed as | What it is |
|---|---|---|
| `systemd/orion-comm.service` | `/usr/lib/systemd/system/orion-comm.service` | The comm core: `orion comm`. |
| `systemd/orion-node.service` | `/usr/lib/systemd/system/orion-node.service` | Node logic: `orion node`. |
| `systemd/orion.conf.example` | `/etc/orion/orion.conf` | The settings both read. |
| `systemd/orion.sysusers` | `/usr/lib/sysusers.d/orion.conf` | The `orion` user and group (uid and gid 901). |
| `systemd/orion.preset` | `/usr/lib/systemd/system-preset/80-orion.preset` | Enables both services. |

## The two services

`orion` is one binary (docs/comm.md, "One binary: `orion`"). The comm core and node logic run
as two services so that each restarts alone and each has only the rights it needs:

- **`orion-comm.service`**, `orion comm`, as root with group `orion`. Root because it lets
  SSH logins in as the users they name; group `orion` because its socket
  (`/run/orion/orion.sock`, mode 0660) is for the programs in that group.
  - `Type=notify`: it is started when every listener is up, and it tells systemd it is alive
    every half of `WatchdogSec=30s` from its own thread. A comm core that stops turning is
    killed and started again.
  - `Restart=always`, after 1 s, growing to at most 30 s over five tries, and no start limit
    (`StartLimitIntervalSec=0`): the way into the board is never switched off for good.
  - `RuntimeDirectory=orion` is kept across a restart, so programs reconnect to what they
    shared. Its keys and SSH host key are in `/var/lib/orion`, root's.
  - Hardened only as far as logins allow. Each session is the user's own shell, so the unit
    sets nothing that would follow a login into it: no `NoNewPrivileges=`, `ProtectHome=`,
    `ProtectSystem=strict`, `PrivateTmp=` or `PrivateDevices=`.
- **`orion-node.service`**, `orion node`, as the `orion` user, after the comm core. It is the
  comm core's first client and connects again when the comm core restarts, so it wants the comm
  core but does not stop with it. The settings let uid 901 in as node logic
  (`socket.node_uids = 901`); without that line the comm core refuses it. Its state is in
  `/var/lib/orion/node` (`ORION_STATE_DIR`), apart from the comm core's keys, which it reads in
  `/var/lib/orion` (`ORION_KEYS_DIR`) to act as the node toward other nodes. It is hardened
  fully: `systemd-analyze security` rates it 1.8, and the check fails it above 2.5.

Change either unit with a drop-in (`/etc/systemd/system/orion-comm.service.d/*.conf`), not an
edited copy.

## Settings

Both services read `/etc/orion/orion.conf`, and `ORION_*` in the environment overrides a key
(`node.name` is `ORION_NODE_NAME`). A key neither reads is logged at start. The example sets
the board's name and lets the `orion` user in as node logic; everything else is commented out
with its meaning. The keys are in docs/comm.md and docs/node.md.

## The `orion` user

`orion.sysusers` declares the user and group with uid and gid 901, the user also in `dialout`
for serial links. `systemd-sysusers` reads it at boot, which needs a writable `/etc`; an image
with a read-only root filesystem makes the user from the same file when it is built. 901 is
also in the settings (`socket.node_uids`): keep the two the same.

## Building

`orion` for a board, as it ships (profile `service`: size-tuned, link-time optimized,
stripped):

```bash
cargo build --profile service --target aarch64-unknown-linux-gnu -p orion-bin
```

Install `target/aarch64-unknown-linux-gnu/service/orion` as `/usr/bin/orion`; started as
`orionctl` (a link or a two-line script running `orion ctl`) it is the command line. The
units need systemd 254 or newer (`RestartSteps=`). arm64 kernels with 16 KiB or 64 KiB pages run
the binary: the workspace `.cargo/config.toml` aligns segments to 64 KiB.

`orion-recovery`, the comm core alone, builds static for `aarch64-unknown-linux-musl` and needs
nothing from the image but a shell (docs/comm.md, "Recovery"); starting it, from an initramfs
or a unit of the image's own, is the image's part. `orion-capi` builds `liborion_c.so`, whose
SONAME is `liborion_c.so.1`.
