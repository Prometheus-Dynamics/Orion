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
nothing from the image but a shell (docs/comm.md, "Recovery"). With BusyBox init,
`busybox/S40orion-recovery` starts it (below); under systemd, a unit of the image's own does. `orion-capi` builds `liborion_c.so`, whose
SONAME is `liborion_c.so.1`.

## Without systemd

Boards with BusyBox init (the smallest ones) start Orion from `packaging/busybox/`:

| File | Installed as |
| --- | --- |
| `S40orion-comm` | `/etc/init.d/S40orion-comm` |
| `S41orion-node` | `/etc/init.d/S41orion-node` |
| `orion-supervise` | `/usr/lib/orion/orion-supervise` |
| `S40orion-recovery` | `/etc/init.d/S40orion-recovery`, in place of the two above |
| `orion-recovery.conf.example` | `/etc/orion/orion.conf`, edited, in a flasher image |

The rcS that runs `/etc/init.d/S??*` in order starts them; each takes `start`, `stop`,
`restart` and `status`. They keep what the units keep: the same settings file, socket and state
directories; the comm core as root with its socket given to the `orion` group; node logic as the
`orion` user (901) with `ORION_STATE_DIR` and `ORION_KEYS_DIR` as in its unit; the same failure
rule (start again after 1 s, doubling to 30 s, never giving up; a run of a minute starts the wait
over); and a clean stop (TERM, 30 s, then KILL). Output goes to `/var/log/orion-comm.log` and
`/var/log/orion-node.log` (`SUPERVISE_LOG` names another). The user and group are made by the image
from `orion.sysusers`.

**An initramfs or a flasher image** runs `orion-recovery`, the comm core alone, with
`S40orion-recovery` and no node logic. `orion-recovery.conf.example` is the settings for a board
reached over a USB gadget: the USB link by name (the program that sets up the gadget hands its
endpoints over), SSH on the gadget's network only, security off while the cable is the only way
in. With no settings file at all, `orion-recovery` listens for SSH on port 22 of every address,
with security off: anyone who can reach the board gets in. That suits an image whose only network
is the cable; an image that brings up any other network ships a settings file.

What is lost without systemd:

- **The watchdog.** A comm core that hangs without exiting is not restarted; one that exits or
  is killed is.
- **The sandbox.** Node logic runs as `orion` with none of its unit's hardening: no syscall
  filter, no read-only system, no `NoNewPrivileges=`, no private `/tmp`. The comm core loses the
  little its unit sets.
- **Readiness.** Node logic is started after the comm core's script, not after its listeners are
  up; it connects again until they are, as after any restart.
- **The journal.** Logs are files, not rotated by Orion.
- **`/run/orion` across restarts** is kept by the script, which never removes it; that much is
  the same.

## Building for a board

Board files are static musl builds, one package per invocation (built together, cargo gives each
the features of both):

```bash
cargo build --locked --profile service --target aarch64-unknown-linux-musl -p orion-bin
cargo build --locked --profile service --target aarch64-unknown-linux-musl -p orion-recovery-bin
```

The same for `armv7-unknown-linux-musleabihf` and `riscv64gc-unknown-linux-musl`, which build and
pass the size gate but have not run on a board. The SSH way in (`russh`) brings `ring`, which
compiles C: name a C compiler for the target, `CC_<triple>`. `scripts/zigcc/<triple>` is one,
`zig cc` from `PATH`, with `scripts/zigcc/<triple>-ar` for archives; zig carries musl for every
target, so it is the only C toolchain needed. Rust links with `rust-lld`
(`CARGO_TARGET_<TRIPLE>_LINKER=rust-lld`). `.cargo/config.toml` makes the riscv64 build static
(`+crt-static`: that target links dynamically otherwise, and fails); keep its flags when setting
your own (`CARGO_TARGET_<TRIPLE>_RUSTFLAGS` adds to them, `RUSTFLAGS` replaces them).
