# Packaging `orion-node` for Linux images

This directory runs `orion-node` as a managed systemd service on any Linux image with systemd and
glibc, for example a Buildroot aarch64 image. It is generic: nothing here depends on what else the
image runs.

| File | Installed as | Purpose |
| --- | --- | --- |
| `systemd/orion-node.service` | `/etc/systemd/system/orion-node.service` or `/usr/lib/systemd/system/` | The unit: `Type=notify`, watchdog, restart policy, `orion` user, hardening. |
| `systemd/orion-node.env.example` | `/etc/default/orion-node.env` | Node settings: the single-node appliance profile from `docs/node-env.md`, with clustering, discovery and link examples commented out. |
| `systemd/orion.sysusers` | `/usr/lib/sysusers.d/orion.conf` | Creates the `orion` user and group (member of `dialout`) at boot through `systemd-sysusers`. |
| `buildroot/orion-users.table` | `BR2_ROOTFS_USERS_TABLES` entry | The same user, created at image build time (read-only root filesystems). |
| `systemd/orion-node.preset` | `/usr/lib/systemd/system-preset/80-orion-node.preset` | Enables the unit on images that apply presets (`systemctl preset-all`). |
| `gaia/orion-node.toml` | imported by a Gaia build | Builds, installs and stages all of the above in a Gaia image (Gaia >= 2.0). |
| `gaia/docker/aarch64-cross.Dockerfile` | used by the Gaia layer | Cross-build environment for `aarch64-unknown-linux-gnu`. |

## Build

```sh
cargo build -p orion-node --release --target aarch64-unknown-linux-gnu \
  --no-default-features --features peer-tcp,discovery-mdns,systemd-notify
```

- `systemd-notify` is required by the unit (`Type=notify` and `WatchdogSec=`). Without it,
  systemd times out waiting for `READY=1` after `TimeoutStartSec=`.
- `peer-tcp` and `discovery-mdns` let the device join other nodes over `orion+tcp` without the
  HTTP stack. Drop them for a strictly standalone node.
- Add `link-gateway` to serve microcontrollers over serial or SocketCAN (`ORION_NODE_LINKS`).
- The release profile is size-optimized (`opt-level = "z"`, fat LTO, stripped). The appliance build
  above is about 3.1 MiB for aarch64.

### Cross-linking

Any aarch64 glibc cross toolchain works; set
`CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc` (Debian/Ubuntu:
`gcc-aarch64-linux-gnu`, which also installs the target glibc and `libgcc_s`). Buildroot's own
toolchain (`output/host/bin/aarch64-buildroot-linux-gnu-gcc`) is the safest choice for a Buildroot
image, because it links against exactly the glibc the image ships. A binary built against an older
glibc runs on newer ones; the appliance build above needs glibc 2.34 or newer at run time.

The link needs the target's `libgcc_s` (the Rust standard library uses it for unwinding). Fedora's
`gcc-aarch64-linux-gnu` package ships only a compiler and an empty sysroot; Fedora's aarch64
glibc sysroot under `/usr/aarch64-redhat-linux/sys-root/` has glibc but no `libgcc_s`. There,
point the linker at that sysroot and give it a `libgcc_s.so` linker script that names LLVM's
`libunwind.a` from the Rust toolchain, which links the unwinder statically:

```sh
SYSROOT=/usr/aarch64-redhat-linux/sys-root/fc43
SHIM=$PWD/target/aarch64-shim; mkdir -p "$SHIM"
cp "$(rustc --print sysroot)/lib/rustlib/aarch64-unknown-linux-musl/lib/self-contained/libunwind.a" "$SHIM/"
echo "INPUT($SHIM/libunwind.a)" > "$SHIM/libgcc_s.so"   # needs: rustup target add aarch64-unknown-linux-musl
cat > "$SHIM/cc" <<EOF
#!/bin/sh
exec aarch64-linux-gnu-gcc --sysroot=$SYSROOT -L$SHIM "\$@"
EOF
chmod +x "$SHIM/cc"
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER="$SHIM/cc" cargo build ...
```

This is for local verification only; image builds should use the image's own toolchain.

### Page size (16 KiB and 64 KiB kernels)

arm64 kernels run with 4 KiB, 16 KiB (for example Raspberry Pi 5 / CM5 kernels) or 64 KiB pages,
and refuse to load an ELF whose `PT_LOAD` segments are aligned to less than the page size. The
workspace `.cargo/config.toml` links `aarch64-unknown-linux-gnu` binaries with
`-z max-page-size=65536`, so one binary runs on all three. GNU ld and LLD already default to 64 KiB
for aarch64; the setting protects against toolchains configured otherwise. A `RUSTFLAGS` or
`CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS` environment variable replaces it, so carry it
over (`-C link-arg=-z -C link-arg=max-page-size=65536`) when you set one. Check a binary with:

```sh
readelf -lW orion-node | grep LOAD    # the last column (Align) must be 0x10000 (or at least 0x4000)
```

## Install

```sh
install -m 0755 orion-node /usr/bin/orion-node
install -m 0644 systemd/orion-node.service /etc/systemd/system/orion-node.service
install -m 0644 systemd/orion-node.env.example /etc/default/orion-node.env
install -m 0644 systemd/orion.sysusers /usr/lib/sysusers.d/orion.conf
systemd-sysusers && systemctl daemon-reload && systemctl enable --now orion-node
```

`systemctl status orion-node` shows the status line the node reports (`serving node=... ipc=...`).

### The `orion` user

The unit runs as the fixed system user `orion` (not `DynamicUser=`, so the state directory has a
stable owner and IPC clients can be granted access through the group). The user must exist before
the unit starts, or it fails with `status=217/USER`:

- writable `/etc` at boot: install `orion.sysusers`; `systemd-sysusers.service` creates the user.
- read-only root filesystem (squashfs and similar): create it at build time. In Buildroot, add
  `packaging/buildroot/orion-users.table` to `BR2_ROOTFS_USERS_TABLES` (space-separated list; keep
  the image's own tables), or run `systemd-sysusers --root=<target dir>` in a post-build script.
- to keep running as root instead, use a drop-in with `User=root` and `Group=root`.

### IPC clients

The node listens on `/run/orion/control.sock` and `/run/orion/control-stream.sock`
(`RuntimeDirectory=orion`, mode 0750, sockets created with `UMask=0007`). With the default
`ORION_NODE_LOCAL_AUTH=same-user`, clients must run as `orion`. To admit other services, set
`ORION_NODE_LOCAL_AUTH=same-user-or-group` in the environment file and give each client service
`Group=orion` as its primary group (the node checks the peer's primary GID, not supplementary
groups). Order clients after the node with `After=orion-node.service` and `Wants=` or `Requires=`.

## The unit

- `Type=notify`, `NotifyAccess=main`: the node sends `READY=1` once every listener is up,
  `STOPPING=1` on shutdown, and `STATUS=` lines (see "Running under systemd" in
  `docs/node-env.md`).
- `WatchdogSec=30s`: the node pings every 15 s while its reconcile loop keeps finishing passes,
  and stops pinging when the loop is wedged, so systemd restarts it. Raise it on slow storage or
  very large state; `WatchdogSec=0` disables it. State replay at start-up is covered by
  `TimeoutStartSec=120s` instead.
- `Restart=always` with `RestartSec=1s` growing to 30 s (`RestartSteps=5`,
  `RestartMaxDelaySec=30s`, systemd 254 or newer; older versions restart every second), and
  `StartLimitIntervalSec=0`, so systemd never stops retrying and never leaves the unit `failed`.
  A crash loop therefore keeps restarting at most every 30 s. Set `StartLimitIntervalSec=` and
  `StartLimitBurst=` in a drop-in if a permanently failing node should stay down instead.
- `SIGTERM` stops the node gracefully (coalesced observed state is flushed); `TimeoutStopSec=30s`.
- Settings: `Environment=` defaults (`ORION_NODE_STATE_DIR=/var/lib/orion`, the two IPC sockets
  in `/run/orion`, `ORION_NODE_HTTP_ADDR=off`, `MALLOC_ARENA_MAX=2`), overridden by
  `EnvironmentFile=-/etc/default/orion-node.env` (optional; the leading `-` ignores a missing file).
- `User=orion`, `Group=orion`, `SupplementaryGroups=dialout` for serial ports. SocketCAN needs no
  group, but the interface must be configured and up (for example by systemd-networkd).
- `StateDirectory=orion` (`/var/lib/orion`, 0750) and `RuntimeDirectory=orion` (`/run/orion`, 0750).
- Hardening: `NoNewPrivileges`, an empty capability set, `ProtectSystem=strict`, `ProtectHome`,
  `PrivateTmp`, kernel tunables/modules/logs and cgroups protected, `ProtectProc=invisible`,
  `MemoryDenyWriteExecute`, `RestrictNamespaces`, `RestrictRealtime`, `RestrictSUIDSGID`,
  `LockPersonality`, `RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK AF_CAN`, and
  `SystemCallFilter=@system-service adjtimex`. `systemd-analyze security` rates it 2.4 (OK).
  Deliberately not set: `ProtectClock=` (it denies the read-only `adjtimex` the node uses for
  clock facts), `PrivateDevices=` (hides serial ports), `PrivateNetwork=` (peer sync,
  discovery), `PrivateUsers=` (breaks group-based IPC access). If `ORION_NODE_AUDIT_LOG` points
  outside `/var/lib/orion`, add `ReadWritePaths=` for it.

### Overriding with drop-ins

Change the unit with drop-ins rather than an edited copy, so package updates still apply:

```ini
# /etc/systemd/system/orion-node.service.d/10-device.conf
[Service]
# Extra device groups (SupplementaryGroups= accumulates; an empty assignment resets it).
SupplementaryGroups=gpio i2c
# Slower storage: allow longer passes before the watchdog fires.
WatchdogSec=60s
```

## Gaia images

`gaia/orion-node.toml` is a Gaia layer. Import it from a pinned Orion source:

```toml
[[sources]]
id = "orion"                       # the layer's artifact builds from this id
kind = "git"
repo = "https://github.com/Prometheus-Dynamics/Orion.git"
rev = "<pinned commit>"

imports = [
  # ...base layers...
  { source = "orion", path = "packaging/gaia/orion-node.toml" },
  # ...layers that override orion-node defaults...
]
```

Use `--set sources.orion.path=/abs/path/to/Orion` to build from a local checkout. Pin the same Orion
commit as any Orion client libraries in the image: the node and its clients must speak the same
control-protocol version.

The layer declares (all ids can be replaced by a later layer that declares the same id):

| Id | Kind | What |
| --- | --- | --- |
| `orion-node` | `[[artifacts]]`, `kind = "rust"` | `package = "orion-node"`, `target = "aarch64-unknown-linux-gnu"`, `profile = "release"`, `no_default_features = true`, `features = ["peer-tcp", "discovery-mdns", "systemd-notify"]`, built in the `gaia/docker/aarch64-cross.Dockerfile` image. |
| `install-orion-node` | `[[install]]` | `/usr/bin/orion-node`, mode 0755. |
| `orion-node-service` | `[[stage.services]]` | `orion-node.service` (staged to `/etc/systemd/system/`). |
| `orion-node-env` | `[[stage.files]]` | `/etc/default/orion-node.env` from the example. |
| `orion-node-sysusers` | `[[stage.files]]` | `/usr/lib/sysusers.d/orion.conf`. |
| `orion-node-preset` | `[[stage.files]]` | `/usr/lib/systemd/system-preset/80-orion-node.preset`. |

It also sets `[providers.rust] allow_nested_build = true` (otherwise Gaia does not run cargo),
`[image] kind = "buildroot"`, and lists its ids in `[image.feed]`.

Things the image provides:

- the `orion` user on a read-only root filesystem (see "The `orion` user"); Gaia has no user
  schema, so add `@source:orion/packaging/buildroot/orion-users.table` to the image's
  `BR2_ROOTFS_USERS_TABLES`;
- its own environment file: redeclare `orion-node-env` with another `src` (and do not also declare
  a `[[stage.env_sets]]` named `orion-node`; both write `/etc/default/orion-node.env`);
- drop-ins: stage them as files under `/etc/systemd/system/orion-node.service.d/`;
- another build image or feature set (for example the `link-gateway` variant): redeclare the
  `orion-node` artifact. Items merge by id and the later one replaces the whole item.

`gaia validate` of a build that only imports this layer (with `--set sources.orion.path=...`)
reports no errors.
