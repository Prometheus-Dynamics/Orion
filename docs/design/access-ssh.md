# Design: the access core (SSH server in `orion-node`) and the built-in `reboot`

Status: design note, for review. Nothing is implemented yet.

This note is the first slice of the communication core ([comm-core.md](comm-core.md)). The access
process described here is where the comm core starts. Later slices move the transports, links,
discovery and relay into the same process, and it is then named `orion-comm`. The privilege,
supervision and safe-mode model below is the comm core's model.

## Why

Board images ship OpenSSH (`sshd`, `sftp-server`, `ssh-keygen`) and with it OpenSSL, only so that
people can log in, copy files, and reboot a board. Orion already runs on every board. If the
`orion-node` binary speaks the standard SSH protocol itself, an image can drop `sshd` and
OpenSSL. Board agents are being dissolved, so `reboot` also needs a home inside Orion.

Login is the path of last resort: it is how people repair a board when everything else is broken.
So the access core must keep working when the rest of Orion does not. That means a crash, a
deadlock, a full disk, broken config or a broken state directory in the data plane must not stop
it.

## Feature boundary

- A Cargo feature `access-ssh` on `orion-node`, off by default. With it off the binary is byte for
  byte what it is today (the same check as NT4's zero-byte delta).
- Dependencies, all pure Rust on `ring`, no OpenSSL, no C:
  - `russh` 0.65 with `default-features = false, features = ["ring"]`. This drops `aws-lc-rs`
    (C and assembly), `flate2` and `rsa`. It keeps Ed25519, ECDSA P-256/384/521, the
    curve25519, ML-KEM hybrid and DH key exchanges, chacha20-poly1305 and AES-GCM/CTR. RSA user
    keys can come back with the `rsa` feature if images still have RSA `authorized_keys` (see
    risks).
  - `russh-sftp` 3.0 for the `sftp` subsystem. Modern `scp` uses SFTP since OpenSSH 9.0, so this
    also covers `scp`.
  - `sha-crypt` (RustCrypto) only if password login is wanted (`/etc/shadow` `$6$`/`$5$` hashes).
- What the feature adds to the binary:
  - one CLI mode, `orion-node access`;
  - one hidden helper mode, `orion-node access-sftp` (see "sftp");
  - nothing in the node's library API, wire protocols, C surface or default units.
- What it does **not** add: no change to `orion+tcp`, the IPC protocol, liborion, the
  status lane or the HTTP API. The SSH port is separate from Orion's own transports.

## Isolation model

The node service runs as `orion` with an empty capability set and `NoNewPrivileges=yes`
(`packaging/systemd/orion-node.service`). An SSH server must bind port 22, read every user's
`authorized_keys`, and start shells as those users (setuid, setgid, initgroups). It cannot run
inside that process. So isolation is by **process**, not by task set: the strongest boundary there
is, and the one the privilege model forces anyway.

```
orion-node.service (user orion, no capabilities)      orion-access.service (root)
  data plane, control plane, links, peers               orion-node access
                                                          ├─ accept runtime (1 OS thread, current_thread tokio)
            no shared memory, no shared locks             ├─ per-connection tasks (panic-contained)
            no shared files except read-only config       ├─ session children: shell / exec / access-sftp
                                                          │    (fork+exec after setuid to the user)
  IPC sockets  <─── optional client link ─────────────── └─ reboot claim task (optional, see "reboot")
```

- **One binary, two processes.** The same `/usr/bin/orion-node` file runs both. On disk this
  costs only the feature's size delta, and the two processes share text pages, so the access
  process's resident cost is its own heap and stacks.
- **Its own unit**, `orion-access.service`, with no `Requires=` or `BindsTo=` on
  `orion-node.service` in either direction. Stopping, crashing or masking the node never touches
  login, and the reverse holds too.
- **No shared state.** The access process does not open the node's state directory, the node's
  database or the node's lock file. It reads its own config (`/etc/orion/access.toml` or env)
  and the system's account files (`/etc/passwd`, `/etc/group`, `/etc/shadow`,
  `~/.ssh/authorized_keys`). The only link to the node is the optional reboot claim (an IPC
  client connection). If the node is down, that link retries in the background; the SSH path never
  waits on it.
- **Runtime.**
  - The accept loop runs on its own OS thread with a `current_thread` tokio runtime. It
    does no blocking file I/O. Account lookups and key-file reads go to a small bounded blocking
    pool with timeouts.
  - Each connection is one tokio task. A connection may hold at most
    `max_auth_attempts` attempts and `inactivity_timeout`. Global caps:
    - concurrent unauthenticated connections (default 10, the same idea as sshd `MaxStartups`);
    - total connections (default 64).
- **Panic containment.**
  - The release profile unwinds (`panic` is not `abort`), so a panicking connection task ends that
    connection only. Tokio catches it at the task boundary, and the accept loop logs it and keeps
    accepting.
  - Code that runs on the accept thread itself (config reload, host-key load) runs under
    `catch_unwind`.
  - A panic counter feeds the watchdog decision below.
- **Supervision, in layers:**
  1. In-process: the accept loop re-binds the listener with backoff (100 ms doubling to 5 s) if
     `accept` returns a hard error such as `EMFILE`.
  2. A watchdog thread, separate from the runtime, pets `WATCHDOG=1` only while the accept loop
     has made progress within half the watchdog period. It checks a heartbeat the loop bumps once
     per second, even when idle. A wedged runtime therefore stops the pets, and systemd restarts
     the process. This reuses `crates/node/src/systemd.rs` (sd_notify without libsystemd).
  3. systemd settings: `Restart=always`, `RestartSec=1`, `RestartSteps=5`, `RestartMaxDelaySec=30`
     (systemd 254+; images on older systemd keep a fixed `RestartSec=2`). Also
     `StartLimitIntervalSec=0`, so the unit never gives up, and `WatchdogSec=10`. This is the
     crash-loop backoff: it grows to 30 s and never stops.
  4. Safe-mode start (next section) when config or host keys are broken.
- **Resources.**
  - `OOMScoreAdjust=-900`, so the kernel kills workloads before the way in.
  - `MemoryMin=` reserves the access process's working set, measured once implemented.
  - `Nice=-5` and `CPUWeight=` above the node, so a board pegged by workloads still accepts a
    login.
  - Sessions started by login are placed in their own cgroup scope under `orion-access.service`
    with `Delegate=no`. A user's runaway process is then accounted to the session, not to the
    accept loop. Splitting sessions into separate scopes needs logind or a cgroup write (see
    open questions).

## Safe mode for the access core

The access core must start even when its config is wrong. On start:

| Problem | What it does |
| --- | --- |
| Config file missing | Built-in defaults: port 22, public-key auth, `authorized_keys` of all local users. |
| Config file unparsable or invalid | Logs the error, sets status `safe_mode: config_invalid`, uses the built-in defaults, and keeps the bad file untouched for the operator. |
| Host key missing | Generates an Ed25519 host key and writes it with mode 0600. |
| Host key unreadable or corrupt | Moves it aside to `<name>.corrupt-<unix ms>`, generates a new one, logs the fingerprint change loudly. |
| Port in use | Retries with backoff; meanwhile also listens on the fallback port (default 2222) so a second way in exists. |
| No user has any key and password auth is off | Stays up, logs that nobody can log in; does not silently open. |

Safe mode never widens access. It only falls back to the documented default policy.

## What replaces `sshd`

| OpenSSH piece | Replacement |
| --- | --- |
| `sshd` listener, kex, ciphers | `russh` server: curve25519/ML-KEM hybrid kex, Ed25519 and ECDSA host keys, chacha20-poly1305 and AES-GCM. No CBC, no SHA-1 MACs, no DSA. |
| Host keys `/etc/ssh/ssh_host_*_key` | Read in place (OpenSSH key format). Existing `known_hosts` entries stay valid when an image swaps `sshd` out. A new image without keys generates `/etc/ssh/ssh_host_ed25519_key`. |
| `authorized_keys` | `~/.ssh/authorized_keys` and `AuthorizedKeysFile`-style extra paths. Key types: Ed25519 and ECDSA, plus RSA with the `rsa` feature. Options: `restrict`, `no-pty`, `no-port-forwarding`, `command=`, `from=`, `environment=` (off by default as in sshd). A line with an option the access core does not understand is **skipped** (fail closed), never applied without its restriction. |
| Password login (`PasswordAuthentication`) | Off by default. When on: `/etc/shadow` `$6$` (SHA-512 crypt) and `$5$` hashes through `sha-crypt`. Other schemes (yescrypt `$y$`, bcrypt) reject that user's password login and log why. No PAM. |
| Shell and exec sessions | `openpty`, then fork. In the child: `setsid`, `TIOCSCTTY`, `initgroups`, `setgid`, `setuid` (checked to stick), `chdir $HOME`, minimal env (`HOME`, `USER`, `LOGNAME`, `SHELL`, `PATH`, `TERM`, `SSH_CONNECTION`), exec the user's shell. Window change, signals, exit status and exit signal are passed through. No pty for exec unless requested. |
| `sftp-server` | `orion-node access-sftp`: forked and exec'd **after** dropping to the user, with `russh-sftp` running over stdin and stdout. File operations therefore run with the user's rights, exactly as sshd runs `sftp-server`. A crash takes down only that transfer. |
| `scp` | Covered by SFTP (OpenSSH 9.0+ clients). Legacy `scp -t` exec works only if the image keeps an `scp` binary; not provided. |
| Port forwarding (`direct-tcpip`) | Local forwarding (`ssh -L`), on by default for key logins, off when `no-port-forwarding` or `restrict` applies. Remote forwarding (`-R`), agent and X11 forwarding: not provided. |
| `ssh-keygen` for host keys | Not needed on the board. The access core generates its own host key. |
| utmp/wtmp/lastlog | Not written (images on these boards do not read them). One structured log line per login and logout, with user, key fingerprint, source address, session kind and duration, goes through the node's tracing setup and, when configured, the audit log format (`docs/audit-logging.md`). |
| OpenSSL | Gone from the image if nothing else links it. |

### The board security switch ("security off" images)

Some images want the board open while a physical security switch is off, as today. The policy is
one setting, `access.auth`:

| Value | Meaning |
| --- | --- |
| `keys` (default) | Public keys only. |
| `keys+password` | Public keys, then `/etc/shadow` passwords. |
| `open` | Any configured user logs in with no credential (`none` auth), the same behaviour as images that ship an account with an empty password today. |

An image chooses the value. The access core never chooses `open` on its own, and safe mode never
falls back to it. A device package that wants "open while the switch is off" sets `open` from its
switch reader. Images whose switch defaults to off therefore boot open and close as soon as the
switch is turned on; the access core re-reads the policy on `SIGHUP` and on a change of its
config file. When the policy tightens, sessions that the new policy would not admit are closed (an `open` session without a key ends when the switch turns on); new logins
are checked against the new policy.

## The built-in `reboot` action

`reboot` is a well-known node action today (`docs/actions.md`, "Well-known action names"), but
Orion implements none of those actions; a device agent claims it. Board agents are going away, so
`orion-node` ships a handler.

- **Where it runs.** The node process has no capabilities, so it cannot reboot. The handler
  therefore runs in the access process, which is root. That process claims `reboot` from the node
  over local IPC, exactly as a device agent does (`claim_node_actions(["reboot"])`,
  `docs/device-agent.md`). This means:
  - no new interface, wire message or privilege on the node side;
  - the claim conflict rule already says who wins if a device package still ships its own agent;
  - if the node is down there is no action path, but SSH still reaches `systemctl reboot`.
- **Configuration.** `access.reboot_action = true` (the default when the feature is built).
  When false, the access process never connects to the node.
- **Execution.** Validate `delay_ms` (default 1,000, at most 600,000) and `reason`. Then:
  1. report `Succeeded` with `phase = "rebooting"`, `delay_ms`, `reason`, before rebooting, as
     `docs/actions.md` requires of handlers that reboot;
  2. publish `reboot.requested_at` and `reboot.reason` on the status lane (the claim allows
     `<claimed name>.*` keys);
  3. sleep `delay_ms`, then ask systemd to reboot (SIGRTMIN+5 to PID 1, the documented equivalent
     of `systemctl reboot`, with no D-Bus and no fork);
  4. without systemd as PID 1: `sync` then `reboot(RB_AUTOBOOT)`.
  The start of the node after the boot reports the new boot ID in host facts, which is how the
  requester confirms the reboot happened.
- **Node access.** The node admits the root client through its existing allow list. The
  packaged node env lists the access process in `ORION_NODE_LOCAL_PROVIDER_ALLOW` (provider and
  claim rights only, no operator rights; `docs/node-env.md`, "Local IPC access"). So a compromise
  of the access process gains no Orion operator rights it did not already have as root.
- **Client side.** `orion-client` is already a library in the workspace. The binary gains its IPC
  client code only when `access-ssh` is on; the size table counts it.
- **Without SSH.** An image that wants only `reboot` and no SSH server gets a second feature,
  `access-reboot`. It builds the same access process without `russh`.

## Size delta

Measured with the release profile (`lto = "fat"`, `codegen-units = 1`, `opt-level = "z"`,
`strip = "symbols"`), x86_64. The probe is the real `russh` + `russh-sftp` server path (kex, auth,
session, exec, sftp subsystem). Real code adds handlers on top, so the final delta is the probe
plus the access core's own lines.

| Binary | Bytes | Delta |
| --- | --- | --- |
| Probe baseline (tokio + ring) | 458,384 | |
| Probe + russh + russh-sftp | 1,698,248 | +1,239,864 |
| `orion-node` (default features) | 6,366,720 | |
| `orion-node` + probe | 7,429,464 | **+1,062,744** |

On aarch64 musl static (the board and initramfs target), the comm-core-min feature set grows
from 3,962,776 to 4,928,128 B with the probe: +965,352 B ([comm-core.md](comm-core.md), "Size").

Inside `orion-node` the delta is 177 KB smaller than standalone, because `ring`, `tokio`,
`sha2` and `rand_core` are already linked. What is left is `russh` itself plus the RustCrypto
crates it brings: `ssh-key`, `ssh-encoding` and `ssh-cipher`; `p256`, `p384` and `p521`;
`aes-gcm`; `ml-kem`; and `bcrypt-pbkdf` and `argon2`, which come in through key-file decryption.
The probe does not include the IPC client code for the `reboot` claim; the implementation's size
gate measures it.

Against an image with OpenSSH: images drop `sshd`, `sftp-server`, `ssh-keygen` and, when nothing
else links them, `libcrypto` and `libssl`. Their exact sizes depend on the image (measured per
image when it switches).

## Scope

| Part | Production lines | Interfaces touched | Risk |
| --- | --- | --- | --- |
| Accept loop, limits, config, host keys, safe mode | ~600 | new CLI mode, new unit, new config file | medium: runs as root pre-auth (see risks) |
| Auth: `authorized_keys` parsing and options, shadow passwords, `open` policy | ~450 | none outside the access core | high: a parsing mistake widens access; fuzzed and tested against OpenSSH's own option semantics |
| Sessions: pty, shell, exec, env, signals, exit status | ~450 | none | medium: privilege drop must be checked (`setuid` result, `getgroups` after) |
| `access-sftp` helper | ~400 | hidden CLI mode | low: runs as the user |
| Local port forwarding | ~120 | none | low |
| Supervision: watchdog thread, heartbeat, panic counter | ~200 | reuses `systemd.rs` | low |
| `reboot` claim and executor | ~150 | none new (existing claim protocol) | low |
| Packaging: `orion-access.service`, config example, docs | ~250 | new unit | low |
| Tests | ~900 | | |

The tests, all in CI with a `russh` client:
- key login with each key type;
- rejection of an unknown key, a skipped option line and a `from=` mismatch;
- exec output and exit status;
- pty shell;
- sftp put, get, list and rename, with the user's permissions enforced;
- local forward;
- a panic in one connection with the others unaffected;
- safe mode on bad config and on a corrupt host key;
- watchdog stop on a wedged runtime;
- the `reboot` claim end to end, with a fake executor.

Board test: replace `sshd` on the test board, then log in, copy and reboot.

## Risks and open questions

- **Root pre-auth code.** OpenSSH separates privileges so that pre-auth parsing runs
  unprivileged. `russh` has no such separation; its pre-auth parsing runs in the root process.
  - Mitigations: memory safety; the connection caps; `SystemCallFilter=` on the unit (fork, exec,
    setuid and pty calls allowed); and fuzzing the `authorized_keys` and options parser.
  - A later step can split pre-auth into an unprivileged child, the same pattern as
    `access-sftp`.
- **`ssh-key` is a release candidate** (0.7.0-rc.11) pulled in by `russh` 0.65. The note records
  it, and `deny.toml` and the lockfile pin it until it is stable.
- **RSA user keys.** If images in the field still carry RSA `authorized_keys`, the `rsa` feature
  brings back the `rsa` crate (pure Rust; it was subject to the Marvin timing advisory,
  RUSTSEC-2023-0071). It is only used for signature **verification** of user keys, where that
  advisory does not apply. Decide per image.
- **yescrypt passwords.** Newer distributions hash `/etc/shadow` with `$y$`. If a target image
  does, password login needs a yescrypt implementation. Otherwise images set `$6$` hashes, or
  use keys or `open`.
- **Session cgroups.** Without logind there is no per-session scope. The first version keeps
  sessions in the unit's cgroup and kills the whole session process group on disconnect.
- **Duplicate host fingerprints.** Reading `/etc/ssh` host keys keeps old fingerprints. An image
  that never had OpenSSH gets a new key on first boot, the same as sshd's first-boot keygen.
