# Settings

Both services, `orion comm` and `orion node`, read one file of `key = value` lines,
`/etc/orion/orion.conf` unless another is named (`orion comm FILE`). The environment overrides
the file: a key's name in capitals, with `_` for `.`, after `ORION_` (`peer.listen` is
`ORION_PEER_LISTEN`, `socket.node_uids` is `ORION_SOCKET_NODE_UIDS`). A line starting with `#` is a comment.

A key that neither service reads is logged by both when they start, so a typo is seen; a key
that only the other one reads is not. A key for a part that is not built in is a key nobody
reads. A value that does not parse stops the start and names the key; the comm core then opens
the recovery way in (`docs/comm.md`, "Recovery").

`packaging/systemd/orion.conf.example` is the file a board starts from.

## Read by both

| Setting | Default | What it is |
| --- | --- | --- |
| `node.name` | `node.local` | this node's name |
| `socket` | `/run/orion/orion.sock` | the board's socket |
| `state.dir` | `/var/lib/orion` | the identity, the trust store, the saved time |
| `keys.dir` | `state.dir` | where the node's key and the trust file are |
| `hello.state` | `running` | what the hello document says the board is doing: `running`, or `flasher` for a flasher image |
| `state.require_mount` | `false` | the comm core refuses to start when `state.dir` or `keys.dir` would be on the root file system (for boards updated by slot) |

The comm core keeps its SSH host key and saved time in `state.dir`, and its identity and trust
store in `keys.dir`; node logic keeps its records, files and audit log in `state.dir`, and reads
the key and the trust file in `keys.dir` to act as the node toward other nodes. On a board the
two have different state directories and one `keys.dir`, the comm core's
(`packaging/README.md`). The comm core makes `state.dir` and `keys.dir` if they are not there,
and writes and removes a file in each before it starts: a directory it cannot make or write
puts it in recovery, with the setting and the reason as the status line.

## The security switch

The switch is not a setting. Both services read it from the board's record,
`/etc/board/security`, when they start, and log what it says:

```
on
generation 3
```

The first line is `on` or `off`; the second is the record's generation, which its writer raises
each time. No file means off.
A file that is there and cannot be read, or says anything else, means **on**, and both services
say loudly which file and what is wrong with it. A `security` line in this file is ignored, with
a warning that says where the switch is.

## The comm core

The name in brackets is the cargo feature that carries the part.

| Setting | Default | What it is |
| --- | --- | --- |
| `socket.mode` | `0660` | its permission bits: who on the board can connect at all |
| `socket.node_uids` | none | users whose programs may be the node logic, besides root |
| `peer.listen` | off | where other nodes and people's tools connect (`tcp`) |
| `peer.interface` | any | the network interfaces `peer.listen` takes connections from, one listener kept to each (`usb0, eth0`); needs `peer.listen` |
| `comm.*`, `topics.*` | | the server's and the broker's limits |
| `outbound.max_bytes` | 131072 | what may wait to be sent to one subscriber without a ring (`outbound`) |
| `ws.listen` | off | where browsers connect (`ws`) |
| `ws.origins` | any | the pages that may connect, by origin |
| `ws.send_buffer` | 262144 | the most bytes the kernel may hold for one browser that has not taken them; 0 leaves it to the kernel |
| `ssh.listen` | off | where SSH clients connect (`ssh`) |
| `ssh.interface` | any | the network interfaces SSH is open on, one listener kept to each at `ssh.listen`'s port, 22 if not set (`usb0, eth0`); turns SSH on by itself |
| `ssh.host_key` | `<state.dir>/ssh_host_ed25519_key` | the host key, made if it is not there |
| `ssh.helper` | this program | the program sessions run in |
| `ssh.forward` | `true` | whether someone logged in may ask for connections from the board (`ssh-forward`) |
| `ssh.sandbox` | `true` | whether what SSH clients send is read in a child with no rights (`ssh-sandbox`) |
| `ssh.sandbox_user` | `nobody` | the account that child becomes |
| `ssh.max_connections` | 64 | the most SSH connections at once |
| `ssh.max_unauthenticated` | 10 | the most that have not logged in |
| `ssh.login_ms` | 60000 | how long a connection has to log in |
| `shell` | `on` | whether a shell may be opened over an Orion session, `on` or `off` (`shell`); the shell runs in `ssh.helper` |
| `login.helper` | none | the login helper's socket, which starts every login (`login-helper`); none starts them in the comm core, which must then be root to let in anyone but its own user. The systemd unit sets `/run/orion-login/login.sock` |
| `login.comm_user` | `orion-comm` | the comm core's user, a name or a number: the only one the login helper answers |
| `login.idle_ms` | 10000 | how long the login helper stays with no session running and nothing asked |
| `recovery.keys` | `/etc/orion/recovery_keys` | the public keys that may log in when the comm core is in recovery (`recovery`) |
| `nt4.listen` | off | where NT4 clients connect, or `ws` to share `ws.listen` (`nt4`) |
| `nt4.server` | off | the NT4 server to join as a client, `host` or `host:port` (`nt4-client`) |
| `nt4.name` | `orion` | what the board calls itself on that server |
| `nt4.period_ms` | 20 | how often mapped topics are read for NT4 |
| `nt4.publish` | none | the fields that go to NT4, each `topic:field=type@name` |
| `nt4.accept` | none | the NT4 topics whose values are published here, in the same form |
| `nt4.max_payload` | 1024 | the longest text, bytes or array taken from NT4 |
| `peer.nodes` | none | where other nodes are, `name=address:port`, for subscribing to their topics (`topic-peers`) |
| `cluster`, `enrollment.key`, `operators.*` | | whom the node trusts; a `cluster` longer than 252 bytes is more than the mDNS announcement carries, and `_orion._tcp` is then not announced (the log and `orion/state` say so) |
| `status.interval_ms` | 1000 | how often vital signs are read while watched (`self-status`) |
| `log.window_bytes` | 262144 | how many bytes of the newest log lines are kept in memory, whole lines, for `orion/log` (`offers`); 0 keeps none |
| `offers.file` | `/etc/orion/offers.conf` | files the comm core serves while no program offers their prefix: paths and commands (`offers`; format in `docs/comm.md`). Must be root's and written by no one else; none is none |
| `offers.user` | the comm core's own | whom those commands run as; another user needs a comm core that is root |
| `time.*` | off | time sync (`time`) |
| `mdns` | on | the board announced by name on the local network, `<name>.local` with `_orion._tcp`, `_ssh._tcp` and `_sftp-ssh._tcp` where they can be reached; `false` announces nothing (`announce`; `docs/comm.md`) |
| `mdns.interface` | every one that is up | the interfaces it is announced on, by name, comma separated |
| `discovery`, `discovery.ttl_ms`, `discovery.interfaces` | off | finding peers (`discovery`, `mdns`); with the responder built in, discovery only browses |
| `links.serial` | none | `name=path@baud`, comma separated (`link-serial`) |
| `links.sealed` | none | `name=address:port` (`link-sealed`) |
| `links.can` | none | `name=interface:device-to-host:host-to-device:addresses[:fd][:extended]` (`link-can`) |
| `links.usb` | none | names; their handles are handed over at run time (`link-usb`) |
| | | passing records to node logic, no setting (`node`) |

## Node logic

| Setting | Default | What it is |
| --- | --- | --- |
| `records.save_after_ms` | 500 | how long after a change the records are saved |
| `records.max_drift_ms` | 300000 | a stamp from a peer further ahead than this is refused |
| `files.dir` | `files` in `state.dir` | where files sent to this node go |
| `files.max_size` | 16 GiB | the largest file taken |
| `files.receivers` | none | prefixes that belong to programs that receive files, as `slots/, devices/`: a file under one is never stored by the node, and is refused `unavailable` until its receiver is there (`docs/node.md`, "Files that go straight to a receiver") |
| `files.root.<name>` | none | a root clients reach as `roots/<name>/`: `<directory> rw` or `<directory> ro`, one line each (`docs/node.md`, "The image's roots") |
| `workloads.stop_grace_ms` | 10000 | how long a run asked to stop is given before it is told to end |
| `workloads.placement_grace_ms` | 10000 | how long a node that would take over a workload waits for its node to come back |
| `actions.max_detached_ms` | 3600000 | the longest deadline a detached run may have; a detached run must have one |
| `actions.confirm_ms` | 10000 | how long a program has to say how a run ended once node logic told it to stop (a deadline, or stopping); with no word the run ends `Indeterminate`. Also the longest node logic's stop waits |
| `leases.quiet_ms` | 10000 | how long a program on the board that holds a lease with no end must be quiet before another claimant may take it |
| `audit.file` | `audit.log` in `state.dir` | the audit log; `off` for none |
| `audit.max_bytes` | 1 MiB | the most the file holds before it is rotated |
| `audit.keep` | 3 | how many files from before are kept |

## Others

- `ORION_LOG`: how much the services write to standard error, one line an event: `error`,
  `warn`, `info` (the default), `debug` or `trace`.
- `ORION_RECOVERY_KEYS`, `ORION_RECOVERY_LISTEN`: for `orion-recovery`, which reads no file
  (`docs/comm.md`, "The comm core alone").
