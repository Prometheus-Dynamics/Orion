# Decision input: how far to split the communication core from the node logic

Status: input for the user's decision. Nothing here is implemented, and the extraction, backplane
and supervisor slices of [comm-core.md](comm-core.md) are on hold until it is decided. The hello
and access slices go ahead under every option below, because both live on the communication side
in all of them.

## What "node logic" is today

These are `orion-node`'s production modules that are not communication. The communication modules
(transports, links, discovery, auth, status and stream relay, local IPC serving) come to about
16,700 lines; the rest is listed here. The consumers named are the ones this repository and its
docs show.

| Piece | Modules (lines) | What it does | Who consumes it |
| --- | --- | --- | --- |
| Desired state and its merge | `desired_state`, `desired_writes`, `desired_sync`, `hlc_state` (~1,600) | Which workloads, providers, resources and bindings should exist. HLC-merged across peers. | `orionctl apply/get`, remote operators, peer nodes |
| Reconcile and placement | `reconcile*`, `placement`, `cluster_state`, `unsupported_runtime` (~1,500) | Turns desired state into started and stopped workloads. Leaderless placement and leases across nodes. | Executors (local clients that run workloads); multi-node clusters |
| Peer sync semantics | `peer_sync*`, `peer_observed`, `peer_transport` (~1,350) | Exchanges desired and observed state between nodes (the transport itself is communication). | Clusters of more than one node |
| Providers, executors and records | `local_clients`, `state_access`, `link_bridge` record side (~1,800) | A registry of who provides what, resource records and their availability. | Device packages, link devices through the gateway, executors |
| Actions | `actions`, `app/actions/*`, `actions.rs` (~2,350) | Routing, lifecycle, timeouts and claims of one-shot operations. | `orionctl action`, device agents, the fleet tool through the actions endpoint, link devices |
| Persistence | `persistence`, `observed_persist`, storage (~1,750) | rkyv snapshots of desired and observed state, plus link device records. | The node itself across restarts |
| Blobs | `blob_store`, `local_blobs` (~780) | Resumable, digest-checked large files. | `link.firmware.update`, update staging |
| Update intents and safe mode | `update_intents`, `safe_mode`, `maintenance_admin` (~380) | The update data model and maintenance states. | Device agents, `orionctl maintenance` |
| Operators and policy | `operators`, `status_admin`, auth policy (~1,000) | What a remote operator may do. | Remote operator clients, `orionctl` |
| Observability | `observability` (~530) | Metrics and health for all of the above. | Monitoring, `orionctl status` |

Does any of it belong to another dependency?
- **Update intents and blobs** sit closest to the hardware layer's update writer. The writer is
  already outside Orion (the device-agent contract); the intent record and the blob store are the
  Orion side of that contract. They stay unless the hardware layer wants to own the intent model.
- **Workload reconcile and placement** are the part that is least about communication. They are
  what a cluster manager does. If another component of the system is meant to own "which software
  runs where", this is the piece that would move to it. Nothing in this repository depends on it
  except `orionctl` and executors.
- **Everything else** (records, actions, status semantics, persistence of them) is what the
  protocol carries, so it stays with Orion under every option.

## The three options

| | (a) One process, in-process containment only | (b) One repo, two crates, two processes (as designed) | (c) The comm core as a separate dependency |
| --- | --- | --- | --- |
| Shape | Today's `orion-node`. The containment rules of comm-core.md ("Containment inside each process") apply inside it. SSH runs in a second, small root process anyway (privileges force it). | `crates/comm` and `crates/node` in this repository, one binary with two modes, two units, a backplane between them. | A new repository for the comm core (transports, links, hello, access). Orion's node logic depends on it. |
| What survives a node-logic crash, OOM, deadlock or bad upgrade | SSH only. Links, sessions, the relay and hello go down with the node. | Everything on the communication side: links, sessions, relay, hello, SSH. | The same as (b). |
| Extra memory on the test board (measured) | Estimated +~0.5 MB anon for the SSH process (the same order as the measured idle second process; measured once access is built). | +~0.5 MB anon: an idle second `orion-node` process measured 448 kB anon, 7 threads. Text is shared when it is the same binary. The relay state moves from the node to the comm core rather than being duplicated. | The same as (b), plus a second binary's text if the two are not linked into one file: about 3.5 to 4 MB, judging by the aarch64 minimal build. |
| Three-process variant (SSH separate from the comm core) | n/a | Estimated +~0.4 MB more anon, one more socket | same |
| Lines to move or write | ~350 (containment helper) + ~2,600 (access) | + ~16,700 moved, ~1,500 new backplane | (b) plus a new repo's CI, release and versioning, and a public API between the two |
| Interfaces created | none new beyond access | one internal backplane, versioned with the repo, free to change | the backplane becomes a cross-repo interface with its own stability rules |
| Risk | low. It does not deliver "links stay up while the node logic is down". | high, from the refactor size. Mitigated module by module. | (b)'s risk, plus every change to the boundary is a two-repo change |

## Recommendation (the user's call)

**(b), with the cut drawn so (c) stays possible later.** It is the smallest option that delivers
the property the user asked for: links stay up and you can always get in. Measured, it costs about
0.5 MB of anon memory on the test board, against today's single process at 8.2 MB PSS and 5.4 MB
anon. (c) adds a cross-repo interface for no extra runtime property. (a) is the leanest, but it
leaves links and sessions tied to the node logic's health.

If the user leans towards "node logic maybe belongs somewhere else", (b) is also the first step
of that. Once `crates/node` talks to `crates/comm` only through the backplane, moving `crates/node`
(or only reconcile and placement) to another repository is a change of ownership, not a new
design.

## Measurement details

Test board, 2026-11-07:
- Installed `orion-node` (glibc): RSS 9.9 MB, PSS 8.2 MB (5.4 MB anon, 2.8 MB file), 10 threads.
- Second process: the `--no-default-features --features peer-tcp,link-gateway` aarch64 musl
  static build (3,962,776 B), started from `/tmp` with an empty state directory, idle for 15 s.
  - RSS 3.4 MB.
  - PSS 3.4 MB: 448 kB anon, plus 2.96 MB shmem. The shmem is the binary's own pages, because
    `/tmp` is tmpfs.
  - 7 threads.
- Installed from the image, the same binary's text is file-backed and shared with the other mode
  of the same file.
