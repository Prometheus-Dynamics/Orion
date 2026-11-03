# Blob push

A local client streams a large file (an OS image, hundreds of megabytes) to the node in chunks.
The node stores it under a configured directory, checks its SHA-256 and reports where it is. The
transfer is resumable by offset and bounded in memory on both sides. It is a local IPC feature
(control protocol 1); the HTTP and peer transports refuse blob messages.

Use it for bytes that do not fit an action argument (`String` values 1 KiB, `Bytes` 64 KiB, one
request 256 KiB). A device agent that needs the image then reads the committed file at the `path`
the node reports, or the requester names the blob in its own action (see
[device-agent.md](device-agent.md)).

## Flow

```text
client                                   node
BeginBlob(id, size, sha256)   --->       Blob(status: receiving, received = 0 or where it stopped)
WriteBlobChunk(id, offset, data) --->    Blob(status)            offset must equal received
  ... (repeat; after a disconnect BeginBlob again, then continue at status.received)
CommitBlob(id)                --->       Blob(status: verifying)  digest checked in the background
QueryBlobs(Some(id))          --->       Blobs([status])          until committed or rejected
DeleteBlob(id)                --->       Accepted
```

`LocalControlPlaneClient::push_blob(blob_id, path, sha256_hex, chunk_bytes)` runs the whole flow
from a file. It reads one chunk at a time, begins or resumes, commits, and polls until the digest
check settles. It returns the final `BlobStatus`: `Committed`, or `Rejected` with the reason
(a digest mismatch drops the bytes and releases their budget).

## Messages

| Message | Answer | Notes |
| --- | --- | --- |
| `BeginBlob(BlobBegin { blob_id, size, sha256 })` | `Blob(BlobStatus)` | Starts a blob, or resumes one receiving with the same size and digest (the answer carries `received`). A blob that exists with another size or digest is refused. |
| `WriteBlobChunk(BlobChunk { blob_id, offset, data })` | `Blob(BlobStatus)` | Writes only at `offset == received`. Any other offset writes nothing and answers with the status, so the client resumes from `received`. |
| `CommitBlob(blob_id)` | `Blob(BlobStatus)` | Requires every byte. Answers `verifying`; the outcome arrives as `committed` or `rejected`. |
| `QueryBlobs(Option<blob_id>)` | `Blobs(Vec<BlobStatus>)` | All blobs, or one. |
| `DeleteBlob(blob_id)` | `Accepted` | Removes the bytes and the record. A blob being verified cannot be deleted. |

`BlobStatus` has `blob_id`, `size`, `received`, `sha256` (lowercase hex), `state`
(`receiving`, `verifying`, `committed`, or `rejected { reason }`), and `path` (set when committed).

## Who may use it

Local IPC callers whose role is `ControlPlane`, `Provider` or `Executor`, admitted by
`ORION_NODE_LOCAL_AUTH` or `ORION_NODE_LOCAL_AUTH_ALLOW`, or by `ORION_NODE_LOCAL_PROVIDER_ALLOW`
(the provider lane includes blob push, see [node-env.md](node-env.md#local-ipc-access)).

## Storage and limits

| Setting | Default | Meaning |
| --- | --- | --- |
| `ORION_NODE_BLOB_DIR` | unset | Directory for blobs. Unset uses `<ORION_NODE_STATE_DIR>/blobs`; without a state dir blob push is off and every blob message is refused. |
| `ORION_NODE_BLOB_MAX_BYTES` | `2147483648` (2 GiB) | Bytes all blobs may hold together, partial and committed. A `BeginBlob` that would exceed it is refused. |
| `ORION_NODE_BLOB_CHUNK_MAX_BYTES` | `1048576` (1 MiB) | Largest chunk, at most 4 MiB (one control frame is 8 MiB). |

The store holds at most 64 blobs. A blob id is 1 to 128 characters of letters, digits, `.`, `_`
and `-`, starting with a letter or digit, and not ending in `.part` or `.sha256`, so an id can
never name a path. Files are `<dir>/<id>` (committed, with `<id>.sha256` beside it) and
`<dir>/<id>.part` (receiving). The directory is created with the node's umask; restrict it with
the service's user and group, as for the state directory.

## Memory and disk

- A chunk is at most `ORION_NODE_BLOB_CHUNK_MAX_BYTES` and is written straight to the file at its
  offset. Nothing holds the whole file in RAM, on the node or in `push_blob`.
- The digest check reads the file in 1 MiB pieces on one background thread, so a large commit never
  blocks the node. The queue of commits is bounded by the 64-blob cap.
- The byte budget counts declared sizes from `BeginBlob`, so a client cannot pass the cap by
  sending a huge declaration later.

## Restarts and disconnects

- A client that disconnects keeps its partial blob. Begin again with the same size and digest, and
  continue at `received`.
- A node restart keeps partials: at startup each `<id>.part` is adopted with the bytes it holds.
  The size and digest arrive with the next `BeginBlob` for that id, which also charges the budget.
  Committed blobs come back as committed when their `.sha256` file is present.
- A digest mismatch removes the partial and releases its budget. The record stays as `rejected`
  with the reason until `DeleteBlob`.

## Not in this version

- The HTTP transport and peer sync carry no blob messages. Blob push is for the local node.
- Blobs are not replicated and are not in desired state. They live on the node that received them.
- The node does not run the image. A device agent reads the committed file (see above).
