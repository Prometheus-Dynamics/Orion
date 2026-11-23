# The wire

Every carrier sends the same frames. There is one protocol, version 1.

```text
[version u8][kind u8][seq u16 LE][payload][crc32c u32 LE]
```

The CRC is CRC-32C (Castagnoli) over everything before it. It uses the CPU's CRC instructions where
they exist. A receiver reports a wrong version rather than dropping the frame, so it can answer.

## Carriers

| Carrier | Framing |
| --- | --- |
| Same board | `[u32 LE length][frame]` on a Unix socket, with file descriptors attached to a frame |
| Network | The same prefix, inside the signed session |
| Browser | One frame per WebSocket message |
| UART, RS-485, USB-CDC | COBS, `0x00` delimited. A receiver resynchronizes at the next `0x00` |
| CAN, CAN FD | Segments, each with a header byte: bit 7 start, bit 6 end, bits 5..0 counter |

Details of COBS and segmentation:

- Canonical COBS. A sender may put a `0x00` before each frame, so line noise can't damage the next
  one. Empty packets are ignored.
- CAN segments are never padded. Every segment is a valid CAN FD length, so the received length is
  exact. A missing or out-of-order segment discards that message. A byte-identical repeat of the
  last segment is ignored.
- A frame has no maximum on the wire. Receivers bound it with their buffers, and a socket peer's
  claimed length is checked before anything is buffered.

## Kinds

The whole table is in `crates/core/wire/src/kind.rs`. A number is never reused, and a build fails if
one is repeated or sits outside its range. A comm core answers a kind it does not serve with a
`Refusal` of `unsupported` under the frame's `seq`, so a newer client's request to an older node
fails at once, not by a timeout. (A node from before revision 1's level 1 drops such a frame
without a word.) A client ignores kinds it doesn't know.

| Range | Concept |
| --- | --- |
| `0x00` to `0x0F` | Session |
| `0x10` to `0x1F` | Records |
| `0x20` to `0x2F` | Files |
| `0x30` to `0x3F` | Actions |
| `0x40` to `0x4F` | Topics |
| `0x50` to `0x5F` | Workloads and executors |
| `0x60` to `0x6F` | Peers and trust |
| `0x70` to `0x7F` | Time |
| `0x80` to `0x8F` | Shell |
| `0x90` to `0x9F` | Link: a signed session on a link, and its sealed frames |
| `0xA0` to `0xAF` | Node: what the comm core tells node logic of a session, beyond the full session range (`NODE_QUIET`) |

## Bodies

Bodies are postcard, with their fields in a fixed order. Postcard is not self-describing, so these
rules are what keep old and new builds talking:

1. **Append only.** A new capability is a new kind, or a field at the end of a body. Nothing changes
   in place and nothing is removed.
2. **Short is fine.** A body shorter than the reader's type leaves the appended fields at their
   defaults. The fields a body started with are required.
3. **Long is fine.** A reader ignores trailing bytes it doesn't understand.
4. **Enums that may grow are open.** They travel as a number, and a value the reader doesn't know
   arrives as `Unknown(n)` rather than failing the body. Enums with data inside are closed: a new
   variant is appended and sent only to a side at the level that has it.
5. **Lists carry lengths.** A list is a total byte length, then each element with its own length in
   front. An element can grow by the same rules, and a reader can skip one it can't decode.
6. **The version stays 1.** A bump would stop every deployed peer, so it is reserved for a change
   to the framing itself. Other changes use the revision and the level.

## Revision and level

`Hello` and `Welcome` each carry two numbers: the wire revision and, appended, the additive level.

**The revision** (`INTERFACE_REVISION`, 1) goes up only for a change an older side would misread.
Both sides must speak the same one. A comm core turns a `Hello` of another revision away with
`Reject { RevisionMismatch, node_revision, peer_revision }`, on every carrier, recovery's
included. A client turns away a `Welcome` of another revision, and reports either as
`ClientError::Revision { node, client }`. A node from before the check never compares, so the
client's own check is what stops a session with one.

**The level** (`INTERFACE_LEVEL`) goes up by one for every addition whose meaning an older side
would change by ignoring it, or would not answer at all. A side that sends none is at level 0. A
client asks for such a feature only of a node at its level or above: below it, the call fails
with `ClientError::Level { feature, needs, node }` and sends nothing. Additions an older side may
safely ignore have no level.

| Level | Feature (`orion_connect::Feature`) | What an older node would do |
| --- | --- | --- |
| 1 | `SubscribeFromLatest`: `Declare.on_subscribe = Latest` | Start every subscriber from now, silently |
| 1 | `Shell`: `SHELL_OPEN` | Not answer |
| 1 | `SignedLink`: `LINK_SIGN_HELLO` on a serial or USB link | Not answer; the client then says hello plainly to read the level |
| 1 | `DetachedRun`: `Run.detached` | Run it with no deadline and no limit, and never tell its program the caller went |
| 1 | `RunOnBehalf`: `Run.on_behalf`, from a node | Take every caller behind the link as one, and never hear one of them end |
| 2 | The error `restoring` (20) | (told to a session, not asked of a node: see below) |
| 2 | The error `layout` (21) | (told to a session: see below) |
| 2 | The error `lease lost` (22) | (told to a session: see below) |
| 3 | `RunHandle`: `Attach` and `Cancel` naming a run by `instance` and `run` | Read the number alone, which may name another run of the same caller |
| 3 | The error `run gone` (23) | (told to a session: see below) |
| 4 | `FileOffers`: `FILE_OFFER`, `FILE_FETCH`, `FILE_OFFERS` | Not answer |
| 5 | `SecurityPrecheck`: `SECURITY_PRECHECK` | Not answer |
| 6 | `KeptRuns`: `Register.keep_ms`, `Claim.kept` | End the program's detached runs as failed when its session is lost |
| 7 | `BasedOn`: `WorkloadPut.based_on`, `WorkloadRemove.based_on`, a stamp on an entry of a client's `Put` | Write whatever is there, though the writer said to write only if it had not changed |
| 7 | The error `stale` (24) | (told to a session: see below) |

**Errors have levels too.** An error code added after level 1 is told only to a session that said
it is at that code's level or above. A session below it is told what it was told before the code
existed. `restoring` (20) is level 2: a session at level 0 or 1 is told `not found` in its place,
so a client of an older build never meets a code it does not know. `lease lost` (22) is level 2
too, and a session below it is told `busy`. `layout` (21), a ring offered by a publisher built
against another shared-memory layout, is level 2, and a session below it is told `malformed`.
`run gone` (23), a run handle from before node logic last started, is level 3, and a session
below it is told `not found`. `stale` (24), a write refused because the record is no longer the
one it was based on, is level 7. A session below it never asks for such a write; one that put a
stamp on an entry all the same is told `already exists`.

**Levels mixed, both ways, at level 7.** A level-6 client and a level-7 node: the client's
`WorkloadPut` and `WorkloadRemove` have no `based_on`, which the node reads as "based on
nothing", and the node's `WorkloadStatus` has a stamp appended, which the client does not
read. A level-7 client and a level-6 node: a write based on nothing carries one byte more,
which the node does not read; a write based on a version is not sent at all
(`ClientError::Level`), because that node would write whatever is there. `WorkloadStatus` from
it has no stamp, read as zero.
`orion_server::spoken` is the one place this is decided, for the comm core's own refusals and
for node logic's. Words sent with a refusal go only with the code they were written for: a
session told the older code is told that code's own words.

Additions with no level, because an older side ignores them and does nothing different that
matters: `ACTION_STARTED` and the `instance`, `run` and `action` appended to `Progress` and
`ActionResult` (an older client follows by its number, as before), `Cancel.reason`, `Claim.session`, `Claim.detached` (false from an older node), `ACTION_CALLER_GONE` to a program, the hello document's
`state`, `recovery_why`, `security` and `security_generation` facts, `Reject`'s two revisions, and the resource state `Restoring` (4).
An older program reads that state as one it does not know, which is not `Ready`, so it waits as
it should.

Level 1 is the first level. A node at Orion `next` before this change (R2 at `dbd5fc0`) sends no
level, so it is at 0, though it knows every level-1 feature.

The recorded bytes in `crates/testing/wire-vectors` pin every kind, so any change to a body fails a
test.
