# The topic ring

`crates/core/ring` (`orion-ring`). One writer, many readers, in shared memory. This page pins the
layout: layout number 1. A change to anything here needs a new layout number.

## What it gives

- A publish is memory writes only: no system call, no allocation, no reply, no waiting on a reader.
- A reader never writes to the ring. It cannot slow the writer or another reader.
- The newest value of a field, a field's history, and the order of everything published all read
  the same slots. A sample is stored once.
- A busy field overwrites only its own history. A slow field's newest value stays until that field
  publishes again.
- A reader that falls behind skips ahead and is told how many samples it missed.
- A reader trusts nothing in the ring. A writer can damage only its own topic.

## The memory

A ring is a memfd made by its owner (the comm core), sized from the declaration, rounded up to
pages, and sealed with `F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL`. Its size can never change, so
a mapping of it can never fault.

- The **write end** is the memfd. Its one publisher maps it read-write.
- A **read end** is the same memory reopened read-only through `/proc/self/fd`. It cannot be
  mapped writable, so a subscriber cannot touch the topic.
- A reader refuses a descriptor that is not sealed against shrinking.

The memory is an array of little-endian 64-bit words. Every access, on both sides, is an atomic
load or store of one word. That is what makes a reader racing the writer defined behaviour.

## Layout

Offsets are in words. Only the declaration's numbers are stored; every offset is derived from them
the same way by every party, so there is no stored offset to point somewhere else.

| Word | Holds | Written by |
| --- | --- | --- |
| 0 | Magic: `ORIONRG1` | Owner, once, last |
| 1 | Layout number: 1 | Owner, once |
| 2 | Fields (low 32 bits), history (high 32 bits) | Owner, once |
| 3 | Largest payload in bytes (low 32), expiry in ms (high 32) | Owner, once |
| 4 | Total words in the ring | Owner, once |
| 5 to 7 | Reserved, zero | |
| 8 | Head: how many samples were ever published | Writer |
| 9 | Subscriber count (low 32) | Owner |
| 10 to 15 | Reserved, zero | |
| 16 | Lane heads: one word per field, how many samples that field ever had | Writer |
| 16 + fields | Index: `index_slots` words | Writer |
| 16 + fields + index_slots | Slots: `fields × history` slots of `slot_words` words | Writer |

- `history` is the declaration's history rounded up to a power of two, at least 2.
- `index_slots` is `fields × history` rounded up to a power of two.
- `slot_words` is `4 + ceil(largest payload / 8)`.
- Limits: 65,535 fields, a history of 2^24, a payload of 16 MiB, 4 GiB in all.

**A slot** belongs to one field. Slot `n` of a field holds that field's sample number `n` modulo
`history`.

| Word | Holds |
| --- | --- |
| 0 | Stamp. 0: never written. `(seq + 1) << 1`: holds the sample with sequence `seq`. That plus 1: being written |
| 1 | Sample time, nanoseconds |
| 2 | Payload length in bytes (bits 0 to 31), field (32 to 47), flags (48 to 55) |
| 3 | Lane sequence: which of its field's samples this is, counting from zero |
| 4 on | Payload, packed little-endian, the last word zero-padded |

`seq` counts every sample of the ring, across fields, from zero.

**An index entry** says where the sample with sequence `seq` is: entry `seq` modulo `index_slots`
holds the field (bits 0 to 15) and the slot within that field's lane (bits 16 on). It carries no
sequence of its own: the slot's stamp is what proves the sample is the one asked for.

## Publishing

1. Store the busy stamp in the field's next slot. Release fence.
2. Store time, meta, lane sequence and payload words.
3. Store the finished stamp.
4. Store the index entry, then the lane head, then the head.

## Reading

**In order (`next`).** The reader keeps the sequence it wants next.

1. Load the head. Equal to the wanted sequence: nothing new.
2. More than `index_slots` behind: jump to the oldest sequence the index still covers and report
   how many were skipped.
3. Load the index entry. Check the field and slot are in range. Read the slot, expecting exactly
   the wanted sequence's stamp. A slot that holds anything else was overwritten by its own field:
   that sample is counted as missed and the reader moves on, at most 64 steps per call.

**Newest of a field (`latest`).** Load the lane head, read the slot before it, expecting exactly
that lane sequence. One memory read to find it, whatever the number of fields. If the writer has
already reused the slot, look again with a fresh lane head, at most 4 times.

**History (`at`).** A field's sample number `n` is in slot `n` modulo `history` until that field
has published `history` more. The slot's lane sequence must be exactly `n`.

A slot is reused, so a read always says which sample it wants. A slot holding a newer sample is
not an answer: returning it would let the next `latest` go backwards, to a sample published
before it. The model check found exactly that, which is why the lane sequence is in the slot.

**Reading a slot**, always the same way:

1. Load the stamp. Zero, busy or not the expected one: not there.
2. Load time, meta and lane sequence. A length over the largest payload, or a lane sequence that
   is not the wanted one: not there.
3. Copy the payload words into the reader's own buffer.
4. Acquire fence, then load the stamp again. Changed: not there.

What is returned is the reader's copy, checked. Nothing borrowed from the ring is ever handed out.

## Why a reader never sees a torn sample

The writer's order is: busy stamp, release fence, data, finished stamp. The reader's order is:
stamp, data, acquire fence, stamp. If any word the reader copied was written by a newer publish,
the fence rule orders that publish's busy stamp before the reader's second stamp load, so the
second load sees a different stamp and the copy is thrown away. If no copied word came from a
newer publish, the copy is the sample the first stamp named. Stamps carry the sequence, which never
repeats, so an old stamp can never come back.

The head, lane heads and finished stamps are stored with release ordering and loaded with acquire,
so a reader that sees a count also sees everything stored before it.

`crates/testing/ring-model` checks this with `loom`, which explores the interleavings of a writer
against readers and what a relaxed load may return, running the ring's own code. It also holds
the hostile-ring check that the `ring` fuzz target drives. A deliberately broken reader, one that
skips the second stamp load, is in the same file and must be caught.

## What a reader assumes about the writer: nothing

- The shape is read once at attach, checked (magic, layout, limits, the stored size against the
  derived one, the derived size against the mapping) and copied. It is never read again.
- Every index into the ring is made from that copy, or checked against it.
- A length is checked before it is used. A field number from the index is checked before it is
  used.
- Every loop is bounded: 64 steps in `next`, 4 tries in `latest`. A head that jumps anywhere costs
  one skip.
- An index past the mapping reads as zero instead of faulting.

A hostile writer can make its own topic's readers see nothing, or see samples it invented. It
cannot make them read out of bounds, spin, or fault.

## One writer

There must be one writer per ring. The owner hands the write end to one publisher. A new writer
attaching to an existing ring (a publisher that restarted) carries on from the stored head; a slot
the old writer died in is busy until its field's lane comes round to it.

## Doorbells

`crates/core/ring-doorbell`. A subscriber has one doorbell for everything it subscribes to: an
eventfd it polls, and a sealed page that only it writes. A publisher holds the eventfd and the page
read-only.

Word 0 of the page is `sleeps << 1 | asleep`.

- Subscriber, before sleeping: store asleep, sequentially consistent fence, look for samples once
  more, and sleep only if there are none.
- Publisher, after publishing: sequentially consistent fence, load the word. Asleep, and not the
  sleep it last rang for: one `write` to the eventfd.

One of the two fences comes first. Either the subscriber's second look sees the sample, or the
publisher sees it asleep. A subscriber that is awake costs the publisher a fence and a memory read,
no system call. One sleep gets one ring however many samples follow. Ringing a full or orphaned
eventfd does nothing and never blocks.

Checked with `loom` in the crate's tests, with two broken variants that must be caught: a
subscriber that does not look again, and a publisher that does not fence.

## Latest values

`crates/core/ring-latest`. The newest sample of a field is the slot before its lane head, so
reading it does not depend on how many fields there are. A value is expired when the reader's
clock is more than the declaration's expiry past the sample's time; it is still returned, marked
expired.

## Lookup by sample time

`crates/core/ring-time`. The value of a field at or before a sample time is a search of the field's
lane, on the samples' times alone (`time_at` reads a slot's time without copying its payload): the
newest four one by one, then a binary search of the rest. The reply is a hit (a sample for exactly
that time), lagged (the newest earlier one, with how far behind), or missing.

A bounded wait sleeps on the reader's doorbell while the publisher's newest sample is for an
earlier frame, and returns as soon as the publisher reaches the frame or passes it.

## Buffer topics

`crates/core/ring-buffers`. A frame is too large for a ring, so a buffer topic's entries say which
buffers hold the frame, and the buffers are shared memory handed over once.

**The pool.** The publisher's buffers are slices of a few descriptors: a camera's dma-bufs, or a
memfd. A subscriber gets those descriptors, the list of slices and the pool page, once, and maps
them read-only. It checks every slice against its descriptor and refuses a regular or
shared-memory file that is not sealed against shrinking.

**The pool page** is a sealed page the publisher writes and readers map read-only.

| Word | Holds |
| --- | --- |
| 0 | Magic: `ORIONPL1` |
| 1 | Generation (low 32 bits), buffers (high 32) |
| 2 to 7 | Reserved, zero |
| 8 | Tickets: one word per buffer, `uses << 1 | taken back` |
| 8 + buffers | Cut-off: one word per reader slot (64), nonzero once that reader is cut off |

**An entry** is the payload of the topic's field:

| Bytes | Holds |
| --- | --- |
| 0 to 3 | The pool's generation |
| 4 | How many buffers, 1 to 8 |
| 5 to 7 | Reserved, zero |
| 8 on | For each buffer: its index (4 bytes), its ticket when published (4 bytes) |
| then | Metadata, opaque, to the end |

One entry can name several buffers, so an image and its pyramid levels travel and come back
together. The generation goes up when the publisher replaces its pool (a camera changing mode); a
reader that sees a newer generation is told to fetch the new pool, and frames of the old pool it
still holds stay mapped until it lets go.

**Tickets** are what make a reused buffer safe to name. A buffer's ticket is even while a frame is
published in it and odd from the moment the publisher starts taking it back. A reader may hold a
frame only while each buffer's ticket is exactly the one in the entry.

## Hold marks

`crates/core/ring-holds`. A reader's marks are a sealed page only that reader writes: word 0 is a
version, and word `1 + b` is nonzero while it holds buffer `b`. The publisher maps each reader's
page read-only. A buffer is free when no reader marks it.

The version is odd while the reader is changing a mark and different after every change. It is
what lets the publisher count a reader's marks at one instant.

- Reader: store the mark, sequentially consistent fence, load the ticket. Still the entry's: the
  frame is held. Changed: take the mark off; the frame is gone.
- Publisher, taking a buffer back: store the odd ticket, sequentially consistent fence, load every
  reader's mark. None: the buffer is free. Some: leave it, and use another buffer or drop the
  frame.

One fence comes first, so either the publisher sees the mark or the reader sees the ticket. A held
buffer is never reused; checked with `loom`, with a reader that checks before marking as the
variant that must be caught.

The publisher never waits. It takes back the buffer published longest ago that nobody holds, never
the newest frame, and when there is none it drops the frame.

Limits, per reader, applied by the publisher from what it sees:

- **Buffers held.** An honest reader stops taking frames at its limit. One that marks more is cut
  off. The count is taken at one instant: the version before and after must match, or the count is
  taken again. Reading the marks one word at a time is not good enough. A reader holding two
  buffers that lets one go and takes another during the read is seen holding three, and cutting it
  off for that lets the publisher write over frames it holds. That happened, about once in ten
  runs of the threaded test on a busy machine.
- **Marks that cannot be counted.** A reader whose thread is stopped halfway through a change
  leaves its version odd until it runs again. It is judged by time on the publisher's clock, never
  by how often the publisher looked: after the hold time (ten seconds if it has none) it is cut off
  as stuck.
- **Hold time**, on the publisher's clock from when it first saw the mark on that use of the
  buffer. A reader past it is stuck, and is cut off.

A reader that is cut off no longer counts: its marks are ignored and its buffers come back. It is
told through its cut-off word in the pool page. A reader that dies leaves its marks in its page;
they go when the publisher drops that page, on the comm core's word that the connection closed, and
the hold time covers the gap.

## Unsafe code

In the ring, all of it is in `src/map.rs`: `memfd_create`, the seals, `mmap`, `munmap`, and
turning a checked word index into an `&AtomicU64`. The rest of the crate denies `unsafe`. The
doorbell's is in `src/eventfd.rs`: `eventfd`, `read`, `write` and `poll` on it. The buffer
topics' is in `ring-buffers/src/map.rs`: mapping a pool's descriptors and making slices of the
mapping. Hold marks have none.

A frame's bytes are handed out as a plain slice of memory the publisher owns. While the frame is
held an honest publisher does not write there. A publisher that does gives its own subscribers a
torn picture, and nothing else: the memory stays mapped and in bounds. The writer and reader are
safe code over the `Mem` trait, an array of atomic words, which is what lets the same code run on
heap memory in tests and under the model checker.

## Differences from the design's sketch

- Slots are grouped per field with an index for the order, where the sketch had one run of
  entries. With one run, a field that publishes fast erases the newest value of every slow field
  in the same declaration.
- `publish` returns a `Result`: a field outside the declaration or an oversized payload is refused.
- `latest` takes `&mut self`: a sample is a checked copy in the reader's buffer, never a borrow of
  memory the writer can change.
