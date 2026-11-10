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
| 1 | Frame time, nanoseconds |
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

## Unsafe code

All of it is in `src/map.rs`: `memfd_create`, the seals, `mmap`, `munmap`, and turning a checked
word index into an `&AtomicU64`. The rest of the crate denies `unsafe`. The writer and reader are
safe code over the `Mem` trait, an array of atomic words, which is what lets the same code run on
heap memory in tests and under the model checker.

## Differences from the design's sketch

- Slots are grouped per field with an index for the order, where the sketch had one run of
  entries. With one run, a field that publishes fast erases the newest value of every slow field
  in the same declaration.
- `publish` returns a `Result`: a field outside the declaration or an oversized payload is refused.
- `latest` takes `&mut self`: a sample is a checked copy in the reader's buffer, never a borrow of
  memory the writer can change.
