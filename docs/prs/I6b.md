# I6b: The board limits, tightened to what a board runs

**Status:** done

**Group:** I. Finish

**Wave:** 18

**Needs:** C52b

## Changes

- `scripts/size-gate.sh`: the aarch64 limits come down to the C52b build plus the gate's margin. That margin is 20–30 KB, rounded to 10,000.

  | aarch64, static musl | measured at C52b | limit before | limit now |
  | --- | ---: | ---: | ---: |
  | `orion-recovery` | 2,654,240 | 2,900,000 | 2,680,000 |
  | `orion` | 5,213,424 | 5,550,000 | 5,240,000 |

  The old limits were set at C41, before `codegen-units = 1` (C52) cut about 320 KB from each file. With the old limits, a regression of that size would have passed the gate unseen.
- `testing/board-runs/C52.md` and `C52-run.sh`: the C52 and C52b board runs. They record who each process runs as, its capabilities and its memory, the failed tries and why, and the rehearsal on a PC.
- armv7 and riscv64 keep their C41 limits. They have not run on a board, and their limits move when they do (the queue's size-gate item).

## Proved by

The gate passes at the new limits.

## Different from the brief

Only aarch64 moves, as asked. The host limits were not part of the ask either, so they stay as they are.
