# Releasing

Every change to `next` passes `scripts/gate.sh`. A release passes that, and this list as well.
There is no release workflow: whoever cuts the release runs each step on the tree being
released and pastes what it printed into the release notes.

1. **The gate.** `scripts/gate.sh` passes on the tree being released.
2. **Every size, on every target.** `scripts/size-gate.sh --release` passes. It builds and
   measures each shipped file for this machine and for each board target, nothing reused, and
   fails on any over its budget. The every-change gate measures this machine and `orion` for aarch64; `orion-recovery` for aarch64 is measured here.
   armv7 and riscv64 have never run on a board, so they are measured here only, before a
   release. A target this machine cannot build fails the step; it is not skipped. It needs:
   - the Rust targets: `rustup target add aarch64-unknown-linux-musl
     armv7-unknown-linux-musleabihf riscv64gc-unknown-linux-musl`;
   - a C compiler for each: zig 0.15.2 on `PATH` (the wrappers in `scripts/zigcc/`), or
     `CC_<target>` set.

   A file over its budget either wins the bytes back or has its budget raised in
   `scripts/size-gate.sh`, with the reason, in a change of its own that passes the gate.
