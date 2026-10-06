# Cross-compiles orion-node for aarch64-unknown-linux-gnu (glibc). Used by orion-node.toml as the
# Gaia docker execution backend; works with plain `docker run` too (see packaging/README.md).
#
# Debian bookworm's glibc 2.36 is the oldest glibc the binary can then run against; a binary
# built against an older glibc runs on newer ones, not the other way round. The linker is
# configured here; the 64 KiB segment alignment for 16 KiB / 64 KiB page kernels comes from the
# workspace .cargo/config.toml.
FROM rust:1.99.0-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc-aarch64-linux-gnu libc6-dev-arm64-cross \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add aarch64-unknown-linux-gnu

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar
