FROM rust:1.86-bookworm AS builder
WORKDIR /workspace

COPY . .
RUN cargo build --release -p orion-node --bin orion-node -p orion-client --examples
RUN cargo build --release -p orionctl --bin orionctl

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/orion
COPY --from=builder /workspace/target/release/orion-node /usr/local/bin/orion-node
COPY --from=builder /workspace/target/release/examples/control_plane_watch /usr/local/bin/control_plane_watch
COPY --from=builder /workspace/target/release/examples/control_plane_write_watch /usr/local/bin/control_plane_write_watch
COPY --from=builder /workspace/target/release/examples/camera_provider_publish /usr/local/bin/camera_provider_publish
COPY --from=builder /workspace/target/release/examples/camera_pipeline_publish /usr/local/bin/camera_pipeline_publish
COPY --from=builder /workspace/target/release/examples/executor_watch /usr/local/bin/executor_watch
COPY --from=builder /workspace/target/release/examples/executor_register_and_watch /usr/local/bin/executor_register_and_watch
COPY --from=builder /workspace/target/release/examples/provider_watch /usr/local/bin/provider_watch
COPY --from=builder /workspace/target/release/examples/provider_register_and_watch /usr/local/bin/provider_register_and_watch
COPY --from=builder /workspace/target/release/examples/multi_watch /usr/local/bin/multi_watch
COPY --from=builder /workspace/target/release/orionctl /usr/local/bin/orionctl

EXPOSE 9100
ENTRYPOINT ["/usr/local/bin/orion-node"]
