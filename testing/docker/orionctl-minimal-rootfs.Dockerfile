FROM rust:1.86-bookworm AS builder
WORKDIR /workspace

COPY . .
RUN cargo build --release -p orionctl --bin orionctl

FROM debian:bookworm-slim
WORKDIR /opt/orion

RUN mkdir -p /etc/ssl/certs /etc/ssl/misc /etc/ssl/private \
    && touch /etc/ssl/ct_log_list.cnf \
    && touch /etc/ssl/ct_log_list.cnf.dist \
    && touch /etc/ssl/openssl.cnf \
    && touch /etc/ssl/openssl.cnf.dist

COPY --from=builder /workspace/target/release/orionctl /usr/local/bin/orionctl
COPY testing/docker/test-root-ca.pem /etc/orion/test-root-ca.pem

ENTRYPOINT ["/usr/local/bin/orionctl"]
