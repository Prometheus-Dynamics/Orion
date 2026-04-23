#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root_dir"

image_tag="${ORION_MINIMAL_ROOTFS_IMAGE:-orionctl-minimal-rootfs:ci}"
platform="${ORION_MINIMAL_ROOTFS_PLATFORM:-linux/arm64}"

docker buildx build \
  --load \
  --platform "$platform" \
  -t "$image_tag" \
  -f testing/docker/orionctl-minimal-rootfs.Dockerfile \
  .

run_expect_runtime_request_error() {
  local name="$1"
  shift

  set +e
  local output
  output="$(docker run --rm --platform "$platform" "$image_tag" "$@" 2>&1)"
  local status=$?
  set -e

  if [[ $status -eq 0 ]]; then
    echo "$name unexpectedly succeeded"
    echo "$output"
    return 1
  fi

  if grep -Fq "failed to configure HTTP TLS: builder error" <<<"$output"; then
    echo "$name failed during HTTP client construction"
    echo "$output"
    return 1
  fi

  if grep -Fq "failed to initialize HTTPS trust verification from the system trust store" <<<"$output"; then
    echo "$name incorrectly depended on system trust roots"
    echo "$output"
    return 1
  fi

  if ! grep -Fq "failed to send HTTP request" <<<"$output"; then
    echo "$name did not reach the request path"
    echo "$output"
    return 1
  fi

  echo "$name passed"
}

run_expect_runtime_request_error \
  "plain-http-health" \
  get health \
  --http http://127.0.0.1:9100

run_expect_runtime_request_error \
  "plain-http-snapshot" \
  get snapshot \
  --http http://127.0.0.1:9100

run_expect_runtime_request_error \
  "https-health-with-explicit-ca" \
  get health \
  --http https://127.0.0.1:9100 \
  --ca-cert /etc/orion/test-root-ca.pem
