#!/usr/bin/env bash
# The CI job bodies, shared by .github/workflows/ci.yml and local runs, so both run exactly the
# same commands. Each step prints its wall time and CPU time (user + system), and a summary at the
# end; under GitHub Actions the steps are collapsible groups.
#
#   ./scripts/ci-jobs.sh <job>...        jobs: workspace lints no-std link allocators appliance
#   ./scripts/ci-jobs.sh all             every job above, in order
#   CI_JOBS_TIMINGS=file.tsv ...         also append "job<TAB>step<TAB>wall_s<TAB>cpu_s<TAB>rc" rows
#
# What each job covers, and why the matrix is not the full cross product, is documented in
# docs/development.md ("CI jobs").
set -uo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

timings="${CI_JOBS_TIMINGS:-}"
summary=()
failed=0

now() { date +%s.%N; }

# CPU seconds (user + system) of all waited-for child processes so far, from the `times`
# builtin. `times` must run in this shell (in a command substitution it would report the
# subshell's children), so it writes to a file that is parsed afterwards.
cpu_file="$(mktemp)"
trap 'rm -f "$cpu_file"' EXIT
children_cpu_seconds() {
    awk 'NR == 2 {
        total = 0
        for (i = 1; i <= 2; i++) { split($i, p, /[ms]/); total += p[1] * 60 + p[2] }
        printf "%.1f", total }' "$cpu_file"
}

# step <job> <name> <command>...: runs the command (a program or a function of this script),
# records wall and CPU time, keeps going after a failure (so one run reports every failing step),
# and fails the run at the end.
step() {
    local job="$1" name="$2"
    shift 2
    if [[ -n "${GITHUB_ACTIONS:-}" ]]; then echo "::group::$job: $name"; else echo "==> $job: $name"; fi
    local start end rc cpu_before cpu_after cpu wall
    times >"$cpu_file"
    cpu_before="$(children_cpu_seconds)"
    start="$(now)"
    "$@"
    rc=$?
    end="$(now)"
    times >"$cpu_file"
    cpu_after="$(children_cpu_seconds)"
    cpu="$(awk -v a="$cpu_before" -v b="$cpu_after" 'BEGIN { printf "%.1f", b - a }')"
    wall="$(awk -v a="$start" -v b="$end" 'BEGIN { printf "%.1f", b - a }')"
    [[ -n "${GITHUB_ACTIONS:-}" ]] && echo "::endgroup::"
    echo "    $job: $name: ${wall}s wall, ${cpu}s cpu, exit $rc"
    summary+=("$(printf '%-10s %-58s %8ss %8ss %s' "$job" "$name" "$wall" "$cpu" "$([[ $rc == 0 ]] && echo ok || echo FAILED)")")
    if [[ -n "$timings" ]]; then
        printf '%s\t%s\t%s\t%s\t%s\n' "$job" "$name" "$wall" "$cpu" "$rc" >>"$timings"
    fi
    if [[ $rc != 0 ]]; then failed=1; fi
    return 0
}

clippy() { cargo clippy --locked "$@" -- -D warnings; }

# fmt, file sizes, and every test of the workspace with every feature on.
job_workspace() {
    step workspace "fmt" cargo fmt --check
    step workspace "file sizes" ./scripts/check-file-sizes.sh
    step workspace "test --workspace --all-features" cargo test --locked --workspace --all-features
}

# Clippy everywhere, the orion-node / orionctl / transport-http / remote-client feature matrix,
# the orion-node test builds that differ from the all-features build, and the docs.
job_lints() {
    step lints "clippy --workspace --all-features" clippy --workspace --all-targets --all-features

    # orion-node: the empty build, each optional feature alone (cfg gaps show up as unused code or
    # missing items), the shipped appliance sets, and the defaults. Supersets of these (defaults
    # plus one feature, peer-tcp plus one transport) add no new cfg combination.
    local features
    for features in "" transport-http transport-tcp transport-quic peer-tcp link-gateway \
        systemd-notify peer-tcp,discovery-mdns peer-tcp,discovery-mdns,systemd-notify \
        peer-tcp,discovery-mdns,systemd-notify,link-gateway; do
        step lints "clippy orion-node --no-default-features [${features}]" \
            clippy -p orion-node --no-default-features ${features:+--features "$features"} --all-targets
    done
    step lints "clippy orion-node [defaults]" clippy -p orion-node --all-targets

    # orion-node tests in the builds the all-features run cannot stand for: IPC-only, the packaged
    # appliance (orion+tcp sync, remote operators, discovery, sd_notify), and the IPC-only gateway.
    step lints "test orion-node --no-default-features" \
        cargo test --locked -p orion-node --no-default-features
    step lints "test orion-node [peer-tcp,discovery-mdns,systemd-notify]" \
        cargo test --locked -p orion-node --no-default-features --features peer-tcp,discovery-mdns,systemd-notify
    step lints "test orion-node [link-gateway]" \
        cargo test --locked -p orion-node --no-default-features --features link-gateway

    # The remote operator client alone (no IPC client, no HTTP stack), with mDNS browsing, through
    # the facade, and orion-auth's shared signing / enrollment code.
    step lints "clippy orion-client [remote]" clippy -p orion-client --no-default-features --features remote --all-targets
    step lints "clippy orion-client [discovery]" clippy -p orion-client --no-default-features --features discovery --all-targets
    step lints "clippy orion [remote]" clippy -p orion --no-default-features --features remote --all-targets
    step lints "clippy orion-auth [enrollment]" clippy -p orion-auth --features enrollment --all-targets
    step lints "test orion-client [remote]" cargo test --locked -p orion-client --no-default-features --features remote

    # Protocol layer only, the HTTP client alone, the server alone; orionctl IPC-only + JSON, with
    # YAML/TOML, with the HTTP client. (The default feature sets equal the all-features builds.)
    step lints "clippy orion-transport-http []" clippy -p orion-transport-http --no-default-features --all-targets
    step lints "clippy orion-transport-http [client]" clippy -p orion-transport-http --no-default-features --features client --all-targets
    step lints "clippy orion-transport-http [server]" clippy -p orion-transport-http --no-default-features --features server --all-targets
    step lints "clippy orionctl []" clippy -p orionctl --no-default-features --all-targets
    step lints "clippy orionctl [yaml,toml]" clippy -p orionctl --no-default-features --features yaml,toml --all-targets
    step lints "clippy orionctl [http]" clippy -p orionctl --no-default-features --features http --all-targets
    step lints "test orionctl --no-default-features" cargo test --locked -p orionctl --no-default-features

    step lints "doc" env RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps
}

job_no_std() {
    step no-std "check-no-std.sh" ./scripts/check-no-std.sh
}

# orion-link for microcontrollers, and the MCU template with its size budget.
job_link() {
    local all_targets=(thumbv6m-none-eabi thumbv7em-none-eabihf thumbv8m.main-none-eabihf
        riscv32imc-unknown-none-elf riscv32imac-unknown-none-elf)
    # Lints do not depend on the core, only on which atomics exist: none (riscv32imc),
    # load/store only (thumbv6m), and full (thumbv7em stands for the other two).
    local lint_targets=(thumbv6m-none-eabi riscv32imc-unknown-none-elf thumbv7em-none-eabihf)
    local target features
    for target in "${lint_targets[@]}"; do
        for features in "" embedded-io,embedded-io-async,embedded-can,crc-table device \
            device,embedded-io,embedded-can alloc alloc,embedded-io,embedded-can; do
            step link "clippy orion-link [${features}] ${target}" \
                clippy -p orion-link --no-default-features ${features:+--features "$features"} --target "$target"
        done
    done
    # Code generation on every core (atomics lowering differs per core).
    for target in "${all_targets[@]}"; do
        step link "build --release orion-link [], [device], [alloc] ${target}" bash -c "
            cargo build --locked -p orion-link --release --no-default-features --target $target &&
            cargo build --locked -p orion-link --release --no-default-features --features device --target $target &&
            cargo build --locked -p orion-link --release --no-default-features --features alloc --target $target"
    done
    step link "device path has no model, serde, or postcard dependency" bash -c '
        tree="$(cargo tree --locked -p orion-link --no-default-features \
            --features device,embedded-io,embedded-io-async,embedded-can,crc-table \
            --target thumbv6m-none-eabi -e normal --prefix none)"
        if grep -E "^(orion-core|orion-control-plane|serde|postcard) " <<<"$tree"; then
            echo "error: the no-alloc device path must not depend on the crates above" >&2
            exit 1
        fi'
    # Host tests: bitwise CRC and no allocator, the device path, the alloc path, and everything on
    # (table CRC, the adapters, std sessions).
    for features in "" device alloc; do
        step link "test orion-link [${features}]" \
            cargo test --locked -p orion-link --no-default-features ${features:+--features "$features"}
    done
    step link "clippy orion-link --all-targets [device], [alloc]" bash -c "
        cargo clippy --locked -p orion-link --no-default-features --features device --all-targets -- -D warnings &&
        cargo clippy --locked -p orion-link --no-default-features --features alloc --all-targets -- -D warnings"
    step link "test orion-link --all-features" cargo test --locked -p orion-link --all-features
    step link "sim_device example" cargo run --locked -p orion-link --example sim_device --features std

    step link "mcu-template fmt, clippy, host tests" bash -c '
        cd examples/mcu-template &&
        cargo fmt --check &&
        cargo clippy --locked --all-targets -- -D warnings &&
        cargo clippy --locked --all-targets --features alloc -- -D warnings &&
        cargo test --locked &&
        cargo test --locked --features alloc'
    for target in thumbv6m-none-eabi thumbv7em-none-eabihf riscv32imc-unknown-none-elf riscv32imac-unknown-none-elf; do
        step link "mcu-template clippy ${target}" bash -c "
            cd examples/mcu-template &&
            cargo clippy --locked --release --target $target -- -D warnings &&
            cargo clippy --locked --release --target $target --no-default-features --features standalone,ffi-can -- -D warnings"
    done
    for target in thumbv7em-none-eabihf riscv32imac-unknown-none-elf; do
        step link "mcu-template clippy [global-heap] ${target}" bash -c "
            cd examples/mcu-template &&
            cargo clippy --locked --release --target $target --features global-heap -- -D warnings"
    done
    # Builds and links every template variant (UART, CAN, the global-heap variant where the core
    # has compare-and-swap) on every core, and checks the flash/RAM budgets.
    step link "mcu-size (build, link, budgets)" env MCU_SIZE_CHECK_BUDGET=1 MCU_SIZE_BREAKDOWN=1 ./scripts/mcu-size.sh
}

# Each opt-in global allocator alone (--all-features enables both; jemalloc wins): lint, build,
# start, and cross-build for aarch64 in the appliance feature set the allocators are meant for.
job_allocators() {
    local allocator appliance=peer-tcp,discovery-mdns,systemd-notify
    for allocator in alloc-mimalloc alloc-jemalloc; do
        step allocators "clippy orion-node [${allocator}]" \
            clippy -p orion-node --no-default-features --features "$appliance,$allocator" --all-targets
        step allocators "build orion-node [${allocator}]" \
            cargo build --locked -p orion-node --no-default-features --features "$appliance,$allocator"
        step allocators "start and stop orion-node [${allocator}]" bash -c '
            dir="$(mktemp -d)"
            ORION_NODE_ID=node.alloc-smoke ORION_NODE_HTTP_ADDR=off ORION_NODE_PEER_AUTH=disabled \
            ORION_NODE_SHUTDOWN_AFTER_INIT_MS=200 RUST_LOG=warn \
            ORION_NODE_IPC_SOCKET="$dir/c.sock" ORION_NODE_IPC_STREAM_SOCKET="$dir/s.sock" \
            "${CARGO_TARGET_DIR:-target}/debug/orion-node"'
        step allocators "cross-build orion-node [${allocator}] aarch64" \
            cargo build --locked -p orion-node --no-default-features --features "$appliance,$allocator" \
            --target aarch64-unknown-linux-gnu
    done
}

# The packaged appliance build for aarch64 (packaging/README.md): the release cross-build with
# its 64 KiB segment alignment, the link-gateway variant type-checked, and the systemd unit.
job_appliance() {
    local appliance=peer-tcp,discovery-mdns,systemd-notify
    local target_dir="${CARGO_TARGET_DIR:-target}"
    step appliance "check orion-node [$appliance,link-gateway] aarch64" \
        cargo check --locked -p orion-node --target aarch64-unknown-linux-gnu --no-default-features \
        --features "$appliance,link-gateway"
    step appliance "release cross-build orion-node [$appliance] aarch64" \
        cargo build --locked --release -p orion-node --target aarch64-unknown-linux-gnu \
        --no-default-features --features "$appliance"
    step appliance "PT_LOAD alignment is 64 KiB" bash -c "
        bin='$target_dir/aarch64-unknown-linux-gnu/release/orion-node'
        readelf=\"\$(command -v aarch64-linux-gnu-readelf || command -v readelf)\"
        aligns=\"\$(\"\$readelf\" -lW \"\$bin\" | awk '\$1 == \"LOAD\" { print \$NF }')\"
        echo \"PT_LOAD alignments: \$aligns\"
        test -n \"\$aligns\"
        for align in \$aligns; do
            if (( align < 65536 )); then
                echo \"error: PT_LOAD aligned to \$align; 16 KiB / 64 KiB page kernels need 0x10000\" >&2
                exit 1
            fi
        done"
    # systemd-analyze only needs ExecStart= to name an existing executable, so the aarch64 binary
    # serves and no host build is needed.
    if command -v systemd-analyze >/dev/null 2>&1; then
        step appliance "systemd-analyze verify the unit" bash -c "
            unit=\"\$(mktemp -d)/orion-node.service\"
            sed \"s#/usr/bin/orion-node#\$(realpath '$target_dir')/aarch64-unknown-linux-gnu/release/orion-node#\" \
                packaging/systemd/orion-node.service >\"\$unit\"
            systemd-analyze verify \"\$unit\""
    fi
}

jobs=("$@")
if [[ ${#jobs[@]} == 0 ]]; then
    echo "usage: $0 <workspace|lints|no-std|link|allocators|appliance|all>..." >&2
    exit 2
fi
if [[ "${jobs[*]}" == "all" ]]; then
    jobs=(workspace lints no-std link allocators appliance)
fi
for job in "${jobs[@]}"; do
    case "$job" in
        workspace) job_workspace ;;
        lints) job_lints ;;
        no-std) job_no_std ;;
        link) job_link ;;
        allocators) job_allocators ;;
        appliance) job_appliance ;;
        *) echo "unknown job: $job" >&2; exit 2 ;;
    esac
done

echo
printf '%-10s %-58s %9s %9s %s\n' "job" "step" "wall" "cpu" "result"
printf '%s\n' "${summary[@]}"
exit "$failed"
