#!/usr/bin/env bash
# The size gate: each form Orion ships in, built as it ships (profile `service`) and held to
# a number of bytes.
#
# - `orion-recovery`, the comm core alone, small enough for an initramfs;
# - `orion`, the comm core, node logic and the command line: what a board installs;
# - `liborion_c.so`, the C library programs load.
#
# The microcontroller form has its own gate, `scripts/device-size.sh`.
#
#   scripts/size-gate.sh               measure what changed; reuse a recorded measurement by key
#   scripts/size-gate.sh --measure     build and measure everything, ignoring the keys
#   scripts/size-gate.sh --release     every target, the release-only ones too, all measured: what
#                                      a release must pass (docs/release.md)
#   scripts/size-gate.sh --self-test   prove the keys and the reuse (no builds); the gate runs it
#                                      when it measures, and when this script or the key's changes
#
# Each is built on its own: built together, cargo gives every one the features of all, and
# `orion-recovery` would carry what `orion` has.
#
# Always gated: the builds for this machine. Gated as well where they can be built: the static
# musl files that go on a board, for each board target installed. Those need a C compiler for
# the target (the SSH way in brings `ring`): CC_<triple> if set, else scripts/zigcc/<triple> when
# `zig` is on PATH, and that zig must be 0.15.2 (another is refused, with what to do).
#
# aarch64 is gated on every change. armv7 and riscv64 have never run on a board, so they are
# gated before a release only (`--release`), not on every change. With `--release`, every
# target must be built: one that cannot be built here fails, where the gate would skip it.
#
# A change that pushes a size past its limit either wins the bytes back or raises the
# limit here, in the same commit, with the reason.
#
# Keys. A fat-LTO link is most of each build and runs on one thread. Each build has a content
# key (scripts/size-key.py: its dependency closure by content, Cargo.lock, the profiles, the
# cargo configs, the toolchain, the target, the C compiler and linker, the environment builds
# read). A build whose key was measured before is not built again: its recorded size and
# SHA-256 are held to today's limit, and the output says it was reused and from which key. The
# bytes do not depend on the worktree's path, so a key measured in one worktree holds in
# another (a seeded target directory carries the records). A key that misses an input cannot
# hide for long: at least once a day, with --measure, or with SIZE_GATE_MEASURE=1 (CI, every
# run), everything is built and measured, each result is checked against its record (a
# difference fails: the key missed an input), and liborion_c.so is built a second time from a
# copy of the source at another path and must come out byte for byte the same.
set -euo pipefail
# Never incremental, whatever the caller set: the bytes measured are of a build made whole, as it ships.
export CARGO_INCREMENTAL=0
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Bytes. Each is what was measured when it was last set, and a small margin: 20-30 KB, rounded
# to 10,000. A limit is raised only with the reason, in docs/prs, and what was cut first.
#                         this machine (x86_64 gnu), C40 after its cut
#   orion-recovery        3,255,208
#   orion                 6,560,752   (5,936,216 at I6: +624,536, a tenth; by crate and
#                                      symbol in docs/prs/C41.md)
#   liborion_c.so           735,656
RECOVERY_HOST=3270000
ORION_HOST=6590000
CAPI_HOST=740000
# A board's files, static musl. aarch64, the one run on a board: orion-recovery as measured at
# I72 (docs/prs/I72.md), measured on 2026-11-22; its limit was not raised. I72 is 2,759,704,
# +20,144 over I71's 2,739,560: a listener for each interface on the network and SSH ways in,
# each following its interface when it is made again. By crate, before the last cut: orion_ssh
# +7.4 KB (one task per interface: 5.0 KB), orion_comm_app +4.7 KB, orion_tcp +1.8 KB, std
# +1.5 KB, orion_config +1.4 KB, tokio -1.4 KB. Cut: SSH using the network way in's listener
# (-424 with a merged log line), and one structured tracing field (`on = ...`) said in the
# message instead (-1,696). Look at structured fields first: that one field cost 1,696 bytes
# here. The known next cut: one listener type in orion-tcp that accepts and listens again by
# itself, for both ways in (about 1.4 KB). The limit is not raised without a table like this.
# Orion as measured at I50 (runs kept while their program reconnects: +7,696 over I54's
# 5,322,664, after a cut of 1,320; docs/prs/I50.md); the others as at C41, until they run on
# one:
#                                   orion-recovery   orion
#   aarch64-unknown-linux-musl      2,759,704        5,330,360
#   armv7-unknown-linux-musleabihf  2,782,344        5,327,160   (never run on a board)
#   riscv64gc-unknown-linux-musl    3,017,416        5,668,376   (never run on a board)
# The last column: gated on `every` change, or before a `release` only.
BOARDS=(
    "aarch64-unknown-linux-musl     2760000          5360000   every"
    "armv7-unknown-linux-musleabihf 2810000          5350000   release"
    "riscv64gc-unknown-linux-musl   3040000          5690000   release"
)
# How long a full measurement holds before the gate makes another, in seconds.
MEASURE_EVERY=86400

# judge <label> <size> <limit> <static> <how>: holds a size to its limit, and a board's file
# (static: yes or no; `-` for this machine) to being static. Prints the line; fails if either fails.
judge() {
    local label="$1" size="$2" limit="$3" static="$4" how="$5" failed=0
    if [ "$size" -gt "$limit" ]; then
        echo "    FAIL: $label is $size bytes, over its limit of $limit$how"
        failed=1
    else
        echo "    $label: $size bytes (limit $limit)$how"
    fi
    if [ "$static" = no ]; then
        echo "    FAIL: $label is not statically linked$how"
        failed=1
    fi
    return "$failed"
}

# field <entry> <name>: one field of a record.
field() { sed -n "s/^$2=//p" "$1"; }

# against <entry> <size> <sha256>: a measurement checked against the record of its key. Fails
# when they differ: the same key made other bytes, so the key missed an input.
against() {
    local entry="$1" size="$2" sha="$3"
    [ -f "$entry" ] || return 0
    if [ "$(field "$entry" size)" != "$size" ] || [ "$(field "$entry" sha256)" != "$sha" ]; then
        echo "    FAIL: key ${entry##*/} recorded $(field "$entry" size) bytes," \
            "sha256 $(field "$entry" sha256 | cut -c1-12), and made $size bytes, sha256" \
            "${sha:0:12} now: the key misses an input (scripts/size-key.py)"
        return 1
    fi
}

# reused <entry> <label> <limit>: a recorded measurement held to today's limit.
reused() {
    local entry="$1" label="$2" limit="$3" key="${1##*/}"
    judge "$label" "$(field "$entry" size)" "$limit" "$(field "$entry" static)" \
        ", reused from key ${key:0:12} (sha256 $(field "$entry" sha256 | cut -c1-12), measured $(date -u -d "@$(field "$entry" measured)" +%FT%TZ))"
}

if [ "${1:-}" = "--self-test" ]; then
    work="$(mktemp -d "${TMPDIR:-/tmp}/orion-size.XXXXXX")"
    trap 'rm -rf "$work"' EXIT
    failures=0
    fail() { echo "self-test FAILED: $*"; failures=$((failures + 1)); }
    # The keys, on a copy of the source at another path.
    mkdir "$work/src"
    git ls-files -z --cached --others --exclude-standard | tar -c --null -T - -f - | tar -x -C "$work/src" -f -
    git -C "$work/src" init -q
    builds=(r:orion-recovery-bin:host o:orion-bin:host c:orion-capi:host a:orion-bin:aarch64-unknown-linux-musl v:orion-bin:armv7-unknown-linux-musleabihf)
    keys() { env "$@" scripts/size-key.py "$work/src" "${builds[@]}" | tr '\n' ' '; }
    here=$(scripts/size-key.py . "${builds[@]}" | tr '\n' ' ')
    base=$(keys)
    [ "$here" = "$base" ] || fail "a copy at another path has other keys"
    key_of() { echo "$1" | tr ' ' '\n' | grep -A1 -x "$2" | tail -1; }
    # changed <what> <expected: which of r o c a v change> [env...]: a planted change to one input.
    changed() {
        local what="$1" expect="$2" now b
        shift 2
        now=$(keys "$@")
        for b in r o c a v; do
            if [ "$(key_of "$base" "$b")" != "$(key_of "$now" "$b")" ]; then
                [[ "$expect" == *"$b"* ]] || fail "$what changed the key of $b"
            else
                [[ "$expect" != *"$b"* ]] || fail "$what left the key of $b as it was"
            fi
        done
    }
    plant() { command cp -f "$work/src/$1" "$work/kept" && echo "$2" >> "$work/src/$1"; }
    unplant() { command cp -f "$work/kept" "$work/src/$1"; }
    for planted in \
        "crates/core/check/src/lib.rs|// planted|rocav" \
        "crates/node/node-app/src/lib.rs|// planted|oav" \
        "crates/bins/orion-recovery/Cargo.toml|# planted|r" \
        "crates/client/capi/src/lib.rs|// planted|c" \
        "Cargo.lock|# planted|rocav" \
        "Cargo.toml|# planted|rocav" \
        ".cargo/config.toml|# planted|rocav" \
        "rust-toolchain.toml|# planted|rocav" \
        "scripts/size-key.py|# planted|rocav" \
        "scripts/size-gate.sh|# planted|rocav" \
        "scripts/zigcc/aarch64-unknown-linux-musl|# planted|a" \
        "docs/lock.md|planted|" \
        "TODO.md|planted|"; do
        IFS='|' read -r file text expect <<< "$planted"
        plant "$file" "$text"
        changed "$file" "$expect"
        unplant "$file"
    done
    # An untracked new file in a crate counts; one in docs does not.
    echo "// new" > "$work/src/crates/core/wire/src/planted.rs"
    changed "a new file in a crate" rocav
    rm "$work/src/crates/core/wire/src/planted.rs"
    changed "RUSTFLAGS" rocav RUSTFLAGS=-Cdebuginfo=1
    changed "a profile set from the environment" rocav CARGO_PROFILE_SERVICE_OPT_LEVEL=s
    changed "a board's C compiler" rocav CC_aarch64_unknown_linux_musl=/usr/bin/false
    changed "the toolchain" rocav RUSTUP_TOOLCHAIN=stable
    changed "CARGO_TARGET_DIR" "" CARGO_TARGET_DIR=/elsewhere
    # The reuse: a record held to today's limit, a planted size regression red, a key that
    # made other bytes red.
    store="$work/store" && mkdir -p "$store"
    printf 'size=1000\nsha256=%064d\nstatic=-\nmeasured=0\nfrom=/x\n' 0 > "$store/aaaa"
    printf 'size=1000\nsha256=%064d\nstatic=no\nmeasured=0\nfrom=/x\n' 0 > "$store/bbbb"
    out=$(reused "$store/aaaa" "a file" 2000) || fail "a reuse under its limit failed"
    [[ "$out" == *"reused from key aaaa"* ]] || fail "a reuse does not say its key: $out"
    reused "$store/aaaa" "a file" 999 > /dev/null && fail "a reuse over its limit passed"
    reused "$store/bbbb" "a board's file" 2000 > /dev/null && fail "a reused file that is not static passed"
    against "$store/aaaa" 1000 "$(printf '%064d' 0)" > /dev/null || fail "the same bytes again failed"
    against "$store/aaaa" 1000 "$(printf '%064d' 1)" > /dev/null && fail "other bytes under the same key passed"
    against "$store/aaaa" 1001 "$(printf '%064d' 0)" > /dev/null && fail "another size under the same key passed"
    [ "$failures" -eq 0 ] || exit 1
    echo "size gate self-test passed"
    exit 0
fi

measure="${SIZE_GATE_MEASURE:-0}"
release=0
for arg in "$@"; do
    case "$arg" in
        --measure) measure=1 ;;
        # A release is measured whole: nothing reused.
        --release) release=1 measure=1 ;;
        *)
            echo "usage: scripts/size-gate.sh [--measure] [--release] | --self-test" >&2
            exit 2
            ;;
    esac
done

# Absolute: the path check builds from another directory, and a relative target directory would
# put that build somewhere else than where it is read (CI sets none: `target`).
target_dir="$(realpath -m "${CARGO_TARGET_DIR:-target}")"
failures=0

# Each build has a target directory of its own, under this one, so all of them run at once: a
# fat-LTO link is one thread, and cargo holds a target directory for one build at a time. Each
# says what it measured in a file of its own, read back in order once all have ended.
lanes="$target_dir/size-gate"
store="$lanes/store"
mkdir -p "$store"
last="$store/last-measure"
if [ "$measure" = 0 ]; then
    if [ ! -f "$last" ] || [ $(($(date +%s) - $(cat "$last"))) -ge "$MEASURE_EVERY" ]; then
        echo "    no full measurement in the last $((MEASURE_EVERY / 3600)) h: measuring everything"
        measure=1
    fi
fi
results=$(mktemp -d)
trap 'rm -rf "$results"' EXIT
# The self-test, beside the builds: when measuring, and whenever this script or the key's has
# changed since it last passed.
proved="$store/self-test-$(cat "${BASH_SOURCE[0]}" scripts/size-key.py | sha256sum | cut -c1-16)"
self_test=""
if [ "$measure" = 1 ] || [ ! -f "$proved" ]; then
    (scripts/size-gate.sh --self-test && touch "$proved") > "$results/self-test" 2>&1 &
    self_test=$!
fi
plans=()
pids=()

# plan <name> <package> <file> <label> <limit> [target]: builds <package> for [target] (this
# machine without one) and holds <file> to <limit> bytes; a board's file must be static.
plan() { local IFS="|"; plans+=("$*"); }

# build <key> <name> <package> <file> <label> <limit> [target]: one plan, built or reused.
build() {
    local key="$1" name="$2" package="$3" file="$4" label="$5" limit="$6" target="${7:-}"
    local dir="$lanes/$name" out="$results/$name" entry="$store/$1"
    (
        if [ "$measure" = 0 ] && [ -f "$entry" ]; then
            reused "$entry" "$label" "$limit"
            exit
        fi
        if [ -n "$target" ]; then
            CARGO_TARGET_DIR="$dir" cargo build --quiet --locked --profile service \
                --target "$target" -p "$package" || exit 1
            file="$dir/$target/service/$file"
        else
            CARGO_TARGET_DIR="$dir" cargo build --quiet --profile service -p "$package" || exit 1
            file="$dir/service/$file"
        fi
        size=$(stat -c %s "$file")
        sha=$(sha256sum "$file" | cut -d' ' -f1)
        static=-
        if [ -n "$target" ] && command -v file > /dev/null; then
            if file "$file" | grep -q "statically linked"; then static=yes; else static=no; fi
        fi
        failed=0
        against "$entry" "$size" "$sha" || failed=1
        judge "$label" "$size" "$limit" "$static" ", measured, key ${key:0:12}" || failed=1
        if [ "$failed" -eq 0 ] || [ ! -f "$entry" ]; then
            printf 'size=%s\nsha256=%s\nstatic=%s\nmeasured=%s\nfrom=%s\n' \
                "$size" "$sha" "$static" "$(date +%s)" "$PWD" > "$entry.$$" && mv "$entry.$$" "$entry"
        fi
        exit "$failed"
    ) > "$out" 2>&1 &
    pids+=("$!:$name")
}

plan recovery-host orion-recovery-bin orion-recovery "orion-recovery for this machine" "$RECOVERY_HOST"
plan orion-host orion-bin orion "orion for this machine" "$ORION_HOST"
plan capi-host orion-capi liborion_c.so "liborion_c.so for this machine" "$CAPI_HOST"

installed=$(rustup target list --installed 2>/dev/null || true)

# The board files are built and measured with this zig and no other: another version builds C
# differently, and the numbers above stop meaning anything.
ZIG_VERSION=0.15.2
zig_checked=""
check_zig() {
    [ -n "$zig_checked" ] && return 0
    local found
    found=$(zig version 2>/dev/null || echo "none")
    if [ "$found" != "$ZIG_VERSION" ]; then
        echo "    FAIL: zig $found is on PATH; the board files are built with zig $ZIG_VERSION only." >&2
        echo "          Install zig $ZIG_VERSION, or set CC_<target> to another C compiler for that target." >&2
        failures=$((failures + 1))
        return 1
    fi
    zig_checked=yes
}
# not_here <board> <why>: a board this machine cannot build. Skipped by the gate; a failure for
# a release, which must measure every target.
not_here() {
    if [ "$release" = 1 ]; then
        echo "    FAIL: $1 cannot be built here, and a release measures it: $2"
        failures=$((failures + 1))
    else
        echo "    $1: not built here, $2"
    fi
}
for line in "${BOARDS[@]}"; do
    read -r board recovery_limit orion_limit when <<< "$line"
    if [ "$when" = release ] && [ "$release" = 0 ]; then
        echo "    $board: before a release only (scripts/size-gate.sh --release)"
        continue
    fi
    triple=$(echo "$board" | tr 'a-z-' 'A-Z_')
    cc_var="CC_$(echo "$board" | tr '-' '_')"
    if ! grep -qx "$board" <<< "$installed"; then
        not_here "$board" "no Rust target (rustup target add $board)"
        continue
    fi
    if [ -z "${!cc_var:-}" ]; then
        if command -v zig > /dev/null; then
            check_zig || break
            export "$cc_var=$PWD/scripts/zigcc/$board"
            export "AR_$(echo "$board" | tr '-' '_')=$PWD/scripts/zigcc/$board-ar"
        else
            not_here "$board" "no C compiler ($cc_var, or zig on PATH)"
            continue
        fi
    fi
    linker="CARGO_TARGET_${triple}_LINKER"
    export "$linker=${!linker:-rust-lld}"
    plan "recovery-$board" orion-recovery-bin orion-recovery \
        "orion-recovery for a board ($board, static)" "$recovery_limit" "$board"
    plan "orion-$board" orion-bin orion "orion for a board ($board, static)" "$orion_limit" "$board"
done

# The keys, all at once, after the environment each build sees is set.
specs=()
for p in "${plans[@]}"; do
    IFS='|' read -r name package _ _ _ target <<< "$p"
    specs+=("$name:$package:${target:-host}")
done
# Taken whole first: a key script that fails stops the gate here, by name, and a process
# substitution's failure would not.
key_list="$(scripts/size-key.py . "${specs[@]}")"
declare -A keys
while read -r name key; do keys[$name]="$key"; done <<< "$key_list"
for p in "${plans[@]}"; do
    if [ -z "${keys[${p%%|*}]:-}" ]; then
        echo "    no size key for ${p%%|*}: scripts/size-key.py gave none" >&2
        exit 1
    fi
done

for p in "${plans[@]}"; do
    IFS='|' read -r name package file label limit target <<< "$p"
    build "${keys[$name]}" "$name" "$package" "$file" "$label" "$limit" "$target"
done

for entry in "${pids[@]}"; do
    pid="${entry%%:*}" name="${entry#*:}"
    if ! wait "$pid"; then
        failures=$((failures + 1))
    fi
    cat "$results/$name"
done

if [ -n "$self_test" ]; then
    wait "$self_test" || failures=$((failures + 1))
    sed 's/^/    /' "$results/self-test"
fi

# A full measurement: liborion_c.so again, from a copy of the source at another path, byte for
# byte the same; then the time of this one, if all passed.
if [ "$measure" = 1 ]; then
    copy="$lanes/path-check"
    mkdir -p "$copy/src"
    git ls-files -z --cached --others --exclude-standard \
        | rsync -a --delete --from0 --files-from=- ./ "$copy/src/"
    if (cd "$copy/src" && CARGO_TARGET_DIR="$copy/target" cargo build --quiet --profile service -p orion-capi); then
        if [ ! -f "$copy/target/service/liborion_c.so" ] || [ ! -f "$lanes/capi-host/service/liborion_c.so" ]; then
            echo "    FAIL: the path check has no liborion_c.so to compare at $copy/target/service or $lanes/capi-host/service"
            failures=$((failures + 1))
        elif cmp -s "$copy/target/service/liborion_c.so" "$lanes/capi-host/service/liborion_c.so"; then
            echo "    liborion_c.so from a copy at another path: the same bytes"
        else
            echo "    FAIL: liborion_c.so built at another path has other bytes; a key measured in one worktree would not hold in another"
            scripts/elf-diff.py "$lanes/capi-host/service/liborion_c.so" "$copy/target/service/liborion_c.so" || true
            failures=$((failures + 1))
        fi
    else
        echo "    FAIL: liborion_c.so from a copy at another path did not build"
        failures=$((failures + 1))
    fi
    [ "$failures" -eq 0 ] && date +%s > "$last"
fi

[ "$failures" -eq 0 ]
