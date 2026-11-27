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
# hide for long: with --measure, or with SIZE_GATE_MEASURE=1 (CI, every run, on every push),
# everything is built and measured, each result is checked against its record (a
# difference fails: the key missed an input), and liborion_c.so is built a second time from a
# copy of the source at another path and must come out byte for byte the same. The gate here
# makes no full measurement of its own for a day gone by: it must end within a minute at any
# load (the one-minute rule, 2026-11-24), and CI makes one on every push.
#
# CI only, where a key is new. With SIZE_GATE_CI_ONLY=1 (the whole gate on a worker's machine)
# and any build's key unmeasured, nothing is built here: a core change or a cold target means
# shipped-profile fat-LTO links, 360 to 420 CPU-s, over the minute at any load. The lane says
# `not-run(ci-only:N-of-M-size-keys-new)`; CI, which builds every one on every push, measures
# them, and scripts/release-check.sh takes that not-run only where CI's size gate passed on
# that commit. With every key measured, the gate runs here as ever: each record held to
# today's limit, in seconds.
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
ORION_HOST=6635000
CAPI_HOST=745000
# Raised at I100 for gap 6, the boards heard (docs/prs/I100.md has the table): orion for this
# machine 6,616,144 measured, +33,728 over I99 after a cut of 9,008; the limit leaves room
# for I95 and gap 5 as guessed, and a margin. liborion_c.so 736,616, +272, was 3,384 under its
# limit, less than the 4,000 rule: raised by 5,000 with it.
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
# orion-recovery as measured at I79 (any file's name carried, escaped, FileName at 255 bytes;
# docs/prs/I79.md): 2,761,360, +1,592 over I78, its limit raised from 2,760,000 to 2,770,000 once,
# with room for a fix or a dependency patch and not for a feature. By crate (.text): orion_types
# +1,023 (FileName::new's check of the one form: +824), orion_ssh +614 and orion_broker -512
# (code moved between them), orion_file_offers +307 (a FileName of 256 bytes copied), ring -307.
# Recovery decodes file names from the wire, so the check is in it; no second, smaller check of
# the same rule was written. This limit is a ratchet, set at measurements, not a physical bound:
# it is replaced by the space recovery has in the flasher's initramfs, less a margin, once that
# is known. Until then each raise needs a table like this one. Since then (I80): no image
# carries orion-recovery (the flasher took the full orion for its pushes and actions, and
# where it could live the bound is RAM, hundreds of MB), so the figure only keeps a binary
# nothing ships lean. A change that grows it for a real reason brings its table and a raise;
# no byte hunts for it.
# Orion as measured at I76 (the files service, roots/<name>/: +54,976 over I75's 5,353,368,
# after a cut of 10,112; docs/prs/I76.md), its limit raised from 5,360,000. By crate: node logic
# +15.8 KB (the roots' actions, fetches and pushes, inlined where node logic handles frames),
# orion_file_store +7.7 KB (placed beneath the store; its functions no longer inlined, with two
# callers), orion_file_roots +6.6 KB, std +2.7 KB, rustix +2.3 KB (openat2, reading
# directories). Cut first: the index in the order it is read, not sorted (a sort of its own),
# and the helpers that read a file for its digest polled by their threads, not over a channel
# (-10,112 together). Orion as measured at I77 (the board announced by name over mDNS: +78,680
# over I76's 5,408,344, after a cut of 17,008; docs/prs/I77.md), its limit raised from
# 5,430,000. By section: .text +54,144, unwinding +22,288 (.eh_frame +11,216,
# .gcc_except_table +7,792, .eh_frame_hdr +3,280), .data.rel.ro +1,760. By crate (.text):
# orion_mdns +35.1 KB (the responder: 18.4 KB of it its task with the responder inlined),
# orion_comm_app +7.4 KB (the TXT record, the settings, the wiring), std +7.2 KB, [Unknown]
# +4.1 KB (musl's getifaddrs), tracing_core +1.9 KB, tokio -2.2 KB. Cut first: linear lists
# for its sockets, announcement times and once-a-second record times, not hash and B-tree maps;
# no sort of an interface's addresses; three debug lines with fields (-17,008 together).
# Orion as measured at I88 (5,510,072: the clock row so far, I80 and I81, +13,896 after a cut
# of 8,600, and +848 when the shm pins moved with no source changed: the rev is in the crates'
# identity, and the link came out a little different), its limit raised from 5,510,000 by
# 40,000 for the whole clock row (docs/prs/I88.md has the table). The figure is a ratchet,
# not the room there is: the slot the file ships in has room to spare. A limit is kept at
# least 4,000 bytes above what was last measured: with less, a change of no source can trip it.
# Asked three times and answered at I92 (docs/prs/I92.md): built with panic = "abort", orion
# for a board is 876,064 bytes smaller (15.9%), orion-recovery 392,688 (14.2%), liborion_c.so
# 28,352 (3.9%). Not taken: unwinding is what lets one request's panic cost one session and
# not everyone's, and with security off anyone on the network can send that request. Not to
# be asked again without something new. I92 also has what is in orion-recovery, by section
# and crate, and what could come out: its margin is under 5,000 bytes.
# Orion for a board at I100, gap 6: 5,573,144 bytes, +30,440 over I99 after a cut of 6,344;
# the limit raised from 5,550,000 to 5,590,000 for gap 6, I95 and gap 5, with a margin. Gap 4
# brings its own raise (docs/prs/I100.md).
# Ruled by the user, 2026-11-23: the separate orion-recovery binary is kept. It stays built and
# its tests stay in the gate; nothing is cut from it and it is not built with abort. Its size
# for a board is held to its limit by `--release` (I93), which a release must pass
# (docs/release.md, step 2). Not to be raised again.
# The others as at C41, until they run on one; armv7 and riscv64 were not measured at I76 or
# I77 and will be about as much larger at the next release's measurement:
#                                   orion-recovery   orion
#   aarch64-unknown-linux-musl      2,763,440        5,510,072
#   armv7-unknown-linux-musleabihf  2,782,344        5,327,160   (never run on a board)
#   riscv64gc-unknown-linux-musl    3,017,416        5,668,376   (never run on a board)
# The fourth column: gated on `every` change, or before a `release` only. A fifth, where there
# is one, is the same for orion-recovery alone. On aarch64 it is `release` since I93: no image
# carries orion-recovery, and a limit on a file nothing ships should not force a raise on a
# change to code it shares with orion. It is still built and held to its limit here on every
# change for this machine, its tests run in the gate, and a release measures it for a board.
BOARDS=(
    "aarch64-unknown-linux-musl     2770000          5590000   every     release"
    "armv7-unknown-linux-musleabihf 2810000          5350000   release"
    "riscv64gc-unknown-linux-musl   3040000          5690000   release"
)

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

# new_keys <store> <key>...: how many of the keys have no record in the store.
new_keys() {
    local store="$1" key count=0
    shift
    for key in "$@"; do
        [ -f "$store/$key" ] || count=$((count + 1))
    done
    echo "$count"
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
    # Each case on a copy of its own, all at once: a key is a second or more of cargo metadata
    # and hashing, one after another they were most of this test's minute, and a key does not
    # depend on where the source is (the first check below).
    # case <name> <what> <expected: which of r o c a v change> <file|-> <text|-> [env...]:
    # <file> gets <text> appended (made, if new), then the keys are taken with [env...].
    cases=()
    case_() {
        local name="$1" file="$4" text="$5" dir="$work/case-$1"
        cases+=("$1|$2|$3")
        shift 5
        cp -a "$work/src" "$dir"
        [ "$file" = - ] || echo "$text" >> "$dir/$file"
        (env "$@" scripts/size-key.py "$dir" "${builds[@]}" | tr '\n' ' ' > "$work/$name.keys") &
        pids+=("$!")
    }
    pids=()
    (scripts/size-key.py . "${builds[@]}" | tr '\n' ' ' > "$work/here.keys") &
    pids+=("$!")
    (scripts/size-key.py "$work/src" "${builds[@]}" | tr '\n' ' ' > "$work/base.keys") &
    pids+=("$!")
    case_ core "crates/core/check/src/lib.rs" rocav crates/core/check/src/lib.rs "// planted"
    case_ node "crates/node/node-app/src/lib.rs" oav crates/node/node-app/src/lib.rs "// planted"
    case_ recovery "crates/bins/orion-recovery/Cargo.toml" r crates/bins/orion-recovery/Cargo.toml "# planted"
    case_ capi "crates/client/capi/src/lib.rs" c crates/client/capi/src/lib.rs "// planted"
    case_ lock "Cargo.lock" rocav Cargo.lock "# planted"
    case_ manifest "Cargo.toml" rocav Cargo.toml "# planted"
    case_ config ".cargo/config.toml" rocav .cargo/config.toml "# planted"
    case_ toolchain-file "rust-toolchain.toml" rocav rust-toolchain.toml "# planted"
    case_ key-script "scripts/size-key.py" rocav scripts/size-key.py "# planted"
    case_ gate-script "scripts/size-gate.sh" rocav scripts/size-gate.sh "# planted"
    case_ zigcc "scripts/zigcc/aarch64-unknown-linux-musl" a scripts/zigcc/aarch64-unknown-linux-musl "# planted"
    case_ notes "docs/lock.md" "" docs/lock.md "planted"
    case_ todo "TODO.md" "" TODO.md "planted"
    # An untracked new file in a crate counts; one in docs does not.
    case_ new-file "a new file in a crate" rocav crates/core/wire/src/planted.rs "// new"
    case_ new-doc "a new file in the notes" "" docs/planted.md "new"
    case_ rustflags "RUSTFLAGS" rocav - - RUSTFLAGS=-Cdebuginfo=1
    case_ profile "a profile set from the environment" rocav - - CARGO_PROFILE_SERVICE_OPT_LEVEL=s
    case_ board-cc "a board's C compiler" rocav - - CC_aarch64_unknown_linux_musl=/usr/bin/false
    case_ toolchain "the toolchain" rocav - - RUSTUP_TOOLCHAIN=stable
    case_ target-dir "CARGO_TARGET_DIR" "" - - CARGO_TARGET_DIR=/elsewhere
    for pid in "${pids[@]}"; do
        wait "$pid" || fail "a key could not be taken (scripts/size-key.py failed)"
    done
    here=$(cat "$work/here.keys")
    base=$(cat "$work/base.keys")
    [ -n "$base" ] || fail "no keys at all"
    [ "$here" = "$base" ] || fail "a copy at another path has other keys"
    key_of() { echo "$1" | tr ' ' '\n' | grep -A1 -x "$2" | tail -1; }
    for entry in "${cases[@]}"; do
        IFS='|' read -r name what expect <<< "$entry"
        now=$(cat "$work/$name.keys")
        for b in r o c a v; do
            if [ "$(key_of "$base" "$b")" != "$(key_of "$now" "$b")" ]; then
                [[ "$expect" == *"$b"* ]] || fail "$what changed the key of $b"
            else
                [[ "$expect" != *"$b"* ]] || fail "$what left the key of $b as it was"
            fi
        done
    done
    # Where the whole gate builds: a core change makes keys new, and goes to CI; a change to
    # the notes makes none, and runs here.
    where="$work/where" && mkdir -p "$where"
    for b in r o c a v; do : > "$where/$(key_of "$base" "$b")"; done
    values() { local b; for b in r o c a v; do key_of "$1" "$b"; done; }
    # shellcheck disable=SC2046
    [ "$(new_keys "$where" $(values "$base"))" = 0 ] || fail "keys all measured count as new"
    # shellcheck disable=SC2046
    [ "$(new_keys "$where" $(values "$(cat "$work/core.keys")"))" = 5 ] || fail "a core change does not make every key new (CI only)"
    # shellcheck disable=SC2046
    [ "$(new_keys "$where" $(values "$(cat "$work/notes.keys")"))" = 0 ] || fail "a change to the notes makes a key new (it must run here)"
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
    read -r board recovery_limit orion_limit when recovery_when <<< "$line"
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
    if [ "${recovery_when:-$when}" = release ] && [ "$release" = 0 ]; then
        echo "    orion-recovery for a board ($board): before a release only (scripts/size-gate.sh --release)"
    else
        plan "recovery-$board" orion-recovery-bin orion-recovery \
            "orion-recovery for a board ($board, static)" "$recovery_limit" "$board"
    fi
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

if [ "${SIZE_GATE_CI_ONLY:-0}" = 1 ] && [ "$measure" = 0 ]; then
    plan_keys=()
    for p in "${plans[@]}"; do
        plan_keys+=("${keys[${p%%|*}]}")
    done
    new=$(new_keys "$store" "${plan_keys[@]}")
    if [ "$new" -gt 0 ]; then
        why="ci-only:$new-of-${#plans[@]}-size-keys-new"
        echo "    not built here: $new of ${#plans[@]} size keys new; CI builds and measures them ($why)"
        if [ -n "${GATE_LANE_STATE_DIR:-}" ]; then
            echo "not-run($why)" > "$GATE_LANE_STATE_DIR/size.status"
        fi
        if [ -n "$self_test" ]; then
            wait "$self_test" || failures=$((failures + 1))
            sed 's/^/    /' "$results/self-test"
        fi
        exit $((failures > 0))
    fi
fi

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
