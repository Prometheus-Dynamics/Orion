//! Publishing and reading make no system call.
//!
//! A child process installs a seccomp filter that kills it on any system call other than `exit`,
//! then publishes and reads. It can only exit cleanly if neither made one.

#![allow(unsafe_code)]

use orion_ring::{FieldId, Flags, FrameTime, Next, Ring, Spec};

const DONE: libc::c_long = 0;
const WRONG: libc::c_long = 9;

/// Kills the calling process on any system call but `exit`. Async-signal-safe.
unsafe fn only_exit_from_here() {
    let filter = [
        // The system call number.
        libc::sock_filter {
            code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
            jt: 0,
            jf: 0,
            k: 0,
        },
        libc::sock_filter {
            code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
            jt: 0,
            jf: 1,
            k: libc::SYS_exit as u32,
        },
        libc::sock_filter {
            code: (libc::BPF_RET | libc::BPF_K) as u16,
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_ALLOW,
        },
        libc::sock_filter {
            code: (libc::BPF_RET | libc::BPF_K) as u16,
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_KILL_PROCESS,
        },
    ];
    let program = libc::sock_fprog {
        len: filter.len() as u16,
        filter: filter.as_ptr().cast_mut(),
    };
    // SAFETY: plain `prctl` calls; `program` outlives the call that reads it. If either fails
    // the child exits with a code the parent doesn't accept.
    unsafe {
        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0
            || libc::prctl(
                libc::PR_SET_SECCOMP,
                libc::SECCOMP_MODE_FILTER,
                &raw const program,
            ) != 0
        {
            libc::syscall(libc::SYS_exit, 8);
        }
    }
}

/// Runs `body` in a child under the filter and returns how the child ended.
fn under_the_filter(body: impl FnOnce() -> libc::c_long) -> libc::c_int {
    // SAFETY: the child touches only memory it already has, runs `body` (which must not allocate
    // or lock) and leaves through a raw `exit`; it never returns into the test harness.
    unsafe {
        let child = libc::fork();
        assert!(child >= 0, "fork");
        if child == 0 {
            only_exit_from_here();
            let code = body();
            libc::syscall(libc::SYS_exit, code);
            // Not reached; and if it were, the filter kills the process here.
            libc::abort();
        }
        let mut status = 0;
        assert_eq!(libc::waitpid(child, &raw mut status, 0), child);
        status
    }
}

#[test]
fn publish_and_read_make_no_system_call() {
    let ring = Ring::create(&Spec {
        fields: 2,
        history: 8,
        max_payload: 512,
        expiry_ms: 0,
    })
    .expect("ring");
    let mut writer = ring.writer().expect("writer");
    let mut reader = ring.reader().expect("reader");
    let payload = [5_u8; 512];

    let status = under_the_filter(|| {
        let mut bytes = 0_usize;
        for seq in 0..20_000_u64 {
            let field = FieldId((seq % 2) as u16);
            if writer
                .publish(field, FrameTime::from_nanos(seq), Flags::NONE, &payload)
                .is_err()
                || writer.subscribers() != 0
            {
                return WRONG;
            }
            // Every third round the reader falls behind, so skipping is covered too.
            if seq % 3 == 0 {
                continue;
            }
            loop {
                match reader.next() {
                    Next::Sample(sample) => bytes += sample.payload.len(),
                    Next::Skipped(_) => {}
                    Next::Empty => break,
                }
            }
            bytes += reader
                .latest(field)
                .map_or(0, |sample| sample.payload.len());
        }
        if bytes > 0 { DONE } else { WRONG }
    });
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "the child was killed or failed: status {status:#x}"
    );
}

#[test]
fn the_filter_does_kill_on_a_system_call() {
    // The proof above means nothing unless a system call really ends the child.
    let status = under_the_filter(|| {
        // SAFETY: a harmless system call.
        unsafe { libc::syscall(libc::SYS_getpid) }
    });
    assert!(
        libc::WIFSIGNALED(status) && libc::WTERMSIG(status) == libc::SIGSYS,
        "status {status:#x}"
    );
}
