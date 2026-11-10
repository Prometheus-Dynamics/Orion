//! Ringing a subscriber that is awake makes no system call: a child under a seccomp filter that
//! kills it on any system call but `exit` publishes and rings, and exits cleanly.

#![allow(unsafe_code)]

use orion_ring::{FieldId, Flags, FrameTime, Ring, Spec};
use orion_ring_doorbell::Doorbell;

/// Runs `body` in a child that is killed on any system call but `exit`; returns how it ended.
fn under_the_filter(body: impl FnOnce() -> libc::c_long) -> libc::c_int {
    let allow_exit_only = [
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
        len: allow_exit_only.len() as u16,
        filter: allow_exit_only.as_ptr().cast_mut(),
    };
    // SAFETY: the child touches only memory it already has, runs `body` (which must not allocate
    // or lock) and leaves through a raw `exit`; it never returns into the test harness.
    unsafe {
        let child = libc::fork();
        assert!(child >= 0, "fork");
        if child == 0 {
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0
                || libc::prctl(
                    libc::PR_SET_SECCOMP,
                    libc::SECCOMP_MODE_FILTER,
                    &raw const program,
                ) != 0
            {
                libc::syscall(libc::SYS_exit, 8);
            }
            let code = body();
            libc::syscall(libc::SYS_exit, code);
            libc::abort();
        }
        let mut status = 0;
        assert_eq!(libc::waitpid(child, &raw mut status, 0), child);
        status
    }
}

fn ring_of_one() -> Ring {
    Ring::create(&Spec {
        fields: 1,
        history: 8,
        max_payload: 64,
        expiry_ms: 0,
    })
    .expect("ring")
}

#[test]
fn publishing_to_an_awake_subscriber_makes_no_system_call() {
    let ring = ring_of_one();
    let mut writer = ring.writer().expect("writer");
    let doorbell = Doorbell::create().expect("doorbell");
    let mut bell = doorbell.bell().expect("bell");
    let status = under_the_filter(|| {
        for seq in 0..20_000_u64 {
            if writer
                .publish(
                    FieldId(0),
                    FrameTime::from_nanos(seq),
                    Flags::NONE,
                    &[1; 64],
                )
                .is_err()
                || bell.ring()
            {
                return 9;
            }
        }
        0
    });
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "the child was killed or failed: status {status:#x}"
    );
}

#[test]
fn ringing_a_sleeping_subscriber_is_the_one_system_call() {
    // The same child, but the subscriber is asleep: the ring is a `write`, and the filter kills.
    let ring = ring_of_one();
    let mut writer = ring.writer().expect("writer");
    let mut doorbell = Doorbell::create().expect("doorbell");
    let mut bell = doorbell.bell().expect("bell");
    doorbell.arm();
    let status = under_the_filter(|| {
        let _ = writer.publish(FieldId(0), FrameTime::ZERO, Flags::NONE, &[1; 64]);
        libc::c_long::from(bell.ring())
    });
    assert!(
        libc::WIFSIGNALED(status) && libc::WTERMSIG(status) == libc::SIGSYS,
        "status {status:#x}"
    );
}
