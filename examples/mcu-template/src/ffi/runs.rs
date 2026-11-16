//! Runs: taking the one that waits and what it is, and what ended under it.

use super::{ORION_ERR_ARG, ORION_ERR_STATE, PORT, number};

/// `orion_cancel_reason`: its caller asked.
pub const ORION_CANCEL_ASKED: u8 = 1;
/// `orion_cancel_reason`: its caller's session ended, and it was not detached.
pub const ORION_CANCEL_CALLER_GONE: u8 = 2;
/// `orion_cancel_reason`: its deadline passed. The caller is told it timed out, code 110.
pub const ORION_CANCEL_DEADLINE: u8 = 3;
/// `orion_cancel_reason`: node logic is stopping.
pub const ORION_CANCEL_STOPPING: u8 = 4;

/// Takes the run that is waiting: its number into `call`, up to `cap` bytes of its arguments
/// into `args` and their whole length into `len`. Returns the action's number, or -1 if
/// none waits. Answer it with `orion_finish`.
///
/// # Safety
///
/// `call` and `len` are valid for writing; `args` is null or valid for writing `cap` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_run(
    call: *mut u64,
    args: *mut u8,
    cap: usize,
    len: *mut usize,
) -> i32 {
    if call.is_null() || len.is_null() {
        return ORION_ERR_ARG;
    }
    PORT.running(|running| {
        let Some(run) = running.layers.run.take() else {
            return -1;
        };
        running.layers.taken = Some(run);
        let held = running.layers.args.get(..run.len).unwrap_or_default();
        // SAFETY: the caller's promise.
        unsafe {
            *call = run.call;
            *len = run.len;
            if !args.is_null() {
                let n = held.len().min(cap);
                core::ptr::copy_nonoverlapping(held.as_ptr(), args, n);
            }
        }
        number(run.action)
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// The run the host asked to stop, or 0. The host ends it as cancelled; a result sent after
/// is dropped.
#[unsafe(no_mangle)]
pub extern "C" fn orion_cancelled() -> u64 {
    PORT.running(|running| core::mem::take(&mut running.layers.cancelled))
        .unwrap_or(0)
}

/// What the run `orion_run` last handed over is: the token of the session that asked for
/// it into `session` (0 from a host that does not say), its deadline in milliseconds from
/// when it was asked into `deadline_ms` (0 for none), and into `detached` whether it goes on
/// without its caller, so that `orion_caller_gone` for its session does not end it. 0, or
/// `ORION_ERR_STATE` before any run was taken. A null pointer is skipped.
///
/// # Safety
///
/// Each pointer is null or valid for writing.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_run_detail(
    session: *mut u64,
    deadline_ms: *mut u64,
    detached: *mut bool,
) -> i32 {
    PORT.running(|running| {
        let Some(run) = running.layers.taken else {
            return ORION_ERR_STATE;
        };
        // SAFETY: the caller's promise; null ones are skipped.
        unsafe {
            if !session.is_null() {
                *session = run.session;
            }
            if !deadline_ms.is_null() {
                *deadline_ms = run.deadline_ms;
            }
            if !detached.is_null() {
                *detached = run.detached;
            }
        }
        0
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// Why the run `orion_cancelled` last named is to stop: `ORION_CANCEL_*`, or 0 when the host
/// did not say. `ORION_CANCEL_DEADLINE` is a run whose caller has already been told it timed
/// out.
#[unsafe(no_mangle)]
pub extern "C" fn orion_cancel_reason() -> u8 {
    PORT.running(|running| running.layers.cancel_reason)
        .unwrap_or(0)
}

/// The token of a session that asked for runs and has ended, oldest first, or 0 when none
/// is left. Each is said once. Let go of what is held for that caller; a detached run of it
/// goes on until its deadline.
#[unsafe(no_mangle)]
pub extern "C" fn orion_caller_gone() -> u64 {
    PORT.running(|running| {
        let layers = &mut running.layers;
        if layers.gone_len == 0 {
            return 0;
        }
        let first = layers.gone[0];
        layers.gone.copy_within(1.., 0);
        layers.gone_len -= 1;
        first
    })
    .unwrap_or(0)
}
