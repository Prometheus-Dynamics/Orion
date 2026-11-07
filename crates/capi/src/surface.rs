//! The `extern "C"` functions, their types and constants (docs/capi.md).
//!
//! Every entry point runs inside `abi::guard` (a panic is `PANIC`, never an unwind into C) and
//! records the text of its last failure for [`orion_last_error`], per thread. Pointers are checked
//! for null; everything else a pointer points at is the caller's to keep valid for the call.

use core::ffi::{CStr, c_char};
use core::mem::{align_of, offset_of, size_of};
use std::cell::RefCell;
use std::os::fd::IntoRawFd;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use abi::{AbiVersion, Features, Handle, MessageSink, Slab, Status};
use orion_control_plane::{StatusQuery, StatusSubject, TypedConfigValue};
use orion_core::{ExecutorId, NodeId, ProviderId, ResourceId, WorkloadId};

use crate::latest::{LatestClient, Reply};
use crate::link::{self, Event, LinkClient};

/// Breaking changes to this surface.
pub const ORION_ABI_MAJOR: u16 = 1;
/// Additions to this surface.
pub const ORION_ABI_MINOR: u16 = 0;

/// Feature bit: the link client (`orion_link_*`).
pub const ORION_FEATURE_LINK: u32 = 0;
/// Feature bit: the fd latest-value client (`orion_latest_*`).
pub const ORION_FEATURE_LATEST: u32 = 1;

/// Status entries of any subject.
pub const ORION_SUBJECT_ANY: u32 = 0;
pub const ORION_SUBJECT_PROVIDER: u32 = 1;
pub const ORION_SUBJECT_EXECUTOR: u32 = 2;
pub const ORION_SUBJECT_RESOURCE: u32 = 3;
pub const ORION_SUBJECT_WORKLOAD: u32 = 4;
pub const ORION_SUBJECT_NODE: u32 = 5;

/// An entry set or replaced.
pub const ORION_EVENT_STATUS: u32 = 1;
/// An entry that expired or was removed (no value).
pub const ORION_EVENT_EXPIRED: u32 = 2;
/// The answer to `req` is complete: all of a query, or one change set of a watch.
pub const ORION_EVENT_ANSWERED: u32 = 3;
/// The node refused `req`, which ends: `uint_value` is the link error code, the data its reason.
pub const ORION_EVENT_REQUEST_ERROR: u32 = 4;

/// The event belongs to a bootstrap answer (the whole current set), not a change.
pub const ORION_EVENT_BOOTSTRAP: u32 = 1;

pub const ORION_VALUE_NONE: u32 = 0;
/// `int_value` 0 or 1.
pub const ORION_VALUE_BOOL: u32 = 1;
/// `int_value`.
pub const ORION_VALUE_INT: u32 = 2;
/// `uint_value`.
pub const ORION_VALUE_UINT: u32 = 3;
/// `float_value`.
pub const ORION_VALUE_F64: u32 = 4;
/// UTF-8 in the data bytes.
pub const ORION_VALUE_STRING: u32 = 5;
/// The data bytes.
pub const ORION_VALUE_BYTES: u32 = 6;

/// A frame, with its payload and descriptors.
pub const ORION_LATEST_FRAME: u32 = 0;
/// Nothing published yet.
pub const ORION_LATEST_EMPTY: u32 = 1;
/// The newest frame is older than the channel's maximum age.
pub const ORION_LATEST_STALE: u32 = 2;
/// No newer frame within the wait.
pub const ORION_LATEST_TIMEOUT: u32 = 3;
/// The channel has its maximum of clients.
pub const ORION_LATEST_BUSY: u32 = 4;
/// Descriptors a latest-value reply carries at most.
pub const ORION_LATEST_MAX_FDS: usize = 16;

/// One thing the node said. Its bytes (subject id, then key, then data) are in the caller's
/// buffer, back to back and not NUL-terminated.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OrionEvent {
    /// `sizeof(OrionEvent)`, set by the caller.
    pub struct_size: u32,
    /// `ORION_EVENT_*`.
    pub kind: u32,
    /// The query or watch this answers.
    pub req: u32,
    /// `ORION_EVENT_BOOTSTRAP`.
    pub flags: u32,
    /// `ORION_SUBJECT_*` of the entry.
    pub subject_kind: u32,
    /// `ORION_VALUE_*`.
    pub value_type: u32,
    pub int_value: i64,
    pub uint_value: u64,
    pub float_value: f64,
    /// The entry's time-to-live and publication time, in milliseconds.
    pub ttl_ms: u64,
    pub published_at_ms: u64,
    pub subject_len: u32,
    pub key_len: u32,
    pub data_len: u32,
    pub reserved: u32,
}

/// One latest-value reply. The descriptors are the caller's to close.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OrionLatestFrame {
    /// `sizeof(OrionLatestFrame)`, set by the caller.
    pub struct_size: u32,
    /// `ORION_LATEST_*`.
    pub status: u32,
    /// The frame's sequence, or the newest known one for `ORION_LATEST_TIMEOUT`.
    pub sequence: u64,
    pub published_at_unix_nanos: u64,
    /// Bytes of payload (in the caller's buffer).
    pub payload_len: u64,
    pub fd_count: u32,
    pub fds: [i32; ORION_LATEST_MAX_FDS],
    pub reserved: u32,
}

const fn shape<T>() -> [u64; 2] {
    [size_of::<T>() as u64, align_of::<T>() as u64]
}

/// Hash of the layouts crossing this surface: the ABI crate's types and Orion's own. Every struct
/// is fixed-width (no pointers), so the hash is the same on every target with 8-byte aligned
/// 64-bit integers. A layout change fails the build here until this value is updated (on dev
/// with no version change; versions move at a release, docs/capi.md "Versions").
pub const ORION_LAYOUT_HASH: u64 = 0x2801_f660_7019_ee2a;

const COMPUTED_LAYOUT_HASH: u64 = {
    let [es, ea] = shape::<OrionEvent>();
    let [ls, la] = shape::<OrionLatestFrame>();
    abi::layout_hash(&[
        abi::LAYOUT_HASH,
        es,
        ea,
        offset_of!(OrionEvent, int_value) as u64,
        offset_of!(OrionEvent, float_value) as u64,
        offset_of!(OrionEvent, subject_len) as u64,
        offset_of!(OrionEvent, reserved) as u64,
        ls,
        la,
        offset_of!(OrionLatestFrame, sequence) as u64,
        offset_of!(OrionLatestFrame, payload_len) as u64,
        offset_of!(OrionLatestFrame, fds) as u64,
        offset_of!(OrionLatestFrame, reserved) as u64,
    ])
};

const _: () = assert!(
    ORION_LAYOUT_HASH == COMPUTED_LAYOUT_HASH,
    "a layout crossing the C surface changed: update ORION_LAYOUT_HASH (docs/capi.md, Versions)"
);

const VERSION: AbiVersion = AbiVersion::new(ORION_ABI_MAJOR, ORION_ABI_MINOR, ORION_LAYOUT_HASH);

// ---------------------------------------------------------------------------------------------
// Handles and errors.

enum Entry {
    Link(Arc<Mutex<LinkClient>>),
    Latest(Arc<Mutex<LatestState>>),
}

struct LatestState {
    client: LatestClient,
    /// A reply whose payload did not fit the caller's buffer, handed out by the next call.
    pending: Option<Reply>,
}

static HANDLES: Mutex<Slab<Entry>> = Mutex::new(Slab::new());

thread_local! {
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

fn handles() -> MutexGuard<'static, Slab<Entry>> {
    HANDLES.lock().unwrap_or_else(PoisonError::into_inner)
}

fn lock<T>(entry: &Mutex<T>) -> MutexGuard<'_, T> {
    entry.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Records `text` as this thread's last error and returns `status`.
fn fail(status: Status, text: impl std::fmt::Display) -> Status {
    LAST_ERROR.with(|last| {
        let mut last = last.borrow_mut();
        last.clear();
        use std::fmt::Write;
        let _ = write!(last, "{text}");
    });
    status
}

fn link_status(error: &link::Error) -> Status {
    match error {
        link::Error::Io(error) => Status::from_io_error(error),
        link::Error::Rejected(orion_link::message::RejectReason::UnknownDevice) => Status::ACCESS,
        link::Error::Rejected(_) => Status::NOT_SUPPORTED,
        link::Error::Closed => Status::from_errno(libc::ECONNRESET),
        link::Error::TimedOut => Status::TIMED_OUT,
        link::Error::Protocol(_) => Status::from_errno(libc::EPROTO),
    }
}

fn link_fail(error: link::Error) -> Status {
    fail(link_status(&error), error)
}

fn io_fail(error: std::io::Error) -> Status {
    fail(Status::from_io_error(&error), error)
}

/// A NUL-terminated UTF-8 argument; `None` for null.
///
/// # Safety
///
/// `text` is null or a NUL-terminated string valid for the call.
unsafe fn text<'a>(text: *const c_char, what: &str) -> Result<Option<&'a str>, Status> {
    if text.is_null() {
        return Ok(None);
    }
    // SAFETY: the caller's contract.
    unsafe { CStr::from_ptr(text) }
        .to_str()
        .map(Some)
        .map_err(|_| {
            fail(
                Status::INVALID_ARGUMENT,
                format_args!("{what} is not UTF-8"),
            )
        })
}

fn link(handle: Handle) -> Result<Arc<Mutex<LinkClient>>, Status> {
    match handles().get(handle) {
        Ok(Entry::Link(client)) => Ok(Arc::clone(client)),
        Ok(Entry::Latest(_)) => Err(fail(Status::INVALID_ARGUMENT, "not a link handle")),
        Err(status) => Err(fail(status, "stale handle")),
    }
}

fn latest_state(handle: Handle) -> Result<Arc<Mutex<LatestState>>, Status> {
    match handles().get(handle) {
        Ok(Entry::Latest(state)) => Ok(Arc::clone(state)),
        Ok(Entry::Link(_)) => Err(fail(Status::INVALID_ARGUMENT, "not a latest-value handle")),
        Err(status) => Err(fail(status, "stale handle")),
    }
}

fn run(body: impl FnOnce() -> Result<Status, Status>) -> Status {
    abi::guard(|| body().unwrap_or_else(|status| status))
}

fn timeout(ms: i32) -> Option<Duration> {
    u64::try_from(ms).ok().map(Duration::from_millis)
}

// ---------------------------------------------------------------------------------------------
// Version, features, errors.

/// This library's version: `ORION_ABI_MAJOR`, `ORION_ABI_MINOR`, `ORION_LAYOUT_HASH`.
#[unsafe(no_mangle)]
pub extern "C" fn orion_version() -> AbiVersion {
    VERSION
}

/// Whether this library serves a caller built against `caller` (the header's version): `OK` or
/// `VERSION_MISMATCH`. Call it before anything else.
///
/// # Safety
///
/// `caller` is null or points at a valid `LemnosAbiVersion`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_check_version(caller: *const AbiVersion) -> Status {
    // SAFETY: the caller's contract.
    match unsafe { caller.as_ref() } {
        Some(caller) => VERSION.accepts(caller),
        None => Status::INVALID_ARGUMENT,
    }
}

/// The optional parts this build has: bits `ORION_FEATURE_*`.
#[unsafe(no_mangle)]
pub extern "C" fn orion_features() -> Features {
    Features::NONE
        .with(ORION_FEATURE_LINK)
        .with(ORION_FEATURE_LATEST)
}

/// Writes the text of this thread's last failure to `sink`.
#[unsafe(no_mangle)]
pub extern "C" fn orion_last_error(sink: MessageSink) -> Status {
    abi::guard(|| {
        LAST_ERROR.with(|last| sink.write(&last.borrow()));
        Status::OK
    })
}

// ---------------------------------------------------------------------------------------------
// Link client.

/// Opens a client session on the node's link socket at `socket_path`, as `device_name`, and
/// waits up to `timeout_ms` for the node to accept it.
///
/// # Safety
///
/// The strings are NUL-terminated; `out` points at a writable handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_link_connect(
    socket_path: *const c_char,
    device_name: *const c_char,
    timeout_ms: u32,
    out: *mut Handle,
) -> Status {
    run(|| {
        // SAFETY: the caller's contract.
        let (path, name, out) = unsafe {
            (
                text(socket_path, "socket_path")?,
                text(device_name, "device_name")?,
                out.as_mut(),
            )
        };
        let (Some(path), Some(name), Some(out)) = (path, name, out) else {
            return Err(fail(Status::INVALID_ARGUMENT, "null argument"));
        };
        let timeout = Duration::from_millis(u64::from(timeout_ms));
        let client = LinkClient::connect(Path::new(path), name, timeout).map_err(link_fail)?;
        *out = handles()
            .insert(Entry::Link(Arc::new(Mutex::new(client))))
            .map_err(|status| fail(status, "no handle left"))?;
        Ok(Status::OK)
    })
}

/// Closes a session (or a latest-value connection).
#[unsafe(no_mangle)]
pub extern "C" fn orion_close(handle: Handle) -> Status {
    run(|| {
        handles()
            .remove(handle)
            .map_err(|status| fail(status, "stale handle"))?;
        Ok(Status::OK)
    })
}

/// The session's socket, to poll for reading. Still call `orion_link_next` within
/// `orion_link_service_in_ms` of the last call, readable or not: that keeps the session alive.
///
/// # Safety
///
/// `fd` points at a writable int.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_link_fd(handle: Handle, fd: *mut i32) -> Status {
    run(|| {
        let client = link(handle)?;
        // SAFETY: the caller's contract.
        let fd = unsafe { fd.as_mut() }.ok_or(Status::INVALID_ARGUMENT)?;
        *fd = lock(&client).fd();
        Ok(Status::OK)
    })
}

/// Milliseconds until the session needs an `orion_link_next` call at the latest.
///
/// # Safety
///
/// `ms` points at a writable u32.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_link_service_in_ms(handle: Handle, ms: *mut u32) -> Status {
    run(|| {
        let client = link(handle)?;
        // SAFETY: the caller's contract.
        let ms = unsafe { ms.as_mut() }.ok_or(Status::INVALID_ARGUMENT)?;
        *ms = u32::try_from(lock(&client).service_in().as_millis()).unwrap_or(u32::MAX);
        Ok(Status::OK)
    })
}

/// # Safety
///
/// The strings are null or NUL-terminated.
unsafe fn status_query(
    subject_kind: u32,
    subject_id: *const c_char,
    key_prefix: *const c_char,
) -> Result<StatusQuery, Status> {
    // SAFETY: the caller's contract.
    let (id, prefix) = unsafe {
        (
            text(subject_id, "subject_id")?,
            text(key_prefix, "key_prefix")?,
        )
    };
    let id = || match id {
        Some(id) if !id.trim().is_empty() => Ok(id),
        _ => Err(fail(Status::INVALID_ARGUMENT, "the subject needs an id")),
    };
    let subject = match subject_kind {
        ORION_SUBJECT_ANY => None,
        ORION_SUBJECT_PROVIDER => Some(StatusSubject::Provider(ProviderId::new(id()?))),
        ORION_SUBJECT_EXECUTOR => Some(StatusSubject::Executor(ExecutorId::new(id()?))),
        ORION_SUBJECT_RESOURCE => Some(StatusSubject::Resource(ResourceId::new(id()?))),
        ORION_SUBJECT_WORKLOAD => Some(StatusSubject::Workload(WorkloadId::new(id()?))),
        ORION_SUBJECT_NODE => Some(StatusSubject::Node(NodeId::new(id()?))),
        _ => return Err(fail(Status::INVALID_ARGUMENT, "unknown subject kind")),
    };
    Ok(StatusQuery {
        subject,
        key_prefix: prefix.map(str::to_owned),
    })
}

/// # Safety
///
/// As `orion_link_status_query`.
unsafe fn status_request(
    handle: Handle,
    filter: (u32, *const c_char, *const c_char),
    req: *mut u32,
    watch: bool,
) -> Status {
    run(|| {
        let client = link(handle)?;
        // SAFETY: the caller's contract.
        let filter = unsafe { status_query(filter.0, filter.1, filter.2)? };
        let mut client = lock(&client);
        let id = if watch {
            client.status_watch(filter)
        } else {
            client.status_query(filter)
        }
        .map_err(link_fail)?;
        // SAFETY: the caller's contract.
        if let Some(req) = unsafe { req.as_mut() } {
            *req = id;
        }
        Ok(Status::OK)
    })
}

/// Asks once for the status entries of the subject `subject_kind` (`ORION_SUBJECT_*`; any
/// subject with `ORION_SUBJECT_ANY`) and `subject_id` (null for `ORION_SUBJECT_ANY`), with keys
/// starting with `key_prefix` (null: any key). `req` (may be null) receives the request id: the
/// answer is `ORION_EVENT_STATUS` events with it, then `ORION_EVENT_ANSWERED`.
///
/// # Safety
///
/// The strings are null or NUL-terminated UTF-8; `req` is null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_link_status_query(
    handle: Handle,
    subject_kind: u32,
    subject_id: *const c_char,
    key_prefix: *const c_char,
    req: *mut u32,
) -> Status {
    // SAFETY: the caller's contract.
    unsafe { status_request(handle, (subject_kind, subject_id, key_prefix), req, false) }
}

/// Watches the status entries selected as for `orion_link_status_query`: a bootstrap answer,
/// then each change set, each ending with `ORION_EVENT_ANSWERED`. One watch per session.
///
/// # Safety
///
/// As `orion_link_status_query`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_link_status_watch(
    handle: Handle,
    subject_kind: u32,
    subject_id: *const c_char,
    key_prefix: *const c_char,
    req: *mut u32,
) -> Status {
    // SAFETY: the caller's contract.
    unsafe { status_request(handle, (subject_kind, subject_id, key_prefix), req, true) }
}

/// Ends the watch `req`.
#[unsafe(no_mangle)]
pub extern "C" fn orion_link_end(handle: Handle, req: u32) -> Status {
    run(|| {
        let client = link(handle)?;
        lock(&client).end(req).map_err(link_fail)?;
        Ok(Status::OK)
    })
}

fn subject_parts(subject: &StatusSubject) -> (u32, &str) {
    match subject {
        StatusSubject::Provider(id) => (ORION_SUBJECT_PROVIDER, id.as_str()),
        StatusSubject::Executor(id) => (ORION_SUBJECT_EXECUTOR, id.as_str()),
        StatusSubject::Resource(id) => (ORION_SUBJECT_RESOURCE, id.as_str()),
        StatusSubject::Workload(id) => (ORION_SUBJECT_WORKLOAD, id.as_str()),
        StatusSubject::Node(id) => (ORION_SUBJECT_NODE, id.as_str()),
    }
}

/// Fills `out` from `event`; returns the bytes it needs, in order.
fn describe<'a>(event: &'a Event, out: &mut OrionEvent) -> [&'a [u8]; 3] {
    let none: &[u8] = &[];
    let mut parts = [none; 3];
    out.flags = 0;
    out.subject_kind = ORION_SUBJECT_ANY;
    out.value_type = ORION_VALUE_NONE;
    (out.int_value, out.uint_value, out.float_value) = (0, 0, 0.0);
    (out.ttl_ms, out.published_at_ms, out.reserved) = (0, 0, 0);
    match event {
        Event::Status {
            req,
            bootstrap,
            entry,
        } => {
            (out.kind, out.req) = (ORION_EVENT_STATUS, *req);
            out.flags = if *bootstrap { ORION_EVENT_BOOTSTRAP } else { 0 };
            let (kind, id) = subject_parts(&entry.subject);
            out.subject_kind = kind;
            (parts[0], parts[1]) = (id.as_bytes(), entry.key.as_bytes());
            (out.ttl_ms, out.published_at_ms) = (entry.ttl_ms, entry.published_at_ms);
            match &entry.value {
                TypedConfigValue::Bool(v) => {
                    (out.value_type, out.int_value) = (ORION_VALUE_BOOL, i64::from(*v));
                }
                TypedConfigValue::Int(v) => (out.value_type, out.int_value) = (ORION_VALUE_INT, *v),
                TypedConfigValue::UInt(v) => {
                    (out.value_type, out.uint_value) = (ORION_VALUE_UINT, *v);
                }
                TypedConfigValue::F64(v) => {
                    (out.value_type, out.float_value) = (ORION_VALUE_F64, *v);
                }
                TypedConfigValue::String(v) => {
                    (out.value_type, parts[2]) = (ORION_VALUE_STRING, v.as_bytes());
                }
                TypedConfigValue::Bytes(v) => (out.value_type, parts[2]) = (ORION_VALUE_BYTES, v),
            }
        }
        Event::Expired { req, key } => {
            (out.kind, out.req) = (ORION_EVENT_EXPIRED, *req);
            let (kind, id) = subject_parts(&key.subject);
            out.subject_kind = kind;
            (parts[0], parts[1]) = (id.as_bytes(), key.key.as_bytes());
        }
        Event::Answered { req, bootstrap } => {
            (out.kind, out.req) = (ORION_EVENT_ANSWERED, *req);
            out.flags = if *bootstrap { ORION_EVENT_BOOTSTRAP } else { 0 };
        }
        Event::RequestError { req, code, reason } => {
            (out.kind, out.req) = (ORION_EVENT_REQUEST_ERROR, *req);
            out.uint_value = u64::from(*code);
            parts[2] = reason.as_bytes();
        }
    }
    let len = |part: &[u8]| u32::try_from(part.len()).unwrap_or(u32::MAX);
    (out.subject_len, out.key_len, out.data_len) = (len(parts[0]), len(parts[1]), len(parts[2]));
    parts
}

/// The next event, waiting up to `timeout_ms` (-1: until one comes, 0: only what has arrived).
/// Pings the node while it waits.
///
/// `OK`: `event` is filled, its bytes are in `buf`, and the event is consumed. `AGAIN`: nothing
/// within the wait. `BUFFER_TOO_SMALL`: `event` is filled with the lengths, nothing is consumed;
/// call again with `subject_len + key_len + data_len` bytes. Any other failure ends the session
/// (close the handle).
///
/// # Safety
///
/// `event` points at a writable `OrionEvent` whose `struct_size` is set; `buf` is null (with
/// `cap` 0) or writable for `cap` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_link_next(
    handle: Handle,
    timeout_ms: i32,
    event: *mut OrionEvent,
    buf: *mut u8,
    cap: usize,
) -> Status {
    run(|| {
        let client = link(handle)?;
        // SAFETY: the caller's contract.
        let out = unsafe { event.as_mut() }.ok_or(Status::INVALID_ARGUMENT)?;
        if (out.struct_size as usize) < size_of::<OrionEvent>() {
            return Err(fail(Status::INVALID_ARGUMENT, "event struct_size"));
        }
        let buf: &mut [u8] = if buf.is_null() || cap == 0 {
            &mut []
        } else {
            // SAFETY: the caller's contract.
            unsafe { core::slice::from_raw_parts_mut(buf, cap) }
        };
        let mut client = lock(&client);
        let Some(next) = client.next_event(timeout(timeout_ms)).map_err(link_fail)? else {
            return Ok(Status::AGAIN);
        };
        let parts = describe(next, out);
        let needed: usize = parts.iter().map(|part| part.len()).sum();
        if needed > buf.len() {
            return Ok(Status::BUFFER_TOO_SMALL);
        }
        let mut at = 0;
        for part in parts {
            buf[at..at + part.len()].copy_from_slice(part);
            at += part.len();
        }
        client.pop_event();
        Ok(Status::OK)
    })
}

// ---------------------------------------------------------------------------------------------
// Latest-value client.

/// Connects to the fd latest-value channel at `socket_path`.
///
/// # Safety
///
/// `socket_path` is NUL-terminated; `out` points at a writable handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_latest_connect(
    socket_path: *const c_char,
    out: *mut Handle,
) -> Status {
    run(|| {
        // SAFETY: the caller's contract.
        let (path, out) = unsafe { (text(socket_path, "socket_path")?, out.as_mut()) };
        let (Some(path), Some(out)) = (path, out) else {
            return Err(fail(Status::INVALID_ARGUMENT, "null argument"));
        };
        let client = LatestClient::connect(Path::new(path)).map_err(io_fail)?;
        let state = LatestState {
            client,
            pending: None,
        };
        *out = handles()
            .insert(Entry::Latest(Arc::new(Mutex::new(state))))
            .map_err(|status| fail(status, "no handle left"))?;
        Ok(Status::OK)
    })
}

/// # Safety
///
/// As `orion_latest_next`.
unsafe fn latest_reply(
    handle: Handle,
    after: Option<(u64, u32)>,
    frame: *mut OrionLatestFrame,
    payload: *mut u8,
    cap: usize,
) -> Status {
    run(|| {
        let state = latest_state(handle)?;
        // SAFETY: the caller's contract.
        let out = unsafe { frame.as_mut() }.ok_or(Status::INVALID_ARGUMENT)?;
        if (out.struct_size as usize) < size_of::<OrionLatestFrame>() {
            return Err(fail(Status::INVALID_ARGUMENT, "frame struct_size"));
        }
        let mut state = lock(&state);
        let reply = match state.pending.take() {
            Some(reply) => reply,
            None => match after {
                None => state.client.latest(),
                Some((sequence, wait_ms)) => state
                    .client
                    .next_after(sequence, Duration::from_millis(u64::from(wait_ms))),
            }
            .map_err(io_fail)?,
        };
        out.status = reply.status as u32;
        out.sequence = reply.sequence;
        out.published_at_unix_nanos = reply.published_at_unix_nanos;
        out.payload_len = reply.payload.len() as u64;
        out.fd_count = 0;
        out.fds = [-1; ORION_LATEST_MAX_FDS];
        out.reserved = 0;
        if reply.payload.len() > cap || (payload.is_null() && !reply.payload.is_empty()) {
            state.pending = Some(reply);
            return Ok(Status::BUFFER_TOO_SMALL);
        }
        if !reply.payload.is_empty() {
            // SAFETY: the caller's contract; the length was checked against `cap`.
            unsafe {
                core::ptr::copy_nonoverlapping(reply.payload.as_ptr(), payload, reply.payload.len())
            };
        }
        for (slot, fd) in out.fds.iter_mut().zip(reply.fds) {
            *slot = fd.into_raw_fd();
            out.fd_count += 1;
        }
        Ok(Status::OK)
    })
}

/// The newest frame, at once. `BUFFER_TOO_SMALL`: `frame.payload_len` says what the payload needs;
/// the reply is kept and the next call on this handle returns it.
///
/// # Safety
///
/// `frame` points at a writable `OrionLatestFrame` whose `struct_size` is set; `payload` is null
/// (with `cap` 0) or writable for `cap` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_latest_get(
    handle: Handle,
    frame: *mut OrionLatestFrame,
    payload: *mut u8,
    cap: usize,
) -> Status {
    // SAFETY: the caller's contract.
    unsafe { latest_reply(handle, None, frame, payload, cap) }
}

/// The first frame newer than `after_sequence`, blocking up to `wait_ms` (the channel caps the
/// wait); `ORION_LATEST_TIMEOUT` when none came. As `orion_latest_get` otherwise.
///
/// # Safety
///
/// As `orion_latest_get`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_latest_next(
    handle: Handle,
    after_sequence: u64,
    wait_ms: u32,
    frame: *mut OrionLatestFrame,
    payload: *mut u8,
    cap: usize,
) -> Status {
    // SAFETY: the caller's contract.
    unsafe { latest_reply(handle, Some((after_sequence, wait_ms)), frame, payload, cap) }
}
