//! The C surface as declared in `orion/orion.h`: constants, the two structs, and the functions.
//! The same declarations serve both build modes; only what resolves them differs.

#![allow(missing_docs)]

use core::ffi::c_char;
use core::mem::{align_of, offset_of, size_of};

pub use abi::{AbiVersion, Features, Handle, MessageSink, Status};

pub const ORION_ABI_MAJOR: u16 = 1;
pub const ORION_ABI_MINOR: u16 = 0;
pub const ORION_LAYOUT_HASH: u64 = 0x2801_f660_7019_ee2a;

pub const ORION_FEATURE_LINK: u32 = 0;
pub const ORION_FEATURE_LATEST: u32 = 1;

pub const ORION_SUBJECT_ANY: u32 = 0;
pub const ORION_SUBJECT_PROVIDER: u32 = 1;
pub const ORION_SUBJECT_EXECUTOR: u32 = 2;
pub const ORION_SUBJECT_RESOURCE: u32 = 3;
pub const ORION_SUBJECT_WORKLOAD: u32 = 4;
pub const ORION_SUBJECT_NODE: u32 = 5;

pub const ORION_EVENT_STATUS: u32 = 1;
pub const ORION_EVENT_EXPIRED: u32 = 2;
pub const ORION_EVENT_ANSWERED: u32 = 3;
pub const ORION_EVENT_REQUEST_ERROR: u32 = 4;
pub const ORION_EVENT_ACTION: u32 = 5;
pub const ORION_EVENT_RECORD: u32 = 6;
pub const ORION_EVENT_RECORD_REMOVED: u32 = 7;
pub const ORION_EVENT_STREAM_SERIES: u32 = 8;
pub const ORION_EVENT_STREAM_SAMPLES: u32 = 9;
pub const ORION_EVENT_STREAM_RESET: u32 = 10;

pub const ORION_EVENT_BOOTSTRAP: u32 = 1;
pub const ORION_EVENT_LIVE: u32 = 2;

pub const ORION_VALUE_NONE: u32 = 0;
pub const ORION_VALUE_BOOL: u32 = 1;
pub const ORION_VALUE_INT: u32 = 2;
pub const ORION_VALUE_UINT: u32 = 3;
pub const ORION_VALUE_F64: u32 = 4;
pub const ORION_VALUE_STRING: u32 = 5;
pub const ORION_VALUE_BYTES: u32 = 6;

pub const ORION_ACTION_ACCEPTED: u32 = 1;
pub const ORION_ACTION_RUNNING: u32 = 2;
pub const ORION_ACTION_SUCCEEDED: u32 = 3;
pub const ORION_ACTION_FAILED: u32 = 4;
pub const ORION_ACTION_REJECTED: u32 = 5;
pub const ORION_ACTION_TIMED_OUT: u32 = 6;
pub const ORION_ACTION_CANCELLED: u32 = 7;

pub const ORION_STREAM_RESET_NEW_EPOCH: u32 = 1;
pub const ORION_STREAM_RESET_OUT_OF_RANGE: u32 = 2;
pub const ORION_STREAM_RESET_OVERRUN: u32 = 3;
pub const ORION_STREAM_RESET_REMOVED: u32 = 4;

pub const ORION_LATEST_FRAME: u32 = 0;
pub const ORION_LATEST_EMPTY: u32 = 1;
pub const ORION_LATEST_STALE: u32 = 2;
pub const ORION_LATEST_TIMEOUT: u32 = 3;
pub const ORION_LATEST_BUSY: u32 = 4;
pub const ORION_LATEST_MAX_FDS: usize = 16;

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct OrionEvent {
    pub struct_size: u32,
    pub kind: u32,
    pub req: u32,
    pub flags: u32,
    pub subject_kind: u32,
    pub value_type: u32,
    pub int_value: i64,
    pub uint_value: u64,
    pub float_value: f64,
    pub ttl_ms: u64,
    pub published_at_ms: u64,
    pub subject_len: u32,
    pub key_len: u32,
    pub data_len: u32,
    pub reserved: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrionLatestFrame {
    pub struct_size: u32,
    pub status: u32,
    pub sequence: u64,
    pub published_at_unix_nanos: u64,
    pub payload_len: u64,
    pub fd_count: u32,
    pub fds: [i32; ORION_LATEST_MAX_FDS],
    pub reserved: u32,
}

const fn shape<T>() -> [u64; 2] {
    [size_of::<T>() as u64, align_of::<T>() as u64]
}

// The same hash as the library's: these declarations match the ones it was built with.
const LAYOUT: u64 = {
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
    LAYOUT == ORION_LAYOUT_HASH,
    "the wrapper's layouts differ from liborion's"
);

unsafe extern "C" {
    pub fn orion_version() -> AbiVersion;
    pub fn orion_check_version(caller: *const AbiVersion) -> Status;
    pub fn orion_features() -> Features;
    pub fn orion_last_error(sink: MessageSink) -> Status;
    pub fn orion_close(handle: Handle) -> Status;

    pub fn orion_link_connect(
        socket_path: *const c_char,
        device_name: *const c_char,
        timeout_ms: u32,
        out: *mut Handle,
    ) -> Status;
    pub fn orion_link_fd(handle: Handle, fd: *mut i32) -> Status;
    pub fn orion_link_service_in_ms(handle: Handle, ms: *mut u32) -> Status;
    pub fn orion_link_status_query(
        handle: Handle,
        subject_kind: u32,
        subject_id: *const c_char,
        key_prefix: *const c_char,
        req: *mut u32,
    ) -> Status;
    pub fn orion_link_status_watch(
        handle: Handle,
        subject_kind: u32,
        subject_id: *const c_char,
        key_prefix: *const c_char,
        req: *mut u32,
    ) -> Status;
    pub fn orion_link_action_run(
        handle: Handle,
        target_kind: u32,
        target_id: *const c_char,
        action_id: *const c_char,
        name: *const c_char,
        args_json: *const c_char,
        deadline_ms: u64,
        req: *mut u32,
    ) -> Status;
    pub fn orion_link_action_watch(
        handle: Handle,
        action_id: *const c_char,
        req: *mut u32,
    ) -> Status;
    pub fn orion_link_record_query(
        handle: Handle,
        kind: u32,
        record_id: *const c_char,
        node_id: *const c_char,
        req: *mut u32,
    ) -> Status;
    pub fn orion_link_record_watch(
        handle: Handle,
        kind: u32,
        record_id: *const c_char,
        node_id: *const c_char,
        req: *mut u32,
    ) -> Status;
    pub fn orion_link_stream_subscribe(
        handle: Handle,
        owner_kind: u32,
        owner_id: *const c_char,
        stream_id: *const c_char,
        series: *const c_char,
        min_interval_us: u32,
        backfill_ms: u64,
        req: *mut u32,
    ) -> Status;
    pub fn orion_link_end(handle: Handle, req: u32) -> Status;
    pub fn orion_link_next(
        handle: Handle,
        timeout_ms: i32,
        event: *mut OrionEvent,
        buf: *mut u8,
        cap: usize,
    ) -> Status;

    pub fn orion_latest_connect(socket_path: *const c_char, out: *mut Handle) -> Status;
    pub fn orion_latest_get(
        handle: Handle,
        frame: *mut OrionLatestFrame,
        payload: *mut u8,
        cap: usize,
    ) -> Status;
    pub fn orion_latest_next(
        handle: Handle,
        after_sequence: u64,
        wait_ms: u32,
        frame: *mut OrionLatestFrame,
        payload: *mut u8,
        cap: usize,
    ) -> Status;
}

// In the static build the core is linked in: its constants must be these.
#[cfg(feature = "static")]
const _: () = {
    assert!(orion_capi::ORION_LAYOUT_HASH == ORION_LAYOUT_HASH);
    assert!(orion_capi::ORION_ABI_MAJOR == ORION_ABI_MAJOR);
    assert!(orion_capi::ORION_ABI_MINOR == ORION_ABI_MINOR);
    assert!(orion_capi::ORION_EVENT_STREAM_RESET == ORION_EVENT_STREAM_RESET);
    assert!(orion_capi::ORION_EVENT_LIVE == ORION_EVENT_LIVE);
    assert!(orion_capi::ORION_ACTION_CANCELLED == ORION_ACTION_CANCELLED);
    assert!(orion_capi::ORION_STREAM_RESET_REMOVED == ORION_STREAM_RESET_REMOVED);
    assert!(orion_capi::ORION_LATEST_BUSY == ORION_LATEST_BUSY);
    assert!(orion_capi::ORION_LATEST_MAX_FDS == ORION_LATEST_MAX_FDS);
};
