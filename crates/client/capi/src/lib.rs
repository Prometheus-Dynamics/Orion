//! The Orion client for C.
//!
//! A small surface over the `orion` crate: connect, declare a topic and publish to it,
//! subscribe to one and read it. `include/orion.h` is the header, and its comments are the
//! reference. The library builds as `liborion_c.a` and as `liborion_c.so`, whose name on
//! disk and in the loader carries the major version.
//!
//! Every function here is the only place that touches a raw pointer from C. Each checks its
//! arguments, runs the Rust inside `catch_unwind` so a panic never crosses into C, and turns
//! the result into a status and a message the caller can ask for.

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::{Duration, Instant};

use orion::topics::{
    Field, FieldId, FieldType, Next, Publisher, SampleTime, Subscriber, TopicDecl, TopicError,
};
use orion::{Address, ClientError, Error, Options, Orion};

/// The major version of the surface. A change that breaks a caller raises it.
pub const ABI_MAJOR: u32 = 1;
/// The minor version. Something added raises it.
pub const ABI_MINOR: u32 = 0;

/// All is well.
pub const ORION_OK: i32 = 0;
/// An argument is null, not text, not a name, or out of range.
pub const ORION_E_ARGUMENT: i32 = -1;
/// The connection broke or the comm core went quiet.
pub const ORION_E_LOST: i32 = -2;
/// The comm core said no.
pub const ORION_E_REFUSED: i32 = -3;
/// The time allowed ran out.
pub const ORION_E_TIMEOUT: i32 = -4;
/// The caller's buffer is too small.
pub const ORION_E_BUFFER: i32 = -5;
/// The library was built for another major version than the caller.
pub const ORION_E_VERSION: i32 = -6;
/// Something inside the library went wrong.
pub const ORION_E_INTERNAL: i32 = -7;

/// One field of a topic, as C gives it.
#[repr(C)]
#[derive(Debug)]
pub struct OrionField {
    /// The field's name.
    pub name: *const c_char,
    /// Its type: 1 bool, 2 int, 3 float, 4 text, 5 bytes.
    pub ty: u8,
    /// Its unit. May be null.
    pub unit: *const c_char,
}

/// One sample, as C receives it.
#[repr(C)]
#[derive(Debug, Default)]
pub struct OrionSample {
    /// Which field.
    pub field: u16,
    /// The time the sample belongs to.
    pub time_ns: u64,
    /// Bytes of the sample.
    pub len: usize,
    /// Samples lost before this one.
    pub skipped: u64,
}

/// A session.
#[derive(Debug)]
pub struct OrionHandle {
    orion: Orion,
}

/// A topic's publisher.
#[derive(Debug)]
pub struct OrionPublisher {
    publisher: Publisher,
}

/// A topic's subscriber.
#[derive(Debug)]
pub struct OrionSubscriber {
    subscriber: Subscriber,
    /// Samples lost since the last one handed to the caller.
    skipped: u64,
}

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn remember(message: &str) {
    let text = CString::new(message.replace('\0', " ")).unwrap_or_default();
    LAST_ERROR.with(|last| *last.borrow_mut() = text);
}

/// Why a call failed: its status, and what to say about it.
struct Failed(i32, String);

impl From<Error> for Failed {
    fn from(error: Error) -> Self {
        let status = match &error {
            Error::Client(ClientError::Io(io)) if io.kind() == std::io::ErrorKind::TimedOut => {
                ORION_E_TIMEOUT
            }
            _ if error.is_lost() => ORION_E_LOST,
            Error::Client(
                ClientError::Refused(_)
                | ClientError::Rejected(_)
                | ClientError::Denied(_)
                | ClientError::OtherKey { .. }
                | ClientError::Revision { .. }
                | ClientError::Level { .. },
            ) => ORION_E_REFUSED,
            Error::Topic(TopicError::Invalid(_) | TopicError::Publish(_)) => ORION_E_ARGUMENT,
            _ => ORION_E_INTERNAL,
        };
        Self(status, error.to_string())
    }
}

impl From<TopicError> for Failed {
    fn from(error: TopicError) -> Self {
        Error::from(error).into()
    }
}

fn argument(what: &str) -> Failed {
    Failed(ORION_E_ARGUMENT, what.to_owned())
}

/// Runs `body`, turning a failure into its status and message, and a panic into
/// `ORION_E_INTERNAL`.
fn guarded(body: impl FnOnce() -> Result<i32, Failed>) -> i32 {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(status)) => status,
        Ok(Err(Failed(status, message))) => {
            remember(&message);
            status
        }
        Err(_) => {
            remember("the library panicked; close the handle");
            ORION_E_INTERNAL
        }
    }
}

/// The text at `pointer`.
///
/// # Safety
///
/// `pointer` is null or points to a NUL-terminated string that lives for `'a`.
unsafe fn text<'a>(pointer: *const c_char, what: &str) -> Result<&'a str, Failed> {
    if pointer.is_null() {
        return Err(argument(&format!("{what} is null")));
    }
    // SAFETY: not null, and the caller promises a NUL-terminated string that outlives `'a`.
    unsafe { CStr::from_ptr(pointer) }
        .to_str()
        .map_err(|_| argument(&format!("{what} is not UTF-8")))
}

/// Whether this library can serve a caller built against `major.minor`.
#[unsafe(no_mangle)]
pub extern "C" fn orion_check_version(major: u32, minor: u32) -> i32 {
    guarded(|| {
        // The minor is zero today, which makes this look like `==`. It is not, once it moves.
        #[allow(clippy::absurd_extreme_comparisons)]
        let served = major == ABI_MAJOR && minor <= ABI_MINOR;
        if served {
            Ok(ORION_OK)
        } else {
            Err(Failed(
                ORION_E_VERSION,
                format!(
                    "the caller was built for {major}.{minor}; this library is {ABI_MAJOR}.{ABI_MINOR}"
                ),
            ))
        }
    })
}

/// The calling thread's last failure, in words.
#[unsafe(no_mangle)]
pub extern "C" fn orion_last_error() -> *const c_char {
    LAST_ERROR.with(|last| last.borrow().as_ptr())
}

/// Connects to a comm core.
///
/// # Safety
///
/// `address` is null or a NUL-terminated string; `name` is a NUL-terminated string; `out`
/// is null or points to space for one pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_connect(
    address: *const c_char,
    name: *const c_char,
    timeout_ms: u32,
    out: *mut *mut OrionHandle,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return Err(argument("out is null"));
        }
        let address = if address.is_null() {
            Address::default()
        } else {
            // SAFETY: not null, and the caller promises a NUL-terminated string.
            unsafe { text(address, "address") }?
                .parse()
                .map_err(|error: String| argument(&error))?
        };
        // SAFETY: the caller promises a NUL-terminated string.
        let name = unsafe { text(name, "name") }?;
        let options = Options {
            timeout: Duration::from_millis(u64::from(timeout_ms.max(1))),
            ..Options::default()
        };
        let orion = Orion::connect_with(&address, name, options)?;
        // SAFETY: `out` is not null and the caller promises space for one pointer.
        unsafe { out.write(Box::into_raw(Box::new(OrionHandle { orion }))) };
        Ok(ORION_OK)
    })
}

/// Connects again after the session was lost, and says everything again.
///
/// # Safety
///
/// `orion` is null or a handle from [`orion_connect`] that has not been closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_reconnect(orion: *mut OrionHandle, timeout_ms: u32) -> i32 {
    guarded(|| {
        // SAFETY: null, or a live handle as the caller promises.
        let handle = unsafe { orion.as_ref() }.ok_or_else(|| argument("orion is null"))?;
        let deadline = Instant::now() + Duration::from_millis(u64::from(timeout_ms));
        handle.orion.reconnect(|_| Instant::now() < deadline)?;
        Ok(ORION_OK)
    })
}

/// Keeps the session and takes in what the comm core sends.
///
/// # Safety
///
/// `orion` is null or a handle from [`orion_connect`] that has not been closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_pump(orion: *mut OrionHandle, wait_ms: u32) -> i32 {
    guarded(|| {
        // SAFETY: null, or a live handle as the caller promises.
        let handle = unsafe { orion.as_ref() }.ok_or_else(|| argument("orion is null"))?;
        handle
            .orion
            .pump(Duration::from_millis(u64::from(wait_ms)))?;
        Ok(ORION_OK)
    })
}

/// Closes a session.
///
/// # Safety
///
/// `orion` is null or a handle from [`orion_connect`] that has not been closed. It must not
/// be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_close(orion: *mut OrionHandle) {
    if !orion.is_null() {
        // SAFETY: not null, made by `Box::into_raw` in `orion_connect`, and not yet freed.
        let handle = unsafe { Box::from_raw(orion) };
        let _ = catch_unwind(AssertUnwindSafe(move || drop(handle)));
    }
}

/// Declares a topic and returns its publisher.
///
/// # Safety
///
/// `orion` is null or a live handle; `topic` is a NUL-terminated string; `fields` points to
/// `field_count` fields whose strings are NUL-terminated; `out` is null or points to space
/// for one pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_declare(
    orion: *mut OrionHandle,
    topic: *const c_char,
    fields: *const OrionField,
    field_count: usize,
    history: u32,
    max_payload: u32,
    out: *mut *mut OrionPublisher,
) -> i32 {
    guarded(|| {
        // SAFETY: null, or a live handle as the caller promises.
        let handle = unsafe { orion.as_ref() }.ok_or_else(|| argument("orion is null"))?;
        if out.is_null() || fields.is_null() || field_count == 0 || field_count > 256 {
            return Err(argument(
                "out or fields is null, or there are no fields, or over 256",
            ));
        }
        // SAFETY: the caller promises a NUL-terminated string.
        let topic = unsafe { text(topic, "topic") }?;
        // SAFETY: not null, and the caller promises `field_count` fields there.
        let given = unsafe { std::slice::from_raw_parts(fields, field_count) };
        let mut declared = Vec::with_capacity(field_count);
        for field in given {
            let ty = match field.ty {
                1 => FieldType::Bool,
                2 => FieldType::Int,
                3 => FieldType::Float,
                4 => FieldType::Text,
                5 => FieldType::Bytes,
                other => return Err(argument(&format!("{other} is not a field type"))),
            };
            declared.push(Field {
                // SAFETY: the caller promises NUL-terminated strings in every field.
                name: unsafe { text(field.name, "a field's name") }?,
                ty,
                // SAFETY: as above; a null unit is no unit.
                unit: if field.unit.is_null() {
                    ""
                } else {
                    unsafe { text(field.unit, "a field's unit") }?
                },
                type_name: "",
            });
        }
        let publisher = handle.orion.declare(&TopicDecl {
            name: topic,
            fields: &declared,
            history,
            expiry_ms: 0,
            max_payload,
            on_subscribe: Default::default(),
        })?;
        // SAFETY: `out` is not null and the caller promises space for one pointer.
        unsafe { out.write(Box::into_raw(Box::new(OrionPublisher { publisher }))) };
        Ok(ORION_OK)
    })
}

/// Publishes one sample.
///
/// # Safety
///
/// `publisher` is null or a live publisher; `payload` points to `len` bytes, or is null with
/// `len` zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_publish(
    publisher: *mut OrionPublisher,
    field: u16,
    time_ns: u64,
    payload: *const u8,
    len: usize,
) -> i32 {
    guarded(|| {
        // SAFETY: null, or a live publisher as the caller promises.
        let handle = unsafe { publisher.as_mut() }.ok_or_else(|| argument("publisher is null"))?;
        let payload = if len == 0 {
            &[][..]
        } else if payload.is_null() {
            return Err(argument("payload is null"));
        } else {
            // SAFETY: not null, and the caller promises `len` bytes there.
            unsafe { std::slice::from_raw_parts(payload, len) }
        };
        handle
            .publisher
            .publish(FieldId(field), SampleTime::from_nanos(time_ns), payload)?;
        Ok(ORION_OK)
    })
}

/// How many subscribers the topic has.
///
/// # Safety
///
/// `publisher` is null or a live publisher; `out` is null or points to a `u32`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_publisher_subscribers(
    publisher: *const OrionPublisher,
    out: *mut u32,
) -> i32 {
    guarded(|| {
        // SAFETY: null, or a live publisher as the caller promises.
        let handle = unsafe { publisher.as_ref() }.ok_or_else(|| argument("publisher is null"))?;
        if out.is_null() {
            return Err(argument("out is null"));
        }
        // SAFETY: not null, and the caller promises a `u32` there.
        unsafe { out.write(handle.publisher.subscribers()) };
        Ok(ORION_OK)
    })
}

/// Frees a publisher.
///
/// # Safety
///
/// `publisher` is null or a live publisher. It must not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_publisher_free(publisher: *mut OrionPublisher) {
    if !publisher.is_null() {
        // SAFETY: not null, made by `Box::into_raw` in `orion_declare`, and not yet freed.
        let handle = unsafe { Box::from_raw(publisher) };
        let _ = catch_unwind(AssertUnwindSafe(move || drop(handle)));
    }
}

/// Subscribes to a topic.
///
/// # Safety
///
/// `orion` is null or a live handle; `topic` is a NUL-terminated string; `out` is null or
/// points to space for one pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_subscribe(
    orion: *mut OrionHandle,
    topic: *const c_char,
    out: *mut *mut OrionSubscriber,
) -> i32 {
    guarded(|| {
        // SAFETY: null, or a live handle as the caller promises.
        let handle = unsafe { orion.as_ref() }.ok_or_else(|| argument("orion is null"))?;
        if out.is_null() {
            return Err(argument("out is null"));
        }
        // SAFETY: the caller promises a NUL-terminated string.
        let topic = unsafe { text(topic, "topic") }?;
        let subscriber = handle.orion.subscribe(topic)?;
        let subscriber = OrionSubscriber {
            subscriber,
            skipped: 0,
        };
        // SAFETY: `out` is not null and the caller promises space for one pointer.
        unsafe { out.write(Box::into_raw(Box::new(subscriber))) };
        Ok(ORION_OK)
    })
}

/// Sleeps until there is something to read. 1 if there is, 0 if the time ran out.
///
/// # Safety
///
/// `subscriber` is null or a live subscriber.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_wait(subscriber: *mut OrionSubscriber, wait_ms: u32) -> i32 {
    guarded(|| {
        // SAFETY: null, or a live subscriber as the caller promises.
        let handle =
            unsafe { subscriber.as_mut() }.ok_or_else(|| argument("subscriber is null"))?;
        Ok(i32::from(
            handle
                .subscriber
                .wait(Duration::from_millis(u64::from(wait_ms)))?,
        ))
    })
}

/// Hands one sample to C: its facts in `sample`, its bytes in `buffer`.
///
/// # Safety
///
/// `sample` points to an `OrionSample`; `buffer` points to `capacity` bytes, or is null with
/// `capacity` zero.
unsafe fn hand_over(
    found: orion::topics::Sample<'_>,
    skipped: u64,
    sample: *mut OrionSample,
    buffer: *mut u8,
    capacity: usize,
) -> Result<i32, Failed> {
    let len = found.payload.len();
    // SAFETY: the caller promises an `OrionSample` there.
    unsafe {
        sample.write(OrionSample {
            field: found.field.0,
            time_ns: found.time.as_nanos(),
            len,
            skipped,
        });
    }
    if len > capacity {
        return Err(Failed(
            ORION_E_BUFFER,
            format!("the sample is {len} bytes and the buffer {capacity}"),
        ));
    }
    if len > 0 {
        // SAFETY: `buffer` holds `capacity >= len` bytes, as the caller promises, and the
        // sample's bytes are the library's own copy, so the two do not overlap.
        unsafe { std::ptr::copy_nonoverlapping(found.payload.as_ptr(), buffer, len) };
    }
    Ok(1)
}

/// The next sample. 1 with a sample, 0 with nothing new.
///
/// # Safety
///
/// `subscriber` is null or a live subscriber; `sample` is null or points to an
/// `OrionSample`; `buffer` points to `capacity` bytes, or is null with `capacity` zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_next(
    subscriber: *mut OrionSubscriber,
    sample: *mut OrionSample,
    buffer: *mut u8,
    capacity: usize,
) -> i32 {
    guarded(|| {
        // SAFETY: null, or a live subscriber as the caller promises.
        let handle =
            unsafe { subscriber.as_mut() }.ok_or_else(|| argument("subscriber is null"))?;
        if sample.is_null() || (buffer.is_null() && capacity > 0) {
            return Err(argument("sample or buffer is null"));
        }
        loop {
            match handle.subscriber.next() {
                Next::Empty => return Ok(0),
                Next::Skipped(lost) => handle.skipped += lost,
                Next::Sample(found) => {
                    let skipped = std::mem::take(&mut handle.skipped);
                    // SAFETY: `sample` and `buffer` were checked above and are the caller's.
                    return unsafe { hand_over(found, skipped, sample, buffer, capacity) };
                }
            }
        }
    })
}

/// The newest sample of a field. 1 with a sample, 0 if there is none yet.
///
/// # Safety
///
/// As [`orion_next`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_latest(
    subscriber: *mut OrionSubscriber,
    field: u16,
    sample: *mut OrionSample,
    buffer: *mut u8,
    capacity: usize,
) -> i32 {
    guarded(|| {
        // SAFETY: null, or a live subscriber as the caller promises.
        let handle =
            unsafe { subscriber.as_mut() }.ok_or_else(|| argument("subscriber is null"))?;
        if sample.is_null() || (buffer.is_null() && capacity > 0) {
            return Err(argument("sample or buffer is null"));
        }
        match handle.subscriber.latest(FieldId(field)) {
            None => Ok(0),
            // SAFETY: `sample` and `buffer` were checked above and are the caller's.
            Some(found) => unsafe { hand_over(found, 0, sample, buffer, capacity) },
        }
    })
}

/// Frees a subscriber.
///
/// # Safety
///
/// `subscriber` is null or a live subscriber. It must not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_subscriber_free(subscriber: *mut OrionSubscriber) {
    if !subscriber.is_null() {
        // SAFETY: not null, made by `Box::into_raw` in `orion_subscribe`, and not yet freed.
        let handle = unsafe { Box::from_raw(subscriber) };
        let _ = catch_unwind(AssertUnwindSafe(move || drop(handle)));
    }
}
