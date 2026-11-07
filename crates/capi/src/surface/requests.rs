//! Actions, records and stream subscriptions on a link session (docs/capi.md, "Actions, records
//! and streams"). Their answers are `OrionEvent`s like status, read with `orion_link_next`.

use core::ffi::c_char;
use std::borrow::Cow;
use std::collections::BTreeMap;

use abi::{Handle, Status};
use orion_control_plane::{
    ActionQuery, ActionRequest, ActionResult, ActionState, ActionTarget, Backfill, SeriesKind,
    StatusSubject, StreamRef, StreamResetReason, StreamSubscription, TypedConfigValue,
};
use orion_core::{ExecutorId, NodeId, ProviderId, ResourceId, WorkloadId};
use orion_link::message::{Record, RecordFilter, RecordKind};

use super::{
    ORION_EVENT_BOOTSTRAP, ORION_SUBJECT_EXECUTOR, ORION_SUBJECT_NODE, ORION_SUBJECT_PROVIDER,
    ORION_SUBJECT_RESOURCE, ORION_SUBJECT_WORKLOAD, ORION_VALUE_F64, ORION_VALUE_INT,
    ORION_VALUE_UINT, OrionEvent, fail, link, link_fail, lock, run, text,
};
use crate::link::Event;

/// A state of an action: `subject_kind` and the subject are its target, the key its action id,
/// the data its result as JSON. `uint_value` is `ORION_ACTION_*`, `int_value` the progress in
/// per-mille (-1: unknown), `published_at_ms` the time of the state.
pub const ORION_EVENT_ACTION: u32 = 5;
/// A record set or replaced: `subject_kind` is its kind, the subject its id, the data the record
/// as JSON. `ORION_EVENT_BOOTSTRAP` when part of the current set.
pub const ORION_EVENT_RECORD: u32 = 6;
/// A record gone from a watch's set: `subject_kind` and the subject name it.
pub const ORION_EVENT_RECORD_REMOVED: u32 = 7;
/// One series of a subscribed stream (the catalog, one event per series in index order, first
/// and again after a reset): the key is its name, the data its unit (may be empty), `int_value`
/// its index, `uint_value` the publisher's epoch, `value_type` its kind (`ORION_VALUE_F64`,
/// `ORION_VALUE_INT` or `ORION_VALUE_UINT`; samples are always doubles), `ttl_ms` its sampling
/// period in microseconds (0: on change). `ORION_EVENT_LIVE` when the stream has a publisher.
pub const ORION_EVENT_STREAM_SERIES: u32 = 8;
/// Samples of one series: `int_value` is its index, `uint_value` the batch's sequence, and the
/// data `data_len / 16` samples, each a native-endian `uint64_t` time (microseconds since the
/// Unix epoch) then a `double` value, oldest first (copy them out: the buffer may be unaligned).
/// `ORION_EVENT_BOOTSTRAP` for history (the backfill).
pub const ORION_EVENT_STREAM_SAMPLES: u32 = 9;
/// The subscription's position no longer holds: `uint_value` is `ORION_STREAM_RESET_*`. The
/// catalog and a backfill follow, unless the stream is gone.
pub const ORION_EVENT_STREAM_RESET: u32 = 10;

/// `ORION_EVENT_STREAM_SERIES`: the stream has a publisher.
pub const ORION_EVENT_LIVE: u32 = 2;

pub const ORION_ACTION_ACCEPTED: u32 = 1;
pub const ORION_ACTION_RUNNING: u32 = 2;
pub const ORION_ACTION_SUCCEEDED: u32 = 3;
pub const ORION_ACTION_FAILED: u32 = 4;
pub const ORION_ACTION_REJECTED: u32 = 5;
pub const ORION_ACTION_TIMED_OUT: u32 = 6;
pub const ORION_ACTION_CANCELLED: u32 = 7;

/// The publisher restarted or declared other series.
pub const ORION_STREAM_RESET_NEW_EPOCH: u32 = 1;
/// The position is older than the history the node keeps.
pub const ORION_STREAM_RESET_OUT_OF_RANGE: u32 = 2;
/// The subscriber fell too far behind; queued samples were dropped.
pub const ORION_STREAM_RESET_OVERRUN: u32 = 3;
/// The stream is gone.
pub const ORION_STREAM_RESET_REMOVED: u32 = 4;

/// A required, non-blank string argument.
///
/// # Safety
///
/// As [`text`].
unsafe fn required<'a>(value: *const c_char, what: &str) -> Result<&'a str, Status> {
    // SAFETY: the caller's contract.
    match unsafe { text(value, what)? } {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(fail(
            Status::INVALID_ARGUMENT,
            format_args!("{what} is required"),
        )),
    }
}

fn subject(kind: u32, id: &str) -> Result<StatusSubject, Status> {
    Ok(match kind {
        ORION_SUBJECT_PROVIDER => StatusSubject::Provider(ProviderId::new(id)),
        ORION_SUBJECT_EXECUTOR => StatusSubject::Executor(ExecutorId::new(id)),
        ORION_SUBJECT_RESOURCE => StatusSubject::Resource(ResourceId::new(id)),
        ORION_SUBJECT_WORKLOAD => StatusSubject::Workload(WorkloadId::new(id)),
        ORION_SUBJECT_NODE => StatusSubject::Node(NodeId::new(id)),
        _ => return Err(fail(Status::INVALID_ARGUMENT, "unknown subject kind")),
    })
}

fn target(kind: u32, id: &str) -> Result<ActionTarget, Status> {
    Ok(match kind {
        ORION_SUBJECT_NODE => ActionTarget::Node(NodeId::new(id)),
        ORION_SUBJECT_PROVIDER => ActionTarget::Provider(ProviderId::new(id)),
        ORION_SUBJECT_RESOURCE => ActionTarget::Resource(ResourceId::new(id)),
        ORION_SUBJECT_EXECUTOR => ActionTarget::Executor(ExecutorId::new(id)),
        _ => {
            return Err(fail(
                Status::INVALID_ARGUMENT,
                "an action targets a node, provider, resource or executor",
            ));
        }
    })
}

fn target_parts(target: &ActionTarget) -> (u32, &str) {
    match target {
        ActionTarget::Node(id) => (ORION_SUBJECT_NODE, id.as_str()),
        ActionTarget::Provider(id) => (ORION_SUBJECT_PROVIDER, id.as_str()),
        ActionTarget::Resource(id) => (ORION_SUBJECT_RESOURCE, id.as_str()),
        ActionTarget::Executor(id) => (ORION_SUBJECT_EXECUTOR, id.as_str()),
    }
}

/// A JSON object of scalars as action arguments: booleans, integers (signed when they fit,
/// else unsigned), other numbers as doubles, and strings.
fn arguments(json: &str) -> Result<BTreeMap<String, TypedConfigValue>, Status> {
    let invalid = |what: String| fail(Status::INVALID_ARGUMENT, what);
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|error| invalid(format!("args: {error}")))?;
    let serde_json::Value::Object(object) = value else {
        return Err(invalid("args is not a JSON object".into()));
    };
    object
        .into_iter()
        .map(|(key, value)| {
            let value = match value {
                serde_json::Value::Bool(v) => TypedConfigValue::Bool(v),
                serde_json::Value::String(v) => TypedConfigValue::String(v),
                serde_json::Value::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
                    (Some(v), _, _) => TypedConfigValue::Int(v),
                    (None, Some(v), _) => TypedConfigValue::UInt(v),
                    (None, None, Some(v)) => TypedConfigValue::F64(v),
                    _ => return Err(invalid(format!("args.{key}: not a number"))),
                },
                _ => return Err(invalid(format!("args.{key}: not a scalar"))),
            };
            Ok((key, value))
        })
        .collect()
}

fn record_kind(kind: u32) -> Result<RecordKind, Status> {
    Ok(match kind {
        ORION_SUBJECT_WORKLOAD => RecordKind::Workload,
        ORION_SUBJECT_RESOURCE => RecordKind::Resource,
        ORION_SUBJECT_PROVIDER => RecordKind::Provider,
        ORION_SUBJECT_EXECUTOR => RecordKind::Executor,
        _ => {
            return Err(fail(
                Status::INVALID_ARGUMENT,
                "records are workloads, resources, providers or executors",
            ));
        }
    })
}

fn record_subject(kind: RecordKind) -> u32 {
    match kind {
        RecordKind::Workload => ORION_SUBJECT_WORKLOAD,
        RecordKind::Resource => ORION_SUBJECT_RESOURCE,
        RecordKind::Provider => ORION_SUBJECT_PROVIDER,
        RecordKind::Executor => ORION_SUBJECT_EXECUTOR,
    }
}

fn write_req(req: *mut u32, id: u32) {
    // SAFETY: the caller's contract: `req` is null or writable.
    if let Some(req) = unsafe { req.as_mut() } {
        *req = id;
    }
}

/// Runs the action `name` on the target `target_kind` (`ORION_SUBJECT_NODE`, `_PROVIDER`,
/// `_RESOURCE` or `_EXECUTOR`) `target_id`, as `action_id` (resubmitting the same request follows
/// the existing action instead of running it again). `args_json` is null or a JSON object of
/// scalars; `deadline_ms` is 0 or the time the action has. `req` (may be null) receives the
/// request id: `ORION_EVENT_ACTION` events with it follow the action until a final state.
///
/// # Safety
///
/// The strings are null (`args_json` only) or NUL-terminated UTF-8; `req` is null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_link_action_run(
    handle: Handle,
    target_kind: u32,
    target_id: *const c_char,
    action_id: *const c_char,
    name: *const c_char,
    args_json: *const c_char,
    deadline_ms: u64,
    req: *mut u32,
) -> Status {
    run(|| {
        let client = link(handle)?;
        // SAFETY: the caller's contract.
        let (target_id, action_id, name, args) = unsafe {
            (
                required(target_id, "target_id")?,
                required(action_id, "action_id")?,
                required(name, "name")?,
                text(args_json, "args_json")?,
            )
        };
        let mut request = ActionRequest::new(action_id, target(target_kind, target_id)?, name)
            .with_deadline_ms(deadline_ms);
        if let Some(args) = args {
            request.args = arguments(args)?;
        }
        let id = lock(&client).action_run(request).map_err(link_fail)?;
        write_req(req, id);
        Ok(Status::OK)
    })
}

/// Watches the action `action_id` (null: every action the session may see): `ORION_EVENT_ACTION`
/// events for the current states, then each change, each answer ending with
/// `ORION_EVENT_ANSWERED`.
///
/// # Safety
///
/// `action_id` is null or NUL-terminated UTF-8; `req` is null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_link_action_watch(
    handle: Handle,
    action_id: *const c_char,
    req: *mut u32,
) -> Status {
    run(|| {
        let client = link(handle)?;
        // SAFETY: the caller's contract.
        let filter = match unsafe { text(action_id, "action_id")? } {
            Some(id) => ActionQuery::action(id),
            None => ActionQuery::all(),
        };
        let id = lock(&client).action_watch(filter).map_err(link_fail)?;
        write_req(req, id);
        Ok(Status::OK)
    })
}

/// # Safety
///
/// As `orion_link_record_query`.
unsafe fn record_request(
    handle: Handle,
    filter: (u32, *const c_char, *const c_char),
    req: *mut u32,
    watch: bool,
) -> Status {
    run(|| {
        let client = link(handle)?;
        // SAFETY: the caller's contract.
        let (id, node) = unsafe { (text(filter.1, "record_id")?, text(filter.2, "node_id")?) };
        let filter = RecordFilter {
            kind: record_kind(filter.0)?,
            id: id.map(str::to_owned),
            node: node.map(str::to_owned),
        };
        let mut client = lock(&client);
        let id = if watch {
            client.record_watch(filter)
        } else {
            client.record_query(filter)
        }
        .map_err(link_fail)?;
        write_req(req, id);
        Ok(Status::OK)
    })
}

/// Asks once for the records of `kind` (`ORION_SUBJECT_WORKLOAD`, `_RESOURCE`, `_PROVIDER` or
/// `_EXECUTOR`), only `record_id` if not null, only those of `node_id` if not null:
/// `ORION_EVENT_RECORD` events, then `ORION_EVENT_ANSWERED`.
///
/// # Safety
///
/// The strings are null or NUL-terminated UTF-8; `req` is null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_link_record_query(
    handle: Handle,
    kind: u32,
    record_id: *const c_char,
    node_id: *const c_char,
    req: *mut u32,
) -> Status {
    // SAFETY: the caller's contract.
    unsafe { record_request(handle, (kind, record_id, node_id), req, false) }
}

/// Watches the records selected as for `orion_link_record_query`: the current set (a bootstrap),
/// then each change (`ORION_EVENT_RECORD`, `ORION_EVENT_RECORD_REMOVED`), each answer ending with
/// `ORION_EVENT_ANSWERED`.
///
/// # Safety
///
/// As `orion_link_record_query`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_link_record_watch(
    handle: Handle,
    kind: u32,
    record_id: *const c_char,
    node_id: *const c_char,
    req: *mut u32,
) -> Status {
    // SAFETY: the caller's contract.
    unsafe { record_request(handle, (kind, record_id, node_id), req, true) }
}

/// Subscribes to the stream `stream_id` of the subject `owner_kind` `owner_id`: the series in
/// `series` (null or empty: all; names or prefixes ending in `*`, separated by commas), at most
/// one sample per series per `min_interval_us` (0: every sample), starting with the last
/// `backfill_ms` of history (0: live only). The answers are `ORION_EVENT_STREAM_SERIES`, then
/// `ORION_EVENT_STREAM_SAMPLES`, each batch ending with `ORION_EVENT_ANSWERED`, until
/// `orion_link_end`.
///
/// # Safety
///
/// The strings are null (`series` only) or NUL-terminated UTF-8; `req` is null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_link_stream_subscribe(
    handle: Handle,
    owner_kind: u32,
    owner_id: *const c_char,
    stream_id: *const c_char,
    series: *const c_char,
    min_interval_us: u32,
    backfill_ms: u64,
    req: *mut u32,
) -> Status {
    run(|| {
        let client = link(handle)?;
        // SAFETY: the caller's contract.
        let (owner_id, stream_id, series) = unsafe {
            (
                required(owner_id, "owner_id")?,
                required(stream_id, "stream_id")?,
                text(series, "series")?,
            )
        };
        let stream = StreamRef {
            owner: subject(owner_kind, owner_id)?,
            id: stream_id.into(),
        };
        let mut subscription = StreamSubscription::all(stream).with_series(
            series
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty()),
        );
        subscription.min_interval_us = min_interval_us;
        if backfill_ms > 0 {
            subscription.backfill = Backfill::LastMs(backfill_ms);
        }
        let id = lock(&client)
            .stream_subscribe(subscription)
            .map_err(link_fail)?;
        write_req(req, id);
        Ok(Status::OK)
    })
}

fn action_state(state: &ActionState) -> (u32, i64) {
    match state {
        ActionState::Accepted => (ORION_ACTION_ACCEPTED, -1),
        ActionState::Running { progress } => (ORION_ACTION_RUNNING, progress.map_or(-1, i64::from)),
        ActionState::Succeeded => (ORION_ACTION_SUCCEEDED, -1),
        ActionState::Failed { .. } => (ORION_ACTION_FAILED, -1),
        ActionState::Rejected { .. } => (ORION_ACTION_REJECTED, -1),
        ActionState::TimedOut => (ORION_ACTION_TIMED_OUT, -1),
        ActionState::Cancelled => (ORION_ACTION_CANCELLED, -1),
    }
}

fn json(value: &impl serde::Serialize) -> Cow<'static, [u8]> {
    // Records and results are plain data and always serialize.
    Cow::Owned(serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec()))
}

fn result_json(result: &ActionResult) -> Cow<'static, [u8]> {
    json(result)
}

fn record_json(record: &Record) -> Cow<'static, [u8]> {
    match record {
        Record::Workload(record) => json(record),
        Record::Resource(record) => json(record),
        Record::Provider(record) => json(record),
        Record::Executor(record) => json(record),
    }
}

fn series_value_type(kind: SeriesKind) -> u32 {
    match kind {
        SeriesKind::F32 | SeriesKind::F64 => ORION_VALUE_F64,
        SeriesKind::I64 => ORION_VALUE_INT,
        SeriesKind::U32 => ORION_VALUE_UINT,
    }
}

fn reset_code(reason: StreamResetReason) -> u32 {
    match reason {
        StreamResetReason::NewEpoch => ORION_STREAM_RESET_NEW_EPOCH,
        StreamResetReason::OutOfRange => ORION_STREAM_RESET_OUT_OF_RANGE,
        StreamResetReason::Overrun => ORION_STREAM_RESET_OVERRUN,
        StreamResetReason::Removed => ORION_STREAM_RESET_REMOVED,
    }
}

/// Fills `out` (already cleared) for the events of this module; returns the subject, key and
/// data bytes. `None` for the status events, which the caller describes.
pub(super) fn describe<'a>(event: &'a Event, out: &mut OrionEvent) -> Option<[Cow<'a, [u8]>; 3]> {
    let none = || Cow::Borrowed(&[][..]);
    let bytes = |text: &'a str| Cow::Borrowed(text.as_bytes());
    let bootstrap = |yes: bool| if yes { ORION_EVENT_BOOTSTRAP } else { 0 };
    Some(match event {
        Event::Action { req, result } => {
            (out.kind, out.req) = (ORION_EVENT_ACTION, *req);
            let (kind, id) = target_parts(&result.target);
            out.subject_kind = kind;
            (out.uint_value, out.int_value) = {
                let (state, progress) = action_state(&result.state);
                (u64::from(state), progress)
            };
            out.published_at_ms = result.updated_at_ms;
            [bytes(id), bytes(&result.action_id), result_json(result)]
        }
        Event::Record {
            req,
            bootstrap: part_of_set,
            record,
        } => {
            (out.kind, out.req) = (ORION_EVENT_RECORD, *req);
            out.flags = bootstrap(*part_of_set);
            let key = record.key();
            out.subject_kind = record_subject(key.kind);
            [Cow::Owned(key.id.into_bytes()), none(), record_json(record)]
        }
        Event::RecordRemoved { req, key } => {
            (out.kind, out.req) = (ORION_EVENT_RECORD_REMOVED, *req);
            out.subject_kind = record_subject(key.kind);
            [bytes(&key.id), none(), none()]
        }
        Event::StreamSeries {
            req,
            epoch,
            index,
            live,
            series,
        } => {
            (out.kind, out.req) = (ORION_EVENT_STREAM_SERIES, *req);
            out.flags = if *live { ORION_EVENT_LIVE } else { 0 };
            (out.int_value, out.uint_value) = (i64::from(*index), *epoch);
            out.value_type = series_value_type(series.kind);
            out.ttl_ms = u64::from(series.period_us);
            let unit = series.unit.as_deref().unwrap_or_default();
            [none(), bytes(&series.name), bytes(unit)]
        }
        Event::StreamSamples {
            req,
            cursor,
            backfill,
            series,
            samples,
        } => {
            (out.kind, out.req) = (ORION_EVENT_STREAM_SAMPLES, *req);
            out.flags = bootstrap(*backfill);
            (out.int_value, out.uint_value) = (i64::from(*series), cursor.seq);
            let mut data = Vec::with_capacity(samples.len() * 16);
            for (t_us, value) in samples {
                data.extend_from_slice(&t_us.to_ne_bytes());
                data.extend_from_slice(&value.to_ne_bytes());
            }
            [none(), none(), Cow::Owned(data)]
        }
        Event::StreamReset { req, reason } => {
            (out.kind, out.req) = (ORION_EVENT_STREAM_RESET, *req);
            out.uint_value = u64::from(reset_code(*reason));
            [none(), none(), none()]
        }
        Event::Status { .. }
        | Event::Expired { .. }
        | Event::Answered { .. }
        | Event::RequestError { .. } => return None,
    })
}
