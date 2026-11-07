//! The client role's messages (interface revision 1): status queries and watches, and running,
//! querying and watching actions (`docs/design/stable-interface.md`, "Step 3 wire").
//!
//! The public messages carry the `orion-control-plane` types; the wire is the link's own mirror
//! types below, as for the records in `body.rs`. A field the wire does not carry is named in the
//! conversion, so a record that gains a field fails to compile here until that is decided.

use alloc::string::String;
use alloc::vec::Vec;

use orion_control_plane::{
    ActionQuery, ActionRequest, ActionResult, ActionState, ActionTarget, StatusEntry, StatusKey,
    StatusQuery, StatusSubject, TypedConfigValue,
};
use orion_core::{ExecutorId, NodeId, ProviderId, ResourceId, WorkloadId};
use serde::{Deserialize, Serialize};

use super::body::ValueWire;
use super::codec::{bounded_vec, id};
use super::{MessageError, kind};

/// Why the host refused a client request, in a [`RequestError`]. The list only grows; a code this
/// build does not know is kept as its number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ErrorCode(pub u16);

impl ErrorCode {
    /// A request the host does not serve (for example from a session without the client role).
    pub const UNSUPPORTED: Self = Self(1);
    /// The caller may not make this request.
    pub const NOT_AUTHORIZED: Self = Self(2);
    /// A malformed or refused request.
    pub const INVALID: Self = Self(3);
    /// Too many open watches or actions, or the rate limit.
    pub const LIMIT: Self = Self(4);
    /// The host failed to serve the request.
    pub const INTERNAL: Self = Self(5);
}

/// The host refused the request `req`, which ends. Host → client, kind [`kind::REQUEST_ERROR`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestError {
    /// The request.
    pub req: u32,
    /// Why.
    pub code: ErrorCode,
    /// A human-readable reason.
    pub reason: String,
}

/// A status query or watch. Client → host, kinds [`kind::STATUS_QUERY`] and
/// [`kind::STATUS_WATCH`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusRequest {
    /// Client-chosen request id, echoed in the answers.
    pub req: u32,
    /// Which entries. `forwarded_by`-style fields do not exist here: the filter is the whole query.
    pub filter: StatusQuery,
}

/// One part of the answer to a status query or watch. Host → client, kind
/// [`kind::STATUS_CHANGES`].
///
/// An answer larger than a frame is sent in parts; every part but the last has `more` set. A client
/// applies a `bootstrap` answer (all its parts) as the whole current set for its filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusChanges {
    /// The request this answers.
    pub req: u32,
    /// The answer is the whole current set, not a change.
    pub bootstrap: bool,
    /// More parts of this answer follow.
    pub more: bool,
    /// Entries set or replaced.
    pub updated: Vec<StatusEntry>,
    /// Entries that expired or were removed.
    pub expired: Vec<StatusKey>,
}

/// Runs an action. Client → host, kind [`kind::ACTION_RUN`].
///
/// `requested_by` and `wait_ms` of the request are not on the wire: the host names the requester,
/// and the answer follows the action until it is final.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionRun {
    /// Client-chosen request id, echoed in the answers.
    pub req: u32,
    /// The action.
    pub request: ActionRequest,
}

/// An action query or watch. Client → host, kinds [`kind::ACTION_QUERY`] and
/// [`kind::ACTION_WATCH`]. `forwarded_by` is not on the wire (it is the node's own field).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionFilterRequest {
    /// Client-chosen request id, echoed in the answers.
    pub req: u32,
    /// Which actions.
    pub filter: ActionQuery,
}

/// One part of the answer to an action run, query or watch. Host → client, kind
/// [`kind::ACTION_RESULTS`]. Every part but the last of an answer has `more` set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionResults {
    /// The request this answers.
    pub req: u32,
    /// More parts of this answer follow.
    pub more: bool,
    /// The results.
    pub results: Vec<ActionResult>,
}

// --- Wire mirrors -------------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
pub(super) enum SubjectWire {
    #[serde(deserialize_with = "id")]
    Provider(String),
    #[serde(deserialize_with = "id")]
    Executor(String),
    #[serde(deserialize_with = "id")]
    Resource(String),
    #[serde(deserialize_with = "id")]
    Workload(String),
    #[serde(deserialize_with = "id")]
    Node(String),
}

impl From<&StatusSubject> for SubjectWire {
    fn from(subject: &StatusSubject) -> Self {
        match subject {
            StatusSubject::Provider(id) => Self::Provider(id.as_str().into()),
            StatusSubject::Executor(id) => Self::Executor(id.as_str().into()),
            StatusSubject::Resource(id) => Self::Resource(id.as_str().into()),
            StatusSubject::Workload(id) => Self::Workload(id.as_str().into()),
            StatusSubject::Node(id) => Self::Node(id.as_str().into()),
        }
    }
}

impl From<SubjectWire> for StatusSubject {
    fn from(wire: SubjectWire) -> Self {
        match wire {
            SubjectWire::Provider(id) => Self::Provider(ProviderId::new(id)),
            SubjectWire::Executor(id) => Self::Executor(ExecutorId::new(id)),
            SubjectWire::Resource(id) => Self::Resource(ResourceId::new(id)),
            SubjectWire::Workload(id) => Self::Workload(WorkloadId::new(id)),
            SubjectWire::Node(id) => Self::Node(NodeId::new(id)),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum TargetWire {
    #[serde(deserialize_with = "id")]
    Node(String),
    #[serde(deserialize_with = "id")]
    Provider(String),
    #[serde(deserialize_with = "id")]
    Resource(String),
    #[serde(deserialize_with = "id")]
    Executor(String),
}

impl From<&ActionTarget> for TargetWire {
    fn from(target: &ActionTarget) -> Self {
        match target {
            ActionTarget::Node(id) => Self::Node(id.as_str().into()),
            ActionTarget::Provider(id) => Self::Provider(id.as_str().into()),
            ActionTarget::Resource(id) => Self::Resource(id.as_str().into()),
            ActionTarget::Executor(id) => Self::Executor(id.as_str().into()),
        }
    }
}

impl From<TargetWire> for ActionTarget {
    fn from(wire: TargetWire) -> Self {
        match wire {
            TargetWire::Node(id) => Self::Node(NodeId::new(id)),
            TargetWire::Provider(id) => Self::Provider(ProviderId::new(id)),
            TargetWire::Resource(id) => Self::Resource(ResourceId::new(id)),
            TargetWire::Executor(id) => Self::Executor(ExecutorId::new(id)),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StatusFilterWire {
    subject: Option<SubjectWire>,
    key_prefix: Option<String>,
}

impl From<&StatusQuery> for StatusFilterWire {
    fn from(query: &StatusQuery) -> Self {
        let StatusQuery {
            subject,
            key_prefix,
        } = query;
        Self {
            subject: subject.as_ref().map(SubjectWire::from),
            key_prefix: key_prefix.clone(),
        }
    }
}

impl From<StatusFilterWire> for StatusQuery {
    fn from(wire: StatusFilterWire) -> Self {
        Self {
            subject: wire.subject.map(Into::into),
            key_prefix: wire.key_prefix,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StatusRecordWire {
    subject: SubjectWire,
    key: String,
    value: ValueWire,
    ttl_ms: u64,
    published_at_ms: u64,
}

impl From<&StatusEntry> for StatusRecordWire {
    fn from(entry: &StatusEntry) -> Self {
        let StatusEntry {
            subject,
            key,
            value,
            ttl_ms,
            published_at_ms,
        } = entry;
        Self {
            subject: subject.into(),
            key: key.clone(),
            value: value.into(),
            ttl_ms: *ttl_ms,
            published_at_ms: *published_at_ms,
        }
    }
}

impl From<StatusRecordWire> for StatusEntry {
    fn from(wire: StatusRecordWire) -> Self {
        Self {
            subject: wire.subject.into(),
            key: wire.key,
            value: wire.value.into(),
            ttl_ms: wire.ttl_ms,
            published_at_ms: wire.published_at_ms,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StatusKeyWire {
    subject: SubjectWire,
    key: String,
}

impl From<&StatusKey> for StatusKeyWire {
    fn from(key: &StatusKey) -> Self {
        let StatusKey { subject, key } = key;
        Self {
            subject: subject.into(),
            key: key.clone(),
        }
    }
}

impl From<StatusKeyWire> for StatusKey {
    fn from(wire: StatusKeyWire) -> Self {
        Self {
            subject: wire.subject.into(),
            key: wire.key,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ActionFilterWire {
    action_id: Option<String>,
    target: Option<TargetWire>,
}

impl From<&ActionQuery> for ActionFilterWire {
    fn from(query: &ActionQuery) -> Self {
        let ActionQuery {
            action_id,
            target,
            // The node's own field (peer forwarding); not part of the link interface.
            forwarded_by: _,
        } = query;
        Self {
            action_id: action_id.clone(),
            target: target.as_ref().map(TargetWire::from),
        }
    }
}

impl From<ActionFilterWire> for ActionQuery {
    fn from(wire: ActionFilterWire) -> Self {
        Self {
            action_id: wire.action_id,
            target: wire.target.map(Into::into),
            forwarded_by: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
enum ActionStateWire {
    Accepted,
    Running { progress: Option<u16> },
    Succeeded,
    Failed { reason: String },
    Rejected { reason: String },
    TimedOut,
    Cancelled,
}

impl From<&ActionState> for ActionStateWire {
    fn from(state: &ActionState) -> Self {
        match state {
            ActionState::Accepted => Self::Accepted,
            ActionState::Running { progress } => Self::Running {
                progress: *progress,
            },
            ActionState::Succeeded => Self::Succeeded,
            ActionState::Failed { reason } => Self::Failed {
                reason: reason.clone(),
            },
            ActionState::Rejected { reason } => Self::Rejected {
                reason: reason.clone(),
            },
            ActionState::TimedOut => Self::TimedOut,
            ActionState::Cancelled => Self::Cancelled,
        }
    }
}

impl From<ActionStateWire> for ActionState {
    fn from(wire: ActionStateWire) -> Self {
        match wire {
            ActionStateWire::Accepted => Self::Accepted,
            ActionStateWire::Running { progress } => Self::Running { progress },
            ActionStateWire::Succeeded => Self::Succeeded,
            ActionStateWire::Failed { reason } => Self::Failed { reason },
            ActionStateWire::Rejected { reason } => Self::Rejected { reason },
            ActionStateWire::TimedOut => Self::TimedOut,
            ActionStateWire::Cancelled => Self::Cancelled,
        }
    }
}

fn map_to_wire(
    map: &alloc::collections::BTreeMap<String, TypedConfigValue>,
) -> Vec<(String, ValueWire)> {
    map.iter()
        .map(|(key, value)| (key.clone(), value.into()))
        .collect()
}

fn map_from_wire(
    pairs: Vec<(String, ValueWire)>,
) -> alloc::collections::BTreeMap<String, TypedConfigValue> {
    pairs
        .into_iter()
        .map(|(key, value)| (key, value.into()))
        .collect()
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ActionRecordWire {
    action_id: String,
    target: TargetWire,
    name: String,
    state: ActionStateWire,
    #[serde(deserialize_with = "bounded_vec")]
    output: Vec<(String, ValueWire)>,
    #[serde(deserialize_with = "id")]
    handled_by: String,
    requested_by: String,
    created_at_ms: u64,
    updated_at_ms: u64,
}

impl From<&ActionResult> for ActionRecordWire {
    fn from(result: &ActionResult) -> Self {
        let ActionResult {
            action_id,
            target,
            name,
            state,
            output,
            handled_by,
            requested_by,
            created_at_ms,
            updated_at_ms,
        } = result;
        Self {
            action_id: action_id.clone(),
            target: target.into(),
            name: name.clone(),
            state: state.into(),
            output: map_to_wire(output),
            handled_by: handled_by.as_str().into(),
            requested_by: requested_by.clone(),
            created_at_ms: *created_at_ms,
            updated_at_ms: *updated_at_ms,
        }
    }
}

impl From<ActionRecordWire> for ActionResult {
    fn from(wire: ActionRecordWire) -> Self {
        Self {
            action_id: wire.action_id,
            target: wire.target.into(),
            name: wire.name,
            state: wire.state.into(),
            output: map_from_wire(wire.output),
            handled_by: NodeId::new(wire.handled_by),
            requested_by: wire.requested_by,
            created_at_ms: wire.created_at_ms,
            updated_at_ms: wire.updated_at_ms,
        }
    }
}

// --- Bodies -------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct RequestErrorBody {
    req: u32,
    code: ErrorCode,
    reason: String,
}

#[derive(Serialize, Deserialize)]
struct StatusRequestBody {
    req: u32,
    filter: StatusFilterWire,
}

#[derive(Serialize)]
struct StatusChangesRef<'a> {
    req: u32,
    bootstrap: bool,
    more: bool,
    updated: &'a [StatusRecordWire],
    expired: &'a [StatusKeyWire],
}

#[derive(Deserialize)]
struct StatusChangesBody {
    req: u32,
    bootstrap: bool,
    more: bool,
    #[serde(deserialize_with = "bounded_vec")]
    updated: Vec<StatusRecordWire>,
    #[serde(deserialize_with = "bounded_vec")]
    expired: Vec<StatusKeyWire>,
}

#[derive(Serialize, Deserialize)]
struct ActionRunBody {
    req: u32,
    action_id: String,
    target: TargetWire,
    name: String,
    #[serde(deserialize_with = "bounded_vec")]
    args: Vec<(String, ValueWire)>,
    deadline_ms: u64,
}

#[derive(Serialize, Deserialize)]
struct ActionFilterBody {
    req: u32,
    filter: ActionFilterWire,
}

#[derive(Serialize)]
struct ActionResultsRef<'a> {
    req: u32,
    more: bool,
    results: &'a [ActionRecordWire],
}

#[derive(Deserialize)]
struct ActionResultsBody {
    req: u32,
    more: bool,
    #[serde(deserialize_with = "bounded_vec")]
    results: Vec<ActionRecordWire>,
}

fn decode<'a, T: Deserialize<'a>>(kind: u8, payload: &'a [u8]) -> Result<T, MessageError> {
    super::codec::decode_body(kind, payload)
}

/// The client message carried by a frame of `kind`, if `kind` is a client kind.
pub(super) fn decode_client(
    kind: u8,
    payload: &[u8],
) -> Option<Result<super::Message, MessageError>> {
    use super::Message;
    let message = match kind {
        kind::REQUEST_ERROR => decode::<RequestErrorBody>(kind, payload).map(|body| {
            Message::RequestError(RequestError {
                req: body.req,
                code: body.code,
                reason: body.reason,
            })
        }),
        kind::REQUEST_END => decode::<u32>(kind, payload).map(|req| Message::RequestEnd { req }),
        kind::STATUS_QUERY | kind::STATUS_WATCH => {
            decode::<StatusRequestBody>(kind, payload).map(|body| {
                let request = StatusRequest {
                    req: body.req,
                    filter: body.filter.into(),
                };
                if kind == kind::STATUS_QUERY {
                    Message::StatusQuery(request)
                } else {
                    Message::StatusWatch(request)
                }
            })
        }
        kind::STATUS_CHANGES => decode::<StatusChangesBody>(kind, payload).map(|body| {
            Message::StatusChanges(StatusChanges {
                req: body.req,
                bootstrap: body.bootstrap,
                more: body.more,
                updated: body.updated.into_iter().map(Into::into).collect(),
                expired: body.expired.into_iter().map(Into::into).collect(),
            })
        }),
        kind::ACTION_RUN => decode::<ActionRunBody>(kind, payload).map(|body| {
            Message::ActionRun(alloc::boxed::Box::new(ActionRun {
                req: body.req,
                request: ActionRequest {
                    action_id: body.action_id,
                    target: body.target.into(),
                    name: body.name,
                    args: map_from_wire(body.args),
                    deadline_ms: body.deadline_ms,
                    requested_by: String::new(),
                    wait_ms: 0,
                },
            }))
        }),
        kind::ACTION_QUERY | kind::ACTION_WATCH => {
            decode::<ActionFilterBody>(kind, payload).map(|body| {
                let request = ActionFilterRequest {
                    req: body.req,
                    filter: body.filter.into(),
                };
                if kind == kind::ACTION_QUERY {
                    Message::ActionQuery(request)
                } else {
                    Message::ActionWatch(request)
                }
            })
        }
        kind::ACTION_RESULTS => decode::<ActionResultsBody>(kind, payload).map(|body| {
            Message::ActionResults(ActionResults {
                req: body.req,
                more: body.more,
                results: body.results.into_iter().map(Into::into).collect(),
            })
        }),
        _ => return None,
    };
    Some(message)
}

/// The request id at the start of a client request's body, for answering a body that does not
/// decode.
#[must_use]
pub fn request_id(payload: &[u8]) -> Option<u32> {
    postcard::take_from_bytes::<u32>(payload)
        .ok()
        .map(|(req, _)| req)
}

/// Encodes a client message of `kind` (one of the client kinds) into `buf`.
pub(super) fn encode_client(
    message: &super::Message,
    seq: u16,
    buf: &mut [u8],
) -> Option<Result<usize, MessageError>> {
    use super::Message;
    use super::codec::encode_with;
    let kind = message.kind();
    Some(match message {
        Message::RequestError(error) => {
            encode_request_error(error.req, error.code, &error.reason, seq, buf)
        }
        Message::RequestEnd { req } => encode_with(kind, seq, req, buf),
        Message::StatusQuery(request) | Message::StatusWatch(request) => encode_with(
            kind,
            seq,
            &StatusRequestBody {
                req: request.req,
                filter: (&request.filter).into(),
            },
            buf,
        ),
        Message::StatusChanges(changes) => {
            let updated: Vec<StatusRecordWire> = changes.updated.iter().map(Into::into).collect();
            let expired: Vec<StatusKeyWire> = changes.expired.iter().map(Into::into).collect();
            encode_with(
                kind,
                seq,
                &StatusChangesRef {
                    req: changes.req,
                    bootstrap: changes.bootstrap,
                    more: changes.more,
                    updated: &updated,
                    expired: &expired,
                },
                buf,
            )
        }
        Message::ActionRun(run) => {
            let ActionRequest {
                action_id,
                target,
                name,
                args,
                deadline_ms,
                // Named by the host, and implied by the answer (see `ActionRun`).
                requested_by: _,
                wait_ms: _,
            } = &run.request;
            encode_with(
                kind,
                seq,
                &ActionRunBody {
                    req: run.req,
                    action_id: action_id.clone(),
                    target: target.into(),
                    name: name.clone(),
                    args: map_to_wire(args),
                    deadline_ms: *deadline_ms,
                },
                buf,
            )
        }
        Message::ActionQuery(request) | Message::ActionWatch(request) => encode_with(
            kind,
            seq,
            &ActionFilterBody {
                req: request.req,
                filter: (&request.filter).into(),
            },
            buf,
        ),
        Message::ActionResults(results) => {
            let records: Vec<ActionRecordWire> = results.results.iter().map(Into::into).collect();
            encode_with(
                kind,
                seq,
                &ActionResultsRef {
                    req: results.req,
                    more: results.more,
                    results: &records,
                },
                buf,
            )
        }
        _ => return None,
    })
}

/// Longest `RequestError` reason the host sends; longer reasons are cut at a character boundary.
pub const MAX_REASON_BYTES: usize = 256;

/// Encodes a [`kind::REQUEST_ERROR`] frame, cutting `reason` to [`MAX_REASON_BYTES`].
///
/// # Errors
///
/// [`MessageError::BufferTooSmall`] if the frame does not fit in `buf`.
pub fn encode_request_error(
    req: u32,
    code: ErrorCode,
    reason: &str,
    seq: u16,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    let mut end = reason.len().min(MAX_REASON_BYTES);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    #[derive(Serialize)]
    struct Ref<'a> {
        req: u32,
        code: ErrorCode,
        reason: &'a str,
    }
    super::codec::encode_with(
        kind::REQUEST_ERROR,
        seq,
        &Ref {
            req,
            code,
            reason: reason.get(..end).unwrap_or_default(),
        },
        buf,
    )
}

// --- Splitting answers into frames --------------------------------------------------------------

/// An answer that cannot be split to fit the frame size: one record alone is larger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordTooLarge {
    /// The record's encoded length.
    pub record_len: usize,
    /// The largest record a part of this frame size holds.
    pub max_record: usize,
}

#[cfg(feature = "std")]
pub(crate) use parts::{ActionParts, StatusParts, pack};

/// Host side: answers split to the frame size.
#[cfg(feature = "std")]
mod parts;

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::parts::pack;
    use super::*;

    #[test]
    fn records_pack_into_parts_within_the_budget() {
        assert_eq!(pack(&[], 10).unwrap(), alloc::vec![0..0]);
        assert_eq!(pack(&[4, 4, 4], 10).unwrap(), alloc::vec![0..2, 2..3]);
        assert_eq!(pack(&[10, 1], 10).unwrap(), alloc::vec![0..1, 1..2]);
        assert_eq!(
            pack(&[3, 11], 10),
            Err(RecordTooLarge {
                record_len: 11,
                max_record: 10
            })
        );
    }
}
