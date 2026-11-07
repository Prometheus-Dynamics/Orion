//! The executor role and record reads (interface revision 3; `docs/design/stable-interface.md`,
//! "Step 5 wire").
//!
//! The public messages carry the `orion-control-plane` records; the wire is the link's own mirror
//! types below, as for the other kinds. A record field the wire does not carry is named in its
//! conversion, so a record that gains a field fails to compile here until that is decided.

use alloc::string::String;
use alloc::vec::Vec;
#[cfg(feature = "std")]
use core::ops::Range;

use orion_control_plane::{
    DesiredState, ExecutorRecord, ProviderRecord, RemoteBinding, ResourceBinding, ResourceRecord,
    RestartPolicy, WorkloadConfig, WorkloadObservedState, WorkloadRecord,
};
use orion_core::{
    ArtifactId, ConfigSchemaId, ExecutorId, NodeId, ResourceId, RuntimeType, WorkloadId,
};
use serde::{Deserialize, Deserializer, Serialize};

#[cfg(feature = "std")]
use super::RecordTooLarge;
use super::body::{ProviderWire, ResourceWire, ValueWire};
use super::client::{map_from_wire, map_to_wire};
use super::codec::{bounded_vec, encode_with};
use super::{MessageError, kind};
#[cfg(feature = "std")]
use crate::frame::FRAME_OVERHEAD;

/// An id string from the peer: never blank (the `orion-core` id constructors panic on a blank
/// value). Encoded as a plain string.
#[derive(Clone, Serialize)]
#[serde(transparent)]
struct Id(String);

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        super::codec::id(deserializer).map(Self)
    }
}

impl Id {
    fn of(value: &str) -> Self {
        Self(value.into())
    }
}

// --- Public messages ----------------------------------------------------------------------------

/// The observed state of one workload an executor runs, in an [`ExecutorState`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedWorkload {
    /// The workload.
    pub workload_id: WorkloadId,
    /// Its state on the executor.
    pub state: WorkloadObservedState,
}

/// An executor's state. Executor → host, kind [`kind::EXECUTOR_STATE`] (revision 3).
/// Acknowledged with `Ack`, like provider state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorState {
    /// The executor record. The gateway owns `node_id` and overwrites it.
    pub executor: ExecutorRecord,
    /// The workloads the executor runs, with their observed state.
    pub workloads: Vec<ObservedWorkload>,
    /// Resources the executor realizes.
    pub resources: Vec<ResourceRecord>,
}

/// Workloads assigned to the session's executor. Host → executor, kind [`kind::WORKLOADS`]
/// (revision 3).
///
/// A workload's requirements and placement are not carried: the records decode with none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workloads {
    /// The whole assigned set (all parts), not changes.
    pub bootstrap: bool,
    /// More parts of this answer follow.
    pub more: bool,
    /// New or changed workloads.
    pub workloads: Vec<WorkloadRecord>,
    /// Workloads no longer assigned.
    pub removed: Vec<WorkloadId>,
}

/// Which kind of record a [`RecordFilter`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecordKind {
    /// Workloads.
    Workload,
    /// Resources.
    Resource,
    /// Providers.
    Provider,
    /// Executors.
    Executor,
}

/// The records a [`RecordRequest`] reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordFilter {
    /// The kind of record.
    pub kind: RecordKind,
    /// Only the record with this id.
    pub id: Option<String>,
    /// Only records of this node: a workload's assigned node, a provider's or executor's node,
    /// the node of a resource's provider.
    pub node: Option<String>,
}

impl RecordFilter {
    /// Every record of `kind`.
    #[must_use]
    pub fn all(kind: RecordKind) -> Self {
        Self {
            kind,
            id: None,
            node: None,
        }
    }
}

/// A record query or watch. Client → host, kinds [`kind::RECORD_QUERY`] and
/// [`kind::RECORD_WATCH`] (revision 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordRequest {
    /// Request id, echoed by every answer.
    pub req: u32,
    /// Which records.
    pub filter: RecordFilter,
}

/// One record in a [`Records`] answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    /// A workload (without requirements and placement, as in [`Workloads`]).
    Workload(WorkloadRecord),
    /// A resource.
    Resource(ResourceRecord),
    /// A provider.
    Provider(ProviderRecord),
    /// An executor.
    Executor(ExecutorRecord),
}

impl Record {
    /// The record's key.
    #[must_use]
    pub fn key(&self) -> RecordKey {
        let (kind, id) = match self {
            Self::Workload(record) => (RecordKind::Workload, record.workload_id.as_str()),
            Self::Resource(record) => (RecordKind::Resource, record.resource_id.as_str()),
            Self::Provider(record) => (RecordKind::Provider, record.provider_id.as_str()),
            Self::Executor(record) => (RecordKind::Executor, record.executor_id.as_str()),
        };
        RecordKey {
            kind,
            id: id.into(),
        }
    }
}

/// A record gone from a watch's set, in [`Records::removed`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordKey {
    /// The record's kind.
    pub kind: RecordKind,
    /// The record's id.
    pub id: String,
}

/// An answer to a record query or watch. Host → client, kind [`kind::RECORDS`] (revision 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Records {
    /// The request answered.
    pub req: u32,
    /// The whole current set (all parts), not changes.
    pub bootstrap: bool,
    /// More parts of this answer follow.
    pub more: bool,
    /// New or changed records.
    pub updated: Vec<Record>,
    /// Records gone from the set.
    pub removed: Vec<RecordKey>,
}

// --- Wire mirrors -------------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct ExecutorWire {
    executor_id: Id,
    node_id: Id,
    #[serde(deserialize_with = "bounded_vec")]
    runtime_types: Vec<Id>,
}

impl From<&ExecutorRecord> for ExecutorWire {
    fn from(record: &ExecutorRecord) -> Self {
        let ExecutorRecord {
            executor_id,
            node_id,
            runtime_types,
        } = record;
        Self {
            executor_id: Id::of(executor_id.as_str()),
            node_id: Id::of(node_id.as_str()),
            runtime_types: runtime_types.iter().map(|t| Id::of(t.as_str())).collect(),
        }
    }
}

impl From<ExecutorWire> for ExecutorRecord {
    fn from(wire: ExecutorWire) -> Self {
        Self {
            executor_id: ExecutorId::new(wire.executor_id.0),
            node_id: NodeId::new(wire.node_id.0),
            runtime_types: wire
                .runtime_types
                .into_iter()
                .map(|t| RuntimeType::new(t.0))
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum ObservedWire {
    Pending,
    Assigned,
    Starting,
    Running,
    Stopped,
    Completed,
    Failed,
}

impl From<WorkloadObservedState> for ObservedWire {
    fn from(state: WorkloadObservedState) -> Self {
        match state {
            WorkloadObservedState::Pending => Self::Pending,
            WorkloadObservedState::Assigned => Self::Assigned,
            WorkloadObservedState::Starting => Self::Starting,
            WorkloadObservedState::Running => Self::Running,
            WorkloadObservedState::Stopped => Self::Stopped,
            WorkloadObservedState::Completed => Self::Completed,
            WorkloadObservedState::Failed => Self::Failed,
        }
    }
}

impl From<ObservedWire> for WorkloadObservedState {
    fn from(wire: ObservedWire) -> Self {
        match wire {
            ObservedWire::Pending => Self::Pending,
            ObservedWire::Assigned => Self::Assigned,
            ObservedWire::Starting => Self::Starting,
            ObservedWire::Running => Self::Running,
            ObservedWire::Stopped => Self::Stopped,
            ObservedWire::Completed => Self::Completed,
            ObservedWire::Failed => Self::Failed,
        }
    }
}

#[derive(Serialize, Deserialize)]
enum DesiredWire {
    Running,
    Stopped,
}

#[derive(Serialize, Deserialize)]
enum RestartWire {
    Never,
    OnFailure,
    Always,
}

#[derive(Serialize, Deserialize)]
struct ConfigWire {
    schema_id: Id,
    #[serde(deserialize_with = "bounded_vec")]
    payload: Vec<(String, ValueWire)>,
}

#[derive(Serialize, Deserialize)]
struct RemoteWire {
    #[serde(deserialize_with = "bounded_vec")]
    endpoints: Vec<String>,
    available: bool,
}

#[derive(Serialize, Deserialize)]
struct BindingWire {
    resource_id: Id,
    node_id: Id,
    remote: Option<RemoteWire>,
}

#[derive(Serialize, Deserialize)]
struct WorkloadWire {
    workload_id: Id,
    runtime_type: Id,
    artifact_id: Id,
    config: Option<ConfigWire>,
    desired: DesiredWire,
    observed: ObservedWire,
    assigned_node: Option<Id>,
    #[serde(deserialize_with = "bounded_vec")]
    bindings: Vec<BindingWire>,
    restart: RestartWire,
}

impl From<&WorkloadRecord> for WorkloadWire {
    fn from(record: &WorkloadRecord) -> Self {
        let WorkloadRecord {
            workload_id,
            runtime_type,
            artifact_id,
            config,
            desired_state,
            observed_state,
            assigned_node_id,
            // The node's inputs for choosing where the workload runs; not part of the interface.
            requirements: _,
            resource_bindings,
            restart_policy,
            placement: _,
        } = record;
        Self {
            workload_id: Id::of(workload_id.as_str()),
            runtime_type: Id::of(runtime_type.as_str()),
            artifact_id: Id::of(artifact_id.as_str()),
            config: config.as_ref().map(|config| {
                let WorkloadConfig { schema_id, payload } = config;
                ConfigWire {
                    schema_id: Id::of(schema_id.as_str()),
                    payload: map_to_wire(payload),
                }
            }),
            desired: match desired_state {
                DesiredState::Running => DesiredWire::Running,
                DesiredState::Stopped => DesiredWire::Stopped,
            },
            observed: (*observed_state).into(),
            assigned_node: assigned_node_id.as_ref().map(|node| Id::of(node.as_str())),
            bindings: resource_bindings
                .iter()
                .map(|binding| {
                    let ResourceBinding {
                        resource_id,
                        node_id,
                        remote,
                    } = binding;
                    BindingWire {
                        resource_id: Id::of(resource_id.as_str()),
                        node_id: Id::of(node_id.as_str()),
                        remote: remote.as_ref().map(|remote| {
                            let RemoteBinding {
                                endpoints,
                                available,
                            } = remote;
                            RemoteWire {
                                endpoints: endpoints.clone(),
                                available: *available,
                            }
                        }),
                    }
                })
                .collect(),
            restart: match restart_policy {
                RestartPolicy::Never => RestartWire::Never,
                RestartPolicy::OnFailure => RestartWire::OnFailure,
                RestartPolicy::Always => RestartWire::Always,
            },
        }
    }
}

impl From<WorkloadWire> for WorkloadRecord {
    fn from(wire: WorkloadWire) -> Self {
        Self {
            workload_id: WorkloadId::new(wire.workload_id.0),
            runtime_type: RuntimeType::new(wire.runtime_type.0),
            artifact_id: ArtifactId::new(wire.artifact_id.0),
            config: wire.config.map(|config| WorkloadConfig {
                schema_id: ConfigSchemaId::new(config.schema_id.0),
                payload: map_from_wire(config.payload),
            }),
            desired_state: match wire.desired {
                DesiredWire::Running => DesiredState::Running,
                DesiredWire::Stopped => DesiredState::Stopped,
            },
            observed_state: wire.observed.into(),
            assigned_node_id: wire.assigned_node.map(|node| NodeId::new(node.0)),
            requirements: Vec::new(),
            resource_bindings: wire
                .bindings
                .into_iter()
                .map(|binding| ResourceBinding {
                    resource_id: ResourceId::new(binding.resource_id.0),
                    node_id: NodeId::new(binding.node_id.0),
                    remote: binding.remote.map(|remote| RemoteBinding {
                        endpoints: remote.endpoints,
                        available: remote.available,
                    }),
                })
                .collect(),
            restart_policy: match wire.restart {
                RestartWire::Never => RestartPolicy::Never,
                RestartWire::OnFailure => RestartPolicy::OnFailure,
                RestartWire::Always => RestartPolicy::Always,
            },
            placement: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy)]
enum RecordKindWire {
    Workload,
    Resource,
    Provider,
    Executor,
}

impl From<RecordKind> for RecordKindWire {
    fn from(kind: RecordKind) -> Self {
        match kind {
            RecordKind::Workload => Self::Workload,
            RecordKind::Resource => Self::Resource,
            RecordKind::Provider => Self::Provider,
            RecordKind::Executor => Self::Executor,
        }
    }
}

impl From<RecordKindWire> for RecordKind {
    fn from(wire: RecordKindWire) -> Self {
        match wire {
            RecordKindWire::Workload => Self::Workload,
            RecordKindWire::Resource => Self::Resource,
            RecordKindWire::Provider => Self::Provider,
            RecordKindWire::Executor => Self::Executor,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RecordFilterWire {
    kind: RecordKindWire,
    id: Option<String>,
    node: Option<String>,
}

#[derive(Serialize, Deserialize)]
enum RecordWire {
    Workload(WorkloadWire),
    Resource(alloc::boxed::Box<ResourceWire>),
    Provider(ProviderWire),
    Executor(ExecutorWire),
}

impl From<&Record> for RecordWire {
    fn from(record: &Record) -> Self {
        match record {
            Record::Workload(record) => Self::Workload(record.into()),
            Record::Resource(record) => Self::Resource(alloc::boxed::Box::new(record.into())),
            Record::Provider(record) => Self::Provider(record.into()),
            Record::Executor(record) => Self::Executor(record.into()),
        }
    }
}

impl From<RecordWire> for Record {
    fn from(wire: RecordWire) -> Self {
        match wire {
            RecordWire::Workload(wire) => Self::Workload(wire.into()),
            RecordWire::Resource(wire) => Self::Resource((*wire).into()),
            RecordWire::Provider(wire) => Self::Provider(wire.into()),
            RecordWire::Executor(wire) => Self::Executor(wire.into()),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RecordKeyWire {
    kind: RecordKindWire,
    id: Id,
}

// --- Bodies -------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct ObservedBody {
    workload_id: Id,
    state: ObservedWire,
}

#[derive(Serialize, Deserialize)]
struct ExecutorStateBody {
    executor: ExecutorWire,
    #[serde(deserialize_with = "bounded_vec")]
    workloads: Vec<ObservedBody>,
    #[serde(deserialize_with = "bounded_vec")]
    resources: Vec<ResourceWire>,
}

#[derive(Serialize)]
struct WorkloadsRef<'a> {
    bootstrap: bool,
    more: bool,
    workloads: &'a [WorkloadWire],
    removed: &'a [Id],
}

#[derive(Deserialize)]
struct WorkloadsBody {
    bootstrap: bool,
    more: bool,
    #[serde(deserialize_with = "bounded_vec")]
    workloads: Vec<WorkloadWire>,
    #[serde(deserialize_with = "bounded_vec")]
    removed: Vec<Id>,
}

#[derive(Serialize, Deserialize)]
struct RecordRequestBody {
    req: u32,
    filter: RecordFilterWire,
}

#[derive(Serialize)]
struct RecordsRef<'a> {
    req: u32,
    bootstrap: bool,
    more: bool,
    updated: &'a [RecordWire],
    removed: &'a [RecordKeyWire],
}

#[derive(Deserialize)]
struct RecordsBody {
    req: u32,
    bootstrap: bool,
    more: bool,
    #[serde(deserialize_with = "bounded_vec")]
    updated: Vec<RecordWire>,
    #[serde(deserialize_with = "bounded_vec")]
    removed: Vec<RecordKeyWire>,
}

fn executor_state_body(state: &ExecutorState) -> ExecutorStateBody {
    ExecutorStateBody {
        executor: (&state.executor).into(),
        workloads: state
            .workloads
            .iter()
            .map(|workload| ObservedBody {
                workload_id: Id::of(workload.workload_id.as_str()),
                state: workload.state.into(),
            })
            .collect(),
        resources: state.resources.iter().map(Into::into).collect(),
    }
}

fn record_key_wire(key: &RecordKey) -> RecordKeyWire {
    RecordKeyWire {
        kind: key.kind.into(),
        id: Id::of(&key.id),
    }
}

fn decode<'a, T: Deserialize<'a>>(kind: u8, payload: &'a [u8]) -> Result<T, MessageError> {
    super::codec::decode_body(kind, payload)
}

/// The message carried by a frame of `kind`, if `kind` is a kind of this module.
pub(super) fn decode_records(
    kind: u8,
    payload: &[u8],
) -> Option<Result<super::Message, MessageError>> {
    use super::Message;
    Some(match kind {
        kind::EXECUTOR_STATE => decode::<ExecutorStateBody>(kind, payload).map(|body| {
            Message::ExecutorState(alloc::boxed::Box::new(ExecutorState {
                executor: body.executor.into(),
                workloads: body
                    .workloads
                    .into_iter()
                    .map(|workload| ObservedWorkload {
                        workload_id: WorkloadId::new(workload.workload_id.0),
                        state: workload.state.into(),
                    })
                    .collect(),
                resources: body.resources.into_iter().map(Into::into).collect(),
            }))
        }),
        kind::WORKLOADS => decode::<WorkloadsBody>(kind, payload).map(|body| {
            Message::Workloads(Workloads {
                bootstrap: body.bootstrap,
                more: body.more,
                workloads: body.workloads.into_iter().map(Into::into).collect(),
                removed: body
                    .removed
                    .into_iter()
                    .map(|id| WorkloadId::new(id.0))
                    .collect(),
            })
        }),
        kind::RECORD_QUERY | kind::RECORD_WATCH => {
            decode::<RecordRequestBody>(kind, payload).map(|body| {
                let request = RecordRequest {
                    req: body.req,
                    filter: RecordFilter {
                        kind: body.filter.kind.into(),
                        id: body.filter.id,
                        node: body.filter.node,
                    },
                };
                if kind == kind::RECORD_QUERY {
                    Message::RecordQuery(request)
                } else {
                    Message::RecordWatch(request)
                }
            })
        }
        kind::RECORDS => decode::<RecordsBody>(kind, payload).map(|body| {
            Message::Records(Records {
                req: body.req,
                bootstrap: body.bootstrap,
                more: body.more,
                updated: body.updated.into_iter().map(Into::into).collect(),
                removed: body
                    .removed
                    .into_iter()
                    .map(|key| RecordKey {
                        kind: key.kind.into(),
                        id: key.id.0,
                    })
                    .collect(),
            })
        }),
        _ => return None,
    })
}

/// Encodes a message of this module's kinds into `buf`.
pub(super) fn encode_records(
    message: &super::Message,
    seq: u16,
    buf: &mut [u8],
) -> Option<Result<usize, MessageError>> {
    use super::Message;
    let kind = message.kind();
    Some(match message {
        Message::ExecutorState(state) => encode_with(kind, seq, &executor_state_body(state), buf),
        Message::Workloads(workloads) => {
            let records: Vec<WorkloadWire> = workloads.workloads.iter().map(Into::into).collect();
            let removed: Vec<Id> = workloads
                .removed
                .iter()
                .map(|id| Id::of(id.as_str()))
                .collect();
            encode_with(
                kind,
                seq,
                &WorkloadsRef {
                    bootstrap: workloads.bootstrap,
                    more: workloads.more,
                    workloads: &records,
                    removed: &removed,
                },
                buf,
            )
        }
        Message::RecordQuery(request) | Message::RecordWatch(request) => encode_with(
            kind,
            seq,
            &RecordRequestBody {
                req: request.req,
                filter: RecordFilterWire {
                    kind: request.filter.kind.into(),
                    id: request.filter.id.clone(),
                    node: request.filter.node.clone(),
                },
            },
            buf,
        ),
        Message::Records(records) => {
            let updated: Vec<RecordWire> = records.updated.iter().map(Into::into).collect();
            let removed: Vec<RecordKeyWire> = records.removed.iter().map(record_key_wire).collect();
            encode_with(
                kind,
                seq,
                &RecordsRef {
                    req: records.req,
                    bootstrap: records.bootstrap,
                    more: records.more,
                    updated: &updated,
                    removed: &removed,
                },
                buf,
            )
        }
        _ => return None,
    })
}

/// Encodes an [`ExecutorState`] frame (an executor's side, without building a [`super::Message`]).
///
/// # Errors
///
/// [`MessageError::BufferTooSmall`] if the frame does not fit in `buf`.
pub fn encode_executor_state(
    state: &ExecutorState,
    seq: u16,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    encode_with(kind::EXECUTOR_STATE, seq, &executor_state_body(state), buf)
}

// --- Parts (host side) --------------------------------------------------------------------------

#[cfg(feature = "std")]
/// Upper bound of a part's body without its records: the request id (5), two flags, and two
/// sequence lengths (5 each).
const PART_HEADER: usize = 17;

#[cfg(feature = "std")]
fn budget(max_frame: usize) -> usize {
    max_frame.saturating_sub(FRAME_OVERHEAD + PART_HEADER)
}

#[cfg(feature = "std")]
/// Ranges over `updated` then `removed`, as one sequence, that each fit a frame.
fn pack_both<U: Serialize, R: Serialize>(
    updated: &[U],
    removed: &[R],
    max_frame: usize,
) -> Result<Vec<Range<usize>>, RecordTooLarge> {
    let mut scratch = alloc::vec![0u8; max_frame.max(FRAME_OVERHEAD)];
    let mut lens: Vec<usize> = updated
        .iter()
        .map(|record| encoded_len(record, &mut scratch))
        .collect();
    lens.extend(removed.iter().map(|key| encoded_len(key, &mut scratch)));
    super::client_pack(&lens, budget(max_frame))
}

#[cfg(feature = "std")]
/// The slices of `updated` and `removed` in `range` over both as one sequence.
fn split<'a, U, R>(updated: &'a [U], removed: &'a [R], range: Range<usize>) -> (&'a [U], &'a [R]) {
    let at = updated.len();
    (
        updated
            .get(range.start.min(at)..range.end.min(at))
            .unwrap_or_default(),
        removed
            .get(range.start.saturating_sub(at)..range.end.saturating_sub(at))
            .unwrap_or_default(),
    )
}

#[cfg(feature = "std")]
fn encoded_len<T: Serialize>(value: &T, scratch: &mut [u8]) -> usize {
    postcard::to_slice(value, scratch).map_or(usize::MAX, |used| used.len())
}

#[cfg(feature = "std")]
/// A [`Workloads`] answer split into frames: call [`WorkloadParts::encode`] per part.
pub(crate) struct WorkloadParts {
    bootstrap: bool,
    workloads: Vec<WorkloadWire>,
    removed: Vec<Id>,
    parts: Vec<Range<usize>>,
}

#[cfg(feature = "std")]
impl WorkloadParts {
    pub(crate) fn new(
        bootstrap: bool,
        workloads: &[WorkloadRecord],
        removed: &[WorkloadId],
        max_frame: usize,
    ) -> Result<Self, RecordTooLarge> {
        let workloads: Vec<WorkloadWire> = workloads.iter().map(Into::into).collect();
        let removed: Vec<Id> = removed.iter().map(|id| Id::of(id.as_str())).collect();
        let parts = pack_both(&workloads, &removed, max_frame)?;
        Ok(Self {
            bootstrap,
            workloads,
            removed,
            parts,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.parts.len()
    }

    pub(crate) fn encode(
        &self,
        part: usize,
        seq: u16,
        buf: &mut [u8],
    ) -> Result<usize, MessageError> {
        let range = self.parts.get(part).cloned().unwrap_or(0..0);
        let (workloads, removed) = split(&self.workloads, &self.removed, range);
        encode_with(
            kind::WORKLOADS,
            seq,
            &WorkloadsRef {
                bootstrap: self.bootstrap,
                more: part + 1 < self.parts.len(),
                workloads,
                removed,
            },
            buf,
        )
    }
}

#[cfg(feature = "std")]
/// A [`Records`] answer split into frames; see [`WorkloadParts`].
pub(crate) struct RecordParts {
    req: u32,
    bootstrap: bool,
    updated: Vec<RecordWire>,
    removed: Vec<RecordKeyWire>,
    parts: Vec<Range<usize>>,
}

#[cfg(feature = "std")]
impl RecordParts {
    pub(crate) fn new(
        req: u32,
        bootstrap: bool,
        updated: &[Record],
        removed: &[RecordKey],
        max_frame: usize,
    ) -> Result<Self, RecordTooLarge> {
        let updated: Vec<RecordWire> = updated.iter().map(Into::into).collect();
        let removed: Vec<RecordKeyWire> = removed.iter().map(record_key_wire).collect();
        let parts = pack_both(&updated, &removed, max_frame)?;
        Ok(Self {
            req,
            bootstrap,
            updated,
            removed,
            parts,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.parts.len()
    }

    pub(crate) fn encode(
        &self,
        part: usize,
        seq: u16,
        buf: &mut [u8],
    ) -> Result<usize, MessageError> {
        let range = self.parts.get(part).cloned().unwrap_or(0..0);
        let (updated, removed) = split(&self.updated, &self.removed, range);
        encode_with(
            kind::RECORDS,
            seq,
            &RecordsRef {
                req: self.req,
                bootstrap: self.bootstrap,
                more: part + 1 < self.parts.len(),
                updated,
                removed,
            },
            buf,
        )
    }
}
