//! Stream messages (interface revision 2): providers declare and publish streams, clients
//! subscribe to them (`docs/design/stable-interface.md`, "Step 4 wire").
//!
//! The public messages carry the `orion-control-plane` stream types; the wire is the link's own
//! mirror types below. Sample chunks keep the node's encoding: their `data` is copied as it is, in
//! both directions.

use alloc::string::String;
use alloc::vec::Vec;

use orion_control_plane as cp;
use serde::{Deserialize, Serialize};

use super::client::SubjectWire;
use super::codec::bounded_vec;
use super::{MessageError, kind};

/// Declares a stream of the session's provider. Provider → host, kind [`kind::STREAM_DECLARE`].
///
/// Answered by a [`StreamDemand`] when accepted, or a `RequestError` echoing `req`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDeclare {
    /// Request id, echoed by a refusal.
    pub req: u32,
    /// The stream's id, unique for the provider.
    pub id: String,
    /// The series, in the order chunks name them by index.
    pub series: Vec<cp::SeriesDecl>,
}

/// A batch of samples of a declared stream. Provider → host, kind [`kind::STREAM_PUBLISH`]; not
/// answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPublish {
    /// The stream's id.
    pub id: String,
    /// One chunk per series sampled.
    pub chunks: Vec<cp::SampleChunk>,
}

/// Which series of a declared stream someone subscribes to now. Host → provider, kind
/// [`kind::STREAM_DEMAND`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDemand {
    /// The stream's id.
    pub id: String,
    /// Subscribers of the stream.
    pub subscribers: u32,
    /// Indexes of the series with at least one subscriber.
    pub series: Vec<u16>,
}

/// Subscribes to a stream. Client → host, kind [`kind::STREAM_SUBSCRIBE`]; ended with
/// `RequestEnd { req }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSubscribe {
    /// Client-chosen request id, echoed in the answers.
    pub req: u32,
    /// The stream, series, rate and backfill.
    pub subscription: cp::StreamSubscription,
}

/// The stream's series, first in a subscription's answers and again after a reset. Host →
/// client, kind [`kind::STREAM_CATALOG`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCatalog {
    /// The subscription.
    pub req: u32,
    /// The publisher's epoch.
    pub epoch: u64,
    /// The series, in index order.
    pub series: Vec<cp::SeriesDecl>,
    /// `false` while the stream has no publisher (its history is still served).
    pub live: bool,
}

/// One part of a subscription's samples. Host → client, kind [`kind::STREAM_DATA`].
///
/// Data larger than a frame is sent in parts by chunk: every part but the last has `more` set, and
/// all carry the same cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamData {
    /// The subscription.
    pub req: u32,
    /// The newest batch included.
    pub cursor: cp::StreamCursor,
    /// The batch the subscriber got before these (0 before the first).
    pub prev_seq: u64,
    /// The backfill, not live batches.
    pub backfill: bool,
    /// More parts follow.
    pub more: bool,
    /// The samples.
    pub chunks: Vec<cp::SampleChunk>,
}

/// The subscription's position no longer holds. Host → client, kind [`kind::STREAM_RESET`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamReset {
    /// The subscription.
    pub req: u32,
    /// Why.
    pub reason: cp::StreamResetReason,
}

// --- Wire mirrors -------------------------------------------------------------------------------

#[derive(Clone, Copy, Serialize, Deserialize)]
enum SeriesKindWire {
    F32,
    F64,
    I64,
    U32,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
enum SamplingWire {
    Always,
    WhileSubscribed,
}

#[derive(Serialize, Deserialize)]
struct HistoryWire {
    full_ms: u32,
    reduced_ms: u32,
    reduced_period_ms: u32,
}

#[derive(Serialize, Deserialize)]
pub(super) struct SeriesWire {
    name: String,
    unit: Option<String>,
    kind: SeriesKindWire,
    period_us: u32,
    history: HistoryWire,
    sampling: SamplingWire,
}

impl From<&cp::SeriesDecl> for SeriesWire {
    fn from(decl: &cp::SeriesDecl) -> Self {
        let cp::SeriesDecl {
            name,
            unit,
            kind,
            period_us,
            history,
            sampling,
        } = decl;
        let cp::HistoryPolicy {
            full_ms,
            reduced_ms,
            reduced_period_ms,
        } = *history;
        Self {
            name: name.clone(),
            unit: unit.clone(),
            kind: match kind {
                cp::SeriesKind::F32 => SeriesKindWire::F32,
                cp::SeriesKind::F64 => SeriesKindWire::F64,
                cp::SeriesKind::I64 => SeriesKindWire::I64,
                cp::SeriesKind::U32 => SeriesKindWire::U32,
            },
            period_us: *period_us,
            history: HistoryWire {
                full_ms,
                reduced_ms,
                reduced_period_ms,
            },
            sampling: match sampling {
                cp::Sampling::Always => SamplingWire::Always,
                cp::Sampling::WhileSubscribed => SamplingWire::WhileSubscribed,
            },
        }
    }
}

impl From<SeriesWire> for cp::SeriesDecl {
    fn from(wire: SeriesWire) -> Self {
        Self {
            name: wire.name,
            unit: wire.unit,
            kind: match wire.kind {
                SeriesKindWire::F32 => cp::SeriesKind::F32,
                SeriesKindWire::F64 => cp::SeriesKind::F64,
                SeriesKindWire::I64 => cp::SeriesKind::I64,
                SeriesKindWire::U32 => cp::SeriesKind::U32,
            },
            period_us: wire.period_us,
            history: cp::HistoryPolicy {
                full_ms: wire.history.full_ms,
                reduced_ms: wire.history.reduced_ms,
                reduced_period_ms: wire.history.reduced_period_ms,
            },
            sampling: match wire.sampling {
                SamplingWire::Always => cp::Sampling::Always,
                SamplingWire::WhileSubscribed => cp::Sampling::WhileSubscribed,
            },
        }
    }
}

/// A sample chunk; `data` is borrowed both ways (postcard writes a byte slice as a length and the
/// bytes, as it writes a `Vec<u8>`), so it is copied once.
#[derive(Serialize, Deserialize)]
pub(super) struct ChunkWire<'a> {
    series: u16,
    t0_us: u64,
    count: u32,
    #[serde(borrow)]
    data: &'a [u8],
}

impl<'a> From<&'a cp::SampleChunk> for ChunkWire<'a> {
    fn from(chunk: &'a cp::SampleChunk) -> Self {
        let cp::SampleChunk {
            series,
            t0_us,
            count,
            data,
        } = chunk;
        Self {
            series: *series,
            t0_us: *t0_us,
            count: *count,
            data,
        }
    }
}

impl From<ChunkWire<'_>> for cp::SampleChunk {
    fn from(wire: ChunkWire<'_>) -> Self {
        Self {
            series: wire.series,
            t0_us: wire.t0_us,
            count: wire.count,
            data: wire.data.to_vec(),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum BackfillWire {
    None,
    LastMs(u64),
    Since { epoch: u64, seq: u64 },
}

#[derive(Clone, Copy, Serialize, Deserialize)]
enum ResetReasonWire {
    NewEpoch,
    OutOfRange,
    Overrun,
    Removed,
}

impl From<cp::StreamResetReason> for ResetReasonWire {
    fn from(reason: cp::StreamResetReason) -> Self {
        match reason {
            cp::StreamResetReason::NewEpoch => Self::NewEpoch,
            cp::StreamResetReason::OutOfRange => Self::OutOfRange,
            cp::StreamResetReason::Overrun => Self::Overrun,
            cp::StreamResetReason::Removed => Self::Removed,
        }
    }
}

impl From<ResetReasonWire> for cp::StreamResetReason {
    fn from(wire: ResetReasonWire) -> Self {
        match wire {
            ResetReasonWire::NewEpoch => Self::NewEpoch,
            ResetReasonWire::OutOfRange => Self::OutOfRange,
            ResetReasonWire::Overrun => Self::Overrun,
            ResetReasonWire::Removed => Self::Removed,
        }
    }
}

// --- Bodies -------------------------------------------------------------------------------------

#[derive(Serialize)]
struct DeclareRef<'a> {
    req: u32,
    id: &'a str,
    series: &'a [SeriesWire],
}

#[derive(Deserialize)]
struct DeclareBody {
    req: u32,
    id: String,
    #[serde(deserialize_with = "bounded_vec")]
    series: Vec<SeriesWire>,
}

#[derive(Serialize)]
pub(super) struct PublishRef<'a> {
    pub(super) id: &'a str,
    pub(super) chunks: &'a [ChunkWire<'a>],
}

#[derive(Deserialize)]
struct PublishBody<'a> {
    id: String,
    #[serde(borrow, deserialize_with = "bounded_vec")]
    chunks: Vec<ChunkWire<'a>>,
}

#[derive(Serialize)]
struct DemandRef<'a> {
    id: &'a str,
    subscribers: u32,
    series: &'a [u16],
}

#[derive(Deserialize)]
struct DemandBody {
    id: String,
    subscribers: u32,
    #[serde(deserialize_with = "bounded_vec")]
    series: Vec<u16>,
}

#[derive(Serialize, Deserialize)]
struct SubscribeBody {
    req: u32,
    owner: SubjectWire,
    id: String,
    #[serde(deserialize_with = "bounded_vec")]
    series: Vec<String>,
    min_interval_us: u32,
    backfill: BackfillWire,
}

#[derive(Serialize)]
struct CatalogRef<'a> {
    req: u32,
    epoch: u64,
    series: &'a [SeriesWire],
    live: bool,
}

#[derive(Deserialize)]
struct CatalogBody {
    req: u32,
    epoch: u64,
    #[serde(deserialize_with = "bounded_vec")]
    series: Vec<SeriesWire>,
    live: bool,
}

#[derive(Serialize)]
pub(super) struct DataRef<'a> {
    pub(super) req: u32,
    pub(super) epoch: u64,
    pub(super) seq: u64,
    pub(super) prev_seq: u64,
    pub(super) backfill: bool,
    pub(super) more: bool,
    pub(super) chunks: &'a [ChunkWire<'a>],
}

#[derive(Deserialize)]
struct DataBody<'a> {
    req: u32,
    epoch: u64,
    seq: u64,
    prev_seq: u64,
    backfill: bool,
    more: bool,
    #[serde(borrow, deserialize_with = "bounded_vec")]
    chunks: Vec<ChunkWire<'a>>,
}

#[derive(Serialize, Deserialize)]
struct ResetBody {
    req: u32,
    reason: ResetReasonWire,
}

fn decode<'a, T: Deserialize<'a>>(kind: u8, payload: &'a [u8]) -> Result<T, MessageError> {
    super::codec::decode_body(kind, payload)
}

/// The stream message carried by a frame of `kind`, if `kind` is a stream kind.
pub(super) fn decode_stream(
    kind: u8,
    payload: &[u8],
) -> Option<Result<super::Message, MessageError>> {
    use super::Message;
    let message = match kind {
        kind::STREAM_DECLARE => decode::<DeclareBody>(kind, payload).map(|body| {
            Message::StreamDeclare(StreamDeclare {
                req: body.req,
                id: body.id,
                series: body.series.into_iter().map(Into::into).collect(),
            })
        }),
        kind::STREAM_PUBLISH => decode::<PublishBody<'_>>(kind, payload).map(|body| {
            Message::StreamPublish(StreamPublish {
                id: body.id,
                chunks: body.chunks.into_iter().map(Into::into).collect(),
            })
        }),
        kind::STREAM_DEMAND => decode::<DemandBody>(kind, payload).map(|body| {
            Message::StreamDemand(StreamDemand {
                id: body.id,
                subscribers: body.subscribers,
                series: body.series,
            })
        }),
        kind::STREAM_SUBSCRIBE => decode::<SubscribeBody>(kind, payload).map(|body| {
            Message::StreamSubscribe(alloc::boxed::Box::new(StreamSubscribe {
                req: body.req,
                subscription: cp::StreamSubscription {
                    stream: cp::StreamRef {
                        owner: body.owner.into(),
                        id: body.id,
                    },
                    series: body.series,
                    min_interval_us: body.min_interval_us,
                    backfill: match body.backfill {
                        BackfillWire::None => cp::Backfill::None,
                        BackfillWire::LastMs(ms) => cp::Backfill::LastMs(ms),
                        BackfillWire::Since { epoch, seq } => {
                            cp::Backfill::Since(cp::StreamCursor { epoch, seq })
                        }
                    },
                },
            }))
        }),
        kind::STREAM_CATALOG => decode::<CatalogBody>(kind, payload).map(|body| {
            Message::StreamCatalog(StreamCatalog {
                req: body.req,
                epoch: body.epoch,
                series: body.series.into_iter().map(Into::into).collect(),
                live: body.live,
            })
        }),
        kind::STREAM_DATA => decode::<DataBody<'_>>(kind, payload).map(|body| {
            Message::StreamData(StreamData {
                req: body.req,
                cursor: cp::StreamCursor {
                    epoch: body.epoch,
                    seq: body.seq,
                },
                prev_seq: body.prev_seq,
                backfill: body.backfill,
                more: body.more,
                chunks: body.chunks.into_iter().map(Into::into).collect(),
            })
        }),
        kind::STREAM_RESET => decode::<ResetBody>(kind, payload).map(|body| {
            Message::StreamReset(StreamReset {
                req: body.req,
                reason: body.reason.into(),
            })
        }),
        _ => return None,
    };
    Some(message)
}

/// Encodes a stream message into `buf`, if `message` is one.
pub(super) fn encode_stream(
    message: &super::Message,
    seq: u16,
    buf: &mut [u8],
) -> Option<Result<usize, MessageError>> {
    use super::Message;
    use super::codec::encode_with;
    let kind = message.kind();
    Some(match message {
        Message::StreamDeclare(declare) => {
            let series: Vec<SeriesWire> = declare.series.iter().map(Into::into).collect();
            encode_with(
                kind,
                seq,
                &DeclareRef {
                    req: declare.req,
                    id: &declare.id,
                    series: &series,
                },
                buf,
            )
        }
        Message::StreamPublish(publish) => encode_publish(&publish.id, &publish.chunks, seq, buf),
        Message::StreamDemand(demand) => encode_with(
            kind,
            seq,
            &DemandRef {
                id: &demand.id,
                subscribers: demand.subscribers,
                series: &demand.series,
            },
            buf,
        ),
        Message::StreamSubscribe(subscribe) => {
            let cp::StreamSubscription {
                stream,
                series,
                min_interval_us,
                backfill,
            } = &subscribe.subscription;
            encode_with(
                kind,
                seq,
                &SubscribeBody {
                    req: subscribe.req,
                    owner: (&stream.owner).into(),
                    id: stream.id.clone(),
                    series: series.clone(),
                    min_interval_us: *min_interval_us,
                    backfill: match backfill {
                        cp::Backfill::None => BackfillWire::None,
                        cp::Backfill::LastMs(ms) => BackfillWire::LastMs(*ms),
                        cp::Backfill::Since(cursor) => BackfillWire::Since {
                            epoch: cursor.epoch,
                            seq: cursor.seq,
                        },
                    },
                },
                buf,
            )
        }
        Message::StreamCatalog(catalog) => encode_catalog(
            catalog.req,
            catalog.epoch,
            &catalog.series,
            catalog.live,
            seq,
            buf,
        ),
        Message::StreamData(data) => {
            let chunks: Vec<ChunkWire<'_>> = data.chunks.iter().map(Into::into).collect();
            encode_with(
                kind,
                seq,
                &DataRef {
                    req: data.req,
                    epoch: data.cursor.epoch,
                    seq: data.cursor.seq,
                    prev_seq: data.prev_seq,
                    backfill: data.backfill,
                    more: data.more,
                    chunks: &chunks,
                },
                buf,
            )
        }
        Message::StreamReset(reset) => encode_with(
            kind,
            seq,
            &ResetBody {
                req: reset.req,
                reason: reset.reason.into(),
            },
            buf,
        ),
        _ => return None,
    })
}

/// Encodes a [`kind::STREAM_PUBLISH`] frame from borrowed chunks (a publisher's hot path).
///
/// # Errors
///
/// [`MessageError::BufferTooSmall`] if the frame does not fit in `buf`.
pub fn encode_publish(
    id: &str,
    chunks: &[cp::SampleChunk],
    seq: u16,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    let chunks: Vec<ChunkWire<'_>> = chunks.iter().map(Into::into).collect();
    super::codec::encode_with(
        kind::STREAM_PUBLISH,
        seq,
        &PublishRef {
            id,
            chunks: &chunks,
        },
        buf,
    )
}

/// Encodes a [`kind::STREAM_DEMAND`] frame.
///
/// # Errors
///
/// [`MessageError::BufferTooSmall`] if the frame does not fit in `buf`.
pub fn encode_demand(
    id: &str,
    subscribers: u32,
    series: &[u16],
    seq: u16,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    super::codec::encode_with(
        kind::STREAM_DEMAND,
        seq,
        &DemandRef {
            id,
            subscribers,
            series,
        },
        buf,
    )
}

/// Encodes a [`kind::STREAM_CATALOG`] frame.
///
/// # Errors
///
/// [`MessageError::BufferTooSmall`] if the frame does not fit in `buf`.
pub fn encode_catalog(
    req: u32,
    epoch: u64,
    series: &[cp::SeriesDecl],
    live: bool,
    seq: u16,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    let series: Vec<SeriesWire> = series.iter().map(Into::into).collect();
    super::codec::encode_with(
        kind::STREAM_CATALOG,
        seq,
        &CatalogRef {
            req,
            epoch,
            series: &series,
            live,
        },
        buf,
    )
}

/// Encodes a [`kind::STREAM_RESET`] frame.
///
/// # Errors
///
/// [`MessageError::BufferTooSmall`] if the frame does not fit in `buf`.
pub fn encode_reset(
    req: u32,
    reason: cp::StreamResetReason,
    seq: u16,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    super::codec::encode_with(
        kind::STREAM_RESET,
        seq,
        &ResetBody {
            req,
            reason: reason.into(),
        },
        buf,
    )
}

/// Upper bound of a `StreamData` body without its chunks: the request id (5), three cursors (10
/// each), two flags and the sequence length (5).
const DATA_HEADER: usize = 42;

/// The largest chunk `data` one `StreamData` frame of `max_frame` bytes holds: the frame less its
/// header and a chunk's own fields (series 3, t0 10, count 5, data length 5). A sender splits
/// larger chunks first (`SampleChunk::split`).
#[must_use]
pub const fn max_chunk_data(max_frame: usize) -> usize {
    max_frame.saturating_sub(crate::frame::FRAME_OVERHEAD + DATA_HEADER + 23)
}

/// Host side: stream data split into parts that fit the session's frame size.
#[cfg(feature = "std")]
pub(crate) mod parts {
    use super::*;
    use crate::frame::FRAME_OVERHEAD;
    use core::ops::Range;

    use super::DATA_HEADER;

    /// Stream data encoded part by part; see `StatusParts`.
    pub(crate) struct DataParts<'a> {
        req: u32,
        cursor: cp::StreamCursor,
        prev_seq: u64,
        backfill: bool,
        chunks: Vec<ChunkWire<'a>>,
        parts: Vec<Range<usize>>,
    }

    impl<'a> DataParts<'a> {
        /// Splits `data`'s chunks into parts; [`super::super::RecordTooLarge`] if one chunk alone
        /// does not fit a part (callers split chunks to [`max_chunk_data`] first).
        pub(crate) fn new(
            data: &'a crate::message::StreamData,
            max_frame: usize,
        ) -> Result<Self, crate::message::RecordTooLarge> {
            let chunks: Vec<ChunkWire<'a>> = data.chunks.iter().map(Into::into).collect();
            let mut scratch = alloc::vec![0u8; max_frame.max(FRAME_OVERHEAD)];
            let lens: Vec<usize> = chunks
                .iter()
                .map(|chunk| {
                    postcard::to_slice(chunk, &mut scratch).map_or(usize::MAX, |used| used.len())
                })
                .collect();
            let budget = max_frame.saturating_sub(FRAME_OVERHEAD + DATA_HEADER);
            let parts = crate::message::client_pack(&lens, budget)?;
            Ok(Self {
                req: data.req,
                cursor: data.cursor,
                prev_seq: data.prev_seq,
                backfill: data.backfill,
                chunks,
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
            crate::message::codec::encode_with(
                kind::STREAM_DATA,
                seq,
                &DataRef {
                    req: self.req,
                    epoch: self.cursor.epoch,
                    seq: self.cursor.seq,
                    prev_seq: self.prev_seq,
                    backfill: self.backfill,
                    more: part + 1 < self.parts.len(),
                    chunks: self.chunks.get(range).unwrap_or_default(),
                },
                buf,
            )
        }
    }
}
