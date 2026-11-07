//! Borrowed stream bodies (interface revision 2, feature `streams`): a provider's declaration and
//! batches, and the host's demand and refusals. They encode to the bytes postcard produces for the
//! link's stream messages (`message::StreamDeclare`, `StreamPublish`, `StreamDemand`).

use super::codec::{Encode, Reader, Writer};
use super::{WireError, sealed};

/// Most series a device stream declares: the host's demand is kept as a 64-bit mask.
pub const MAX_STREAM_SERIES: usize = 64;

/// A series' sample type, as `orion_control_plane::SeriesKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SeriesKind {
    /// 32-bit float, 4 bytes little-endian per sample.
    F32 = 0,
    /// 64-bit float, 8 bytes little-endian per sample.
    F64 = 1,
    /// Signed integer, a zigzag varint per sample.
    I64 = 2,
    /// Unsigned 32-bit integer, 4 bytes little-endian per sample.
    U32 = 3,
}

/// When the series is sampled, as `orion_control_plane::Sampling`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Sampling {
    /// Always, subscribed or not (the node keeps its history).
    Always = 0,
    /// Only while someone subscribes (see [`StreamDemandView`]).
    WhileSubscribed = 1,
}

/// How much history the node keeps of a series, as `orion_control_plane::HistoryPolicy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryView {
    /// Milliseconds kept at full rate.
    pub full_ms: u32,
    /// Milliseconds kept reduced after that.
    pub reduced_ms: u32,
    /// The reduced history's period.
    pub reduced_period_ms: u32,
}

/// One series of a declared stream, as `orion_control_plane::SeriesDecl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeriesView<'a> {
    /// The series' name, unique in the stream.
    pub name: &'a str,
    /// Its unit, if any.
    pub unit: Option<&'a str>,
    /// The sample type.
    pub kind: SeriesKind,
    /// The nominal sample period.
    pub period_us: u32,
    /// The history the node keeps.
    pub history: HistoryView,
    /// When the device samples it.
    pub sampling: Sampling,
}

impl Encode for SeriesView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.name.encode(w);
        self.unit.encode(w);
        w.varint(self.kind as u64);
        self.period_us.encode(w);
        self.history.full_ms.encode(w);
        self.history.reduced_ms.encode(w);
        self.history.reduced_period_ms.encode(w);
        w.varint(self.sampling as u64);
    }
}

/// Samples of one series, as `orion_control_plane::SampleChunk`: `data` holds `count` samples, each
/// a varint time delta in microseconds from the previous one (0 for the first, which is at `t0_us`)
/// followed by the value in the series' encoding (see [`SeriesKind`]). [`SampleWriter`] writes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkView<'a> {
    /// The series' index in the declaration.
    pub series: u16,
    /// Time of the first sample, microseconds on the device's clock.
    pub t0_us: u64,
    /// Samples in `data`.
    pub count: u32,
    /// The samples.
    pub data: &'a [u8],
}

impl Encode for ChunkView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.series.encode(w);
        self.t0_us.encode(w);
        self.count.encode(w);
        w.bytes(self.data);
    }
}

/// `StreamDeclare { req, id, series }`.
pub(crate) struct StreamDeclareBody<'a> {
    pub(crate) req: u32,
    pub(crate) id: &'a str,
    pub(crate) series: &'a [SeriesView<'a>],
}

impl Encode for StreamDeclareBody<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.req.encode(w);
        self.id.encode(w);
        self.series.encode(w);
    }
}

/// `StreamPublish { id, chunks }`.
pub(crate) struct StreamPublishBody<'a> {
    pub(crate) id: &'a str,
    pub(crate) chunks: &'a [ChunkView<'a>],
}

impl Encode for StreamPublishBody<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.id.encode(w);
        self.chunks.encode(w);
    }
}

impl sealed::Sealed for SeriesView<'_> {}
impl sealed::Sealed for ChunkView<'_> {}

/// Who subscribes to the device's stream now (`StreamDemand`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StreamDemandView {
    /// Subscribers of the stream.
    pub subscribers: u32,
    /// Bit `i` is set when series `i` has at least one subscriber.
    pub series: u64,
}

impl StreamDemandView {
    /// Whether series `index` has a subscriber.
    #[must_use]
    pub const fn wants(&self, index: u16) -> bool {
        index < 64 && self.series & (1 << index) != 0
    }
}

/// Decodes a `StreamDemand` payload: the stream's id and the demand. Series indexes past
/// [`MAX_STREAM_SERIES`] are ignored (a device declares no more).
///
/// # Errors
///
/// [`WireError::Decode`] if malformed.
pub fn decode_stream_demand(payload: &[u8]) -> Result<(&str, StreamDemandView), WireError> {
    let mut r = Reader::new(payload);
    let id = r.str()?;
    let subscribers = r.varint_u32()?;
    let count = r.varint()?;
    let mut series = 0u64;
    for _ in 0..count {
        let index = r.varint_u16()?;
        if index < 64 {
            series |= 1 << index;
        }
    }
    Ok((
        id,
        StreamDemandView {
            subscribers,
            series,
        },
    ))
}

/// Decodes a `RequestError` payload's request id and code (the reason is not kept).
///
/// # Errors
///
/// [`WireError::Decode`] if malformed.
pub fn decode_request_error(payload: &[u8]) -> Result<(u32, u16), WireError> {
    let mut r = Reader::new(payload);
    let req = r.varint_u32()?;
    let code = r.varint_u16()?;
    Ok((req, code))
}

/// Writes one series' samples into a caller's buffer, in a [`ChunkView`]'s encoding.
#[derive(Debug)]
pub struct SampleWriter<'a> {
    buf: &'a mut [u8],
    len: usize,
    count: u32,
    t0_us: u64,
    last_us: u64,
}

impl<'a> SampleWriter<'a> {
    /// An empty chunk over `buf`.
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self {
            buf,
            len: 0,
            count: 0,
            t0_us: 0,
            last_us: 0,
        }
    }

    /// Samples written.
    #[must_use]
    pub const fn count(&self) -> u32 {
        self.count
    }

    /// Bytes written.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether no sample was written.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Forgets the samples written (the buffer is reused).
    pub fn clear(&mut self) {
        self.len = 0;
        self.count = 0;
    }

    /// The chunk of series `series`.
    #[must_use]
    pub fn chunk(&self, series: u16) -> ChunkView<'_> {
        ChunkView {
            series,
            t0_us: self.t0_us,
            count: self.count,
            data: self.buf.get(..self.len).unwrap_or_default(),
        }
    }

    /// Appends an `F32` sample. Returns false (and writes nothing) when it does not fit.
    pub fn push_f32(&mut self, t_us: u64, value: f32) -> bool {
        self.push(t_us, |w| w.raw(&value.to_le_bytes()))
    }

    /// Appends an `F64` sample. Returns false (and writes nothing) when it does not fit.
    pub fn push_f64(&mut self, t_us: u64, value: f64) -> bool {
        self.push(t_us, |w| w.raw(&value.to_le_bytes()))
    }

    /// Appends an `I64` sample. Returns false (and writes nothing) when it does not fit.
    pub fn push_i64(&mut self, t_us: u64, value: i64) -> bool {
        self.push(t_us, |w| w.zigzag(value))
    }

    /// Appends a `U32` sample. Returns false (and writes nothing) when it does not fit.
    pub fn push_u32(&mut self, t_us: u64, value: u32) -> bool {
        self.push(t_us, |w| w.raw(&value.to_le_bytes()))
    }

    /// A time before the previous sample's is clamped to it.
    fn push(&mut self, t_us: u64, value: impl FnOnce(&mut Writer<'_>)) -> bool {
        let (t0, last) = if self.count == 0 {
            (t_us, t_us)
        } else {
            (self.t0_us, self.last_us)
        };
        let t_us = t_us.max(last);
        let Some(rest) = self.buf.get_mut(self.len..) else {
            return false;
        };
        let mut w = Writer::new(rest);
        w.varint(t_us.wrapping_sub(last));
        value(&mut w);
        if !w.fits() {
            return false;
        }
        self.len = self.len.saturating_add(w.len());
        self.count = self.count.saturating_add(1);
        self.t0_us = t0;
        self.last_us = t_us;
        true
    }
}
