//! Live streams: typed series a publisher samples at a rate, with a short history the node keeps
//! (`docs/streams.md`).
//!
//! A publisher (a provider or executor client on the node's host) declares a stream of series
//! with [`StreamDecl`] and publishes batches of samples ([`StreamPublish`]) every few tens of
//! milliseconds. The node keeps a bounded history ring per series ([`HistoryPolicy`]) and serves
//! subscribers, local or over a remote push session: a catalog, one backfill of the history,
//! then every batch as it arrives ([`StreamData`]), with cursors to resume after a reconnect.
//!
//! Samples travel in [`SampleChunk`]s: per series, a start time and packed `(Δt, value)` pairs
//! (Δt in microseconds as a varint, the value in the series' [`SeriesKind`]), so 10 Hz of a few
//! dozen series costs a few hundred bytes a second.

use super::status::StatusSubject;
use alloc::{string::String, vec::Vec};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Largest number of series in one stream.
pub const MAX_STREAM_SERIES: usize = 512;
/// Longest stream id and series name.
pub const MAX_STREAM_NAME_LEN: usize = 128;

/// How a series' values are encoded.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum SeriesKind {
    /// 4 bytes, little endian.
    F32,
    /// 8 bytes, little endian.
    F64,
    /// Zigzag varint.
    I64,
    /// 4 bytes, little endian (for example a colour, `0xWWRRGGBB`).
    U32,
}

/// When the publisher samples a series.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum Sampling {
    /// Always, so a late subscriber gets the history.
    #[default]
    Always,
    /// Only while someone subscribes to it ([`StreamDemand`] says when); its history starts
    /// with the first subscriber.
    WhileSubscribed,
}

/// How much history the node keeps for a series.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct HistoryPolicy {
    /// Every sample of the last `full_ms` milliseconds.
    pub full_ms: u32,
    /// Older samples, averaged over `reduced_period_ms`, back to `reduced_ms` (0: none).
    pub reduced_ms: u32,
    pub reduced_period_ms: u32,
}

impl HistoryPolicy {
    /// Every sample of the last `ms` milliseconds.
    pub fn full(ms: u32) -> Self {
        Self {
            full_ms: ms,
            reduced_ms: 0,
            reduced_period_ms: 0,
        }
    }

    /// Also keep averages over `period_ms` back to `ms`.
    pub fn with_reduced(mut self, ms: u32, period_ms: u32) -> Self {
        self.reduced_ms = ms;
        self.reduced_period_ms = period_ms;
        self
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SeriesDecl {
    /// Unique in the stream: `cpu-thermal.temp`, `imu.accel_x`, `led.0`.
    pub name: String,
    pub unit: Option<String>,
    pub kind: SeriesKind,
    /// The period the publisher samples at, in microseconds (0: irregular, on change).
    pub period_us: u32,
    pub history: HistoryPolicy,
    pub sampling: Sampling,
}

impl SeriesDecl {
    pub fn new(name: impl Into<String>, kind: SeriesKind, period_us: u32) -> Self {
        Self {
            name: name.into(),
            unit: None,
            kind,
            period_us,
            history: HistoryPolicy::default(),
            sampling: Sampling::Always,
        }
    }

    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    pub fn with_history(mut self, history: HistoryPolicy) -> Self {
        self.history = history;
        self
    }

    pub fn with_sampling(mut self, sampling: Sampling) -> Self {
        self.sampling = sampling;
        self
    }
}

/// A stream: an id unique for its owner, and its series in a fixed order (a sample chunk names
/// its series by index).
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StreamDecl {
    /// `node/<local node>`, or the publisher's own `provider/<id>` / `executor/<id>`.
    pub owner: StatusSubject,
    pub id: String,
    pub series: Vec<SeriesDecl>,
}

/// Which stream.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct StreamRef {
    pub owner: StatusSubject,
    pub id: String,
}

/// A position in a stream: the publisher's epoch (a new one when it restarts or redeclares
/// with other series) and the number of its last batch.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct StreamCursor {
    pub epoch: u64,
    pub seq: u64,
}

/// Samples of one series: times from `t0_us` on, packed as described in the module docs.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct SampleChunk {
    /// The series' index in the stream's declaration.
    pub series: u16,
    /// Time of the first sample: microseconds since the Unix epoch on the publisher's clock.
    pub t0_us: u64,
    pub count: u32,
    pub data: Vec<u8>,
}

/// A batch of samples from the stream's publisher.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StreamPublish {
    pub stream: StreamRef,
    pub chunks: Vec<SampleChunk>,
}

/// How much history a new subscription starts with.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum Backfill {
    /// Live batches only.
    #[default]
    None,
    /// The last this many milliseconds the node keeps.
    LastMs(u64),
    /// What came after this cursor (after a reconnect); a [`StreamReset`] and the whole history
    /// when the node no longer has it.
    Since(StreamCursor),
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StreamSubscription {
    pub stream: StreamRef,
    /// Series names, or prefixes ending in `*` (`imu.*`); empty for all.
    pub series: Vec<String>,
    /// At most one sample per series per this many microseconds (0: every sample).
    pub min_interval_us: u32,
    pub backfill: Backfill,
}

impl StreamSubscription {
    pub fn all(stream: StreamRef) -> Self {
        Self {
            stream,
            series: Vec::new(),
            min_interval_us: 0,
            backfill: Backfill::None,
        }
    }

    pub fn with_backfill(mut self, backfill: Backfill) -> Self {
        self.backfill = backfill;
        self
    }

    pub fn with_series<I, S>(mut self, series: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.series = series.into_iter().map(Into::into).collect();
        self
    }

    /// Whether `name` is one of the subscription's series.
    pub fn wants(&self, name: &str) -> bool {
        self.series.is_empty()
            || self
                .series
                .iter()
                .any(|pattern| match pattern.strip_suffix('*') {
                    Some(prefix) => name.starts_with(prefix),
                    None => pattern == name,
                })
    }
}

/// The stream's series, sent to a subscriber first (and again after a [`StreamReset`]).
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StreamCatalog {
    pub stream: StreamRef,
    pub epoch: u64,
    pub series: Vec<SeriesDecl>,
    /// `false` while the publisher is gone (the history is still served).
    pub live: bool,
}

/// Samples for a subscriber: the backfill (`backfill`) or one or more batches.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StreamData {
    pub stream: StreamRef,
    /// The newest batch included.
    pub cursor: StreamCursor,
    /// The batch this subscriber got before these (0 before the first): when it isn't the one the
    /// subscriber last saw, something was lost and it should resubscribe from its cursor.
    pub prev_seq: u64,
    pub backfill: bool,
    pub chunks: Vec<SampleChunk>,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum StreamResetReason {
    /// The publisher restarted or declared other series: a new epoch, new history.
    NewEpoch,
    /// The cursor is older than the history the node keeps.
    OutOfRange,
    /// The subscriber fell too far behind; its queued samples were dropped.
    Overrun,
    /// The stream is gone (no publisher and no history left).
    Removed,
}

/// The subscriber's position no longer holds: a catalog and a backfill follow (unless
/// [`StreamResetReason::Removed`]).
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StreamReset {
    pub stream: StreamRef,
    pub reason: StreamResetReason,
}

/// For the publisher: which of its series someone subscribes to now (pushed when it changes, and
/// after a declaration).
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StreamDemand {
    pub stream: StreamRef,
    pub subscribers: u32,
    /// Series indexes with at least one subscriber.
    pub series: Vec<u16>,
}

/// A value of a series, decoded.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SampleValue {
    F32(f32),
    F64(f64),
    I64(i64),
    U32(u32),
}

impl SampleValue {
    pub fn as_f64(self) -> f64 {
        match self {
            Self::F32(v) => f64::from(v),
            Self::F64(v) => v,
            Self::I64(v) => v as f64,
            Self::U32(v) => f64::from(v),
        }
    }

    /// `value` in `kind` (rounding or truncating when they differ).
    pub fn from_f64(kind: SeriesKind, value: f64) -> Self {
        match kind {
            SeriesKind::F32 => Self::F32(value as f32),
            SeriesKind::F64 => Self::F64(value),
            SeriesKind::I64 => Self::I64(value as i64),
            SeriesKind::U32 => Self::U32(value as u32),
        }
    }
}

/// A malformed sample chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SampleDecodeError;

impl core::fmt::Display for SampleDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("malformed sample chunk")
    }
}

/// Builds a [`SampleChunk`] sample by sample, times not decreasing.
#[derive(Clone, Debug)]
pub struct SampleChunkBuilder {
    kind: SeriesKind,
    chunk: SampleChunk,
    last_us: u64,
}

impl SampleChunkBuilder {
    pub fn new(series: u16, kind: SeriesKind) -> Self {
        Self {
            kind,
            chunk: SampleChunk {
                series,
                ..SampleChunk::default()
            },
            last_us: 0,
        }
    }

    /// Appends a sample. A time before the previous one is clamped to it.
    pub fn push(&mut self, t_us: u64, value: f64) {
        if self.chunk.count == 0 {
            self.chunk.t0_us = t_us;
            self.last_us = t_us;
        }
        let t_us = t_us.max(self.last_us);
        write_varint(&mut self.chunk.data, t_us - self.last_us);
        self.last_us = t_us;
        match SampleValue::from_f64(self.kind, value) {
            SampleValue::F32(v) => self.chunk.data.extend_from_slice(&v.to_le_bytes()),
            SampleValue::F64(v) => self.chunk.data.extend_from_slice(&v.to_le_bytes()),
            SampleValue::I64(v) => write_varint(&mut self.chunk.data, zigzag(v)),
            SampleValue::U32(v) => self.chunk.data.extend_from_slice(&v.to_le_bytes()),
        }
        self.chunk.count = self.chunk.count.saturating_add(1);
    }

    pub fn is_empty(&self) -> bool {
        self.chunk.count == 0
    }

    pub fn len(&self) -> usize {
        self.chunk.count as usize
    }

    /// The chunk built so far; the builder starts over.
    pub fn take(&mut self) -> SampleChunk {
        let series = self.chunk.series;
        core::mem::replace(
            &mut self.chunk,
            SampleChunk {
                series,
                ..SampleChunk::default()
            },
        )
    }
}

impl SampleChunk {
    /// The samples, as `(t_us, value)`, decoded with the series' `kind`.
    pub fn samples(&self, kind: SeriesKind) -> Result<Vec<(u64, f64)>, SampleDecodeError> {
        let mut out = Vec::with_capacity(self.count as usize);
        let mut at = 0usize;
        let mut t = self.t0_us;
        for _ in 0..self.count {
            let dt = read_varint(&self.data, &mut at)?;
            t = t.checked_add(dt).ok_or(SampleDecodeError)?;
            let value = match kind {
                SeriesKind::F32 => f64::from(f32::from_le_bytes(take::<4>(&self.data, &mut at)?)),
                SeriesKind::F64 => f64::from_le_bytes(take::<8>(&self.data, &mut at)?),
                SeriesKind::I64 => unzigzag(read_varint(&self.data, &mut at)?) as f64,
                SeriesKind::U32 => f64::from(u32::from_le_bytes(take::<4>(&self.data, &mut at)?)),
            };
            out.push((t, value));
        }
        if at != self.data.len() {
            return Err(SampleDecodeError);
        }
        Ok(out)
    }
}

fn take<const N: usize>(data: &[u8], at: &mut usize) -> Result<[u8; N], SampleDecodeError> {
    let bytes = data
        .get(*at..at.checked_add(N).ok_or(SampleDecodeError)?)
        .ok_or(SampleDecodeError)?;
    *at += N;
    let mut out = [0u8; N];
    out.copy_from_slice(bytes);
    Ok(out)
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(data: &[u8], at: &mut usize) -> Result<u64, SampleDecodeError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *data.get(*at).ok_or(SampleDecodeError)?;
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(SampleDecodeError)
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn chunks_round_trip_every_kind() {
        for (kind, values) in [
            (SeriesKind::F32, [1.5, -2.25, 0.0]),
            (SeriesKind::F64, [1e-9, 12.034512, -1e12]),
            (SeriesKind::I64, [0.0, -5.0, 1e15]),
            (SeriesKind::U32, [0.0, 4_278_190_080.0, 7.0]),
        ] {
            let mut builder = SampleChunkBuilder::new(3, kind);
            let times = [1_000_000u64, 1_100_000, 1_100_050];
            for (t, v) in times.iter().zip(values) {
                builder.push(*t, v);
            }
            let chunk = builder.take();
            assert_eq!((chunk.series, chunk.count, chunk.t0_us), (3, 3, 1_000_000));
            let decoded = chunk.samples(kind).expect("decodes");
            assert_eq!(decoded.iter().map(|s| s.0).collect::<Vec<_>>(), times);
            for (got, want) in decoded.iter().zip(values) {
                assert_eq!(got.1, SampleValue::from_f64(kind, want).as_f64());
            }
            assert!(builder.is_empty(), "take starts over");
        }
    }

    #[test]
    fn a_ten_hertz_f32_sample_costs_about_five_bytes() {
        let mut builder = SampleChunkBuilder::new(0, SeriesKind::F32);
        for i in 0..100u64 {
            builder.push(1_700_000_000_000_000 + i * 100_000, i as f64);
        }
        let chunk = builder.take();
        assert!(chunk.data.len() <= 100 * 7, "{} bytes", chunk.data.len());
    }

    #[test]
    fn truncated_and_trailing_data_is_refused_and_times_never_go_back() {
        let mut builder = SampleChunkBuilder::new(0, SeriesKind::F64);
        builder.push(10, 1.0);
        builder.push(5, 2.0);
        let mut chunk = builder.take();
        assert_eq!(
            chunk.samples(SeriesKind::F64).expect("decodes"),
            vec![(10, 1.0), (10, 2.0)]
        );
        chunk.data.pop();
        assert_eq!(chunk.samples(SeriesKind::F64), Err(SampleDecodeError));
        chunk.data.extend_from_slice(&[0, 0]);
        assert_eq!(chunk.samples(SeriesKind::F64), Err(SampleDecodeError));
    }

    #[test]
    fn subscriptions_match_names_and_prefixes() {
        let stream = StreamRef {
            owner: StatusSubject::Node("n".into()),
            id: "board.live".into(),
        };
        assert!(StreamSubscription::all(stream.clone()).wants("anything"));
        let sub = StreamSubscription::all(stream).with_series(["cpu", "imu.*"]);
        assert!(sub.wants("cpu") && sub.wants("imu.accel_x"));
        assert!(!sub.wants("cpu.core0") && !sub.wants("fan.rpm"));
    }
}
