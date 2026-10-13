//! Live streams on the node (`docs/streams.md`): publishers declare series and publish batches;
//! the node keeps a bounded history ring per series and serves subscribers (local stream
//! clients and remote push sessions alike) a catalog, a backfill, then every batch.
//!
//! Lock order: the stream registry is never held while the client registry is taken. Events are
//! computed under the stream lock, then queued and flushed like status changes.

use super::local_clients::{
    PendingClientStreamFlush, enqueue_stream_event, execute_client_stream_flush,
    finalize_client_stream_flush, prepare_client_stream_flush,
};
use super::{NodeApp, NodeError};
use orion::{
    control_plane::{
        Backfill, ClientEventKind, MAX_STREAM_NAME_LEN, MAX_STREAM_SERIES, SampleChunk,
        SampleChunkBuilder, SeriesDecl, SeriesKind, StatusSubject, StreamCatalog, StreamCursor,
        StreamData, StreamDecl, StreamDemand, StreamPublish, StreamRef, StreamReset,
        StreamResetReason, StreamSubscription,
    },
    transport::ipc::LocalAddress,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

/// Bytes the history costs per sample (time and value).
const SAMPLE_BYTES: usize = 16;
/// Batch numbers remembered for resuming, per stream.
const MAX_BATCH_LOG: usize = 16_384;
/// The rate assumed for a series declared without one (irregular, on change), to size its ring.
const IRREGULAR_PERIOD_US: u64 = 100_000;

#[derive(Debug, Default)]
pub(super) struct StreamsState {
    registry: Mutex<StreamRegistry>,
    next_epoch: AtomicU64,
    pub(super) batches_total: AtomicU64,
    pub(super) samples_total: AtomicU64,
    pub(super) refused_total: AtomicU64,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Debug, Default)]
struct StreamRegistry {
    streams: BTreeMap<StreamRef, Stream>,
    /// Subscriptions waiting for their stream to be declared.
    waiting: BTreeMap<StreamRef, BTreeMap<LocalAddress, StreamSubscription>>,
}

#[derive(Debug)]
struct Stream {
    series: Vec<SeriesDecl>,
    epoch: u64,
    seq: u64,
    publisher: Option<LocalAddress>,
    rings: Vec<Ring>,
    /// `(batch, newest sample time)`, oldest first.
    batches: VecDeque<(u64, u64)>,
    newest_us: u64,
    subscribers: BTreeMap<LocalAddress, Subscriber>,
    /// History budget in bytes this stream was given.
    budget_bytes: usize,
}

#[derive(Debug)]
struct Subscriber {
    sub: StreamSubscription,
    wanted: Vec<bool>,
    last_seq: u64,
    /// Per series, the time of the last sample sent (for `min_interval_us`).
    last_sent_us: Vec<u64>,
}

#[derive(Debug)]
struct Ring {
    full: VecDeque<(u64, f64)>,
    full_us: u64,
    full_cap: usize,
    reduced: VecDeque<(u64, f64)>,
    reduced_us: u64,
    reduced_cap: usize,
    period_us: u64,
    bucket: Option<(u64, f64, u32)>,
}

impl Ring {
    /// The ring for `decl`, scaled by `scale` (1.0: as declared).
    fn new(decl: &SeriesDecl, scale: f64) -> Self {
        let period = if decl.period_us == 0 {
            IRREGULAR_PERIOD_US
        } else {
            u64::from(decl.period_us)
        };
        let full_us = (u64::from(decl.history.full_ms) * 1000) as f64 * scale;
        let reduced_period = u64::from(decl.history.reduced_period_ms) * 1000;
        let reduced_us = if reduced_period == 0 {
            0.0
        } else {
            (u64::from(decl.history.reduced_ms) * 1000) as f64 * scale
        };
        Self {
            full: VecDeque::new(),
            full_us: full_us as u64,
            // Room for twice the declared rate, so a publisher a little fast keeps its window.
            full_cap: ((full_us / period as f64) * 2.0) as usize + 4,
            reduced: VecDeque::new(),
            reduced_us: reduced_us as u64,
            reduced_cap: if reduced_period == 0 {
                0
            } else {
                ((reduced_us / reduced_period as f64) * 2.0) as usize + 4
            },
            period_us: reduced_period,
            bucket: None,
        }
    }

    /// Samples the declaration asks to keep at its declared rate (the rings allow twice that, for a
    /// publisher a little fast; the memory budget counts what is declared).
    fn declared_samples(decl: &SeriesDecl) -> usize {
        let period = if decl.period_us == 0 {
            IRREGULAR_PERIOD_US
        } else {
            u64::from(decl.period_us)
        };
        let full = u64::from(decl.history.full_ms) * 1000 / period;
        let reduced = match u64::from(decl.history.reduced_period_ms) {
            0 => 0,
            p => u64::from(decl.history.reduced_ms) / p,
        };
        usize::try_from(full + reduced).unwrap_or(usize::MAX)
    }

    #[cfg(test)]
    fn capacity(&self) -> usize {
        self.full_cap + self.reduced_cap
    }

    fn push(&mut self, t: u64, v: f64) {
        self.full.push_back((t, v));
        if self.period_us > 0 {
            match &mut self.bucket {
                Some((start, sum, n)) if t < *start + self.period_us => {
                    *sum += v;
                    *n += 1;
                }
                bucket => {
                    if let Some((start, sum, n)) = bucket.take() {
                        self.reduced
                            .push_back((start + self.period_us / 2, sum / f64::from(n.max(1))));
                    }
                    *bucket = Some((t - t % self.period_us, v, 1));
                }
            }
        }
        while let Some(&(t0, _)) = self.full.front() {
            if t.saturating_sub(t0) > self.full_us || self.full.len() > self.full_cap {
                self.full.pop_front();
            } else {
                break;
            }
        }
        while let Some(&(t0, _)) = self.reduced.front() {
            if t.saturating_sub(t0) > self.reduced_us || self.reduced.len() > self.reduced_cap {
                self.reduced.pop_front();
            } else {
                break;
            }
        }
    }

    /// The kept samples at or after `from`: averages older than the full-rate window, then
    /// every sample.
    fn since(&self, from: u64) -> impl Iterator<Item = (u64, f64)> + '_ {
        let full_start = self.full.front().map_or(u64::MAX, |s| s.0);
        self.reduced
            .iter()
            .copied()
            .filter(move |&(t, _)| t >= from && t < full_start)
            .chain(self.full.iter().copied().filter(move |&(t, _)| t >= from))
    }

    fn len(&self) -> usize {
        self.full.len() + self.reduced.len()
    }
}

/// What to queue for which client once the stream lock is released.
#[derive(Default)]
struct Outbox(Vec<(LocalAddress, ClientEventKind)>);

impl Outbox {
    fn push(&mut self, to: &LocalAddress, event: ClientEventKind) {
        self.0.push((to.clone(), event));
    }
}

fn validate(decl: &StreamDecl) -> Result<(), NodeError> {
    let invalid = |why: String| Err(NodeError::Status(format!("stream {}: {why}", decl.id)));
    if decl.id.is_empty() || decl.id.len() > MAX_STREAM_NAME_LEN {
        return invalid(format!("the id must be 1 to {MAX_STREAM_NAME_LEN} bytes"));
    }
    if decl.series.is_empty() || decl.series.len() > MAX_STREAM_SERIES {
        return invalid(format!("it needs 1 to {MAX_STREAM_SERIES} series"));
    }
    let mut names = std::collections::BTreeSet::new();
    for series in &decl.series {
        if series.name.is_empty() || series.name.len() > MAX_STREAM_NAME_LEN {
            return invalid(format!(
                "series names must be 1 to {MAX_STREAM_NAME_LEN} bytes"
            ));
        }
        if !names.insert(series.name.as_str()) {
            return invalid(format!("series `{}` is declared twice", series.name));
        }
    }
    Ok(())
}

impl Stream {
    fn stream_ref_catalog(&self, stream: &StreamRef) -> StreamCatalog {
        StreamCatalog {
            stream: stream.clone(),
            epoch: self.epoch,
            series: self.series.clone(),
            live: self.publisher.is_some(),
        }
    }

    fn cursor(&self) -> StreamCursor {
        StreamCursor {
            epoch: self.epoch,
            seq: self.seq,
        }
    }

    fn subscriber(&self, sub: StreamSubscription) -> Subscriber {
        Subscriber {
            wanted: self.series.iter().map(|s| sub.wants(&s.name)).collect(),
            last_seq: 0,
            last_sent_us: vec![0; self.series.len()],
            sub,
        }
    }

    /// The history at or after `from_us` for `subscriber`, as chunks.
    fn history_chunks(&self, subscriber: &mut Subscriber, from_us: u64) -> Vec<SampleChunk> {
        let mut chunks = Vec::new();
        for (index, ring) in self.rings.iter().enumerate() {
            if !subscriber.wanted[index] {
                continue;
            }
            let kind = self.series[index].kind;
            let mut builder = SampleChunkBuilder::new(index as u16, kind);
            let min = u64::from(subscriber.sub.min_interval_us);
            for (t, v) in ring.since(from_us) {
                if min > 0
                    && subscriber.last_sent_us[index] != 0
                    && t < subscriber.last_sent_us[index] + min
                {
                    continue;
                }
                builder.push(t, v);
                subscriber.last_sent_us[index] = t;
            }
            if !builder.is_empty() {
                chunks.push(builder.take());
            }
        }
        chunks
    }

    /// Queues for `to` its catalog and the backfill its subscription asks for (with a reset
    /// first when its cursor no longer holds).
    fn start_subscriber(
        &self,
        stream: &StreamRef,
        to: &LocalAddress,
        subscriber: &mut Subscriber,
        out: &mut Outbox,
    ) {
        let from_us = match subscriber.sub.backfill {
            Backfill::None => None,
            Backfill::LastMs(ms) => Some(self.newest_us.saturating_sub(ms.saturating_mul(1000))),
            Backfill::Since(cursor) if cursor.epoch == self.epoch && cursor.seq >= self.seq => {
                Some(u64::MAX)
            }
            Backfill::Since(cursor) if cursor.epoch == self.epoch => {
                match self.batches.iter().find(|(seq, _)| *seq == cursor.seq) {
                    Some(&(_, t)) => Some(t.saturating_add(1)),
                    None if cursor.seq == 0 => Some(0),
                    None => {
                        out.push(
                            to,
                            ClientEventKind::StreamReset(StreamReset {
                                stream: stream.clone(),
                                reason: StreamResetReason::OutOfRange,
                            }),
                        );
                        Some(0)
                    }
                }
            }
            Backfill::Since(_) => {
                out.push(
                    to,
                    ClientEventKind::StreamReset(StreamReset {
                        stream: stream.clone(),
                        reason: StreamResetReason::NewEpoch,
                    }),
                );
                Some(0)
            }
        };
        out.push(
            to,
            ClientEventKind::StreamCatalog(self.stream_ref_catalog(stream)),
        );
        let chunks = match from_us {
            Some(from) if from != u64::MAX => self.history_chunks(subscriber, from),
            _ => Vec::new(),
        };
        out.push(
            to,
            ClientEventKind::StreamData(StreamData {
                stream: stream.clone(),
                cursor: self.cursor(),
                prev_seq: 0,
                backfill: true,
                chunks,
            }),
        );
        subscriber.last_seq = self.seq;
    }

    fn demand(&self, stream: &StreamRef) -> StreamDemand {
        let mut series: Vec<u16> = Vec::new();
        for (index, _) in self.series.iter().enumerate() {
            if self.subscribers.values().any(|s| s.wanted[index]) {
                series.push(index as u16);
            }
        }
        StreamDemand {
            stream: stream.clone(),
            subscribers: self.subscribers.len() as u32,
            series,
        }
    }

    fn tell_publisher(&self, stream: &StreamRef, out: &mut Outbox) {
        if let Some(publisher) = &self.publisher {
            out.push(
                publisher,
                ClientEventKind::StreamDemand(self.demand(stream)),
            );
        }
    }
}

impl NodeApp {
    fn deliver_stream_events(&self, out: Outbox) {
        if out.0.is_empty() {
            return;
        }
        let mut flushes = Vec::<PendingClientStreamFlush>::new();
        let mut gone = Vec::new();
        self.with_client_registry_txn(|txn| {
            for (to, event) in out.0 {
                match txn.client_mut(&to) {
                    Ok(client) => {
                        enqueue_stream_event(client, event);
                        if let Some(flush) = prepare_client_stream_flush(&to, client) {
                            if let Some(existing) = flushes.iter().position(|f| f.source == to) {
                                flushes[existing] = flush;
                            } else {
                                flushes.push(flush);
                            }
                        }
                    }
                    Err(_) => gone.push(to),
                }
            }
        });
        for flush in flushes {
            let delivered = execute_client_stream_flush(&flush);
            let _ = self.with_client_mut_if_present(&flush.source, |client| {
                finalize_client_stream_flush(client, &flush, delivered);
            });
        }
        for client in gone {
            self.forget_stream_client(&client);
        }
    }

    /// `DeclareStream` from local client `source`.
    pub(super) fn declare_stream(
        &self,
        source: &LocalAddress,
        decl: StreamDecl,
    ) -> Result<(), NodeError> {
        validate(&decl)?;
        let owned = match &decl.owner {
            StatusSubject::Node(id) => id == &self.config.node_id,
            other => self.local_client_owns(source, other),
        };
        if !owned {
            self.state
                .streams
                .refused_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(NodeError::Authorization(format!(
                "client {} may not declare streams for {}",
                source.as_str(),
                decl.owner
            )));
        }
        let stream = StreamRef {
            owner: decl.owner.clone(),
            id: decl.id.clone(),
        };
        let budget = self.config.runtime_tuning.stream_memory_max_bytes;
        let mut out = Outbox::default();
        {
            let mut registry = lock(&self.state.streams.registry);
            if let Some(existing) = registry.streams.get(&stream) {
                if let Some(publisher) = &existing.publisher
                    && publisher != source
                {
                    self.state
                        .streams
                        .refused_total
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(NodeError::Authorization(format!(
                        "stream {} of {} is published by another client",
                        stream.id, stream.owner
                    )));
                }
                if existing.publisher.as_ref() == Some(source) && existing.series == decl.series {
                    // The same declaration again: nothing changes.
                    existing.tell_publisher(&stream, &mut out);
                    drop(registry);
                    self.deliver_stream_events(out);
                    return Ok(());
                }
            }
            let used: usize = registry
                .streams
                .iter()
                .filter(|(key, _)| **key != stream)
                .map(|(_, s)| s.budget_bytes)
                .sum();
            let wanted: usize = decl
                .series
                .iter()
                .map(Ring::declared_samples)
                .sum::<usize>()
                * SAMPLE_BYTES;
            let left = budget.saturating_sub(used);
            let scale = if wanted <= left || wanted == 0 {
                1.0
            } else {
                left as f64 / wanted as f64
            };
            let epoch = (Self::current_time_ms() << 16)
                | (self
                    .state
                    .streams
                    .next_epoch
                    .fetch_add(1, Ordering::Relaxed)
                    & 0xffff);
            let previous = registry.streams.remove(&stream);
            let mut fresh = Stream {
                rings: decl.series.iter().map(|s| Ring::new(s, scale)).collect(),
                series: decl.series,
                epoch,
                seq: 0,
                publisher: Some(source.clone()),
                batches: VecDeque::new(),
                newest_us: 0,
                subscribers: BTreeMap::new(),
                budget_bytes: (wanted as f64 * scale) as usize,
            };
            // Subscribers of the previous epoch and those that were waiting: a reset (for the
            // former), the new catalog and the (empty) history.
            let mut moved: Vec<(LocalAddress, StreamSubscription, bool)> = previous
                .map(|p| {
                    p.subscribers
                        .into_iter()
                        .map(|(to, s)| (to, s.sub, true))
                        .collect()
                })
                .unwrap_or_default();
            if let Some(waiting) = registry.waiting.remove(&stream) {
                moved.extend(waiting.into_iter().map(|(to, sub)| (to, sub, false)));
            }
            for (to, mut sub, had_epoch) in moved {
                if had_epoch {
                    out.push(
                        &to,
                        ClientEventKind::StreamReset(StreamReset {
                            stream: stream.clone(),
                            reason: StreamResetReason::NewEpoch,
                        }),
                    );
                    sub.backfill = Backfill::LastMs(u64::MAX / 1000);
                }
                let mut subscriber = fresh.subscriber(sub);
                fresh.start_subscriber(&stream, &to, &mut subscriber, &mut out);
                fresh.subscribers.insert(to, subscriber);
            }
            fresh.tell_publisher(&stream, &mut out);
            registry.streams.insert(stream, fresh);
        }
        self.deliver_stream_events(out);
        Ok(())
    }

    /// `PublishSamples` from the stream's publisher.
    pub(super) fn publish_stream_samples(
        &self,
        source: &LocalAddress,
        publish: StreamPublish,
    ) -> Result<(), NodeError> {
        let mut out = Outbox::default();
        {
            let mut registry = lock(&self.state.streams.registry);
            let Some(stream) = registry.streams.get_mut(&publish.stream) else {
                return Err(NodeError::Status(format!(
                    "stream {} of {} is not declared",
                    publish.stream.id, publish.stream.owner
                )));
            };
            if stream.publisher.as_ref() != Some(source) {
                self.state
                    .streams
                    .refused_total
                    .fetch_add(1, Ordering::Relaxed);
                return Err(NodeError::Authorization(format!(
                    "client {} does not publish stream {}",
                    source.as_str(),
                    publish.stream.id
                )));
            }
            // Decode everything first: a malformed batch changes nothing.
            let mut decoded = Vec::with_capacity(publish.chunks.len());
            for chunk in &publish.chunks {
                let kind: SeriesKind = stream
                    .series
                    .get(usize::from(chunk.series))
                    .map(|s| s.kind)
                    .ok_or_else(|| {
                        NodeError::Status(format!(
                            "stream {}: no series {}",
                            publish.stream.id, chunk.series
                        ))
                    })?;
                let samples = chunk.samples(kind).map_err(|err| {
                    NodeError::Status(format!("stream {}: {err}", publish.stream.id))
                })?;
                decoded.push((usize::from(chunk.series), samples));
            }
            let mut count = 0u64;
            for (index, samples) in &decoded {
                for &(t, v) in samples {
                    stream.rings[*index].push(t, v);
                    stream.newest_us = stream.newest_us.max(t);
                    count += 1;
                }
            }
            stream.seq += 1;
            let seq = stream.seq;
            stream.batches.push_back((seq, stream.newest_us));
            let oldest = stream
                .rings
                .iter()
                .filter_map(|r| r.reduced.front().or(r.full.front()).map(|s| s.0))
                .min()
                .unwrap_or(0);
            while stream.batches.len() > MAX_BATCH_LOG
                || stream.batches.front().is_some_and(|&(_, t)| t < oldest)
            {
                stream.batches.pop_front();
            }
            self.state
                .streams
                .batches_total
                .fetch_add(1, Ordering::Relaxed);
            self.state
                .streams
                .samples_total
                .fetch_add(count, Ordering::Relaxed);
            let cursor = stream.cursor();
            let series = &stream.series;
            for (to, subscriber) in stream.subscribers.iter_mut() {
                let everything =
                    subscriber.sub.min_interval_us == 0 && subscriber.wanted.iter().all(|w| *w);
                let chunks: Vec<SampleChunk> = if everything {
                    publish.chunks.clone()
                } else {
                    let min = u64::from(subscriber.sub.min_interval_us);
                    decoded
                        .iter()
                        .filter(|(index, _)| subscriber.wanted[*index])
                        .filter_map(|(index, samples)| {
                            let mut builder =
                                SampleChunkBuilder::new(*index as u16, series[*index].kind);
                            for &(t, v) in samples {
                                let last = subscriber.last_sent_us[*index];
                                if min > 0 && last != 0 && t < last + min {
                                    continue;
                                }
                                builder.push(t, v);
                                subscriber.last_sent_us[*index] = t;
                            }
                            (!builder.is_empty()).then(|| builder.take())
                        })
                        .collect()
                };
                if chunks.is_empty() {
                    continue;
                }
                out.push(
                    to,
                    ClientEventKind::StreamData(StreamData {
                        stream: publish.stream.clone(),
                        cursor,
                        prev_seq: subscriber.last_seq,
                        backfill: false,
                        chunks,
                    }),
                );
                subscriber.last_seq = seq;
            }
        }
        self.deliver_stream_events(out);
        Ok(())
    }

    /// `SubscribeStream` from local client (or remote session) `source`.
    pub(super) fn subscribe_stream(
        &self,
        source: &LocalAddress,
        sub: StreamSubscription,
    ) -> Result<(), NodeError> {
        let key = sub.stream.clone();
        let mut out = Outbox::default();
        {
            let mut registry = lock(&self.state.streams.registry);
            match registry.streams.get_mut(&key) {
                Some(stream) => {
                    let mut subscriber = stream.subscriber(sub);
                    stream.start_subscriber(&key, source, &mut subscriber, &mut out);
                    stream.subscribers.insert(source.clone(), subscriber);
                    stream.tell_publisher(&key, &mut out);
                }
                None => {
                    // Not declared (yet): it starts when the publisher declares it.
                    registry
                        .waiting
                        .entry(key)
                        .or_default()
                        .insert(source.clone(), sub);
                }
            }
        }
        self.deliver_stream_events(out);
        Ok(())
    }

    pub(super) fn unsubscribe_stream(&self, source: &LocalAddress, stream: &StreamRef) {
        let mut out = Outbox::default();
        {
            let mut registry = lock(&self.state.streams.registry);
            if let Some(waiting) = registry.waiting.get_mut(stream) {
                waiting.remove(source);
                if waiting.is_empty() {
                    registry.waiting.remove(stream);
                }
            }
            if let Some(found) = registry.streams.get_mut(stream)
                && found.subscribers.remove(source).is_some()
            {
                found.tell_publisher(stream, &mut out);
            }
        }
        self.deliver_stream_events(out);
    }

    /// `source` went away: its subscriptions end, and the streams it published stay with their
    /// history but no publisher (subscribers see a catalog with `live: false`).
    pub(super) fn forget_stream_client(&self, source: &LocalAddress) {
        let mut out = Outbox::default();
        {
            let mut registry = lock(&self.state.streams.registry);
            registry.waiting.retain(|_, waiting| {
                waiting.remove(source);
                !waiting.is_empty()
            });
            for (key, stream) in registry.streams.iter_mut() {
                if stream.subscribers.remove(source).is_some() {
                    stream.tell_publisher(key, &mut out);
                }
                if stream.publisher.as_ref() == Some(source) {
                    stream.publisher = None;
                    let catalog = stream.stream_ref_catalog(key);
                    for to in stream.subscribers.keys() {
                        out.push(to, ClientEventKind::StreamCatalog(catalog.clone()));
                    }
                }
            }
        }
        self.deliver_stream_events(out);
    }

    pub(super) fn streams_usage(&self) -> orion::control_plane::StreamsUsageSnapshot {
        let registry = lock(&self.state.streams.registry);
        let counters = &self.state.streams;
        orion::control_plane::StreamsUsageSnapshot {
            streams: registry.streams.len() as u64,
            series: registry
                .streams
                .values()
                .map(|s| s.series.len() as u64)
                .sum(),
            samples: registry
                .streams
                .values()
                .flat_map(|s| s.rings.iter())
                .map(|r| r.len() as u64)
                .sum(),
            budget_bytes: registry
                .streams
                .values()
                .map(|s| s.budget_bytes as u64)
                .sum(),
            max_budget_bytes: self.config.runtime_tuning.stream_memory_max_bytes as u64,
            subscribers: registry
                .streams
                .values()
                .map(|s| s.subscribers.len() as u64)
                .sum::<u64>()
                + registry
                    .waiting
                    .values()
                    .map(|w| w.len() as u64)
                    .sum::<u64>(),
            batches_total: counters.batches_total.load(Ordering::Relaxed),
            samples_total: counters.samples_total.load(Ordering::Relaxed),
            refused_total: counters.refused_total.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion::control_plane::HistoryPolicy;

    fn decl(full_ms: u32, reduced_ms: u32, period_ms: u32) -> SeriesDecl {
        SeriesDecl::new("x", SeriesKind::F64, 10_000)
            .with_history(HistoryPolicy::full(full_ms).with_reduced(reduced_ms, period_ms))
    }

    #[test]
    fn a_ring_keeps_its_full_window_and_averages_older_samples() {
        // 100 Hz for 10 s, every sample of the last second, 100 ms averages for 10 s.
        let mut ring = Ring::new(&decl(1_000, 10_000, 100), 1.0);
        for i in 0..1_000u64 {
            ring.push(i * 10_000, i as f64);
        }
        let newest = 999 * 10_000;
        assert!(ring.full.iter().all(|&(t, _)| newest - t <= 1_000_000));
        assert!(ring.full.len() >= 100 && ring.full.len() <= 101);
        // Averages of ten samples each, older than the full window.
        let older: Vec<_> = ring
            .since(0)
            .take_while(|&(t, _)| t < ring.full[0].0)
            .collect();
        assert!(older.len() >= 85, "{} reduced samples", older.len());
        let (t, v) = older[0];
        assert_eq!(t % 100_000, 50_000, "an average sits mid-bucket");
        assert!(
            (v - (t / 10_000) as f64).abs() < 1.0,
            "averaged: {v} at {t}"
        );
        // `since` is in time order, with no sample twice.
        let all: Vec<_> = ring.since(0).collect();
        assert!(all.windows(2).all(|w| w[0].0 < w[1].0));
    }

    #[test]
    fn a_publisher_faster_than_declared_cannot_grow_a_ring_without_bound() {
        let mut ring = Ring::new(&decl(1_000, 0, 0), 1.0);
        for i in 0..100_000u64 {
            ring.push(1_000_000 + i, 0.0);
        }
        assert!(ring.len() <= ring.capacity());
    }

    #[test]
    fn a_board_sized_stream_fits_the_default_budget_unscaled() {
        // 40 gauges at 10 Hz for 5 minutes, 6 IMU channels at 100 Hz for 30 s then 10 Hz for 5.
        let gauge = SeriesDecl::new("g", SeriesKind::F32, 100_000)
            .with_history(HistoryPolicy::full(300_000));
        let imu = SeriesDecl::new("i", SeriesKind::F32, 10_000)
            .with_history(HistoryPolicy::full(30_000).with_reduced(300_000, 100));
        let bytes =
            (40 * Ring::declared_samples(&gauge) + 6 * Ring::declared_samples(&imu)) * SAMPLE_BYTES;
        assert_eq!(Ring::declared_samples(&gauge), 3_000);
        assert_eq!(Ring::declared_samples(&imu), 6_000);
        assert!(bytes <= 4 * 1024 * 1024, "{bytes} bytes");
    }

    #[test]
    fn declarations_are_checked() {
        let good = StreamDecl {
            owner: StatusSubject::Node("n".into()),
            id: "board.live".into(),
            series: vec![SeriesDecl::new("a", SeriesKind::F32, 100_000)],
        };
        assert!(validate(&good).is_ok());
        let mut twice = good.clone();
        twice
            .series
            .push(SeriesDecl::new("a", SeriesKind::F32, 100_000));
        assert!(validate(&twice).is_err());
        let mut empty = good.clone();
        empty.series.clear();
        assert!(validate(&empty).is_err());
        let mut long = good;
        long.id = "x".repeat(MAX_STREAM_NAME_LEN + 1);
        assert!(validate(&long).is_err());
    }
}
