use super::{CommunicationMetrics, NodeError};
use orion::{
    ExecutorId, ProviderId,
    control_plane::{
        ActionQuery, ActionRequest, ActionResult, ClientEvent, ClientEventKind, ClientSession,
        LeaseRecord, StateSnapshot, StateWatch, StatusChange, StatusEntry, StatusKey, StatusQuery,
        WorkloadRecord,
    },
    transport::ipc::{ControlEnvelope, LocalAddress},
};
use std::collections::{BTreeMap, VecDeque};

/// The observed state last seen by observed-state watchers (`StateWatch::include_observed`) and a
/// generation that moves whenever it changes.
#[derive(Default)]
pub(super) struct ObservedWatch {
    pub(super) last: Option<ArchiveFingerprint>,
    pub(super) generation: u64,
}

/// A 128-bit digest of an archive encoding. Watchers only need to know whether a value changed,
/// so the value itself is not kept (it would be a second full copy of the watched state).
pub(super) type ArchiveFingerprint = [u64; 2];

impl ObservedWatch {
    /// Records `fingerprint` and returns the generation it belongs to. `None` (the state could not
    /// be encoded) always counts as a change.
    pub(super) fn observe(&mut self, fingerprint: Option<ArchiveFingerprint>) -> u64 {
        if fingerprint.is_none() || self.last != fingerprint {
            self.last = fingerprint;
            self.generation = self.generation.saturating_add(1);
        }
        self.generation
    }
}

/// Digest of `value`'s archive encoding (two SipHash keyings, 128 bits in total), and the length of
/// that encoding. `None` when the value cannot be encoded; callers treat that as a change.
pub(super) fn archive_digest<T: orion::ArchiveEncode>(
    value: &T,
) -> Option<(ArchiveFingerprint, usize)> {
    use std::hash::Hasher;
    let bytes = orion::encode_to_vec(value).ok()?;
    let mut first = std::collections::hash_map::DefaultHasher::new();
    first.write(&bytes);
    let mut second = std::collections::hash_map::DefaultHasher::new();
    second.write_u8(0xa5);
    second.write(&bytes);
    Some(([first.finish(), second.finish()], bytes.len()))
}

/// The charge of a list event: the encoded length of its digest, or the unmeasured charge when
/// the list could not be encoded.
pub(super) fn digest_bytes(digest: Option<(ArchiveFingerprint, usize)>) -> usize {
    digest.map_or(UNMEASURED_EVENT_BYTES, |(_, bytes)| bytes)
}

/// The digest of [`archive_digest`] without its length.
pub(super) fn archive_fingerprint<T: orion::ArchiveEncode>(
    value: &T,
) -> Option<ArchiveFingerprint> {
    archive_digest(value).map(|(fingerprint, _)| fingerprint)
}

/// Bytes charged for a queued event whose size is not measured: status, action, snapshot and
/// stream control events. Their count is bounded by the event limit and their own merge rules.
const UNMEASURED_EVENT_BYTES: usize = 256;
/// Framing charged per sample chunk of a stream event, on top of its data.
const STREAM_CHUNK_OVERHEAD_BYTES: usize = 16;

/// Bytes charged for an event that carries no measured size (see [`archive_digest`] for lists).
pub(super) fn event_bytes(event: &ClientEventKind) -> usize {
    match event {
        ClientEventKind::StreamData(data) => stream_data_bytes(data),
        _ => UNMEASURED_EVENT_BYTES,
    }
}

fn stream_data_bytes(data: &orion::control_plane::StreamData) -> usize {
    data.chunks
        .iter()
        .map(|chunk| chunk.data.len().saturating_add(STREAM_CHUNK_OVERHEAD_BYTES))
        .fold(0, usize::saturating_add)
}

/// The pending events of one client, in sequence order. Each event is kept with the bytes it is
/// charged, so `bytes` is the total charge of the queue and stays exact through every removal.
#[derive(Clone, Debug, Default)]
pub(super) struct ClientEventQueue {
    items: VecDeque<(ClientEvent, usize)>,
    bytes: usize,
}

impl ClientEventQueue {
    pub(super) fn len(&self) -> usize {
        self.items.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Total bytes charged to the queued events.
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn iter(&self) -> impl DoubleEndedIterator<Item = &ClientEvent> + '_ {
        self.items.iter().map(|(event, _)| event)
    }

    pub(super) fn front(&self) -> Option<&ClientEvent> {
        self.items.front().map(|(event, _)| event)
    }

    pub(super) fn back(&self) -> Option<&ClientEvent> {
        self.items.back().map(|(event, _)| event)
    }

    pub(super) fn position(&self, mut matches: impl FnMut(&ClientEvent) -> bool) -> Option<usize> {
        self.items.iter().position(|(event, _)| matches(event))
    }

    pub(super) fn event_mut(&mut self, index: usize) -> Option<&mut ClientEvent> {
        self.items.get_mut(index).map(|(event, _)| event)
    }

    /// Sets the charge of the event at `index` after its payload changed in place.
    pub(super) fn recharge(&mut self, index: usize, bytes: usize) {
        if let Some((_, charged)) = self.items.get_mut(index) {
            self.bytes = self.bytes.saturating_sub(*charged).saturating_add(bytes);
            *charged = bytes;
        }
    }

    pub(super) fn pop_front(&mut self) -> Option<ClientEvent> {
        let (event, bytes) = self.items.pop_front()?;
        self.bytes = self.bytes.saturating_sub(bytes);
        Some(event)
    }

    pub(super) fn remove(&mut self, index: usize) -> Option<ClientEvent> {
        let (event, bytes) = self.items.remove(index)?;
        self.bytes = self.bytes.saturating_sub(bytes);
        Some(event)
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&ClientEvent) -> bool) {
        self.items.retain(|(event, _)| keep(event));
        self.bytes = self.items.iter().map(|(_, bytes)| *bytes).sum();
    }

    fn push_back(&mut self, event: ClientEvent, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.items.push_back((event, bytes));
    }

    fn insert(&mut self, index: usize, event: ClientEvent, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.items.insert(index, (event, bytes));
    }
}

#[derive(Clone)]
pub(super) struct PendingClientStreamFlush {
    pub(super) source: LocalAddress,
    sender: tokio::sync::mpsc::Sender<ControlEnvelope>,
    last_sequence: u64,
    /// Taken (moved, not cloned) by [`execute_client_stream_flush`].
    envelope: Option<ControlEnvelope>,
}

#[derive(Clone, Debug)]
pub(super) struct LocalClientState {
    pub(super) session: ClientSession,
    pub(super) state_watch: Option<StateWatch>,
    /// Observed-watch generation this client's last snapshot covered (`include_observed`).
    pub(super) observed_generation: u64,
    pub(super) executor_watch: Option<ExecutorWatchState>,
    pub(super) provider_watch: Option<ProviderWatchState>,
    pub(super) status_watch: Option<StatusQuery>,
    pub(super) action_watch: Option<ActionQuery>,
    /// Actions this client submitted over its stream with `wait_ms`: their results are pushed to
    /// the stream until they are final.
    pub(super) action_calls: std::collections::BTreeSet<String>,
    next_event_sequence: u64,
    max_queued_events: usize,
    max_queued_bytes: usize,
    pub(super) queued_events: ClientEventQueue,
    /// Sequence reserved for a state snapshot that is built when the client next polls, instead of
    /// being copied into the queue now (see `NodeApp::materialize_pending_state_snapshot`).
    pub(super) pending_state_snapshot: Option<u64>,
    pub(super) dropped_events: u64,
    pub(super) stream_sender: Option<tokio::sync::mpsc::Sender<ControlEnvelope>>,
    pub(super) unary_metrics: CommunicationMetrics,
    pub(super) stream_metrics: CommunicationMetrics,
    pub(super) stream_peer_pid: Option<u32>,
    pub(super) stream_peer_uid: Option<u32>,
    pub(super) stream_peer_gid: Option<u32>,
    pub(super) stream_seen: bool,
    pub(super) rate_window_started_ms: u64,
    pub(super) rate_window_count: u32,
    pub(super) last_activity_ms: u64,
    /// Set for the client of a link session (`link_gateway`, Unix links): woken when an event is
    /// queued, as the link drains the queue itself. Such a client lives as long as its session.
    pub(super) link_wake: Option<std::sync::Arc<tokio::sync::Notify>>,
}

impl LocalClientState {
    pub(super) fn wants_observed(&self) -> bool {
        self.state_watch
            .as_ref()
            .is_some_and(|watch| watch.include_observed)
    }

    pub(super) fn new(
        session: ClientSession,
        now_ms: u64,
        max_queued_events: usize,
        max_queued_bytes: usize,
    ) -> Self {
        Self {
            session,
            state_watch: None,
            observed_generation: 0,
            executor_watch: None,
            provider_watch: None,
            status_watch: None,
            action_watch: None,
            action_calls: std::collections::BTreeSet::new(),
            next_event_sequence: 1,
            max_queued_events,
            max_queued_bytes,
            queued_events: ClientEventQueue::default(),
            pending_state_snapshot: None,
            dropped_events: 0,
            stream_sender: None,
            unary_metrics: CommunicationMetrics::default(),
            stream_metrics: CommunicationMetrics::default(),
            stream_peer_pid: None,
            stream_peer_uid: None,
            stream_peer_gid: None,
            stream_seen: false,
            rate_window_started_ms: now_ms,
            rate_window_count: 0,
            last_activity_ms: now_ms,
            link_wake: None,
        }
    }
}

pub(super) struct ClientRegistryTxn<'a> {
    clients: &'a mut BTreeMap<LocalAddress, LocalClientState>,
}

impl<'a> ClientRegistryTxn<'a> {
    pub(super) fn new(clients: &'a mut BTreeMap<LocalAddress, LocalClientState>) -> Self {
        Self { clients }
    }

    pub(super) fn client_mut(
        &mut self,
        source: &LocalAddress,
    ) -> Result<&mut LocalClientState, NodeError> {
        self.clients
            .get_mut(source)
            .ok_or_else(|| NodeError::UnknownClient(source.clone()))
    }

    pub(super) fn client_mut_if_present(
        &mut self,
        source: &LocalAddress,
    ) -> Option<&mut LocalClientState> {
        self.clients.get_mut(source)
    }

    pub(super) fn clients_mut(
        &mut self,
    ) -> impl Iterator<Item = (&LocalAddress, &mut LocalClientState)> {
        self.clients.iter_mut()
    }

    pub(super) fn remove(&mut self, source: &LocalAddress) -> Option<LocalClientState> {
        self.clients.remove(source)
    }

    pub(super) fn upsert_session(
        &mut self,
        source: &LocalAddress,
        session: ClientSession,
        now_ms: u64,
        max_queued_events: usize,
        max_queued_bytes: usize,
    ) {
        let state = self.clients.entry(source.clone()).or_insert_with(|| {
            LocalClientState::new(session.clone(), now_ms, max_queued_events, max_queued_bytes)
        });
        state.session = session;
        state.max_queued_events = max_queued_events;
        state.max_queued_bytes = max_queued_bytes;
        state.rate_window_started_ms = now_ms;
        state.rate_window_count = 0;
        state.last_activity_ms = now_ms;
    }
}

#[derive(Clone, Debug)]
pub(super) struct ExecutorWatchState {
    pub(super) executor_id: ExecutorId,
    /// Fingerprint of the workload list last sent to this watcher.
    pub(super) last_fingerprint: Option<ArchiveFingerprint>,
}

#[derive(Clone, Debug)]
pub(super) struct ProviderWatchState {
    pub(super) provider_id: ProviderId,
    /// Fingerprint of the lease list last sent to this watcher.
    pub(super) last_fingerprint: Option<ArchiveFingerprint>,
}

pub(super) fn enqueue_state_snapshot_event(client: &mut LocalClientState, snapshot: StateSnapshot) {
    client.pending_state_snapshot = None;
    client
        .queued_events
        .retain(|event| !matches!(event.event, ClientEventKind::StateSnapshot(_)));
    enqueue_client_event(client, ClientEventKind::StateSnapshot(Box::new(snapshot)));
}

/// Reserves the next sequence for a state snapshot that is built only when the client polls. A
/// client that is not draining its queue then holds no copy of the state, and only the newest
/// reservation counts.
pub(super) fn reserve_state_snapshot_event(client: &mut LocalClientState) {
    client
        .queued_events
        .retain(|event| !matches!(event.event, ClientEventKind::StateSnapshot(_)));
    let sequence = client.next_event_sequence;
    client.next_event_sequence = client.next_event_sequence.saturating_add(1);
    client.pending_state_snapshot = Some(sequence);
}

/// Queues the snapshot built for a reservation, at the reserved sequence.
pub(super) fn insert_state_snapshot_event(
    client: &mut LocalClientState,
    sequence: u64,
    snapshot: StateSnapshot,
) {
    let event = ClientEventKind::StateSnapshot(Box::new(snapshot));
    let bytes = event_bytes(&event);
    make_room(client, bytes);
    let position = client
        .queued_events
        .position(|queued| queued.sequence > sequence)
        .unwrap_or(client.queued_events.len());
    client
        .queued_events
        .insert(position, ClientEvent { sequence, event }, bytes);
}

/// Queues a full executor list. `encoded_bytes` is the list's archive length (see
/// [`archive_digest`]), the charge against the byte limit.
pub(super) fn enqueue_executor_workloads_event(
    client: &mut LocalClientState,
    executor_id: ExecutorId,
    workloads: Vec<WorkloadRecord>,
    encoded_bytes: usize,
) {
    enqueue_client_event_charged(
        client,
        ClientEventKind::ExecutorWorkloads {
            executor_id,
            workloads,
        },
        encoded_bytes,
    );
}

/// Queues a full provider lease list. `encoded_bytes` is as for [`enqueue_executor_workloads_event`].
pub(super) fn enqueue_provider_leases_event(
    client: &mut LocalClientState,
    provider_id: ProviderId,
    leases: Vec<LeaseRecord>,
    encoded_bytes: usize,
) {
    enqueue_client_event_charged(
        client,
        ClientEventKind::ProviderLeases {
            provider_id,
            leases,
        },
        encoded_bytes,
    );
}

/// Queues a status change, merging it into a status event that is still queued so a slow watcher
/// holds at most one status event with the newest value per key (bounded by the lane's caps).
pub(super) fn enqueue_status_event(client: &mut LocalClientState, change: StatusChange) {
    let mut updated: BTreeMap<StatusKey, StatusEntry> = BTreeMap::new();
    let mut expired: BTreeMap<StatusKey, ()> = BTreeMap::new();
    let mut bootstrap = change.bootstrap;
    if let Some(position) = client
        .queued_events
        .position(|event| matches!(event.event, ClientEventKind::Status(_)))
        && let Some(ClientEvent {
            event: ClientEventKind::Status(queued),
            ..
        }) = client.queued_events.remove(position)
    {
        bootstrap |= queued.bootstrap;
        for entry in queued.updated {
            updated.insert(entry.status_key(), entry);
        }
        for key in queued.expired {
            expired.insert(key, ());
        }
    }
    for key in change.expired {
        updated.remove(&key);
        expired.insert(key, ());
    }
    for entry in change.updated {
        let key = entry.status_key();
        expired.remove(&key);
        updated.insert(key, entry);
    }
    enqueue_client_event(
        client,
        ClientEventKind::Status(StatusChange {
            bootstrap,
            updated: updated.into_values().collect(),
            expired: expired.into_keys().collect(),
        }),
    );
}

/// Queues an action request for a handler client. Requests are never coalesced or dropped in
/// favor of newer ones of the same action; the client queue limit still applies.
pub(super) fn enqueue_action_request_event(client: &mut LocalClientState, request: ActionRequest) {
    enqueue_client_event(client, ClientEventKind::ActionRequest(Box::new(request)));
}

/// Queues action results, merging them into a results event that is still queued (newest result
/// per action), so a slow watcher holds at most one results event.
pub(super) fn enqueue_action_results_event(
    client: &mut LocalClientState,
    results: Vec<ActionResult>,
) {
    let mut merged: BTreeMap<String, ActionResult> = BTreeMap::new();
    if let Some(position) = client
        .queued_events
        .position(|event| matches!(event.event, ClientEventKind::ActionResults(_)))
        && let Some(ClientEvent {
            event: ClientEventKind::ActionResults(queued),
            ..
        }) = client.queued_events.remove(position)
    {
        for result in queued {
            merged.insert(result.action_id.clone(), result);
        }
    }
    for result in results {
        merged.insert(result.action_id.clone(), result);
    }
    enqueue_client_event(
        client,
        ClientEventKind::ActionResults(merged.into_values().collect()),
    );
}

/// Queued samples of one stream past which a subscriber that does not keep up gets a reset
/// instead (it resubscribes from its cursor).
const MAX_MERGED_STREAM_BYTES: usize = 1024 * 1024;

/// Queues a stream event. Live batches of one stream (and epoch) merge into the one still
/// queued, so a slow subscriber holds one growing event per stream rather than losing older
/// batches to the queue limit; past [`MAX_MERGED_STREAM_BYTES`] its queued samples are dropped for
/// a `StreamReset { reason: Overrun }`. A newer demand replaces a queued one.
pub(super) fn enqueue_stream_event(client: &mut LocalClientState, event: ClientEventKind) {
    match event {
        ClientEventKind::StreamData(data) if !data.backfill => {
            let queued = client.queued_events.position(|queued| {
                matches!(&queued.event, ClientEventKind::StreamData(q)
                    if !q.backfill && q.stream == data.stream && q.cursor.epoch == data.cursor.epoch)
            });
            if let Some(position) = queued
                && let Some(ClientEvent {
                    event: ClientEventKind::StreamData(merged),
                    ..
                }) = client.queued_events.event_mut(position)
            {
                merged.cursor = data.cursor;
                merged.chunks.extend(data.chunks);
                let bytes = stream_data_bytes(merged);
                if bytes > MAX_MERGED_STREAM_BYTES {
                    let stream = merged.stream.clone();
                    client.queued_events.remove(position);
                    client.dropped_events = client.dropped_events.saturating_add(1);
                    enqueue_client_event(
                        client,
                        ClientEventKind::StreamReset(orion::control_plane::StreamReset {
                            stream,
                            reason: orion::control_plane::StreamResetReason::Overrun,
                        }),
                    );
                } else {
                    client.queued_events.recharge(position, bytes);
                }
                return;
            }
            enqueue_client_event(client, ClientEventKind::StreamData(data));
        }
        ClientEventKind::StreamDemand(demand) => {
            client.queued_events.retain(|queued| {
                !matches!(&queued.event, ClientEventKind::StreamDemand(q) if q.stream == demand.stream)
            });
            enqueue_client_event(client, ClientEventKind::StreamDemand(demand));
        }
        other => enqueue_client_event(client, other),
    }
}

/// Drops the oldest queued events until one more event charged `bytes` fits both the event limit
/// and the byte limit, and counts each drop. The newest event is never dropped to make room, so a
/// single event larger than the byte limit is queued alone.
fn make_room(client: &mut LocalClientState, bytes: usize) {
    while !client.queued_events.is_empty()
        && (client.queued_events.len() >= client.max_queued_events
            || client.queued_events.bytes().saturating_add(bytes) > client.max_queued_bytes)
    {
        client.queued_events.pop_front();
        client.dropped_events = client.dropped_events.saturating_add(1);
    }
}

fn enqueue_client_event(client: &mut LocalClientState, event: ClientEventKind) {
    let bytes = event_bytes(&event);
    enqueue_client_event_charged(client, event, bytes);
}

fn enqueue_client_event_charged(
    client: &mut LocalClientState,
    event: ClientEventKind,
    bytes: usize,
) {
    make_room(client, bytes);
    let sequence = client.next_event_sequence;
    client.next_event_sequence = client.next_event_sequence.saturating_add(1);
    client
        .queued_events
        .push_back(ClientEvent { sequence, event }, bytes);
    if let Some(wake) = &client.link_wake {
        wake.notify_one();
    }
}

pub(super) fn prepare_client_stream_flush(
    source: &LocalAddress,
    client: &LocalClientState,
) -> Option<PendingClientStreamFlush> {
    let sender = client.stream_sender.clone()?;
    if client.queued_events.is_empty() {
        return None;
    }
    let events: Vec<_> = client.queued_events.iter().cloned().collect();
    let last_sequence = client.queued_events.back()?.sequence;
    let envelope = ControlEnvelope {
        source: LocalAddress::new("orion"),
        destination: source.clone(),
        message: orion::control_plane::ControlMessage::ClientEvents(events),
    };
    Some(PendingClientStreamFlush {
        source: source.clone(),
        sender,
        last_sequence,
        envelope: Some(envelope),
    })
}

/// Sends the prepared envelope. The envelope is moved into the channel, so the event batch is
/// not copied a second time; the queue itself is only trimmed by `finalize_client_stream_flush`.
pub(super) fn execute_client_stream_flush(flush: &mut PendingClientStreamFlush) -> bool {
    match flush.envelope.take() {
        Some(envelope) => flush.sender.try_send(envelope).is_ok(),
        None => false,
    }
}

pub(super) fn finalize_client_stream_flush(
    client: &mut LocalClientState,
    flush: &PendingClientStreamFlush,
    delivered: bool,
) {
    if delivered {
        while client
            .queued_events
            .front()
            .map(|event| event.sequence <= flush.last_sequence)
            .unwrap_or(false)
        {
            client.queued_events.pop_front();
        }
    } else if client
        .stream_sender
        .as_ref()
        .map(|sender| sender.same_channel(&flush.sender))
        .unwrap_or(false)
    {
        client.stream_sender = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion::control_plane::ClientRole;
    use orion_core::{ClientName, NodeId, SessionId};

    fn client(max_events: usize, max_bytes: usize) -> LocalClientState {
        let session = ClientSession {
            session_id: SessionId::new("node-a:test"),
            client_name: ClientName::new("watcher"),
            role: ClientRole::Executor,
            node_id: NodeId::new("node-a"),
            source: "test".to_owned(),
        };
        LocalClientState::new(session, 0, max_events, max_bytes)
    }

    fn enqueue_list(client: &mut LocalClientState, charged: usize) {
        enqueue_executor_workloads_event(
            client,
            ExecutorId::new("executor.test"),
            Vec::new(),
            charged,
        );
    }

    fn sequences(client: &LocalClientState) -> Vec<u64> {
        client
            .queued_events
            .iter()
            .map(|event| event.sequence)
            .collect()
    }

    #[test]
    fn byte_limit_drops_the_oldest_lists_and_keeps_the_newest() {
        let mut client = client(256, 1000);
        for _ in 0..10 {
            enqueue_list(&mut client, 300);
        }
        // Three 300-byte lists fit in 1000 bytes; the other seven were dropped, oldest first.
        assert_eq!(sequences(&client), vec![8, 9, 10]);
        assert_eq!(client.queued_events.bytes(), 900);
        assert_eq!(client.dropped_events, 7);
    }

    #[test]
    fn a_slow_watcher_holds_at_most_the_byte_limit_of_full_lists() {
        // 300 lists of 5000 encoded bytes against the 1 MiB default: the count limit (256) is
        // never the one that bites, the byte limit is.
        let limit = 1024 * 1024;
        let mut client = client(256, limit);
        for _ in 0..300 {
            enqueue_list(&mut client, 5000);
        }
        assert!(client.queued_events.bytes() <= limit);
        assert_eq!(client.queued_events.len(), limit / 5000);
        assert_eq!(client.queued_events.bytes(), (limit / 5000) * 5000);
        assert_eq!(client.dropped_events, 300 - (limit / 5000) as u64);
    }

    #[test]
    fn count_limit_still_applies_without_byte_pressure() {
        let mut client = client(2, 1 << 30);
        for _ in 0..5 {
            enqueue_list(&mut client, 10);
        }
        assert_eq!(sequences(&client), vec![4, 5]);
        assert_eq!(client.queued_events.bytes(), 20);
        assert_eq!(client.dropped_events, 3);
    }

    #[test]
    fn an_event_larger_than_the_byte_limit_is_kept_alone() {
        let mut client = client(256, 1000);
        enqueue_list(&mut client, 100);
        enqueue_list(&mut client, 5000);
        // The newest event is never dropped to make room for itself.
        assert_eq!(sequences(&client), vec![2]);
        assert_eq!(client.queued_events.bytes(), 5000);
        assert_eq!(client.dropped_events, 1);
        enqueue_list(&mut client, 10);
        assert_eq!(sequences(&client), vec![3]);
        assert_eq!(client.queued_events.bytes(), 10);
    }

    #[test]
    fn every_removal_keeps_the_byte_total_exact() {
        let mut queue = ClientEventQueue::default();
        let event = |sequence| ClientEvent {
            sequence,
            event: ClientEventKind::ExecutorWorkloads {
                executor_id: ExecutorId::new("executor.test"),
                workloads: Vec::new(),
            },
        };
        queue.push_back(event(1), 10);
        queue.push_back(event(2), 20);
        queue.insert(1, event(3), 30);
        assert_eq!(queue.bytes(), 60);
        assert_eq!(queue.remove(1).map(|e| e.sequence), Some(3));
        assert_eq!(queue.bytes(), 30);
        queue.recharge(0, 5);
        assert_eq!(queue.bytes(), 25);
        assert_eq!(queue.pop_front().map(|e| e.sequence), Some(1));
        assert_eq!(queue.bytes(), 20);
        queue.retain(|_| false);
        assert_eq!((queue.len(), queue.bytes()), (0, 0));
    }

    #[test]
    fn list_charges_are_the_encoded_length_of_the_list() {
        let digest = archive_digest(&Vec::<u64>::new()).expect("encodes");
        let bytes = orion::encode_to_vec(&Vec::<u64>::new()).expect("encodes");
        assert_eq!(digest_bytes(Some(digest)), bytes.len());
        assert_eq!(digest_bytes(None), UNMEASURED_EVENT_BYTES);
    }
}
