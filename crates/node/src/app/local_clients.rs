use super::{CommunicationMetrics, NodeError};
use orion::{
    ExecutorId, ProviderId,
    control_plane::{
        ClientEvent, ClientEventKind, ClientSession, LeaseRecord, StateSnapshot, StateWatch,
        StatusChange, StatusEntry, StatusKey, StatusQuery, WorkloadRecord,
    },
    transport::ipc::{ControlEnvelope, LocalAddress},
};
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone)]
pub(super) struct PendingClientStreamFlush {
    pub(super) source: LocalAddress,
    sender: tokio::sync::mpsc::Sender<ControlEnvelope>,
    last_sequence: u64,
    envelope: ControlEnvelope,
}

#[derive(Clone, Debug)]
pub(super) struct LocalClientState {
    pub(super) session: ClientSession,
    pub(super) state_watch: Option<StateWatch>,
    pub(super) executor_watch: Option<ExecutorWatchState>,
    pub(super) provider_watch: Option<ProviderWatchState>,
    pub(super) status_watch: Option<StatusQuery>,
    next_event_sequence: u64,
    max_queued_events: usize,
    pub(super) queued_events: VecDeque<ClientEvent>,
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
}

impl LocalClientState {
    pub(super) fn new(session: ClientSession, now_ms: u64, max_queued_events: usize) -> Self {
        Self {
            session,
            state_watch: None,
            executor_watch: None,
            provider_watch: None,
            status_watch: None,
            next_event_sequence: 1,
            max_queued_events,
            queued_events: VecDeque::new(),
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
    ) {
        let state = self
            .clients
            .entry(source.clone())
            .or_insert_with(|| LocalClientState::new(session.clone(), now_ms, max_queued_events));
        state.session = session;
        state.max_queued_events = max_queued_events;
        state.rate_window_started_ms = now_ms;
        state.rate_window_count = 0;
        state.last_activity_ms = now_ms;
    }
}

#[derive(Clone, Debug)]
pub(super) struct ExecutorWatchState {
    pub(super) executor_id: ExecutorId,
    pub(super) last_workloads: Vec<WorkloadRecord>,
}

#[derive(Clone, Debug)]
pub(super) struct ProviderWatchState {
    pub(super) provider_id: ProviderId,
    pub(super) last_leases: Vec<LeaseRecord>,
}

pub(super) fn enqueue_state_snapshot_event(client: &mut LocalClientState, snapshot: StateSnapshot) {
    client
        .queued_events
        .retain(|event| !matches!(event.event, ClientEventKind::StateSnapshot(_)));
    enqueue_client_event(client, ClientEventKind::StateSnapshot(Box::new(snapshot)));
}

pub(super) fn enqueue_executor_workloads_event(
    client: &mut LocalClientState,
    executor_id: ExecutorId,
    workloads: Vec<WorkloadRecord>,
) {
    enqueue_client_event(
        client,
        ClientEventKind::ExecutorWorkloads {
            executor_id,
            workloads,
        },
    );
}

pub(super) fn enqueue_provider_leases_event(
    client: &mut LocalClientState,
    provider_id: ProviderId,
    leases: Vec<LeaseRecord>,
) {
    enqueue_client_event(
        client,
        ClientEventKind::ProviderLeases {
            provider_id,
            leases,
        },
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
        .iter()
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

fn enqueue_client_event(client: &mut LocalClientState, event: ClientEventKind) {
    while client.queued_events.len() >= client.max_queued_events {
        client.queued_events.pop_front();
        client.dropped_events = client.dropped_events.saturating_add(1);
    }
    let sequence = client.next_event_sequence;
    client.next_event_sequence = client.next_event_sequence.saturating_add(1);
    client
        .queued_events
        .push_back(ClientEvent { sequence, event });
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
        envelope,
    })
}

pub(super) fn execute_client_stream_flush(flush: &PendingClientStreamFlush) -> bool {
    flush.sender.try_send(flush.envelope.clone()).is_ok()
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
