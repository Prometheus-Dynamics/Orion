//! The client role on a Unix link's sessions (interface revision 1,
//! `docs/design/stable-interface.md`, "Step 3 wire").
//!
//! A session that takes the role is a local client of the node (`NodeApp::register_link_client`),
//! named `link:<link>/<device name>`, with the caller's peer identity: its requests go through the
//! control pipeline, so the control sockets' authorization and rate limit apply. Answers wait here
//! while the session's output is full, and the node's events for the client stay in its queue
//! (status coalesced to the newest value per key) until they have room, so a slow client costs
//! one bounded queue, not unbounded memory.
//!
//! Limits per session: one status watch, one action watch, [`MAX_OPEN_ACTIONS`] actions run by
//! the session that are not yet final, and [`MAX_SUBSCRIPTIONS`] stream subscriptions.
//!
//! Streams (revision 2, "Step 4 wire"): a provider session's declarations and batches are its
//! provider's streams, published under the same local address (registered on its first
//! declaration when the session has no client); subscriptions go through the control pipeline as
//! the other client requests. Chunks larger than a frame are split at sample boundaries here.

use crate::NodeApp;
use crate::NodeError;
use crate::app::{LinkCaller, LinkWatch};
use orion::control_plane::{
    ActionQuery, ActionResult, ClientEvent, ClientEventKind, ControlMessage, SampleChunk,
    SeriesDecl, SeriesKind, StatusEntry, StatusKey, StreamRef, StreamResetReason,
    StreamSubscription, TypedConfigValue,
};
use orion::transport::ipc::LocalAddress;
use orion_link::Stream;
use orion_link::host::{ActionSendError, ClientRequest, HostSession};
use orion_link::message::{self as lm, ErrorCode, ExecutorState, Roles};

use super::unix_executor::{ExecutorLink, RecordChange, RecordWatch, WorkloadChange};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::debug;

/// Actions a session may have run that are not yet final.
pub(super) const MAX_OPEN_ACTIONS: usize = 16;
/// Stream subscriptions per session.
pub(super) const MAX_SUBSCRIPTIONS: usize = 8;
/// Node events taken from the client's queue at a time.
const EVENTS_PER_DRAIN: usize = 32;
/// Drains per pump, so one busy client does not hold the link task.
const DRAINS_PER_PUMP: usize = 4;
/// Bytes of answers a session may have waiting for its output. Past it, queries are refused
/// with `limit` and the connection is not read until the answers drain below it.
pub(super) const ANSWER_BYTES: usize = 256 * 1024;
/// Bytes charged per record or message of an answer, on top of its strings and data.
const RECORD_BYTES: usize = 64;

/// An answer waiting for room in the session's output.
#[derive(Debug)]
enum Answer {
    Status {
        req: u32,
        bootstrap: bool,
        updated: Vec<StatusEntry>,
        expired: Vec<StatusKey>,
    },
    Actions {
        req: u32,
        results: Vec<ActionResult>,
    },
    Error {
        req: u32,
        code: ErrorCode,
        reason: String,
    },
    Catalog {
        req: u32,
        epoch: u64,
        series: Vec<SeriesDecl>,
        live: bool,
    },
    Data(lm::StreamData),
    Reset {
        req: u32,
        reason: StreamResetReason,
    },
    /// To the provider: the demand for one of its streams (no request).
    Demand {
        id: String,
        subscribers: u32,
        series: Vec<u16>,
    },
    /// To the executor: its workloads (no request).
    Workloads(WorkloadChange),
    Records {
        req: u32,
        change: RecordChange,
    },
}

impl Answer {
    /// The request answered; 0 for a provider's demand.
    fn req(&self) -> u32 {
        match self {
            Self::Status { req, .. }
            | Self::Actions { req, .. }
            | Self::Error { req, .. }
            | Self::Catalog { req, .. }
            | Self::Reset { req, .. }
            | Self::Records { req, .. } => *req,
            Self::Data(data) => data.req,
            Self::Demand { .. } | Self::Workloads(_) => 0,
        }
    }
}

fn value_bytes(value: &TypedConfigValue) -> usize {
    match value {
        TypedConfigValue::String(text) => text.len(),
        TypedConfigValue::Bytes(bytes) => bytes.len(),
        _ => 0,
    }
}

impl Answer {
    /// The bytes this answer is charged while it waits: an estimate of what it holds.
    fn bytes(&self) -> usize {
        let records = match self {
            Self::Status {
                updated, expired, ..
            } => {
                let updated = updated
                    .iter()
                    .map(|entry| RECORD_BYTES + entry.key.len() + value_bytes(&entry.value));
                let expired = expired.iter().map(|key| RECORD_BYTES + key.key.len());
                updated.chain(expired).sum()
            }
            Self::Actions { results, .. } => results
                .iter()
                .map(|result| {
                    RECORD_BYTES
                        + result.action_id.len()
                        + result.name.len()
                        + result
                            .output
                            .iter()
                            .map(|(key, value)| RECORD_BYTES + key.len() + value_bytes(value))
                            .sum::<usize>()
                })
                .sum(),
            Self::Error { reason, .. } => reason.len(),
            Self::Catalog { series, .. } => series.len() * RECORD_BYTES,
            Self::Data(data) => data
                .chunks
                .iter()
                .map(|chunk| RECORD_BYTES + chunk.data.len())
                .sum(),
            Self::Reset { .. } => 0,
            Self::Demand { series, .. } => series.len() * 2,
            // Workloads and records carry configs and lists: a flat charge each.
            Self::Workloads(change) => {
                (change.workloads.len() + change.removed.len()) * 4 * RECORD_BYTES
            }
            Self::Records { change, .. } => {
                (change.updated.len() + change.removed.len()) * 4 * RECORD_BYTES
            }
        };
        RECORD_BYTES + records
    }
}

/// The session's waiting answers, in order, with the bytes each is charged; `bytes` is their
/// total and stays exact through every change.
#[derive(Debug, Default)]
struct Answers {
    items: VecDeque<(Answer, usize)>,
    bytes: usize,
}

impl Answers {
    fn push_back(&mut self, answer: Answer) {
        let bytes = answer.bytes();
        self.bytes += bytes;
        self.items.push_back((answer, bytes));
    }

    fn push_front(&mut self, answer: Answer) {
        let bytes = answer.bytes();
        self.bytes += bytes;
        self.items.push_front((answer, bytes));
    }

    fn front(&self) -> Option<&Answer> {
        self.items.front().map(|(answer, _)| answer)
    }

    fn pop_front(&mut self) {
        if let Some((_, bytes)) = self.items.pop_front() {
            self.bytes -= bytes;
        }
    }

    fn retain(&mut self, mut keep: impl FnMut(&Answer) -> bool) {
        let bytes = &mut self.bytes;
        self.items.retain(|(answer, charged)| {
            let kept = keep(answer);
            if !kept {
                *bytes -= *charged;
            }
            kept
        });
    }

    fn clear(&mut self) {
        self.items.clear();
        self.bytes = 0;
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// A stream subscription of the session.
struct Subscription {
    req: u32,
    /// The series' kinds, from the latest catalog, to split chunks larger than a frame.
    kinds: Vec<SeriesKind>,
}

/// The client of one session.
pub(super) struct LinkClient {
    source: LocalAddress,
    link: String,
    device: String,
    caller: LinkCaller,
    wake: Arc<Notify>,
    /// Whether a local client is registered at `source` (the session's client, or its streams'
    /// publisher).
    registered: bool,
    /// Why the client could not be registered; every request is refused with it.
    refused: Option<(ErrorCode, String)>,
    status_watch: Option<u32>,
    action_watch: Option<(u32, ActionQuery)>,
    /// The action watch's first answer (the matching results, possibly none) is still to come.
    action_watch_first: bool,
    /// Actions run by the session that are not final yet, and the request each answers.
    runs: BTreeMap<String, u32>,
    subscriptions: BTreeMap<StreamRef, Subscription>,
    /// Streams the provider session declared, by id.
    declared: BTreeMap<String, StreamRef>,
    /// The session's executor, when it took the executor role (revision 3).
    executor: Option<ExecutorLink>,
    record_watch: Option<RecordWatch>,
    answers: Answers,
}

/// The error code for a request the node refused.
pub(super) fn code_for(error: &NodeError) -> ErrorCode {
    match error {
        NodeError::Authorization(_)
        | NodeError::ClientRoleMismatch { .. }
        | NodeError::AuthenticatedPeerRequired { .. }
        | NodeError::ConfiguredPeerRequired { .. } => ErrorCode::NOT_AUTHORIZED,
        NodeError::RateLimitedClient { .. } => ErrorCode::LIMIT,
        NodeError::Action(_) => ErrorCode::INVALID,
        _ => ErrorCode::INTERNAL,
    }
}

impl LinkClient {
    /// Opens the session of `device` on `link`, and registers its client when the session took
    /// the client role.
    pub(super) fn open(
        app: &NodeApp,
        link: &str,
        device: &str,
        caller: LinkCaller,
        wake: Arc<Notify>,
        roles: Roles,
    ) -> Self {
        let client_role = roles.contains(Roles::CLIENT);
        // Only Unix links serve the executor role (`HostConfig::serve_devices`).
        let executor = match &caller {
            LinkCaller::Local(identity) if roles.contains(Roles::EXECUTOR) => Some(
                ExecutorLink::open(app, link, device, identity, wake.clone()),
            ),
            _ => None,
        };
        let name = format!("link:{link}/{device}");
        let source = match &caller {
            LinkCaller::Local(_) => LocalAddress::new(name.clone()),
            // Under the remote sessions' reserved prefix: nothing on the local socket may speak
            // as an operator's client.
            LinkCaller::Operator { principal, .. } => LocalAddress::new(format!(
                "{}{principal}/{name}",
                orion::control_plane::REMOTE_SESSION_PREFIX
            )),
        };
        let refused = if client_role {
            app.register_link_client(&source, &name, &caller, wake.clone())
                .err()
                .map(|error| {
                    debug!(link, device, %error, "link client not registered; its requests are refused");
                    (code_for(&error), error.to_string())
                })
        } else {
            None
        };
        Self {
            source,
            link: link.to_owned(),
            device: device.to_owned(),
            caller,
            wake,
            registered: client_role && refused.is_none(),
            refused,
            status_watch: None,
            action_watch: None,
            action_watch_first: false,
            runs: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
            declared: BTreeMap::new(),
            executor,
            record_watch: None,
            answers: Answers::default(),
        }
    }

    /// Removes the client from the node (its session ended).
    pub(super) fn close(self, app: &NodeApp) {
        if self.registered {
            app.remove_link_client(&self.source);
        }
        if let Some(executor) = self.executor {
            executor.close(app);
        }
    }

    /// An executor session's `ExecutorState`.
    pub(super) fn executor_state(&mut self, app: &NodeApp, state: ExecutorState) {
        if let (Some(executor), LinkCaller::Local(identity)) =
            (self.executor.as_mut(), &self.caller)
        {
            executor.state(app, identity, state);
        }
    }

    /// A provider session's `StreamDeclare`.
    pub(super) fn declare(&mut self, app: &NodeApp, req: u32, id: String, series: Vec<SeriesDecl>) {
        if !self.registered {
            let name = self.source.as_str().to_owned();
            app.register_link_publisher(&self.source, &name, self.wake.clone());
            self.registered = true;
        }
        match app.declare_link_stream(&self.source, &self.link, &self.device, id.clone(), series) {
            // The first demand answers it.
            Ok(stream) => {
                self.declared.insert(id, stream);
            }
            Err(error) => {
                let code = match &error {
                    NodeError::Status(_) => ErrorCode::INVALID,
                    other => code_for(other),
                };
                self.refuse(req, code, error.to_string());
            }
        }
    }

    /// A provider session's `StreamPublish`: dropped (and logged) when refused.
    pub(super) fn publish(&mut self, app: &NodeApp, id: &str, chunks: Vec<SampleChunk>) {
        let Some(stream) = self.declared.get(id) else {
            debug!(link = %self.link, device = %self.device, id, "batch for a stream the session did not declare");
            return;
        };
        if let Err(error) = app.publish_link_stream(&self.source, stream.clone(), chunks) {
            debug!(link = %self.link, device = %self.device, id, %error, "stream batch dropped");
        }
    }

    fn refuse(&mut self, req: u32, code: ErrorCode, reason: impl Into<String>) {
        self.answers.push_back(Answer::Error {
            req,
            code,
            reason: reason.into(),
        });
    }

    fn serve(
        &mut self,
        app: &NodeApp,
        req: u32,
        message: ControlMessage,
    ) -> Option<ControlMessage> {
        match app.serve_link_client_message(&self.source, &self.caller, message) {
            Ok(response) => Some(response),
            Err(error) => {
                self.refuse(req, code_for(&error), error.to_string());
                None
            }
        }
    }

    /// Handles one request of the session.
    pub(super) fn request(&mut self, app: &NodeApp, request: ClientRequest) {
        if let ClientRequest::End { req } = request {
            self.end(app, req);
            return;
        }
        let req = request_id(&request);
        if let Some((code, reason)) = self.refused.clone() {
            self.refuse(req, code, reason);
            return;
        }
        // Queries copy their whole answer into the queue; past the cap they wait for the client
        // to read what it already asked for. Watches, runs and subscriptions answer through the
        // node's event queue, which has its own byte cap and is drained only once these are out.
        if matches!(
            request,
            ClientRequest::StatusQuery { .. }
                | ClientRequest::ActionQuery { .. }
                | ClientRequest::RecordQuery { .. }
        ) && self.answers.bytes >= ANSWER_BYTES
        {
            self.refuse(
                req,
                ErrorCode::LIMIT,
                "answers to earlier requests are still waiting to be read",
            );
            return;
        }
        match request {
            ClientRequest::StatusQuery { req, filter } => {
                if let Some(ControlMessage::Status(entries)) =
                    self.serve(app, req, ControlMessage::QueryStatus(filter))
                {
                    self.answers.push_back(Answer::Status {
                        req,
                        bootstrap: true,
                        updated: entries,
                        expired: Vec::new(),
                    });
                }
            }
            ClientRequest::StatusWatch { req, filter } => {
                if self.status_watch.is_some() {
                    self.refuse(
                        req,
                        ErrorCode::LIMIT,
                        "one status watch per session; end it or open another connection",
                    );
                } else if self
                    .serve(app, req, ControlMessage::WatchStatus(filter))
                    .is_some()
                {
                    // The bootstrap arrives as the client's first status event.
                    self.status_watch = Some(req);
                }
            }
            ClientRequest::ActionRun { req, request } => self.run(app, req, *request),
            ClientRequest::RecordQuery { req, .. } | ClientRequest::RecordWatch { req, .. }
                if let Err(error) = app.authorize_link_read(&self.source, &self.caller) =>
            {
                self.refuse(req, code_for(&error), error.to_string());
            }
            ClientRequest::RecordQuery { req, filter } => {
                if let Some(change) = RecordWatch::new(req, filter).next_change(app) {
                    self.answers.push_back(Answer::Records { req, change });
                }
            }
            ClientRequest::RecordWatch { req, filter } => {
                if self.record_watch.is_some() {
                    self.refuse(
                        req,
                        ErrorCode::LIMIT,
                        "one record watch per session; end it or open another connection",
                    );
                } else {
                    // The bootstrap goes out with the next pump.
                    self.record_watch = Some(RecordWatch::new(req, filter));
                }
            }
            ClientRequest::StreamSubscribe { req, subscription } => {
                self.subscribe(app, req, *subscription);
            }
            ClientRequest::ActionQuery { req, filter } => {
                if let Some(ControlMessage::ActionResults(results)) =
                    self.serve(app, req, ControlMessage::QueryActions(filter))
                {
                    self.answers.push_back(Answer::Actions { req, results });
                }
            }
            ClientRequest::ActionWatch { req, filter } => {
                if self.action_watch.is_some() {
                    self.refuse(
                        req,
                        ErrorCode::LIMIT,
                        "one action watch per session; end it or open another connection",
                    );
                } else if self
                    .serve(app, req, ControlMessage::WatchActions(filter.clone()))
                    .is_some()
                {
                    // The matching results so far arrive as the client's next action event.
                    self.action_watch = Some((req, filter));
                    self.action_watch_first = true;
                }
            }
            // Handled above; a request kind added later is refused until it is served.
            ClientRequest::End { .. } | _ => {
                self.refuse(req, ErrorCode::UNSUPPORTED, "request not served");
            }
        }
    }

    fn subscribe(&mut self, app: &NodeApp, req: u32, subscription: StreamSubscription) {
        if let Some(existing) = self.subscriptions.get(&subscription.stream) {
            let reason = format!(
                "already subscribed to this stream (request {}); end it first",
                existing.req
            );
            self.refuse(req, ErrorCode::LIMIT, reason);
            return;
        }
        if self.subscriptions.len() >= MAX_SUBSCRIPTIONS {
            let reason = format!("at most {MAX_SUBSCRIPTIONS} stream subscriptions per session");
            self.refuse(req, ErrorCode::LIMIT, reason);
            return;
        }
        let stream = subscription.stream.clone();
        if self
            .serve(app, req, ControlMessage::SubscribeStream(subscription))
            .is_some()
        {
            // The catalog and backfill arrive as the client's events.
            self.subscriptions.insert(
                stream,
                Subscription {
                    req,
                    kinds: Vec::new(),
                },
            );
        }
    }

    fn run(&mut self, app: &NodeApp, req: u32, request: orion::control_plane::ActionRequest) {
        let action_id = request.action_id.clone();
        if self.runs.len() >= MAX_OPEN_ACTIONS && !self.runs.contains_key(&action_id) {
            self.refuse(
                req,
                ErrorCode::LIMIT,
                format!("at most {MAX_OPEN_ACTIONS} unfinished actions per session"),
            );
            return;
        }
        let Some(response) = self.serve(app, req, ControlMessage::RunAction(Box::new(request)))
        else {
            return;
        };
        let ControlMessage::ActionResults(results) = &response else {
            self.refuse(req, ErrorCode::INTERNAL, "unexpected answer to RunAction");
            return;
        };
        if results
            .iter()
            .any(|result| result.action_id == action_id && !result.state.is_terminal())
        {
            self.runs.insert(action_id.clone(), req);
            // Updates come as the client's events until the result is final.
            app.register_stream_action_call(&self.source, Some((action_id, 0)), &response);
        }
        if let ControlMessage::ActionResults(results) = response {
            self.answers.push_back(Answer::Actions { req, results });
        }
    }

    fn end(&mut self, app: &NodeApp, req: u32) {
        if self
            .record_watch
            .as_ref()
            .is_some_and(|watch| watch.req == req)
        {
            self.record_watch = None;
        } else if self.status_watch == Some(req) {
            self.status_watch = None;
            app.end_link_client_watch(&self.source, LinkWatch::Status);
        } else if self
            .action_watch
            .as_ref()
            .is_some_and(|(watch, _)| *watch == req)
        {
            self.action_watch = None;
            app.end_link_client_watch(&self.source, LinkWatch::Actions);
        } else if let Some(stream) = self
            .subscriptions
            .iter()
            .find_map(|(stream, sub)| (sub.req == req).then(|| stream.clone()))
        {
            self.subscriptions.remove(&stream);
            let _ = app.serve_link_client_message(
                &self.source,
                &self.caller,
                ControlMessage::UnsubscribeStream(stream),
            );
        } else {
            return;
        }
        self.answers.retain(|answer| answer.req() != req);
    }

    fn on_event(&mut self, event: ClientEvent, max_chunk: usize) {
        match event.event {
            ClientEventKind::StreamCatalog(catalog) => {
                if let Some(sub) = self.subscriptions.get_mut(&catalog.stream) {
                    sub.kinds = catalog.series.iter().map(|series| series.kind).collect();
                    self.answers.push_back(Answer::Catalog {
                        req: sub.req,
                        epoch: catalog.epoch,
                        series: catalog.series,
                        live: catalog.live,
                    });
                }
            }
            ClientEventKind::StreamData(data) => {
                if let Some(sub) = self.subscriptions.get(&data.stream) {
                    let chunks = split_chunks(data.chunks, &sub.kinds, max_chunk);
                    self.answers.push_back(Answer::Data(lm::StreamData {
                        req: sub.req,
                        cursor: data.cursor,
                        prev_seq: data.prev_seq,
                        backfill: data.backfill,
                        more: false,
                        chunks,
                    }));
                }
            }
            ClientEventKind::StreamReset(reset) => {
                if let Some(sub) = self.subscriptions.get(&reset.stream) {
                    self.answers.push_back(Answer::Reset {
                        req: sub.req,
                        reason: reset.reason,
                    });
                }
            }
            ClientEventKind::StreamDemand(demand) => {
                if self.declared.get(&demand.stream.id) == Some(&demand.stream) {
                    self.answers.push_back(Answer::Demand {
                        id: demand.stream.id,
                        subscribers: demand.subscribers,
                        series: demand.series,
                    });
                }
            }
            ClientEventKind::Status(change) => {
                if let Some(req) = self.status_watch {
                    self.answers.push_back(Answer::Status {
                        req,
                        bootstrap: change.bootstrap,
                        updated: change.updated,
                        expired: change.expired,
                    });
                }
            }
            ClientEventKind::ActionResults(results) => {
                let mut by_req: BTreeMap<u32, Vec<ActionResult>> = BTreeMap::new();
                if let Some((req, _)) = &self.action_watch
                    && std::mem::take(&mut self.action_watch_first)
                {
                    by_req.insert(*req, Vec::new());
                }
                for result in results {
                    if let Some((req, filter)) = &self.action_watch
                        && filter.matches(&result)
                    {
                        by_req.entry(*req).or_default().push(result.clone());
                    }
                    if let Some(&req) = self.runs.get(&result.action_id) {
                        if result.state.is_terminal() {
                            self.runs.remove(&result.action_id);
                        }
                        by_req.entry(req).or_default().push(result);
                    }
                }
                for (req, results) in by_req {
                    self.answers.push_back(Answer::Actions { req, results });
                }
            }
            _ => {}
        }
    }

    /// Bytes of the answers waiting for the session's output.
    pub(super) fn answer_bytes(&self) -> usize {
        self.answers.bytes
    }

    /// Queues waiting answers while the session's output has room, then takes the node's new
    /// events for the client. Returns whether work is left (answers that did not fit, or events
    /// past this pump's share): the caller pumps again once frames have moved on.
    pub(super) fn pump(&mut self, app: &NodeApp, session: &mut HostSession<Stream>) -> bool {
        // Executor workloads and record changes are computed only once earlier answers are out,
        // so neither piles up behind a slow reader.
        if self.answers.is_empty() {
            if let Some(change) = self.executor.as_mut().and_then(|e| e.next_change(app)) {
                self.answers.push_back(Answer::Workloads(change));
            }
            if let Some(watch) = self.record_watch.as_mut()
                && let Some(change) = watch.next_change(app)
            {
                let req = watch.req;
                self.answers.push_back(Answer::Records { req, change });
            }
        }
        if !self.registered {
            self.flush(app, session);
            return !self.answers.is_empty();
        }
        let max_chunk = lm::max_chunk_data(session.max_frame().unwrap_or(0));
        for _ in 0..DRAINS_PER_PUMP {
            self.flush(app, session);
            if !self.answers.is_empty() {
                return true;
            }
            let events = app.take_link_client_events(&self.source, EVENTS_PER_DRAIN);
            if events.is_empty() {
                return false;
            }
            for event in events {
                self.on_event(event, max_chunk);
            }
        }
        self.flush(app, session);
        true
    }

    fn flush(&mut self, app: &NodeApp, session: &mut HostSession<Stream>) {
        while let Some(answer) = self.answers.front() {
            let sent = match answer {
                Answer::Status {
                    req,
                    bootstrap,
                    updated,
                    expired,
                } => session.send_status_changes(*req, *bootstrap, updated, expired),
                Answer::Actions { req, results } => session.send_action_results(*req, results),
                Answer::Error { req, code, reason } => {
                    session.send_request_error(*req, *code, reason)
                }
                Answer::Catalog {
                    req,
                    epoch,
                    series,
                    live,
                } => session.send_stream_catalog(*req, *epoch, series, *live),
                Answer::Data(data) => session.send_stream_data(data),
                Answer::Reset { req, reason } => session.send_stream_reset(*req, *reason),
                Answer::Demand {
                    id,
                    subscribers,
                    series,
                } => session.send_stream_demand(id, *subscribers, series),
                Answer::Workloads(change) => {
                    session.send_workloads(change.bootstrap, &change.workloads, &change.removed)
                }
                Answer::Records { req, change } => {
                    session.send_records(*req, change.bootstrap, &change.updated, &change.removed)
                }
            };
            match sent {
                Ok(()) => {
                    self.answers.pop_front();
                }
                Err(ActionSendError::Congested) => return,
                Err(ActionSendError::TooLarge { .. })
                    if matches!(answer, Answer::Demand { .. }) =>
                {
                    // Too many series to list in one frame: the provider keeps sampling them all.
                    self.answers.pop_front();
                }
                Err(ActionSendError::TooLarge { max_frame, .. })
                    if matches!(answer, Answer::Workloads(_)) =>
                {
                    // A workload record larger than a frame cannot reach the executor.
                    debug!(link = %self.link, device = %self.device, max_frame, "workloads larger than a frame not sent");
                    self.answers.pop_front();
                }
                Err(ActionSendError::TooLarge { max_frame, .. }) => {
                    // The request cannot be served at this frame size: it ends.
                    let req = answer.req();
                    self.answers.pop_front();
                    self.end(app, req);
                    self.runs.retain(|_, run| *run != req);
                    self.answers.push_front(Answer::Error {
                        req,
                        code: ErrorCode::LIMIT,
                        reason: format!(
                            "an answer to this request does not fit the session's {max_frame}-byte frames"
                        ),
                    });
                }
                Err(_) => {
                    self.answers.clear();
                    return;
                }
            }
        }
    }
}

fn request_id(request: &ClientRequest) -> u32 {
    match request {
        ClientRequest::StatusQuery { req, .. }
        | ClientRequest::StatusWatch { req, .. }
        | ClientRequest::ActionRun { req, .. }
        | ClientRequest::ActionQuery { req, .. }
        | ClientRequest::ActionWatch { req, .. }
        | ClientRequest::StreamSubscribe { req, .. }
        | ClientRequest::RecordQuery { req, .. }
        | ClientRequest::RecordWatch { req, .. }
        | ClientRequest::End { req } => *req,
        _ => 0,
    }
}

/// `chunks` with each one larger than `max_data` split at sample boundaries. A chunk of a series
/// the catalog did not name, or that does not decode, is kept as it is (and refused for its size
/// when sent).
fn split_chunks(
    chunks: Vec<SampleChunk>,
    kinds: &[SeriesKind],
    max_data: usize,
) -> Vec<SampleChunk> {
    if chunks.iter().all(|chunk| chunk.data.len() <= max_data) {
        return chunks;
    }
    let mut out = Vec::with_capacity(chunks.len() + 1);
    for chunk in chunks {
        let kind = kinds.get(usize::from(chunk.series)).copied();
        match kind.filter(|_| chunk.data.len() > max_data) {
            Some(kind) => match chunk.split(kind, max_data) {
                Ok(pieces) => out.extend(pieces),
                Err(_) => out.push(chunk),
            },
            None => out.push(chunk),
        }
    }
    out
}
