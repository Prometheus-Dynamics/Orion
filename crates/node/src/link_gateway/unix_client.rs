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
//! Limits per session: one status watch, one action watch, and [`MAX_OPEN_ACTIONS`] actions run by
//! the session that are not yet final.

use crate::NodeApp;
use crate::NodeError;
use crate::app::LinkWatch;
use orion::control_plane::{
    ActionQuery, ActionResult, ClientEvent, ClientEventKind, ControlMessage, StatusEntry, StatusKey,
};
use orion::transport::ipc::{LocalAddress, UnixPeerIdentity};
use orion_link::Stream;
use orion_link::host::{ActionSendError, ClientRequest, HostSession};
use orion_link::message::ErrorCode;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::debug;

/// Actions a session may have run that are not yet final.
pub(super) const MAX_OPEN_ACTIONS: usize = 16;
/// Node events taken from the client's queue at a time.
const EVENTS_PER_DRAIN: usize = 32;
/// Drains per pump, so one busy client does not hold the link task.
const DRAINS_PER_PUMP: usize = 4;

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
}

impl Answer {
    fn req(&self) -> u32 {
        match self {
            Self::Status { req, .. } | Self::Actions { req, .. } | Self::Error { req, .. } => *req,
        }
    }
}

/// The client of one session.
pub(super) struct LinkClient {
    source: LocalAddress,
    identity: UnixPeerIdentity,
    /// Why the client could not be registered; every request is refused with it.
    refused: Option<(ErrorCode, String)>,
    status_watch: Option<u32>,
    action_watch: Option<(u32, ActionQuery)>,
    /// The action watch's first answer (the matching results, possibly none) is still to come.
    action_watch_first: bool,
    /// Actions run by the session that are not final yet, and the request each answers.
    runs: BTreeMap<String, u32>,
    answers: VecDeque<Answer>,
}

/// The error code for a request the node refused.
fn code_for(error: &NodeError) -> ErrorCode {
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
    /// Registers the client of the session of `device` on `link`.
    pub(super) fn open(
        app: &NodeApp,
        link: &str,
        device: &str,
        identity: UnixPeerIdentity,
        wake: Arc<Notify>,
    ) -> Self {
        let name = format!("link:{link}/{device}");
        let source = LocalAddress::new(name.clone());
        let refused = app
            .register_link_client(&source, &name, &identity, wake)
            .err()
            .map(|error| {
                debug!(link, device, %error, "link client not registered; its requests are refused");
                (code_for(&error), error.to_string())
            });
        Self {
            source,
            identity,
            refused,
            status_watch: None,
            action_watch: None,
            action_watch_first: false,
            runs: BTreeMap::new(),
            answers: VecDeque::new(),
        }
    }

    /// Removes the client from the node (its session ended).
    pub(super) fn close(self, app: &NodeApp) {
        if self.refused.is_none() {
            app.remove_link_client(&self.source);
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
        match app.serve_link_client_message(&self.source, &self.identity, message) {
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
        if self.status_watch == Some(req) {
            self.status_watch = None;
            app.end_link_client_watch(&self.source, LinkWatch::Status);
        } else if self
            .action_watch
            .as_ref()
            .is_some_and(|(watch, _)| *watch == req)
        {
            self.action_watch = None;
            app.end_link_client_watch(&self.source, LinkWatch::Actions);
        } else {
            return;
        }
        self.answers.retain(|answer| answer.req() != req);
    }

    fn on_event(&mut self, event: ClientEvent) {
        match event.event {
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

    /// Queues waiting answers while the session's output has room, then takes the node's new
    /// events for the client. Returns whether work is left (answers that did not fit, or events
    /// past this pump's share): the caller pumps again once frames have moved on.
    pub(super) fn pump(&mut self, app: &NodeApp, session: &mut HostSession<Stream>) -> bool {
        if self.refused.is_some() {
            self.flush(app, session);
            return !self.answers.is_empty();
        }
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
                self.on_event(event);
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
            };
            match sent {
                Ok(()) => {
                    self.answers.pop_front();
                }
                Err(ActionSendError::Congested) => return,
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
        | ClientRequest::End { req } => *req,
        _ => 0,
    }
}
