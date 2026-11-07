//! The client role (revision 1) on the host session: requests reported to the gateway, refusals,
//! and answers queued as parts that fit the session's frame size.

use std::string::String;

use super::{HostCore, OUT_CAPACITY};
use crate::frame::{FRAME_OVERHEAD, FrameView};
use crate::host::{ActionSendError, HostEvent};
use crate::message::{self, Message, Roles, kind};

impl HostCore {
    /// A client request in a revision-1 session: reported, or refused here when this link does not
    /// serve clients, the session did not take the client role, or the body does not decode.
    pub(super) fn on_client_request(&mut self, frame: &FrameView<'_>, roles: Roles) {
        use crate::message::ErrorCode;
        let payload = frame.payload();
        let refuse = |core: &mut Self, code: ErrorCode, reason: &str| {
            if frame.kind() == kind::REQUEST_END {
                return; // nothing answers an end
            }
            if let Some(req) = message::request_id(payload) {
                let _ = core.send_request_error(req, code, reason);
            }
        };
        if !self.config.serve_clients {
            refuse(
                self,
                ErrorCode::UNSUPPORTED,
                "this link does not serve clients",
            );
            return;
        }
        if !roles.contains(Roles::CLIENT) {
            refuse(
                self,
                ErrorCode::UNSUPPORTED,
                "the session did not announce the client role",
            );
            return;
        }
        let request = match Message::decode(frame) {
            Ok(Message::StatusQuery(body)) => crate::host::ClientRequest::StatusQuery {
                req: body.req,
                filter: body.filter,
            },
            Ok(Message::StatusWatch(body)) => crate::host::ClientRequest::StatusWatch {
                req: body.req,
                filter: body.filter,
            },
            Ok(Message::ActionRun(run)) => {
                let message::ActionRun { req, request } = *run;
                crate::host::ClientRequest::ActionRun {
                    req,
                    request: std::boxed::Box::new(request),
                }
            }
            Ok(Message::ActionQuery(body)) => crate::host::ClientRequest::ActionQuery {
                req: body.req,
                filter: body.filter,
            },
            Ok(Message::ActionWatch(body)) => crate::host::ClientRequest::ActionWatch {
                req: body.req,
                filter: body.filter,
            },
            Ok(Message::StreamSubscribe(subscribe)) => {
                if !self.config.serve_streams {
                    refuse(
                        self,
                        ErrorCode::UNSUPPORTED,
                        "this link does not serve streams",
                    );
                    return;
                }
                let message::StreamSubscribe { req, subscription } = *subscribe;
                crate::host::ClientRequest::StreamSubscribe {
                    req,
                    subscription: std::boxed::Box::new(subscription),
                }
            }
            Ok(Message::RecordQuery(body) | Message::RecordWatch(body))
                if !self.config.serve_executors =>
            {
                let _ = body;
                refuse(
                    self,
                    ErrorCode::UNSUPPORTED,
                    "this link does not serve records",
                );
                return;
            }
            Ok(Message::RecordQuery(body)) => crate::host::ClientRequest::RecordQuery {
                req: body.req,
                filter: body.filter,
            },
            Ok(Message::RecordWatch(body)) => crate::host::ClientRequest::RecordWatch {
                req: body.req,
                filter: body.filter,
            },
            Ok(Message::RequestEnd { req }) => crate::host::ClientRequest::End { req },
            _ => {
                self.decode_error();
                refuse(self, ErrorCode::INVALID, "malformed request body");
                return;
            }
        };
        if let Some(device_name) = self.device_name().map(String::from) {
            self.emit(HostEvent::ClientRequest {
                device_name,
                request,
            });
        }
    }

    /// The negotiated frame size of a session that serves clients.
    pub(super) fn client_frame(&self) -> Result<usize, ActionSendError> {
        self.frame_for(Roles::CLIENT, 1)
    }

    /// The negotiated frame size of a session with `role` and at least `revision`.
    pub(super) fn frame_for(&self, role: Roles, revision: u16) -> Result<usize, ActionSendError> {
        match &self.conn {
            Some(conn) if conn.roles.contains(role) && conn.revision >= revision => {
                Ok(conn.max_frame)
            }
            _ => Err(ActionSendError::NotConnected),
        }
    }

    /// Queues `parts` frames that `encode(part, seq, buf)` writes: all of them, or none.
    pub(super) fn queue_parts(
        &mut self,
        parts: usize,
        max_frame: usize,
        mut encode: impl FnMut(usize, u16, &mut [u8]) -> Result<usize, message::MessageError>,
    ) -> Result<(), ActionSendError> {
        if parts > OUT_CAPACITY {
            return Err(ActionSendError::TooLarge {
                frame_len: parts.saturating_mul(max_frame),
                max_frame,
            });
        }
        if self.out.len() + parts > OUT_CAPACITY {
            return Err(ActionSendError::Congested);
        }
        let mark = self.out.mark();
        let mut seq = self.seq;
        for part in 0..parts {
            seq = seq.wrapping_add(1);
            let out = &mut self.out;
            let encoded = super::with_frame(
                max_frame,
                |buf| encode(part, seq, buf),
                |frame| out.push_back(frame),
            );
            if encoded.is_err() {
                self.out.rollback(mark);
                return Err(ActionSendError::TooLarge {
                    frame_len: max_frame.saturating_add(1),
                    max_frame,
                });
            }
        }
        self.seq = seq;
        Ok(())
    }

    pub(crate) fn send_status_changes(
        &mut self,
        req: u32,
        bootstrap: bool,
        updated: &[orion_control_plane::StatusEntry],
        expired: &[orion_control_plane::StatusKey],
    ) -> Result<(), ActionSendError> {
        let max_frame = self.client_frame()?;
        let parts = message::StatusParts::new(req, bootstrap, updated, expired, max_frame)
            .map_err(|too_large| ActionSendError::TooLarge {
                frame_len: too_large.record_len.saturating_add(FRAME_OVERHEAD),
                max_frame,
            })?;
        self.queue_parts(parts.len(), max_frame, |part, seq, buf| {
            parts.encode(part, seq, buf)
        })
    }

    pub(crate) fn send_action_results(
        &mut self,
        req: u32,
        results: &[orion_control_plane::ActionResult],
    ) -> Result<(), ActionSendError> {
        let max_frame = self.client_frame()?;
        let parts = message::ActionParts::new(req, results, max_frame).map_err(|too_large| {
            ActionSendError::TooLarge {
                frame_len: too_large.record_len.saturating_add(FRAME_OVERHEAD),
                max_frame,
            }
        })?;
        self.queue_parts(parts.len(), max_frame, |part, seq, buf| {
            parts.encode(part, seq, buf)
        })
    }

    pub(crate) fn send_request_error(
        &mut self,
        req: u32,
        code: message::ErrorCode,
        reason: &str,
    ) -> Result<(), ActionSendError> {
        let Some(max_frame) = self.conn.as_ref().map(|conn| conn.max_frame) else {
            return Err(ActionSendError::NotConnected);
        };
        // A reason cut to the limit fits any frame a session negotiates past the minimum; one that
        // still does not fit loses its reason rather than the error.
        let mut reason = reason;
        loop {
            match self.queue_parts(1, max_frame, |_, seq, buf| {
                message::encode_request_error(req, code, reason, seq, buf)
            }) {
                Err(ActionSendError::TooLarge { .. }) if !reason.is_empty() => reason = "",
                other => return other,
            }
        }
    }
}
