//! Streams (revision 2) on the host session: provider declarations and batches reported to the
//! gateway, and the answers to providers and subscribers queued in frames.

use std::string::String;

use super::HostCore;
use crate::frame::{FRAME_OVERHEAD, FrameView};
use crate::host::{ActionSendError, HostEvent};
use crate::message::{self, ErrorCode, Message, Roles, kind};

impl HostCore {
    /// A provider's `StreamDeclare` or `StreamPublish` in a revision-2 session.
    pub(super) fn on_provider_stream(&mut self, frame: &FrameView<'_>, roles: Roles) {
        let declare = frame.kind() == kind::STREAM_DECLARE;
        let refuse = |core: &mut Self, code: ErrorCode, reason: &str| {
            // A batch is not answered; a declaration is refused with its request id.
            if declare && let Some(req) = message::request_id(frame.payload()) {
                let _ = core.send_request_error(req, code, reason);
            }
        };
        if !roles.contains(Roles::PROVIDER) {
            self.decode_error();
            refuse(
                self,
                ErrorCode::UNSUPPORTED,
                "only a provider session declares streams",
            );
            return;
        }
        if !self.config.serve_streams {
            refuse(
                self,
                ErrorCode::UNSUPPORTED,
                "this link does not serve streams",
            );
            return;
        }
        let Some(device_name) = self.device_name().map(String::from) else {
            return;
        };
        match Message::decode(frame) {
            Ok(Message::StreamDeclare(body)) => self.emit(HostEvent::StreamDeclare {
                device_name,
                req: body.req,
                id: body.id,
                series: body.series,
            }),
            Ok(Message::StreamPublish(body)) => self.emit(HostEvent::StreamPublish {
                device_name,
                id: body.id,
                chunks: body.chunks,
            }),
            _ => {
                self.decode_error();
                refuse(self, ErrorCode::INVALID, "malformed stream declaration");
            }
        }
    }

    pub(crate) fn send_stream_demand(
        &mut self,
        id: &str,
        subscribers: u32,
        series: &[u16],
    ) -> Result<(), ActionSendError> {
        let max_frame = self.frame_for(Roles::PROVIDER, 2)?;
        self.queue_parts(1, max_frame, |_, seq, buf| {
            message::encode_demand(id, subscribers, series, seq, buf)
        })
    }

    pub(crate) fn send_stream_catalog(
        &mut self,
        req: u32,
        epoch: u64,
        series: &[orion_control_plane::SeriesDecl],
        live: bool,
    ) -> Result<(), ActionSendError> {
        let max_frame = self.frame_for(Roles::CLIENT, 2)?;
        self.queue_parts(1, max_frame, |_, seq, buf| {
            message::encode_catalog(req, epoch, series, live, seq, buf)
        })
    }

    pub(crate) fn send_stream_data(
        &mut self,
        data: &message::StreamData,
    ) -> Result<(), ActionSendError> {
        let max_frame = self.frame_for(Roles::CLIENT, 2)?;
        // Most batches fit one frame: encoded straight into the queue, nothing allocated.
        if self.output_room() == 0 {
            return Err(ActionSendError::Congested);
        }
        if self
            .queue_parts(1, max_frame, |_, seq, buf| {
                message::encode_data(data, false, seq, buf)
            })
            .is_ok()
        {
            return Ok(());
        }
        let parts = message::DataParts::new(data, max_frame).map_err(|too_large| {
            ActionSendError::TooLarge {
                frame_len: too_large.record_len.saturating_add(FRAME_OVERHEAD),
                max_frame,
            }
        })?;
        self.queue_parts(parts.len(), max_frame, |part, seq, buf| {
            parts.encode(part, seq, buf)
        })
    }

    pub(crate) fn send_stream_reset(
        &mut self,
        req: u32,
        reason: orion_control_plane::StreamResetReason,
    ) -> Result<(), ActionSendError> {
        let max_frame = self.frame_for(Roles::CLIENT, 2)?;
        self.queue_parts(1, max_frame, |_, seq, buf| {
            message::encode_reset(req, reason, seq, buf)
        })
    }
}
