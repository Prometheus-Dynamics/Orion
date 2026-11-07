//! The device's one action at a time (feature `actions`): the request kept until the first
//! report, cancels, the declared catalog, and the result waiting to be sent.

use super::{DeviceCore, Outgoing, Snapshot, Source};
use crate::device::{ActionReportError, DeviceEvent, PublishError};
use crate::frame::{self, FrameHeader, copy_prefix};
use crate::wire::{
    self, ActionCatalogBody, ActionRequestView, ActionResultBody, ActionState,
    decode_action_cancel, kind,
};

/// The action the device is running (or has finished, until its final result is sent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActionSlot {
    pub(super) id: u32,
    /// Lengths of the name and payload stored at the start of the action buffer.
    pub(super) name_len: usize,
    pub(super) payload_len: usize,
    /// A result was reported, so the request bytes are gone (see `DeviceCore::action`).
    pub(super) reported: bool,
    /// A final result was reported; the slot is freed once that result is sent.
    pub(super) finished: bool,
}

impl<N: AsRef<str>, const RX: usize, const TX: usize, const ST: usize> DeviceCore<N, RX, TX, ST> {
    /// Accepts one request (one action in flight). A repeat of the running request is ignored;
    /// any other request while the device is busy is refused with `busy`.
    pub(super) fn on_action_request(&mut self, payload: &[u8]) {
        let request = match ActionRequestView::decode(payload) {
            Ok(request) => request,
            Err(_) => {
                self.decode_error();
                return;
            }
        };
        if let Some(slot) = self.action_slot {
            if slot.id != request.id {
                self.refusal = Some(request.id);
            }
            return;
        }
        let name_len = request.name.len();
        let total = name_len.saturating_add(request.payload.len());
        let Some(buf) = self.action_buf.get_mut(..total) else {
            self.refusal = Some(request.id);
            return;
        };
        copy_prefix(buf, request.name.as_bytes());
        if let Some(rest) = buf.get_mut(name_len..) {
            copy_prefix(rest, request.payload);
        }
        self.action_slot = Some(ActionSlot {
            id: request.id,
            name_len,
            payload_len: request.payload.len(),
            reported: false,
            finished: false,
        });
        self.emit(DeviceEvent::ActionRequested);
    }

    /// Host's `ActionCancel`. Accepted while the device is busy: a cancel for the running action
    /// (not yet final) queues `ActionCancelRequested` for the application; any other id is ignored.
    pub(super) fn on_action_cancel(&mut self, payload: &[u8]) {
        let Ok(id) = decode_action_cancel(payload) else {
            self.decode_error();
            return;
        };
        if self
            .action_slot
            .is_some_and(|slot| slot.id == id && !slot.finished)
        {
            self.emit(DeviceEvent::ActionCancelRequested { id });
        }
    }

    /// Replaces the declared action names (see `DeviceSession::declare_actions`). Returns whether a
    /// transmission of the previous catalog was aborted.
    pub(crate) fn declare_actions(&mut self, names: &[&str]) -> Result<bool, PublishError> {
        let body = ActionCatalogBody { names };
        let frame_len = frame::frame_len(wire::encoded_len(&body));
        if !names.is_empty() && frame_len > Self::LOCAL_MAX {
            return Err(PublishError::TooLarge {
                frame_len,
                max_frame: Self::LOCAL_MAX,
            });
        }
        let aborted = self.out.is_some_and(|out| out.source == Source::Catalog);
        if aborted {
            self.out = None;
            bump!(self, aborted_frames);
        }
        if names.is_empty() {
            self.catalog = None;
            return Ok(aborted);
        }
        let payload_len = wire::write_payload(&body, &mut self.catalog_buf);
        self.catalog = Some(Snapshot {
            payload_len,
            pending: true,
            latest: None,
            backoff_ms: self.config.state_retry_min_ms.max(1),
            next_at: None,
        });
        Ok(aborted)
    }

    /// The request the device should run. Available until the first report: a report overwrites
    /// the request bytes.
    pub(crate) fn action(&self) -> Option<ActionRequestView<'_>> {
        let slot = self.action_slot?;
        if slot.reported {
            return None;
        }
        let end = slot.name_len.checked_add(slot.payload_len)?;
        let name = wire::str_from_utf8(self.action_buf.get(..slot.name_len)?).ok()?;
        let payload = self.action_buf.get(slot.name_len..end)?;
        Some(ActionRequestView {
            id: slot.id,
            name,
            payload,
        })
    }

    /// Queues a result for the running action. Returns whether a result being transmitted was
    /// aborted (the caller then resets its transmit cursor).
    pub(crate) fn report_action(
        &mut self,
        state: ActionState,
        payload: &[u8],
    ) -> Result<bool, ActionReportError> {
        let slot = self.action_slot.ok_or(ActionReportError::NoAction)?;
        if slot.finished {
            return Err(ActionReportError::Finished);
        }
        let body = ActionResultBody {
            id: slot.id,
            state,
            payload,
        };
        let frame_len = frame::frame_len(wire::encoded_len(&body));
        let max_frame = self.max_frame().min(Self::LOCAL_MAX);
        if frame_len > max_frame {
            return Err(ActionReportError::TooLarge {
                frame_len,
                max_frame,
            });
        }
        let aborted = self.out.is_some_and(|out| out.source == Source::Action);
        if aborted {
            self.out = None;
            bump!(self, aborted_frames);
        }
        let body_len = wire::write_payload(&body, &mut self.action_buf);
        self.action_out = Some((body_len, state.is_final()));
        self.action_slot = Some(ActionSlot {
            reported: true,
            finished: state.is_final(),
            ..slot
        });
        Ok(aborted)
    }

    /// Encodes the queued action result for transmission.
    pub(super) fn select_action(&mut self, max_frame: usize) -> Option<Outgoing> {
        let (body_len, is_final) = self.action_out.take()?;
        if frame::frame_len(body_len) > max_frame {
            // Cannot be sent under the negotiated frame size. A final result is dropped, which
            // frees the device for its next request.
            if is_final {
                self.action_slot = None;
            }
            return None;
        }
        let seq = self.next_seq();
        let len = frame::encode_in_place(
            FrameHeader::new(kind::ACTION_RESULT, seq),
            body_len,
            &mut self.action_buf,
        )
        .ok()?;
        self.out_final = is_final;
        Some(Outgoing {
            source: Source::Action,
            len,
        })
    }
}
