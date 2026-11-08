//! The executor role and record reads (revision 3) on the host session: executor state reported
//! to the gateway and acknowledged, and workload and record answers queued as parts.

use super::HostCore;
use crate::frame::{FRAME_OVERHEAD, FrameView};
use crate::host::{ActionSendError, HostEvent};
use crate::message::{self, Message, Record, RecordKey, Roles, WorkloadId, kind};
use orion_control_plane::WorkloadRecord;

impl HostCore {
    /// An `ExecutorState` in a revision-3 session.
    pub(super) fn on_executor_state(&mut self, frame: &FrameView<'_>, roles: Roles, limit: usize) {
        if !roles.contains(Roles::EXECUTOR) {
            // Only an executor session reports executor state.
            self.decode_error();
            return;
        }
        let Ok(Message::ExecutorState(state)) = Message::decode(frame) else {
            self.decode_error();
            return;
        };
        let _ = self.queue(kind::ACK, &frame.seq(), limit);
        if self.last_executor_state.as_deref() == Some(&*state) {
            return; // retransmission of a state we already have
        }
        let Some(device_name) = self.device_arc() else {
            return;
        };
        self.last_executor_state = Some(state.clone());
        self.emit(HostEvent::ExecutorState { device_name, state });
    }

    pub(crate) fn send_workloads(
        &mut self,
        bootstrap: bool,
        workloads: &[WorkloadRecord],
        removed: &[WorkloadId],
    ) -> Result<(), ActionSendError> {
        let max_frame = self.frame_for(Roles::EXECUTOR, 3)?;
        let parts = message::WorkloadParts::new(bootstrap, workloads, removed, max_frame).map_err(
            |too_large| ActionSendError::TooLarge {
                frame_len: too_large.record_len.saturating_add(FRAME_OVERHEAD),
                max_frame,
            },
        )?;
        self.queue_parts(parts.len(), max_frame, |part, seq, buf| {
            parts.encode(part, seq, buf)
        })
    }

    pub(crate) fn send_records(
        &mut self,
        req: u32,
        bootstrap: bool,
        updated: &[Record],
        removed: &[RecordKey],
    ) -> Result<(), ActionSendError> {
        let max_frame = self.frame_for(Roles::CLIENT, 3)?;
        let parts = message::RecordParts::new(req, bootstrap, updated, removed, max_frame)
            .map_err(|too_large| ActionSendError::TooLarge {
                frame_len: too_large.record_len.saturating_add(FRAME_OVERHEAD),
                max_frame,
            })?;
        self.queue_parts(parts.len(), max_frame, |part, seq, buf| {
            parts.encode(part, seq, buf)
        })
    }
}
