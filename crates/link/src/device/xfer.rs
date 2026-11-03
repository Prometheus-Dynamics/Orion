//! Device side of firmware transfer (feature `xfer`): the transfer state, the replies it owes the
//! host, and the reliable `DeviceInfo` report. The firmware itself is a [`FirmwareSink`].
//!
//! Rules (see `docs/link-protocol.md`, "Firmware transfer"):
//!
//! - `XferBegin` asks the sink to start or resume; the answer is an `XferAck` whose `next_offset`
//!   is what the sink already holds.
//! - `XferChunk` is accepted only at the next expected offset. Its CRC-32C is checked first; a
//!   chunk that repeats stored bytes is acknowledged again without being written; a chunk past the
//!   next offset is refused with `BadOffset`. Every chunk gets an `XferAck` with the cumulative
//!   `next_offset`, so a lost ack or a corrupted chunk is repaired by resending from there.
//! - `XferEnd` verifies the whole image once (the sink's `finish`); the verdict is kept, so a
//!   repeated `End` gets the same answer.
//! - `Activate` and `Confirm` go to the sink. A successful change re-sends `DeviceInfo`.

use crate::crc::crc32c;
use crate::frame::{self, FRAME_OVERHEAD, FrameHeader};
use crate::wire::{
    self, ActivateView, ConfirmView, DeviceInfoView, FirmwareInfo, FirmwareSink, TrialRollback,
    XferAbortView, XferAckView, XferBeginView, XferChunkView, XferEndView, XferResultView,
    XferStatus, kind, max_chunk_for_frame,
};

/// Bytes reserved for the encoded `DeviceInfo` payload (a frame of at most this many bytes). The
/// version and bootloader strings, the fixed fields, and a worst-case rollback report (its version
/// and note at their wire bounds) must fit: about 150 bytes.
pub(crate) const INFO_CAPACITY: usize = 160;

/// The reply slot's longest frame: the header, three varints and a status byte, CRC.
pub(crate) const REPLY_FRAME_MAX: usize = FRAME_OVERHEAD + 15;

/// A reply the device owes the host. Only the newest is kept: each reply carries the full state
/// it reports, so a newer one supersedes an older one that was not sent yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reply {
    Ack(XferAckView),
    Result(XferResultView),
    Abort(XferAbortView),
}

impl Reply {
    /// Encodes the reply as a frame with `seq` into `buf`.
    pub(crate) fn encode(&self, seq: u16, buf: &mut [u8]) -> Result<usize, wire::WireError> {
        match self {
            Self::Ack(body) => wire::encode_frame(kind::XFER_ACK, seq, body, buf),
            Self::Result(body) => wire::encode_frame(kind::XFER_RESULT, seq, body, buf),
            Self::Abort(body) => wire::encode_frame(kind::XFER_ABORT, seq, body, buf),
        }
    }
}

/// The transfer in progress (or the last one), as the device tracks it in RAM. The sink holds the
/// bytes; this only says how far the device has got and what it concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Transfer {
    id: u32,
    next: u32,
    total: u32,
    crc32c: u32,
    done: Option<XferStatus>,
}

/// Reliable `DeviceInfo` state, the same shape as a provider snapshot: encoded once into the info
/// buffer, retransmitted until the host acknowledges one of its transmissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InfoSlot {
    payload_len: usize,
    pending: bool,
    /// Sequence number of the latest transmission (see `Snapshot::latest` in `core.rs`).
    latest: Option<u16>,
    backoff_ms: u32,
    next_at: Option<u32>,
}

/// Transfer and `DeviceInfo` state of a device session.
#[derive(Debug)]
pub(crate) struct XferState {
    current: Option<Transfer>,
    reply: Option<Reply>,
    info_due: bool,
    info: Option<InfoSlot>,
    info_buf: [u8; INFO_CAPACITY],
}

impl XferState {
    pub(crate) const fn new() -> Self {
        Self {
            current: None,
            reply: None,
            info_due: false,
            info: None,
            info_buf: [0; INFO_CAPACITY],
        }
    }

    /// Asks for a fresh `DeviceInfo` at the next transmission.
    pub(crate) fn request_info(&mut self) {
        self.info_due = true;
    }

    pub(crate) fn info_due(&self) -> bool {
        self.info_due
    }

    /// The newest reply, if one is owed.
    pub(crate) fn take_reply(&mut self) -> Option<Reply> {
        self.reply.take()
    }

    /// Payload of the `DeviceInfo` frame in flight (see [`Self::info_frame`]).
    pub(crate) fn info_buf(&self) -> &[u8] {
        &self.info_buf
    }

    /// A new session: the host has not seen the device's state yet.
    pub(crate) fn on_connected(&mut self) {
        self.request_info();
        self.reply = None;
    }

    /// Whether the host acknowledged a `DeviceInfo` transmission (an `Ack` for `acked`).
    pub(crate) fn on_ack(&mut self, acked: u16) {
        let Some(info) = &mut self.info else {
            return;
        };
        if info.pending && info.latest == Some(acked) {
            info.pending = false;
        }
    }

    /// Encodes `info` and the sink's last rollback as the pending `DeviceInfo`. Returns false if it
    /// does not fit the info buffer or `max_frame` (the previous report then stays pending).
    pub(crate) fn encode_info(
        &mut self,
        info: &FirmwareInfo<'_>,
        rollback: Option<TrialRollback<'_>>,
        rx: usize,
        max_frame: usize,
        state_retry_ms: u32,
    ) -> bool {
        if info.slots == 0 {
            // A device without firmware slots does not offer the transfer: it never sends
            // `DeviceInfo`, so hosts see the same traffic as before.
            self.info_due = false;
            return true;
        }
        let max_chunk = u16::try_from(max_chunk_for_frame(rx))
            .unwrap_or(u16::MAX)
            .min(info.max_chunk);
        let view = DeviceInfoView {
            fw_version: info.fw_version,
            active_slot: info.active_slot,
            slots: info.slots,
            trial: info.trial,
            xfer_max_chunk: max_chunk,
            xfer_window: info.window,
            bootloader: info.bootloader,
            rollback: rollback.map(TrialRollback::clamped),
        };
        self.info_due = false;
        let limit = max_frame.min(INFO_CAPACITY);
        let payload_len = wire::write_payload(&view, &mut self.info_buf);
        if frame::frame_len(payload_len) > limit {
            return false;
        }
        // A new report replaces the old one (and aborts a transmission of it).
        self.info = Some(InfoSlot {
            payload_len,
            pending: true,
            latest: None,
            backoff_ms: state_retry_ms.max(1),
            next_at: None,
        });
        true
    }

    /// The `DeviceInfo` retransmission deadline, if one is pending (`None`: send at once).
    pub(crate) fn info_next_at(&self) -> Option<Option<u32>> {
        self.info
            .filter(|info| info.pending)
            .map(|info| info.next_at)
    }

    /// Builds the `DeviceInfo` frame with `seq` at `now`, doubling its retransmission interval up
    /// to `cap_ms`. Returns its length in the info buffer.
    pub(crate) fn info_frame(&mut self, seq: u16, now: u32, cap_ms: u32) -> Option<usize> {
        let mut info = self.info?;
        let len = frame::encode_in_place(
            FrameHeader::new(kind::DEVICE_INFO, seq),
            info.payload_len,
            &mut self.info_buf,
        )
        .ok()?;
        info.latest = Some(seq);
        info.next_at = Some(now.wrapping_add(info.backoff_ms));
        info.backoff_ms = info.backoff_ms.saturating_mul(2).min(cap_ms.max(1));
        self.info = Some(info);
        Some(len)
    }

    /// Handles one host → device transfer frame. Returns false if the body does not decode.
    pub(crate) fn on_frame<F: FirmwareSink>(
        &mut self,
        kind: u8,
        payload: &[u8],
        fw: &mut F,
    ) -> bool {
        match kind {
            kind::XFER_BEGIN => match XferBeginView::decode(payload) {
                Ok(begin) => self.begin(&begin, fw),
                Err(_) => return false,
            },
            kind::XFER_CHUNK => match XferChunkView::decode(payload) {
                Ok(chunk) => self.chunk(&chunk, fw),
                Err(_) => return false,
            },
            kind::XFER_END => match XferEndView::decode(payload) {
                Ok(end) => self.end(end.xfer_id, fw),
                Err(_) => return false,
            },
            kind::ACTIVATE => match ActivateView::decode(payload) {
                Ok(activate) => self.activate(&activate, fw),
                Err(_) => return false,
            },
            kind::CONFIRM => match ConfirmView::decode(payload) {
                Ok(confirm) => match fw.confirm(confirm.slot) {
                    Ok(()) => self.request_info(),
                    Err(reason) => {
                        self.reply = Some(Reply::Abort(XferAbortView { xfer_id: 0, reason }))
                    }
                },
                Err(_) => return false,
            },
            kind::XFER_ABORT => match XferAbortView::decode(payload) {
                Ok(abort) => {
                    if self.current.is_some_and(|t| t.id == abort.xfer_id) {
                        self.current = None;
                    }
                }
                Err(_) => return false,
            },
            _ => {}
        }
        true
    }

    fn begin<F: FirmwareSink>(&mut self, begin: &XferBeginView<'_>, fw: &mut F) {
        match fw.begin(begin) {
            Ok(held) => {
                let next = held.min(begin.total_len);
                self.current = Some(Transfer {
                    id: begin.xfer_id,
                    next,
                    total: begin.total_len,
                    crc32c: begin.image_crc32c,
                    done: None,
                });
                self.reply = Some(Reply::Ack(XferAckView {
                    xfer_id: begin.xfer_id,
                    next_offset: next,
                    status: XferStatus::Ok,
                }));
            }
            Err(status) => {
                self.current = None;
                self.reply = Some(Reply::Ack(XferAckView {
                    xfer_id: begin.xfer_id,
                    next_offset: 0,
                    status,
                }));
            }
        }
    }

    fn chunk<F: FirmwareSink>(&mut self, chunk: &XferChunkView<'_>, fw: &mut F) {
        let Some(t) = &mut self.current else {
            self.reply = Some(Reply::Ack(XferAckView {
                xfer_id: chunk.xfer_id,
                next_offset: 0,
                status: XferStatus::UnknownXfer,
            }));
            return;
        };
        if t.id != chunk.xfer_id {
            self.reply = Some(Reply::Ack(XferAckView {
                xfer_id: chunk.xfer_id,
                next_offset: 0,
                status: XferStatus::UnknownXfer,
            }));
            return;
        }
        let status = chunk_status(t, chunk, fw);
        self.reply = Some(Reply::Ack(XferAckView {
            xfer_id: t.id,
            next_offset: t.next,
            status,
        }));
    }

    fn end<F: FirmwareSink>(&mut self, xfer_id: u32, fw: &mut F) {
        let result = match &mut self.current {
            Some(t) if t.id == xfer_id => {
                if let Some(done) = t.done {
                    done
                } else if t.next != t.total {
                    XferStatus::BadOffset
                } else {
                    let verdict = match fw.finish(t.id, t.total, t.crc32c) {
                        Ok(()) => XferStatus::Ok,
                        Err(status) => status,
                    };
                    t.done = Some(verdict);
                    verdict
                }
            }
            _ => XferStatus::UnknownXfer,
        };
        self.reply = Some(Reply::Result(XferResultView {
            xfer_id,
            status: result,
        }));
    }

    fn activate<F: FirmwareSink>(&mut self, activate: &ActivateView, fw: &mut F) {
        if let Some(t) = self.current
            && t.id == activate.xfer_id
            && t.done != Some(XferStatus::Ok)
        {
            // Activation of an image that did not verify is refused without asking the sink.
            self.reply = Some(Reply::Abort(XferAbortView {
                xfer_id: activate.xfer_id,
                reason: XferStatus::BadDigest,
            }));
            return;
        }
        // Otherwise the sink decides: it may hold an image this session no longer tracks (after a
        // reboot), and it answers `UnknownXfer` if it has none.
        match fw.activate(activate.xfer_id, activate.mode) {
            Ok(()) => self.request_info(),
            Err(reason) => {
                self.reply = Some(Reply::Abort(XferAbortView {
                    xfer_id: activate.xfer_id,
                    reason,
                }));
            }
        }
    }
}

/// Applies one chunk to the transfer and returns the status to report. The cumulative offset
/// lives in `t.next`; a chunk that overlaps stored bytes writes only its new tail.
fn chunk_status<F: FirmwareSink>(
    t: &mut Transfer,
    chunk: &XferChunkView<'_>,
    fw: &mut F,
) -> XferStatus {
    if crc32c(chunk.data) != chunk.crc32c {
        return XferStatus::BadChunkCrc;
    }
    let Ok(len) = u32::try_from(chunk.data.len()) else {
        return XferStatus::BadOffset;
    };
    let Some(end) = chunk.offset.checked_add(len) else {
        return XferStatus::BadOffset;
    };
    if chunk.offset > t.next || end > t.total {
        return XferStatus::BadOffset;
    }
    if end <= t.next {
        return XferStatus::Ok; // repeated chunk: already stored
    }
    let skip = (t.next - chunk.offset) as usize;
    let Some(fresh) = chunk.data.get(skip..) else {
        return XferStatus::BadOffset;
    };
    match fw.write(t.next, fresh) {
        Ok(()) => {
            t.next = end;
            XferStatus::Ok
        }
        Err(status) => status,
    }
}
