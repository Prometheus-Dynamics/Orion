//! A provider device's stream (interface revision 2, feature `streams`): one declaration, sent
//! after the snapshot is acknowledged and repeated until the host answers it (a `StreamDemand`
//! accepts it, a `RequestError` refuses it); then batches, one waiting at a time.

use super::{DeviceCore, Outgoing, Snapshot, Source, reached};
use crate::device::{DeviceEvent, PublishError, StreamError};
use crate::frame::{self, FrameHeader};
use crate::wire::{
    self, ChunkView, MAX_STREAM_SERIES, Reader, SeriesView, StreamDeclareBody, StreamDemandView,
    StreamPublishBody, kind,
};

/// The revision that brought streams.
const STREAMS_REVISION: u16 = 2;
/// `ErrorCode::UNSUPPORTED`, reported when the session's revision has no streams.
const UNSUPPORTED: u16 = 1;

/// The declared stream and where it stands in the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamSlot {
    /// The declaration's request id (new for every declaration).
    req: u32,
    /// The declaration's payload in `stream_decl_buf`, and its transmissions while unanswered.
    decl: Snapshot,
    /// The host accepted the declaration in this session.
    accepted: bool,
    /// The host refused it in this session.
    refused: bool,
    demand: StreamDemandView,
    /// Payload length of the batch waiting in `stream_batch_buf`.
    batch: Option<usize>,
}

impl<N: AsRef<str>, const RX: usize, const TX: usize, const ST: usize> DeviceCore<N, RX, TX, ST> {
    /// The declared stream's id, read back from the declaration's payload.
    fn stream_id(&self) -> Option<&str> {
        let slot = self.stream?;
        let payload =
            frame::payload_area_ref(&self.stream_decl_buf).get(..slot.decl.payload_len)?;
        let mut r = Reader::new(payload);
        r.varint_u32().ok()?;
        r.str().ok()
    }

    /// Declares the device's stream, replacing an earlier declaration. Returns whether a
    /// transmission was aborted.
    pub(crate) fn declare_stream(
        &mut self,
        id: &str,
        series: &[SeriesView<'_>],
    ) -> Result<bool, StreamError> {
        if series.is_empty() || series.len() > MAX_STREAM_SERIES {
            return Err(StreamError::Series);
        }
        self.stream_req = self.stream_req.wrapping_add(1).max(1);
        let body = StreamDeclareBody {
            req: self.stream_req,
            id,
            series,
        };
        let frame_len = frame::frame_len(wire::encoded_len(&body));
        if frame_len > Self::LOCAL_MAX {
            return Err(StreamError::Publish(PublishError::TooLarge {
                frame_len,
                max_frame: Self::LOCAL_MAX,
            }));
        }
        let aborted = self
            .out
            .is_some_and(|out| matches!(out.source, Source::StreamDecl | Source::StreamBatch));
        if aborted {
            self.out = None;
            bump!(self, aborted_frames);
        }
        let payload_len = wire::write_payload(&body, &mut self.stream_decl_buf);
        self.stream = Some(StreamSlot {
            req: self.stream_req,
            decl: self.fresh(payload_len),
            accepted: false,
            refused: false,
            demand: StreamDemandView::default(),
            batch: None,
        });
        Ok(aborted)
    }

    /// Queues a batch of the declared stream. Returns whether a transmission was aborted (never:
    /// a batch waits for the one before it).
    pub(crate) fn publish_stream(&mut self, chunks: &[ChunkView<'_>]) -> Result<bool, StreamError> {
        let Some(slot) = self.stream else {
            return Err(StreamError::NotDeclared);
        };
        if !slot.accepted {
            return Err(StreamError::NotAccepted);
        }
        if slot.batch.is_some()
            || self
                .out
                .is_some_and(|out| out.source == Source::StreamBatch)
        {
            return Err(StreamError::Pending);
        }
        let max_frame = self.max_frame().min(TX);
        let payload_len = {
            let payload = frame::payload_area_ref(&self.stream_decl_buf)
                .get(..slot.decl.payload_len)
                .unwrap_or_default();
            let mut r = Reader::new(payload);
            let id = r
                .varint_u32()
                .and_then(|_| r.str())
                .map_err(|_| StreamError::NotDeclared)?;
            let body = StreamPublishBody { id, chunks };
            let frame_len = frame::frame_len(wire::encoded_len(&body));
            if frame_len > max_frame {
                return Err(StreamError::Publish(PublishError::TooLarge {
                    frame_len,
                    max_frame,
                }));
            }
            wire::write_payload(&body, &mut self.stream_batch_buf)
        };
        self.stream = Some(StreamSlot {
            batch: Some(payload_len),
            ..slot
        });
        Ok(false)
    }

    /// The host's demand for the stream (zero before it answers the declaration).
    pub(crate) fn stream_demand(&self) -> Option<StreamDemandView> {
        self.stream
            .filter(|slot| slot.accepted)
            .map(|slot| slot.demand)
    }

    /// Whether the host accepted the declaration in this session.
    pub(crate) fn stream_accepted(&self) -> bool {
        self.stream.is_some_and(|slot| slot.accepted)
    }

    /// Whether a batch waits to be sent.
    pub(crate) fn stream_pending(&self) -> bool {
        self.stream.is_some_and(|slot| slot.batch.is_some())
            || self
                .out
                .is_some_and(|out| out.source == Source::StreamBatch)
    }

    fn fresh(&self, payload_len: usize) -> Snapshot {
        Snapshot {
            payload_len,
            pending: true,
            latest: None,
            backoff_ms: self.config.state_retry_min_ms.max(1),
            next_at: None,
        }
    }

    /// A new session: the declaration is sent again, once the snapshot is acknowledged; a session
    /// of an earlier revision has no streams.
    pub(super) fn stream_on_welcome(&mut self, revision: u16) {
        let Some(slot) = self.stream else {
            return;
        };
        let supported = revision >= STREAMS_REVISION;
        self.stream = Some(StreamSlot {
            decl: Snapshot {
                pending: supported,
                ..self.fresh(slot.decl.payload_len)
            },
            accepted: false,
            refused: !supported,
            demand: StreamDemandView::default(),
            batch: None,
            ..slot
        });
        if !supported {
            self.emit(DeviceEvent::StreamRefused { code: UNSUPPORTED });
        }
    }

    /// `StreamDemand`: accepts the declaration and says who subscribes.
    pub(super) fn on_stream_demand(&mut self, payload: &[u8]) {
        let Ok((id, demand)) = wire::decode_stream_demand(payload) else {
            self.decode_error();
            return;
        };
        if self.stream_id() != Some(id) {
            return;
        }
        if let Some(slot) = &mut self.stream {
            let changed = !slot.accepted || slot.demand != demand;
            slot.accepted = true;
            slot.refused = false;
            slot.decl.pending = false;
            slot.demand = demand;
            if changed {
                self.emit(DeviceEvent::StreamDemand);
            }
        }
    }

    /// `RequestError` for the declaration: refused until the next session.
    pub(super) fn on_request_error(&mut self, payload: &[u8]) {
        let Ok((req, code)) = wire::decode_request_error(payload) else {
            self.decode_error();
            return;
        };
        if let Some(slot) = &mut self.stream
            && slot.req == req
            && !slot.refused
        {
            slot.refused = true;
            slot.accepted = false;
            slot.decl.pending = false;
            slot.batch = None;
            self.emit(DeviceEvent::StreamRefused { code });
        }
    }

    /// The declaration, while unanswered: after the snapshot is acknowledged, retransmitted with
    /// doubling intervals up to the heartbeat.
    pub(super) fn select_stream_decl(&mut self, max_frame: usize) -> Option<Outgoing> {
        let slot = self.stream?;
        let decl = slot.decl;
        if !decl.pending
            || decl.next_at.is_some_and(|at| !reached(self.now, at))
            || self.snapshot.is_none_or(|snapshot| snapshot.pending)
            || frame::frame_len(decl.payload_len) > max_frame
        {
            return None;
        }
        let seq = self.next_seq();
        let len = frame::encode_in_place(
            FrameHeader::new(kind::STREAM_DECLARE, seq),
            decl.payload_len,
            &mut self.stream_decl_buf,
        )
        .ok()?;
        let cap = self
            .session()
            .map_or(u32::MAX, |session| session.heartbeat_ms);
        self.stream = Some(StreamSlot {
            decl: Snapshot {
                latest: Some(seq),
                next_at: Some(self.now.wrapping_add(decl.backoff_ms)),
                backoff_ms: decl.backoff_ms.saturating_mul(2).min(cap.max(1)),
                ..decl
            },
            ..slot
        });
        Some(Outgoing {
            source: Source::StreamDecl,
            len,
        })
    }

    /// The waiting batch.
    pub(super) fn select_stream_batch(&mut self, max_frame: usize) -> Option<Outgoing> {
        let mut slot = self.stream?;
        let payload_len = slot.batch.take()?;
        self.stream = Some(slot);
        if !slot.accepted || frame::frame_len(payload_len) > max_frame {
            return None;
        }
        let seq = self.next_seq();
        let len = frame::encode_in_place(
            FrameHeader::new(kind::STREAM_PUBLISH, seq),
            payload_len,
            &mut self.stream_batch_buf,
        )
        .ok()?;
        Some(Outgoing {
            source: Source::StreamBatch,
            len,
        })
    }
}
