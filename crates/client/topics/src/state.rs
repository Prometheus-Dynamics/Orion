//! What the comm core has told this program about its topics.

use orion_connect::{ClientError, Cx, Handler, Incoming};
use orion_msg_topics::{
    Bell as BellMessage, Declare, Demand, Opened, Sample, SampleBatch, TopicState, TopicStatus,
};
use orion_ring::{FieldId, Flags, Reader, Ring, Spec, Writer};
use orion_ring_doorbell::{Bell, Doorbell};
use orion_wire::{Decode, kind};

pub(crate) struct PublisherState {
    /// The `seq` of the declaration, which its answer names.
    seq: u16,
    pub(crate) handle: u32,
    open: bool,
    /// The declaration as it was sent, to say it again after a reconnect.
    #[allow(dead_code)]
    pub(crate) declaration: Vec<u8>,
    /// The ring, when this program is on the board.
    pub(crate) writer: Option<Writer>,
    /// The doorbells of the subscribers that have one, by subscriber.
    pub(crate) bells: Vec<(u32, Bell)>,
    /// How many subscribers the comm core last said there are.
    pub(crate) subscribers: u32,
    /// What the newest subscriber asked for.
    pub(crate) request: Vec<u8>,
}

pub(crate) struct SubscriberState {
    seq: u16,
    handle: Option<u32>,
    pub(crate) status: TopicStatus,
    pub(crate) declaration: Vec<u8>,
    /// A ring that has arrived and the subscriber has not taken yet.
    pub(crate) arrived: Option<Reader>,
    /// Off the board: this program's own ring, and the writer that arriving samples go to.
    mirror: Option<(Ring, Writer)>,
}

#[derive(Default)]
pub(crate) struct State {
    publishers: Vec<Option<PublisherState>>,
    subscribers: Vec<Option<SubscriberState>>,
    /// The declaration that came just before the ring it describes.
    declaration: Vec<u8>,
    pub(crate) doorbell: Option<Doorbell>,
}

impl State {
    pub(crate) fn add_publisher(&mut self, seq: u16, declaration: Vec<u8>) -> usize {
        self.publishers.push(Some(PublisherState {
            seq,
            handle: 0,
            open: false,
            declaration,
            writer: None,
            bells: Vec::new(),
            subscribers: 0,
            request: Vec::new(),
        }));
        self.publishers.len() - 1
    }

    pub(crate) fn drop_publisher(&mut self, index: usize) {
        self.publishers[index] = None;
    }

    pub(crate) fn publisher_open(&self, index: usize) -> bool {
        self.publishers[index]
            .as_ref()
            .is_some_and(|publisher| publisher.open)
    }

    pub(crate) fn publisher(&self, index: usize) -> &PublisherState {
        self.publishers[index]
            .as_ref()
            .unwrap_or_else(|| unreachable!("a publisher outlives its handle"))
    }

    pub(crate) fn publisher_mut(&mut self, index: usize) -> &mut PublisherState {
        self.publishers[index]
            .as_mut()
            .unwrap_or_else(|| unreachable!("a publisher outlives its handle"))
    }

    pub(crate) fn add_subscriber(&mut self, seq: u16) -> usize {
        self.subscribers.push(Some(SubscriberState {
            seq,
            handle: None,
            status: TopicStatus::Waiting,
            declaration: Vec::new(),
            arrived: None,
            mirror: None,
        }));
        self.subscribers.len() - 1
    }

    pub(crate) fn drop_subscriber(&mut self, index: usize) {
        self.subscribers[index] = None;
    }

    pub(crate) fn subscriber_answered(&self, index: usize) -> bool {
        self.subscribers[index]
            .as_ref()
            .is_some_and(|subscriber| subscriber.handle.is_some())
    }

    pub(crate) fn subscriber(&self, index: usize) -> &SubscriberState {
        self.subscribers[index]
            .as_ref()
            .unwrap_or_else(|| unreachable!("a subscriber outlives its handle"))
    }

    pub(crate) fn subscriber_mut(&mut self, index: usize) -> &mut SubscriberState {
        self.subscribers[index]
            .as_mut()
            .unwrap_or_else(|| unreachable!("a subscriber outlives its handle"))
    }

    fn publisher_of(&mut self, handle: u32) -> Option<&mut PublisherState> {
        self.publishers
            .iter_mut()
            .flatten()
            .find(|publisher| publisher.open && publisher.handle == handle)
    }

    fn subscriber_of(&mut self, handle: u32) -> Option<&mut SubscriberState> {
        self.subscribers
            .iter_mut()
            .flatten()
            .find(|subscriber| subscriber.handle == Some(handle))
    }

    fn opened(&mut self, opened: &Opened, cx: &mut Cx<'_>) -> Result<(), ClientError> {
        let ring = |_| ClientError::Protocol("a ring that cannot be mapped");
        if let Some(publisher) = self
            .publishers
            .iter_mut()
            .flatten()
            .find(|publisher| publisher.seq == opened.request_seq)
        {
            publisher.handle = opened.handle;
            publisher.open = true;
            publisher.bells.clear();
            publisher.writer = match cx.take_fd(0) {
                Some(fd) if opened.shared_memory => Some(Writer::attach(fd).map_err(ring)?),
                _ => None,
            };
            return Ok(());
        }
        let declaration = &self.declaration;
        let Some(subscriber) = self
            .subscribers
            .iter_mut()
            .flatten()
            .find(|subscriber| subscriber.seq == opened.request_seq)
        else {
            return Ok(());
        };
        subscriber.handle = Some(opened.handle);
        subscriber.status = TopicStatus::Live;
        subscriber.declaration.clone_from(declaration);
        match cx.take_fd(0) {
            Some(fd) if opened.shared_memory => {
                subscriber.mirror = None;
                subscriber.arrived = Some(Reader::attach(fd).map_err(ring)?);
            }
            _ => {
                // No ring can be shared with this program: it keeps one of its own, shaped
                // by the declaration, for the samples that arrive as messages.
                let declare = Declare::decode(declaration)
                    .map_err(|_| ClientError::Protocol("a declaration"))?;
                let mirror = Ring::create(&Spec {
                    fields: declare.fields.len() as u32,
                    history: declare.history,
                    max_payload: declare.max_payload,
                    expiry_ms: declare.expiry_ms,
                })
                .map_err(ring)?;
                subscriber.arrived = Some(mirror.reader().map_err(ring)?);
                let writer = mirror.writer().map_err(ring)?;
                subscriber.mirror = Some((mirror, writer));
            }
        }
        Ok(())
    }

    fn sample(
        &mut self,
        handle: u32,
        field: u16,
        time: orion_ring::FrameTime,
        flags: u8,
        payload: &[u8],
    ) {
        if let Some(subscriber) = self.subscriber_of(handle)
            && let Some((_, writer)) = &mut subscriber.mirror
        {
            // One that does not fit the declaration is dropped: the comm core checked it.
            let _ = writer.publish(FieldId(field), time, Flags(flags), payload);
        }
    }
}

impl Handler for State {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::TOPIC_OPENED,
            kind::TOPIC_DECLARE,
            kind::TOPIC_STATE,
            kind::TOPIC_BELL,
            kind::TOPIC_DEMAND,
            kind::TOPIC_SAMPLE,
            kind::TOPIC_SAMPLE_BATCH,
        ]
    }

    fn handle(&mut self, frame: &Incoming<'_>, cx: &mut Cx<'_>) -> Result<(), ClientError> {
        match frame.kind() {
            kind::TOPIC_DECLARE => {
                self.declaration.clear();
                self.declaration.extend_from_slice(frame.payload());
            }
            kind::TOPIC_OPENED => self.opened(&frame.body()?, cx)?,
            kind::TOPIC_STATE => {
                let state: TopicState = frame.body()?;
                let subscriber = if state.request_seq == 0 {
                    self.subscriber_of(state.handle)
                } else {
                    self.subscribers
                        .iter_mut()
                        .flatten()
                        .find(|subscriber| subscriber.seq == state.request_seq)
                };
                if let Some(subscriber) = subscriber {
                    subscriber.handle = Some(state.handle);
                    subscriber.status = state.status;
                }
            }
            kind::TOPIC_BELL => {
                let bell: BellMessage = frame.body()?;
                let fds = (cx.take_fd(0), cx.take_fd(1));
                if let Some(publisher) = self.publisher_of(bell.handle) {
                    publisher
                        .bells
                        .retain(|(subscriber, _)| *subscriber != bell.subscriber);
                    if bell.present
                        && let (Some(eventfd), Some(page)) = fds
                        && let Ok(attached) = Bell::attach(eventfd, page)
                    {
                        publisher.bells.push((bell.subscriber, attached));
                    }
                }
            }
            kind::TOPIC_DEMAND => {
                let demand: Demand<'_> = frame.body()?;
                if let Some(publisher) = self.publisher_of(demand.handle) {
                    publisher.subscribers = demand.subscribers;
                    if !demand.request.0.is_empty() || demand.subscribers == 0 {
                        publisher.request.clear();
                        publisher.request.extend_from_slice(demand.request.0);
                    }
                }
            }
            kind::TOPIC_SAMPLE => {
                let sample: Sample<'_> = frame.body()?;
                self.sample(
                    sample.handle,
                    sample.field,
                    sample.time,
                    sample.flags.0,
                    sample.payload.0,
                );
            }
            _ => {
                let batch: SampleBatch<'_> = frame.body()?;
                for sample in batch.samples.iter().flatten() {
                    self.sample(
                        batch.handle,
                        sample.field,
                        sample.time,
                        sample.flags.0,
                        sample.payload.0,
                    );
                }
            }
        }
        Ok(())
    }
}
