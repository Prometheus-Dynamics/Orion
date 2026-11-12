//! What the comm core has told this program about its topics.

use std::os::fd::OwnedFd;

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
    pub(crate) declaration: Vec<u8>,
    /// The ring's write end, kept so a comm core that restarts can be handed the ring back.
    pub(crate) ring: Option<OwnedFd>,
    /// The ring, when this program is on the board.
    pub(crate) writer: Option<Writer>,
    /// The writer was taken out, for a publisher that writes the ring some other way.
    pub(crate) taken: bool,
    /// The doorbells of the subscribers that have one, by subscriber.
    pub(crate) bells: Vec<(u32, Bell)>,
    /// How many subscribers the comm core last said there are.
    pub(crate) subscribers: u32,
    /// What the newest subscriber asked for.
    pub(crate) request: Vec<u8>,
}

/// What a subscription was made with, to make it again after a reconnect.
pub(crate) struct Asked {
    pub(crate) topic: String,
    pub(crate) request: Vec<u8>,
    pub(crate) max_held: u32,
    pub(crate) max_hold_ms: u32,
    pub(crate) marks: Option<OwnedFd>,
}

pub(crate) struct SubscriberState {
    seq: u16,
    handle: Option<u32>,
    pub(crate) asked: Asked,
    pub(crate) status: TopicStatus,
    pub(crate) declaration: Vec<u8>,
    /// Which ring this subscriber reads, so one handed over again is known for the same.
    ring: Option<(u64, u64)>,
    /// A ring that has arrived and the subscriber has not taken yet.
    pub(crate) arrived: Option<Reader>,
    /// Off the board: this program's own ring, and the writer that arriving samples go to.
    mirror: Option<(Ring, Writer)>,
}

impl SubscriberState {
    pub(crate) const fn handle(&self) -> Option<u32> {
        self.handle
    }
}

/// What tells one shared memory file from another.
fn identity(fd: &OwnedFd) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::File::from(fd.try_clone().ok()?).metadata().ok()?;
    Some((meta.dev(), meta.ino()))
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
            ring: None,
            taken: false,
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

    pub(crate) fn add_subscriber(&mut self, seq: u16, asked: Asked) -> usize {
        self.subscribers.push(Some(SubscriberState {
            seq,
            handle: None,
            asked,
            status: TopicStatus::Waiting,
            declaration: Vec::new(),
            ring: None,
            arrived: None,
            mirror: None,
        }));
        self.subscribers.len() - 1
    }

    /// Every publisher there is: its index, its declaration and its ring's write end.
    pub(crate) fn publishers_to_restore(&self) -> Vec<(usize, Vec<u8>, Option<OwnedFd>)> {
        self.publishers
            .iter()
            .enumerate()
            .filter_map(|(index, publisher)| {
                let publisher = publisher.as_ref()?;
                let ring = publisher
                    .ring
                    .as_ref()
                    .and_then(|ring| ring.try_clone().ok());
                Some((index, publisher.declaration.clone(), ring))
            })
            .collect()
    }

    /// Marks a publisher as declared again under `seq`, waiting for its answer.
    pub(crate) fn redeclared(&mut self, index: usize, seq: u16) {
        if let Some(publisher) = self.publishers[index].as_mut() {
            publisher.seq = seq;
            publisher.open = false;
            publisher.bells.clear();
            publisher.subscribers = 0;
        }
    }

    /// Every subscriber there is, by index.
    pub(crate) fn subscribers_to_restore(&self) -> Vec<usize> {
        self.subscribers
            .iter()
            .enumerate()
            .filter_map(|(index, subscriber)| subscriber.as_ref().map(|_| index))
            .collect()
    }

    /// Marks a subscriber as subscribed again under `seq`, waiting for its answer.
    pub(crate) fn resubscribed(&mut self, index: usize, seq: u16) {
        if let Some(subscriber) = self.subscribers[index].as_mut() {
            subscriber.seq = seq;
            subscriber.handle = None;
        }
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
            match cx.take_fd(0) {
                Some(fd) if opened.shared_memory => {
                    publisher.ring = fd.try_clone().ok();
                    // A publisher that writes the ring itself already has its writer, on
                    // this same memory if the comm core took the ring back.
                    if !publisher.taken {
                        publisher.writer = Some(Writer::attach(fd).map_err(ring)?);
                    }
                }
                _ => publisher.writer = None,
            }
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
                // The same ring again, after the comm core came back: the reader the
                // subscriber has is still on it, where it was. A new one would start at the
                // newest sample and pass over what was published meanwhile.
                let which = identity(&fd);
                if which.is_none() || which != subscriber.ring {
                    subscriber.ring = which;
                    subscriber.arrived = Some(Reader::attach(fd).map_err(ring)?);
                }
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
        time: orion_ring::SampleTime,
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
