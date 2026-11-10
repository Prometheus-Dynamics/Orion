//! Publishing on behalf.
//!
//! A program on another board, in a browser or on a microcontroller cannot map a ring. It
//! declares a topic like anyone else, is told `shared_memory: false`, and sends each sample as
//! a message. This service writes those samples into the topic's ring and rings the
//! subscribers that are asleep, exactly as a local publisher would. So there is still one
//! model: every subscriber on the board reads a ring, whoever the publisher is.
//!
//! Only a topic's own publisher may send its samples, and only if it was not given the ring: a
//! ring has one writer.
//!
//! What such a publisher gets back is demand. The broker sends it `Demand` whenever its
//! subscribers change, with the count and what the newest subscriber asked for, so it can stop
//! sampling when nobody listens.
//!
//! A sample takes no answer. A sample that cannot be written (a field or a size outside the
//! declaration) is refused, so a publisher with a mistake hears about it.

#![cfg(target_os = "linux")]

use std::cell::RefCell;
use std::rc::Rc;

use orion_broker::{Broker, Topic, TopicId, Topics};
use orion_msg_topics::{Sample, SampleBatch};
use orion_ring::{FieldId, Flags, Writer};
use orion_ring_doorbell::Bell;
use orion_server::{Checked, Cx, Error, Incoming, Policy, Service, SessionId, Sessions, What};
use orion_wire::{FrameView, kind};

/// What the proxy keeps for a topic it writes: the ring's writer and its subscribers'
/// doorbells, as of one `epoch` of the topic.
struct Writing {
    topic: TopicId,
    epoch: u64,
    writer: Writer,
    bells: Vec<Bell>,
}

impl Writing {
    fn new(topic: &Topic) -> Result<Self, Error> {
        let ring = topic.ring.as_ref().ok_or(Error::NotFound)?;
        let writer = ring.writer().map_err(|_| Error::Internal)?;
        let bells = topic
            .subscribers
            .iter()
            .filter_map(|subscription| {
                let (eventfd, page) = subscription.bell.as_ref()?;
                Bell::attach(eventfd.try_clone().ok()?, page.try_clone().ok()?).ok()
            })
            .collect();
        Ok(Self {
            topic: topic.id,
            epoch: topic.epoch,
            writer,
            bells,
        })
    }
}

/// The service. Add it to the server next to the [`Broker`] it was made from.
pub struct BrokerProxy {
    topics: Rc<RefCell<Topics>>,
    policy: Policy,
    writing: Vec<Writing>,
}

impl std::fmt::Debug for BrokerProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BrokerProxy({} topics)", self.writing.len())
    }
}

impl BrokerProxy {
    /// A proxy for `broker`'s topics. `policy` is who may send samples at all; the broker's
    /// own declare policy is the usual choice.
    #[must_use]
    pub fn new(broker: &Broker, policy: Policy) -> Self {
        Self {
            topics: broker.topics(),
            policy,
            writing: Vec::new(),
        }
    }

    /// The writer and doorbells for `handle`, current as of the topic's epoch, after checking
    /// that `from` is the topic's publisher and has no ring of its own.
    fn writing(&mut self, handle: u32, from: SessionId) -> Result<&mut Writing, Error> {
        let topics = self.topics.borrow();
        let topic = topics.get(TopicId(handle)).ok_or(Error::NotFound)?;
        if topic.publisher != Some(from) || topic.publisher_shared {
            return Err(Error::Denied);
        }
        let at = match self
            .writing
            .iter()
            .position(|writing| writing.topic == topic.id)
        {
            Some(at) => at,
            None => {
                self.writing.push(Writing::new(topic)?);
                self.writing.len() - 1
            }
        };
        if self.writing[at].epoch != topic.epoch {
            // The ring, the publisher or the subscribers changed since the last sample.
            self.writing[at] = Writing::new(topic)?;
        }
        Ok(&mut self.writing[at])
    }
}

fn publish(writing: &mut Writing, sample: &Sample<'_>) -> Result<(), Error> {
    writing
        .writer
        .publish(
            FieldId(sample.field),
            sample.time,
            Flags(sample.flags.0),
            sample.payload.0,
        )
        .map(|_| ())
        .map_err(|_| Error::Malformed)
}

impl Service for BrokerProxy {
    fn kinds(&self) -> &'static [u8] {
        &[kind::TOPIC_SAMPLE, kind::TOPIC_SAMPLE_BATCH]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        // Which topic is checked in `handle`, against who declared it.
        Ok((What::Write("topic"), self.policy))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let incoming = request.into_inner();
        let from = cx.from();
        let writing = if incoming.kind() == kind::TOPIC_SAMPLE {
            let sample: Sample<'_> = incoming.body()?;
            let writing = self.writing(sample.handle, from)?;
            publish(writing, &sample)?;
            writing
        } else {
            let batch: SampleBatch<'_> = incoming.body()?;
            let writing = self.writing(batch.handle, from)?;
            let mut bad = false;
            for sample in batch.samples.iter() {
                let Ok(sample) = sample else {
                    bad = true;
                    continue;
                };
                let sample = Sample {
                    handle: batch.handle,
                    field: sample.field,
                    time: sample.time,
                    flags: sample.flags,
                    payload: sample.payload,
                };
                bad |= publish(writing, &sample).is_err();
            }
            if bad {
                // The samples that could be written were. The publisher is told one could not.
                for bell in &mut writing.bells {
                    bell.ring();
                }
                return Err(Error::Malformed);
            }
            writing
        };
        for bell in &mut writing.bells {
            bell.ring();
        }
        Ok(())
    }

    fn closed(&mut self, _session: SessionId, _sessions: &mut Sessions) {
        // Drop what was kept for topics that are gone or changed hands.
        let topics = self.topics.borrow();
        self.writing.retain(|writing| {
            topics
                .get(writing.topic)
                .is_some_and(|topic| topic.epoch == writing.epoch)
        });
    }
}
