//! Writing a topic's ring from inside the comm core: for a publisher that cannot map shared
//! memory, and for the comm core's own topics.

use std::cell::RefCell;
use std::rc::Rc;

use orion_ring::{FieldId, Flags, FrameTime, Writer};
use orion_ring_doorbell::Bell;
use orion_server::Error;

use crate::topics::{Topic, TopicId, Topics};

/// The writer of a topic's ring and its subscribers' doorbells, as of one epoch of the topic.
#[derive(Debug)]
pub struct Writing {
    /// The topic.
    pub topic: TopicId,
    /// The epoch this was made at. When the topic's moves on, make another.
    pub epoch: u64,
    writer: Writer,
    bells: Vec<Bell>,
}

impl Writing {
    /// A writer for `topic` as it is now.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the topic has no ring, [`Error::Internal`] if the ring can't be
    /// mapped.
    pub fn new(topic: &Topic) -> Result<Self, Error> {
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

    /// Writes one sample. Does not wake anyone: call [`Writing::wake`] after the last one.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] if the field or the size is outside the declaration.
    pub fn publish(
        &mut self,
        field: FieldId,
        time: FrameTime,
        flags: Flags,
        payload: &[u8],
    ) -> Result<(), Error> {
        self.writer
            .publish(field, time, flags, payload)
            .map(|_| ())
            .map_err(|_| Error::Malformed)
    }

    /// Rings every subscriber that is asleep.
    pub fn wake(&mut self) {
        for bell in &mut self.bells {
            bell.ring();
        }
    }
}

/// A topic the comm core publishes itself.
#[derive(Debug)]
pub struct LocalPublisher {
    topics: Rc<RefCell<Topics>>,
    id: TopicId,
    writing: Option<Writing>,
}

impl LocalPublisher {
    /// A publisher for the internal topic `id`.
    #[must_use]
    pub const fn new(topics: Rc<RefCell<Topics>>, id: TopicId) -> Self {
        Self {
            topics,
            id,
            writing: None,
        }
    }

    /// The topic.
    #[must_use]
    pub const fn id(&self) -> TopicId {
        self.id
    }

    /// How many subscribers the topic has.
    #[must_use]
    pub fn subscribers(&self) -> u32 {
        self.topics
            .borrow()
            .get(self.id)
            .map_or(0, Topic::subscriber_count)
    }

    /// Publishes `samples` (field and payload, all at `time`) and wakes sleeping subscribers.
    ///
    /// Must not be called from inside a [`crate::TopicWatch`]: the table is in use there.
    ///
    /// # Errors
    ///
    /// As [`Writing::new`] and [`Writing::publish`].
    pub fn publish(&mut self, time: FrameTime, samples: &[(FieldId, &[u8])]) -> Result<(), Error> {
        let topics = self.topics.borrow();
        let topic = topics.get(self.id).ok_or(Error::NotFound)?;
        if self
            .writing
            .as_ref()
            .is_none_or(|writing| writing.epoch != topic.epoch)
        {
            self.writing = Some(Writing::new(topic)?);
        }
        drop(topics);
        let writing = self.writing.as_mut().ok_or(Error::Internal)?;
        for (field, payload) in samples {
            writing.publish(*field, time, Flags::NONE, payload)?;
        }
        writing.wake();
        Ok(())
    }
}
