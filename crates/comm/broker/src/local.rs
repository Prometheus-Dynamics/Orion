//! Writing a topic's ring from inside the comm core: for a publisher that cannot map shared
//! memory, and for the comm core's own topics.

use std::cell::RefCell;
use std::rc::Rc;

use orion_msg_topics::{Declare, Field, FieldType, OnSubscribe};
use orion_server::Error;
use orion_types::TopicName;
use orion_wire::List;
use orion_wire::body::encode_fields;
use shm_ring::{FieldId, Flags, SampleTime, Spec, Writer};
use shm_ring_doorbell::Bell;

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
        time: SampleTime,
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

    /// Declares a topic the comm core publishes itself and returns its publisher. `fields` are
    /// name, type and unit; every field keeps `history` values of up to `max_payload` bytes.
    ///
    /// # Errors
    ///
    /// As [`Topics::declare_internal`], and [`Error::Malformed`] for a name that is not one.
    pub fn declare(
        topics: &Rc<RefCell<Topics>>,
        name: &str,
        fields: &[(&str, FieldType, &str)],
        history: u32,
        max_payload: u32,
    ) -> Result<Self, Error> {
        let name: TopicName = name.parse()?;
        let fields: Vec<Field<'_>> = fields
            .iter()
            .map(|(name, ty, unit)| Field {
                name,
                ty: *ty,
                unit,
                type_name: "",
            })
            .collect();
        let declare = Declare {
            topic: name.clone(),
            fields: List::new(&fields),
            history,
            expiry_ms: 0,
            max_payload,
            on_subscribe: OnSubscribe::FromNow,
        };
        let mut bytes = vec![0_u8; 64 + fields.len() * 96];
        let len = encode_fields(&declare, &mut bytes)?;
        let spec = Spec {
            fields: fields.len() as u32,
            history,
            max_payload,
            expiry_ms: 0,
        };
        let id = topics
            .borrow_mut()
            .declare_internal(&name, &bytes[..len], &spec)?;
        Ok(Self::new(topics.clone(), id))
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
    pub fn publish(&mut self, time: SampleTime, samples: &[(FieldId, &[u8])]) -> Result<(), Error> {
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
