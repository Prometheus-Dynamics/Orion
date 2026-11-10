//! Topic bodies. Set-up messages make a ring and hand it over once. After that a local publish is
//! a memory write. [`Sample`] is for those that can't write shared memory: remote and
//! microcontroller publishers, and the comm core writes the ring for them.

#![no_std]

use orion_types::{FrameTime, TopicName, TopicRef};
use orion_wire::{Bytes, List, body, kind, open_enum};
use serde::{Deserialize, Serialize};

open_enum! {
    /// The type of a field.
    pub enum FieldType: u8 {
        /// `true` or `false`.
        Bool = 1,
        /// 64-bit signed integer.
        Int = 2,
        /// 64-bit float.
        Float = 3,
        /// UTF-8 text.
        Text = 4,
        /// Bytes.
        Bytes = 5,
        /// Array of 64-bit floats.
        FloatArray = 6,
        /// Array of 64-bit signed integers.
        IntArray = 7,
        /// A structured value with a pinned type name.
        Structured = 8,
        /// Buffer numbers: a frame topic.
        Buffers = 9,
    }
}

/// Generic flags on a sample.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SampleFlags(pub u8);

impl SampleFlags {
    /// No flags.
    pub const NONE: Self = Self(0);
    /// Others depend on this one, so a slow reader that lost it should skip to the next sample
    /// without this flag's dependents.
    pub const OTHERS_DEPEND: Self = Self(1 << 0);

    /// Whether every flag in `other` is set.
    #[must_use]
    pub const fn has(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

body! {
    /// One field of a topic.
    pub struct Field<'a> {
        /// Its name.
        pub name: &'a str,
        /// Its type.
        pub ty: FieldType,
        /// Its unit. Empty for none.
        pub unit: &'a str,
        /// The pinned type name of a structured value. Empty otherwise.
        pub type_name: &'a str,
    }
}

body! {
    /// One sample inside a [`SampleBatch`].
    pub struct BatchSample<'a> {
        /// The field.
        pub field: u16,
        /// The time it belongs to.
        pub time: FrameTime,
        /// Its flags.
        pub flags: SampleFlags,
        /// The value.
        pub payload: Bytes<'a>,
    }
}

body! {
    /// One buffer of a frame topic. The descriptors travel with the frame.
    pub struct BufferInfo {
        /// Its number, used in ring entries.
        pub index: u32,
        /// Its size.
        pub size: u64,
        /// Which of the frame's descriptors backs it.
        pub descriptor: u8,
        /// Where in that descriptor it starts.
        pub offset: u64,
    }
}

body! {
    /// Declares a topic.
    pub struct Declare<'a> = kind::TOPIC_DECLARE {
        /// Its name.
        pub topic: TopicName,
        /// Its fields.
        pub fields: List<'a, Field<'a>>,
        /// How many samples to keep. A latest-value topic is 2.
        pub history: u32,
        /// How long a value stays valid. Zero for always.
        pub expiry_ms: u32,
        /// The largest sample.
        pub max_payload: u32,
    }
}

body! {
    /// Asks for the handles to a topic.
    pub struct Open = kind::TOPIC_OPEN {
        /// The topic.
        pub topic: TopicRef,
        /// Whether the caller will publish.
        pub write: bool,
    }
}

body! {
    /// The handles to a topic. The ring and doorbell descriptors travel with the frame.
    pub struct Opened = kind::TOPIC_OPENED {
        /// The `seq` of the request.
        pub request_seq: u16,
        /// This topic's number on this connection.
        pub handle: u32,
        /// Whether descriptors came with this frame. If not, samples arrive as messages.
        pub shared_memory: bool,
        /// The size of the ring.
        pub ring_bytes: u64,
    }
}

body! {
    /// Subscribes. Answered with [`Opened`].
    pub struct Subscribe<'a> = kind::TOPIC_SUBSCRIBE {
        /// The topic.
        pub topic: TopicRef,
        /// Passed to the publisher unread, so one capture can serve all subscribers.
        pub request: Bytes<'a>,
        /// For a frame topic: how many buffers this subscriber may hold. Zero for no limit.
        pub max_held: u32,
        /// For a frame topic: how long it may hold one. Zero for no limit.
        pub max_hold_ms: u32,
    }
}

body! {
    /// Ends a subscription.
    pub struct Unsubscribe = kind::TOPIC_UNSUBSCRIBE {
        /// The handle from [`Opened`].
        pub handle: u32,
    }
}

body! {
    /// One sample, for a publisher or subscriber without shared memory.
    pub struct Sample<'a> = kind::TOPIC_SAMPLE {
        /// The handle from [`Opened`].
        pub handle: u32,
        /// The field.
        pub field: u16,
        /// The time it belongs to.
        pub time: FrameTime,
        /// Its flags.
        pub flags: SampleFlags,
        /// The value.
        pub payload: Bytes<'a>,
    }
}

body! {
    /// Several samples in one frame.
    pub struct SampleBatch<'a> = kind::TOPIC_SAMPLE_BATCH {
        /// The handle from [`Opened`].
        pub handle: u32,
        /// The samples.
        pub samples: List<'a, BatchSample<'a>>,
    }
}

body! {
    /// Tells a publisher without shared memory whether anyone is listening.
    pub struct Demand<'a> = kind::TOPIC_DEMAND {
        /// The handle from [`Opened`].
        pub handle: u32,
        /// How many subscribers there are.
        pub subscribers: u32,
        /// A subscriber's request, passed on unread. Empty if unchanged.
        pub request: Bytes<'a>,
    }
}

body! {
    /// Gives Orion a frame topic's buffers, or hands them to a subscriber. Sent once per pool.
    pub struct BufferOffer<'a> = kind::TOPIC_BUFFER_OFFER {
        /// The handle from [`Opened`].
        pub handle: u32,
        /// Goes up when the pool is replaced, so old ring entries can be told from new.
        pub generation: u32,
        /// The buffers.
        pub buffers: List<'a, BufferInfo>,
    }
}

body! {
    /// Hands a publisher one subscriber's doorbell, or takes it back. The doorbell's eventfd
    /// and its page, read-only, travel with the frame when `present` is set.
    pub struct Bell = kind::TOPIC_BELL {
        /// The handle from the publisher's [`Opened`].
        pub handle: u32,
        /// Which subscriber: a number that names it for as long as it subscribes.
        pub subscriber: u32,
        /// Whether the subscriber is there. `false` takes its doorbell back.
        pub present: bool,
    }
}

open_enum! {
    /// Where a topic is, as its subscribers are told.
    pub enum TopicStatus: u8 {
        /// A publisher is there and the subscriber has the ring.
        Live = 1,
        /// The publisher is gone. The ring stays, and a publisher that comes back carries on
        /// in it.
        PublisherGone = 2,
        /// Nobody has declared the topic yet. An [`Opened`] follows when someone does.
        Waiting = 3,
    }
}

body! {
    /// Tells a subscriber where its topic is.
    pub struct TopicState = kind::TOPIC_STATE {
        /// The `seq` of the subscribe this answers, when it is the first answer to one.
        pub request_seq: u16,
        /// The subscription's handle.
        pub handle: u32,
        /// Where the topic is.
        pub status: TopicStatus,
    }
}

body! {
    /// One subscriber's hold marks on a frame topic, between the broker and the two ends.
    ///
    /// - Broker to publisher, with the subscriber's page of marks attached, read-only: honour
    ///   this subscriber's marks, in reader slot `subscriber`, within these limits. With
    ///   `present` unset: the subscriber is gone, forget its marks.
    /// - Publisher to broker, nothing attached: done. Only then is the subscriber given the
    ///   buffers, so it never reads a frame the publisher is not yet protecting.
    /// - Broker to subscriber: this is your reader slot.
    pub struct Holds = kind::TOPIC_HOLDS {
        /// The topic's handle.
        pub handle: u32,
        /// The subscriber's reader slot in the publisher's pool.
        pub subscriber: u32,
        /// How many buffers it may hold. Zero for no limit.
        pub max_held: u32,
        /// How long it may hold one, in milliseconds. Zero for no limit.
        pub max_hold_ms: u32,
        /// Whether the subscriber is there.
        pub present: bool,
    }
}
