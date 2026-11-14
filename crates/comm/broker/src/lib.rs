//! The topic broker.
//!
//! A topic is a ring in shared memory. The broker's whole job is set-up: it makes the ring
//! from a publisher's declaration, hands the write end to that publisher and a read end to
//! each subscriber, once, and passes each subscriber's doorbell to the publisher. After that a
//! publish is a memory write and a wake, between the two programs. The broker is not in it and
//! does nothing until someone connects, subscribes or leaves.
//!
//! - **Declare** makes the ring. The answer, [`Opened`](orion_msg_topics::Opened), carries the
//!   write end.
//! - **Subscribe** answers with the topic's declaration and then `Opened` with a read end. A
//!   subscriber sends its doorbell with the request; the broker passes it to the publisher. A
//!   subscribe before the topic exists is held: the subscriber is told `Waiting` and gets its
//!   `Opened` when someone declares the topic. Start order does not matter.
//! - **A publisher that goes away** leaves the ring where it is. Its subscribers are told. A
//!   publisher that declares the same topic again carries on in the same ring, and subscribers
//!   need do nothing. A different declaration under the same name replaces the ring, and
//!   subscribers are sent the new one.
//! - **A comm core that restarts** has no rings, but its publishers do: they kept publishing.
//!   A publisher declares again with its ring's write end attached, and the broker adopts that
//!   ring instead of making one. Nothing that was flowing stops.
//! - **One cap** (`topics.max_bytes`) bounds all ring memory on the board. A declaration over
//!   it is refused.
//!
//! A program that cannot map shared memory is answered with `shared_memory: false`. What
//! carries its samples is `orion-broker-proxy`.

#![cfg(target_os = "linux")]

mod local;
mod service;
mod topics;

pub use local::{LocalPublisher, Writing};
pub use service::{Broker, RESTORE_MS, STALE_ID};
pub use topics::{BrokerConfig, Subscription, Topic, TopicId, TopicWatch, Topics};
