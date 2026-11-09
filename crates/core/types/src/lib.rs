//! Names, ids, time and the one error type.
//!
//! Everything here is `no_std` and allocates nothing, so a microcontroller and a daemon share the
//! same types. Names are validated when they are made, including when they arrive from the wire.

#![no_std]

mod error;
mod hlc;
mod names;
mod time;

pub use error::Error;
pub use hlc::{HlcClockSkew, HlcTimestamp, HybridLogicalClock, hlc_node_tag};
pub use names::{
    ActionName, DeviceName, FileName, KeyId, NodeName, ResourceName, TopicName, TopicRef,
    WorkloadKind, WorkloadName,
};
pub use time::FrameTime;
