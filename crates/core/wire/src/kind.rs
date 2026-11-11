//! The kind table. Every message kind in Orion is here, by range, so later work can't collide.
//!
//! Numbers are never reused. Adding a kind means adding a line at the end of its range.

/// What a kind belongs to: the high four bits of its number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Concept {
    /// `0x00` to `0x0F`.
    Session = 0x0,
    /// `0x10` to `0x1F`.
    Records = 0x1,
    /// `0x20` to `0x2F`.
    Files = 0x2,
    /// `0x30` to `0x3F`.
    Actions = 0x3,
    /// `0x40` to `0x4F`.
    Topics = 0x4,
    /// `0x50` to `0x5F`.
    Workloads = 0x5,
    /// `0x60` to `0x6F`.
    Peers = 0x6,
    /// `0x70` to `0x7F`.
    Time = 0x7,
}

/// One row of the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KindInfo {
    /// The number on the wire.
    pub kind: u8,
    /// What it belongs to.
    pub concept: Concept,
    /// Its name.
    pub name: &'static str,
}

macro_rules! kinds {
    ($($concept:ident { $($name:ident = $value:literal,)* })*) => {
        $($(
            #[doc = concat!("Kind number of `", stringify!($name), "`.")]
            pub const $name: u8 = $value;
        )*)*

        /// Every kind, in order.
        pub const TABLE: &[KindInfo] = &[
            $($(KindInfo { kind: $value, concept: Concept::$concept, name: stringify!($name) },)*)*
        ];
    };
}

kinds! {
    Session {
        HELLO = 0x01,
        WELCOME = 0x02,
        REJECT = 0x03,
        PING = 0x04,
        PONG = 0x05,
        REFUSAL = 0x06,
        IDENTIFY = 0x07,
        IDENTITY = 0x08,
        LINK_OFFER = 0x09,
        LINK_TAKEN = 0x0A,
        NODE_ATTACH = 0x0B,
        NODE_REQUEST = 0x0C,
        NODE_ANSWER = 0x0D,
        NODE_GONE = 0x0E,
    }
    Records {
        PUT = 0x10,
        GET = 0x11,
        WATCH = 0x12,
        CHANGED = 0x13,
    }
    Files {
        FILE_BEGIN = 0x20,
        FILE_CHUNK = 0x21,
        FILE_COMMIT = 0x22,
        FILE_STATUS = 0x23,
        FILE_PULL = 0x24,
        FILE_SINK = 0x25,
        FILE_SINK_READY = 0x26,
    }
    Actions {
        ACTION_REGISTER = 0x30,
        ACTION_RUN = 0x31,
        ACTION_CLAIM = 0x32,
        ACTION_PROGRESS = 0x33,
        ACTION_RESULT = 0x34,
        ACTION_CANCEL = 0x35,
        ACTION_ATTACH = 0x36,
        ACTION_REGISTERED = 0x37,
    }
    Topics {
        TOPIC_DECLARE = 0x40,
        TOPIC_OPEN = 0x41,
        TOPIC_OPENED = 0x42,
        TOPIC_SUBSCRIBE = 0x43,
        TOPIC_UNSUBSCRIBE = 0x44,
        TOPIC_SAMPLE = 0x45,
        TOPIC_SAMPLE_BATCH = 0x46,
        TOPIC_DEMAND = 0x47,
        TOPIC_BUFFER_OFFER = 0x48,
        TOPIC_BELL = 0x49,
        TOPIC_STATE = 0x4A,
        TOPIC_HOLDS = 0x4B,
    }
    Workloads {
        WORKLOAD_PUT = 0x50,
        WORKLOAD_REMOVE = 0x51,
        WORKLOAD_STATUS = 0x52,
        EXECUTOR_OFFER = 0x58,
        EXECUTOR_START = 0x59,
        EXECUTOR_STOP = 0x5A,
        EXECUTOR_KILL = 0x5B,
        EXECUTOR_REPORT = 0x5C,
        EXECUTOR_INVENTORY = 0x5D,
        EXECUTOR_ASK = 0x5E,
    }
    Peers {
        PEER_SYNC = 0x60,
        PEER_OBSERVED = 0x61,
        ENROLL_HELLO = 0x62,
        ENROLL_CHALLENGE = 0x63,
        ENROLL_CONFIRM = 0x64,
        PEER_REVOKE = 0x65,
        OPERATOR_ENROLL = 0x66,
        OPERATOR_WELCOME = 0x67,
        OPERATOR_LIST = 0x68,
        OPERATORS = 0x69,
    }
    Time {
        TIME_REQUEST = 0x70,
        TIME_REPLY = 0x71,
        TIME_OFFER = 0x72,
        TIME_TURN = 0x73,
    }
}

// Fails the build if a number is repeated, out of order, or outside its concept's range.
const _: () = {
    let mut index = 0;
    while index < TABLE.len() {
        assert!(TABLE[index].kind >> 4 == TABLE[index].concept as u8);
        assert!(index == 0 || TABLE[index - 1].kind < TABLE[index].kind);
        index += 1;
    }
};

/// The row for `kind`, or `None` for one this build doesn't know. A receiver ignores those.
#[must_use]
pub fn info(kind: u8) -> Option<&'static KindInfo> {
    TABLE
        .binary_search_by_key(&kind, |row| row.kind)
        .ok()
        .and_then(|index| TABLE.get(index))
}

/// The revision each kind arrived in. Everything so far is revision 1, the first one.
#[must_use]
pub const fn since_revision(_kind: u8) -> u16 {
    1
}
