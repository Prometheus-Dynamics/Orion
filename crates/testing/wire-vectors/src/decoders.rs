//! Every decoder on the wire, callable from arbitrary bytes. None of these may panic.

use orion_msg_actions as actions;
use orion_msg_files as files;
use orion_msg_peers as peers;
use orion_msg_records as records;
use orion_msg_session as session;
use orion_msg_topics as topics;
use orion_msg_workloads as workloads;
use orion_wire::kind::{self, Concept};
use orion_wire::socket::{self, Parsed, SocketError};
use orion_wire::{Decode, Reassembler, StreamDecoder, body::encode_fields, frame};

const SCRATCH: usize = 4096;

/// Decodes `bytes` as `T`, and encodes what came out.
fn exercise<'a, T: Decode<'a> + orion_wire::__serde::Serialize>(bytes: &'a [u8]) {
    if let Ok(value) = T::decode(bytes) {
        let mut scratch = [0; SCRATCH];
        let _ = encode_fields(&value, &mut scratch);
    }
}

macro_rules! decoders {
    ($($kind:ident => $ty:path,)*) => {
        /// Runs the decoder for `kind`. `false` if `kind` has none.
        pub fn decode_kind(kind: u8, bytes: &[u8]) -> bool {
            match kind {
                $(kind::$kind => { exercise::<$ty>(bytes); true })*
                _ => false,
            }
        }
    };
}

decoders! {
    HELLO => session::Hello,
    WELCOME => session::Welcome,
    REJECT => session::Reject,
    PING => session::Ping,
    PONG => session::Pong,
    IDENTIFY => session::Identify,
    LINK_OFFER => session::LinkOffer,
    LINK_TAKEN => session::LinkTaken,
    IDENTITY => session::Identity,
    REFUSAL => session::Refusal,
    PUT => records::Put,
    GET => records::Get,
    WATCH => records::Watch,
    CHANGED => records::Changed,
    FILE_BEGIN => files::FileBegin,
    FILE_CHUNK => files::FileChunk,
    FILE_COMMIT => files::FileCommit,
    FILE_STATUS => files::FileStatus,
    FILE_PULL => files::FilePull,
    ACTION_REGISTER => actions::Register,
    ACTION_RUN => actions::Run,
    ACTION_CLAIM => actions::Claim,
    ACTION_PROGRESS => actions::Progress,
    ACTION_RESULT => actions::ActionResult,
    ACTION_CANCEL => actions::Cancel,
    ACTION_ATTACH => actions::Attach,
    ACTION_REGISTERED => actions::Registered,
    TOPIC_DECLARE => topics::Declare,
    TOPIC_OPEN => topics::Open,
    TOPIC_OPENED => topics::Opened,
    TOPIC_SUBSCRIBE => topics::Subscribe,
    TOPIC_UNSUBSCRIBE => topics::Unsubscribe,
    TOPIC_SAMPLE => topics::Sample,
    TOPIC_SAMPLE_BATCH => topics::SampleBatch,
    TOPIC_DEMAND => topics::Demand,
    TOPIC_BUFFER_OFFER => topics::BufferOffer,
    TOPIC_BELL => topics::Bell,
    TOPIC_STATE => topics::TopicState,
    TOPIC_HOLDS => topics::Holds,
    WORKLOAD_PUT => workloads::WorkloadPut,
    WORKLOAD_REMOVE => workloads::WorkloadRemove,
    WORKLOAD_STATUS => workloads::WorkloadStatus,
    EXECUTOR_OFFER => workloads::ExecutorOffer,
    EXECUTOR_START => workloads::ExecutorStart,
    EXECUTOR_STOP => workloads::ExecutorStop,
    EXECUTOR_KILL => workloads::ExecutorKill,
    EXECUTOR_REPORT => workloads::ExecutorReport,
    EXECUTOR_INVENTORY => workloads::ExecutorInventory,
    PEER_SYNC => peers::PeerSync,
    PEER_OBSERVED => peers::PeerObserved,
    ENROLL_HELLO => peers::EnrollHello,
    ENROLL_CHALLENGE => peers::EnrollChallenge,
    ENROLL_CONFIRM => peers::EnrollConfirm,
    PEER_REVOKE => peers::PeerRevoke,
    OPERATOR_ENROLL => peers::OperatorEnroll,
    OPERATOR_WELCOME => peers::OperatorWelcome,
    OPERATOR_LIST => peers::OperatorList,
    OPERATORS => peers::Operators,
    TIME_REQUEST => peers::TimeRequest,
    TIME_REPLY => peers::TimeReply,
    TIME_OFFER => peers::TimeOffer,
    TIME_TURN => peers::TimeTurn,
}

/// List elements decode separately from their bodies, so they get their own turn.
fn decode_elements(concept: Concept, bytes: &[u8]) {
    match concept {
        Concept::Records => {
            exercise::<records::RecordEntry>(bytes);
            exercise::<records::NodeRecord>(bytes);
            exercise::<records::ResourceDescription>(bytes);
            exercise::<records::Offered>(bytes);
            exercise::<records::ResourceRecord>(bytes);
            exercise::<records::LeaseRecord>(bytes);
            exercise::<records::Label>(bytes);
        }
        Concept::Topics => {
            exercise::<topics::Field>(bytes);
            exercise::<topics::BatchSample>(bytes);
            exercise::<topics::BufferInfo>(bytes);
        }
        Concept::Workloads => {
            exercise::<workloads::NeededFile>(bytes);
            exercise::<workloads::RequiredLabel>(bytes);
            exercise::<workloads::OfferedKind>(bytes);
            exercise::<workloads::InventoryItem>(bytes);
        }
        Concept::Peers => {
            exercise::<peers::GrantedAction>(bytes);
            exercise::<peers::OperatorEntry>(bytes);
        }
        Concept::Session | Concept::Files | Concept::Actions | Concept::Time => {}
    }
}

/// Runs every decoder belonging to `concept` on `bytes`.
pub fn decode_concept(concept: Concept, bytes: &[u8]) {
    for row in kind::TABLE.iter().filter(|row| row.concept == concept) {
        decode_kind(row.kind, bytes);
    }
    decode_elements(concept, bytes);
}

/// Decodes `bytes` as a plain frame, and its body by kind.
pub fn plain_frame(bytes: &[u8]) {
    if let Ok(view) = frame::decode(bytes) {
        decode_kind(view.kind(), view.payload());
    }
}

/// Feeds `bytes` to a COBS decoder.
pub fn stream(bytes: &[u8]) {
    let mut decoder = StreamDecoder::<512>::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let (used, _) = decoder.push_slice(rest);
        if used == 0 {
            break;
        }
        rest = &rest[used..];
    }
}

/// Feeds `bytes` to a CAN reassembler. The first byte picks the segment size.
pub fn packet(bytes: &[u8]) {
    let Some((&first, rest)) = bytes.split_first() else {
        return;
    };
    let size = usize::from(first % 64) + 1;
    let mut reassembler = Reassembler::<512>::new();
    for segment in rest.chunks(size) {
        let _ = reassembler.push(segment);
    }
}

/// Parses `bytes` as prefixed socket frames.
pub fn socket(bytes: &[u8]) {
    let mut rest = bytes;
    loop {
        match socket::parse(rest, 1024) {
            Ok(Parsed::Frame { frame, consumed }) => {
                decode_kind(frame.kind(), frame.payload());
                rest = &rest[consumed..];
            }
            Err(SocketError::Frame { consumed, .. }) => rest = &rest[consumed..],
            Ok(Parsed::Incomplete { .. }) | Err(_) => return,
        }
    }
}
