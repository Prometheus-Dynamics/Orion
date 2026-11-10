//! One filled-in sample of every kind. Each is checked to survive a round trip as it is built.

use orion_check::{Admits, Policy};
use orion_msg_actions::{
    ActionResult, Attach, Cancel, Claim, Outcome, Progress, Register, Run, Target,
};
use orion_msg_files::{FileBegin, FileChunk, FileCommit, FilePull, FileState, FileStatus};
use orion_msg_peers::{
    EnrollChallenge, EnrollConfirm, EnrollHello, EnrollRole, GrantedAction, OperatorEnroll,
    OperatorEntry, OperatorList, OperatorState, OperatorWelcome, Operators, PeerObserved,
    PeerRevoke, PeerSync, TimeOffer, TimeReply, TimeRequest, TimeTurn,
};
use orion_msg_records::{Changed, Get, Label, NodeRecord, Put, RecordClass, RecordEntry, Watch};
use orion_msg_session::{
    Hello, Identify, Identity, IdentityField, Ping, Pong, Refusal, Reject, RejectReason, Roles,
    Welcome,
};
use orion_msg_topics::{
    BatchSample, Bell, BufferInfo, BufferOffer, Declare, Demand, Field, FieldType, Holds, Open,
    Opened, Sample as TopicSample, SampleBatch, SampleFlags, Subscribe, TopicState, TopicStatus,
    Unsubscribe,
};
use orion_msg_workloads::{
    ExecutorInventory, ExecutorKill, ExecutorOffer, ExecutorReport, ExecutorStart, ExecutorStop,
    InventoryItem, NeededFile, OfferedKind, RequiredLabel, RestartPolicy, WorkloadPut,
    WorkloadRemove, WorkloadState, WorkloadStatus,
};
use orion_types::{
    ActionName, Error, FileName, FrameTime, HlcTimestamp, KeyId, NodeName, ResourceName, TopicName,
    TopicRef, WorkloadKind, WorkloadName,
};
use orion_wire::{Body, Bytes, Decode, List, body::encode_fields};

/// One recorded body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sample {
    /// Its kind.
    pub kind: u8,
    /// Its encoded fields.
    pub bytes: Vec<u8>,
}

fn kind_of<B: Body>(_: &B) -> u8 {
    B::KIND
}

fn decode_like<'a, T: Decode<'a>>(_: &T, bytes: &'a [u8]) -> Result<T, Error> {
    T::decode(bytes)
}

macro_rules! sample {
    ($out:ident, $value:expr) => {{
        let value = $value;
        let mut buf = [0_u8; 1024];
        let len = encode_fields(&value, &mut buf).expect("sample fits");
        let decoded = decode_like(&value, &buf[..len]).expect("sample decodes");
        assert_eq!(decoded, value);
        let mut again = [0_u8; 1024];
        let again_len = encode_fields(&decoded, &mut again).expect("sample fits");
        assert_eq!(
            &again[..again_len],
            &buf[..len],
            "re-encoding changed the bytes"
        );
        $out.push(Sample {
            kind: kind_of(&value),
            bytes: buf[..len].to_vec(),
        });
    }};
}

fn node(name: &str) -> NodeName {
    NodeName::new(name).expect("valid")
}

fn workload(name: &str) -> WorkloadName {
    WorkloadName::new(name).expect("valid")
}

fn work_kind() -> WorkloadKind {
    WorkloadKind::new("graph").expect("valid")
}

fn action() -> ActionName {
    ActionName::new("restart").expect("valid")
}

fn file() -> FileName {
    FileName::new("slots/boot.img").expect("valid")
}

fn topic() -> TopicRef {
    TopicRef {
        node: Some(node("left")),
        topic: TopicName::new("imu").expect("valid"),
    }
}

fn target() -> Target {
    Target::Resource(ResourceName::new("sensors/imu0").expect("valid"))
}

const KEY: KeyId = KeyId([0xAB; 32]);
const DIGEST: [u8; 32] = [0x5A; 32];
const STAMP: HlcTimestamp = HlcTimestamp::new(1_700_000_000_123, 7, 0x1122_3344_5566_7788);

/// Every sample, one per kind.
#[must_use]
pub fn all() -> Vec<Sample> {
    let mut out = Vec::new();
    session(&mut out);
    records(&mut out);
    files(&mut out);
    actions(&mut out);
    topics(&mut out);
    workloads(&mut out);
    peers(&mut out);
    out
}

fn session(out: &mut Vec<Sample>) {
    sample!(
        out,
        Hello {
            name: "tool",
            roles: Roles::CLIENT.and(Roles::PROVIDER),
            max_frame: 65_536,
            revision: 1
        }
    );
    sample!(
        out,
        Welcome {
            node: node("left"),
            session: 42,
            heartbeat_ms: 1000,
            max_frame: 4096,
            revision: 1
        }
    );
    sample!(
        out,
        Reject {
            reason: RejectReason::Busy
        }
    );
    sample!(out, Ping { now_ms: 123_456 });
    sample!(out, Pong { now_ms: 123_456 });
    sample!(
        out,
        Refusal {
            request_seq: 9,
            code: Error::Denied.code(),
            detail: "no"
        }
    );
    sample!(
        out,
        Identify {
            nonce: Bytes(&[1, 2, 3])
        }
    );
    let fields = [IdentityField {
        name: "model",
        ty: 4,
        value: Bytes(b"x1"),
    }];
    sample!(
        out,
        Identity {
            request_seq: 9,
            node: node("left"),
            cluster: "lab",
            public_key: Bytes(&[7; 4]),
            time_ms: 1_700_000_000_000,
            revision: 1,
            fields: List::new(&fields),
            nonce: Bytes(&[1, 2, 3]),
            signature: Bytes(&[9; 4])
        }
    );
}

fn records(out: &mut Vec<Sample>) {
    let labels = [Label {
        key: "zone",
        value: "a",
    }];
    let value = NodeRecord {
        name: node("left"),
        address: "10.0.0.1:9200",
        labels: List::new(&labels),
    };
    let mut buf = [0; 128];
    let len = encode_fields(&value, &mut buf).expect("fits");
    let entries = [RecordEntry {
        class: RecordClass::Node,
        key: "left",
        stamp: STAMP,
        deleted: false,
        value: Bytes(&buf[..len]),
    }];
    sample!(
        out,
        Put {
            records: List::new(&entries)
        }
    );
    sample!(
        out,
        Get {
            class: RecordClass::Resource,
            prefix: "sensors/"
        }
    );
    sample!(
        out,
        Watch {
            class: RecordClass::Lease,
            prefix: "",
            after: STAMP
        }
    );
    sample!(
        out,
        Changed {
            request_seq: 3,
            records: List::new(&entries),
            more: true
        }
    );
}

fn files(out: &mut Vec<Sample>) {
    sample!(
        out,
        FileBegin {
            transfer: 1,
            destination: file(),
            size: 1 << 33,
            digest: DIGEST
        }
    );
    sample!(
        out,
        FileChunk {
            transfer: 1,
            offset: 4096,
            data: Bytes(b"chunk")
        }
    );
    sample!(out, FileCommit { transfer: 1 });
    sample!(
        out,
        FileStatus {
            transfer: 1,
            received: 8192,
            state: FileState::Receiving,
            detail: ""
        }
    );
    sample!(
        out,
        FilePull {
            transfer: 2,
            url: "https://example.com/a",
            destination: file(),
            size: 10,
            digest: DIGEST
        }
    );
}

fn actions(out: &mut Vec<Sample>) {
    let policy = Policy::new(Admits::LOCAL.and(Admits::OPERATOR));
    sample!(
        out,
        Register {
            name: action(),
            target: Target::Node(node("left")),
            policy,
            description: "restart it"
        }
    );
    sample!(
        out,
        Run {
            call: 7,
            name: action(),
            target: target(),
            args: Bytes(b"a"),
            deadline_ms: 5000,
            detached: true
        }
    );
    sample!(
        out,
        Claim {
            call: 7,
            name: action(),
            target: target(),
            args: Bytes(b"a"),
            deadline_ms: 5000
        }
    );
    sample!(
        out,
        Progress {
            call: 7,
            percent: 40,
            message: "halfway",
            data: Bytes(b"d")
        }
    );
    sample!(
        out,
        ActionResult {
            call: 7,
            outcome: Outcome::Succeeded,
            output: Bytes(b"o"),
            detail: ""
        }
    );
    sample!(out, Cancel { call: 7 });
    sample!(out, Attach { call: 7 });
}

fn topics(out: &mut Vec<Sample>) {
    let fields = [
        Field {
            name: "x",
            ty: FieldType::Float,
            unit: "m",
            type_name: "",
        },
        Field {
            name: "pose",
            ty: FieldType::Structured,
            unit: "",
            type_name: "pose3d/1",
        },
    ];
    let name = TopicName::new("imu").expect("valid");
    sample!(
        out,
        Declare {
            topic: name,
            fields: List::new(&fields),
            history: 512,
            expiry_ms: 100,
            max_payload: 64
        }
    );
    sample!(
        out,
        Open {
            topic: topic(),
            write: false
        }
    );
    sample!(
        out,
        Opened {
            request_seq: 4,
            handle: 2,
            shared_memory: true,
            ring_bytes: 1 << 20
        }
    );
    sample!(
        out,
        Subscribe {
            topic: topic(),
            request: Bytes(b"want"),
            max_held: 3,
            max_hold_ms: 50
        }
    );
    sample!(out, Unsubscribe { handle: 2 });
    sample!(
        out,
        TopicSample {
            handle: 2,
            field: 1,
            time: FrameTime::from_nanos(99),
            flags: SampleFlags::OTHERS_DEPEND,
            payload: Bytes(b"v")
        }
    );
    let batch = [
        BatchSample {
            field: 0,
            time: FrameTime::from_nanos(1),
            flags: SampleFlags::NONE,
            payload: Bytes(b"a"),
        },
        BatchSample {
            field: 1,
            time: FrameTime::from_nanos(2),
            flags: SampleFlags::NONE,
            payload: Bytes(b"bc"),
        },
    ];
    sample!(
        out,
        SampleBatch {
            handle: 2,
            samples: List::new(&batch)
        }
    );
    sample!(
        out,
        Demand {
            handle: 2,
            subscribers: 1,
            request: Bytes(b"want")
        }
    );
    let buffers = [
        BufferInfo {
            index: 0,
            size: 4096,
            descriptor: 0,
            offset: 0,
        },
        BufferInfo {
            index: 1,
            size: 4096,
            descriptor: 1,
            offset: 8192,
        },
    ];
    sample!(
        out,
        BufferOffer {
            handle: 2,
            generation: 1,
            buffers: List::new(&buffers)
        }
    );
    sample!(
        out,
        Bell {
            handle: 2,
            subscriber: 7,
            present: true
        }
    );
    sample!(
        out,
        TopicState {
            request_seq: 9,
            handle: 2,
            status: TopicStatus::PublisherGone
        }
    );
    sample!(
        out,
        Holds {
            handle: 2,
            subscriber: 3,
            max_held: 2,
            max_hold_ms: 2000,
            present: true
        }
    );
}

fn workloads(out: &mut Vec<Sample>) {
    let needed = [NeededFile {
        name: file(),
        digest: DIGEST,
    }];
    let labels = [RequiredLabel {
        key: "zone",
        value: "a",
    }];
    sample!(
        out,
        WorkloadPut {
            name: workload("vision"),
            kind: work_kind(),
            spec: Bytes(b"spec"),
            files: List::new(&needed),
            node: Some(node("left")),
            labels: List::new(&labels),
            cpus: Bytes(&[0b0100]),
            restart: RestartPolicy::OnFailure,
        }
    );
    sample!(
        out,
        WorkloadRemove {
            name: workload("vision"),
            force: true
        }
    );
    sample!(
        out,
        WorkloadStatus {
            name: workload("vision"),
            state: WorkloadState::Running,
            node: None,
            detail: "ok"
        }
    );
    let kinds = [OfferedKind { kind: work_kind() }];
    sample!(
        out,
        ExecutorOffer {
            kinds: List::new(&kinds)
        }
    );
    sample!(
        out,
        ExecutorStart {
            name: workload("vision"),
            kind: work_kind(),
            spec: Bytes(b"spec"),
            cpus: Bytes(&[])
        }
    );
    sample!(
        out,
        ExecutorStop {
            name: workload("vision")
        }
    );
    sample!(
        out,
        ExecutorKill {
            name: workload("vision")
        }
    );
    sample!(
        out,
        ExecutorReport {
            name: workload("vision"),
            state: WorkloadState::Failed,
            detail: "exit 1"
        }
    );
    let items = [InventoryItem {
        name: workload("vision"),
        state: WorkloadState::Lost,
        detail: "",
    }];
    sample!(
        out,
        ExecutorInventory {
            workloads: List::new(&items)
        }
    );
}

fn peers(out: &mut Vec<Sample>) {
    let entries = [RecordEntry {
        class: RecordClass::Resource,
        key: "sensors/imu0",
        stamp: STAMP,
        deleted: true,
        value: Bytes(&[]),
    }];
    sample!(
        out,
        PeerSync {
            node: node("left"),
            newest: STAMP,
            fingerprint: 77,
            records: List::new(&entries)
        }
    );
    sample!(
        out,
        PeerObserved {
            node: node("left"),
            records: List::new(&entries)
        }
    );
    sample!(
        out,
        EnrollHello {
            cluster: "c",
            initiator: node("left"),
            public_key: Bytes(b"pk"),
            nonce: Bytes(b"n"),
            responder: node("right"),
            role: EnrollRole::Node,
        }
    );
    sample!(
        out,
        EnrollChallenge {
            responder: node("right"),
            public_key: Bytes(b"pk"),
            nonce: Bytes(b"n"),
            proof: Bytes(b"p"),
            signature: Bytes(b"s"),
        }
    );
    sample!(
        out,
        EnrollConfirm {
            initiator: node("left"),
            responder: node("right"),
            nonce: Bytes(b"n"),
            proof: Bytes(b"p"),
            signature: Bytes(b"s"),
        }
    );
    sample!(out, PeerRevoke { key: KEY });
    sample!(
        out,
        OperatorEnroll {
            public_key: Bytes(b"pk"),
            name: "me",
            proof: Bytes(&[])
        }
    );
    let granted = [GrantedAction { name: "restart" }];
    let operators = [OperatorEntry {
        key: KEY,
        state: OperatorState::Enrolled,
        read: true,
        actions: List::new(&granted),
        all_actions: false,
    }];
    sample!(
        out,
        OperatorWelcome {
            node: node("left"),
            public_key: Bytes(b"pk"),
            cluster: "c",
            operator: List::new(&operators)
        }
    );
    sample!(out, OperatorList {});
    sample!(
        out,
        Operators {
            request_seq: 1,
            operators: List::new(&operators)
        }
    );
    sample!(out, TimeRequest { sent_ns: 1 });
    sample!(
        out,
        TimeReply {
            sent_ns: 1,
            received_ns: 2,
            replied_ns: 3
        }
    );
    sample!(out, TimeOffer { stratum: 3 });
    sample!(
        out,
        TimeTurn {
            request_seq: 7,
            poll_ms: 64_000
        }
    );
}
