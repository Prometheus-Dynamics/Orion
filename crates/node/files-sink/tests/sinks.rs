//! Receivers: who is handed what, answers going back, and what happens when either end
//! goes.

use orion_files_sink::{MAX_SINKS, Sinks};
use orion_msg_files::{
    FileBegin, FileChunk, FileCommit, FileSink, FileSinkReady, FileState, FileStatus,
};
use orion_reply::{Asked, Out};
use orion_types::Error;
use orion_wire::body::encode_fields;
use orion_wire::{Body, Bytes, Decode, kind};

const MANAGER: u32 = 10;
const SENDER: u32 = 20;
const OTHER: u32 = 30;

#[derive(Default)]
struct Sent {
    frames: Vec<(u32, u8, Vec<u8>)>,
    scratch: Vec<u8>,
}

impl Out for Sent {
    fn frame(&mut self, session: u32, kind: u8, payload: &[u8], _may_drop: bool) {
        self.frames.push((session, kind, payload.to_vec()));
    }

    fn scratch(&mut self) -> &mut Vec<u8> {
        if self.scratch.is_empty() {
            self.scratch = vec![0; 4096];
        }
        &mut self.scratch
    }
}

struct Bench {
    sinks: Sinks,
    sent: Sent,
}

fn asked<B: Body>(from: u32, body: &B, into: &mut Vec<u8>) -> usize {
    let _ = from;
    into.resize(70_000, 0);
    encode_fields(body, into).expect("encode")
}

impl Bench {
    fn new() -> Self {
        Self {
            sinks: Sinks::new(),
            sent: Sent::default(),
        }
    }

    /// With the board manager receiving everything under `slots/`.
    fn with_sink() -> Self {
        let mut bench = Self::new();
        bench.sink(MANAGER, "slots/").expect("sink");
        bench.sent.frames.clear();
        bench
    }

    fn request<B: Body>(&mut self, from: u32, body: &B) -> Result<(), Error> {
        let mut payload = Vec::new();
        let len = asked(from, body, &mut payload);
        let asked = Asked {
            session: from,
            seq: 5,
            kind: B::KIND,
            class: 4,
            uid: 0,
            caller: "",
            payload: &payload[..len],
        };
        self.sinks.request(&asked, &mut self.sent)
    }

    fn pass<B: Body>(&mut self, from: u32, body: &B) -> Result<bool, Error> {
        let mut payload = Vec::new();
        let len = asked(from, body, &mut payload);
        let asked = Asked {
            session: from,
            seq: 5,
            kind: B::KIND,
            class: 4,
            uid: 0,
            caller: "",
            payload: &payload[..len],
        };
        self.sinks.pass(&asked, &mut self.sent)
    }

    fn sink(&mut self, from: u32, prefix: &str) -> Result<(), Error> {
        self.request(
            from,
            &FileSink {
                prefix,
                max_chunk: 0,
            },
        )
    }

    fn begin(&mut self, from: u32, transfer: u64, destination: &str) -> Result<bool, Error> {
        self.pass(
            from,
            &FileBegin {
                transfer,
                destination: destination.parse().expect("name"),
                size: 100,
                digest: [3; 32],
            },
        )
    }

    fn status(
        &mut self,
        from: u32,
        transfer: u64,
        received: u64,
        state: FileState,
    ) -> Result<(), Error> {
        self.request(
            from,
            &FileStatus {
                transfer,
                received,
                state,
                detail: "",
            },
        )
    }

    #[track_caller]
    fn took(&mut self) -> (u32, u8, Vec<u8>) {
        assert!(!self.sent.frames.is_empty(), "nothing was sent");
        self.sent.frames.remove(0)
    }
}

#[test]
fn a_file_under_a_receivers_prefix_passes_through_as_it_came() {
    let mut bench = Bench::with_sink();
    assert_eq!(bench.begin(SENDER, 1, "slots/b.img"), Ok(true));
    let (to, kind, payload) = bench.took();
    assert_eq!((to, kind), (MANAGER, kind::FILE_BEGIN));
    let begin = FileBegin::decode(&payload).expect("begin");
    assert_eq!(
        (begin.transfer, begin.destination.as_str()),
        (1, "slots/b.img")
    );
    assert_eq!((begin.size, begin.digest), (100, [3; 32]));

    // The receiver's answer goes back to the sender, as it was.
    bench
        .status(MANAGER, 1, 0, FileState::Receiving)
        .expect("status");
    let (to, kind, payload) = bench.took();
    assert_eq!((to, kind), (SENDER, kind::FILE_STATUS));
    assert_eq!(FileStatus::decode(&payload).expect("status").received, 0);

    let data = [9_u8; 4000];
    assert_eq!(
        bench.pass(
            SENDER,
            &FileChunk {
                transfer: 1,
                offset: 0,
                data: Bytes(&data)
            }
        ),
        Ok(true)
    );
    let (to, kind, payload) = bench.took();
    assert_eq!((to, kind), (MANAGER, kind::FILE_CHUNK));
    let chunk = FileChunk::decode(&payload).expect("chunk");
    assert_eq!((chunk.offset, chunk.data.0), (0, &data[..]));

    assert_eq!(bench.pass(SENDER, &FileCommit { transfer: 1 }), Ok(true));
    let (to, kind, _) = bench.took();
    assert_eq!((to, kind), (MANAGER, kind::FILE_COMMIT));
    // Committed ends it: the transfer is no longer passing through.
    bench
        .status(MANAGER, 1, 100, FileState::Committed)
        .expect("status");
    let (to, _, payload) = bench.took();
    assert_eq!(to, SENDER);
    assert_eq!(
        FileStatus::decode(&payload).expect("status").state,
        FileState::Committed
    );
    assert_eq!(bench.sinks.passing(), 0);
    assert_eq!(bench.pass(SENDER, &FileCommit { transfer: 1 }), Ok(false));
}

#[test]
fn a_file_under_no_receivers_prefix_is_the_stores() {
    let mut bench = Bench::with_sink();
    assert_eq!(bench.begin(SENDER, 1, "models/a.bin"), Ok(false));
    assert_eq!(bench.begin(SENDER, 2, "slot"), Ok(false));
    let data = [0_u8; 8];
    let chunk = FileChunk {
        transfer: 1,
        offset: 0,
        data: Bytes(&data),
    };
    assert_eq!(bench.pass(SENDER, &chunk), Ok(false));
    assert_eq!(bench.pass(SENDER, &FileCommit { transfer: 1 }), Ok(false));
    assert!(bench.sent.frames.is_empty());
}

#[test]
fn the_longest_prefix_is_the_receiver() {
    let mut bench = Bench::with_sink();
    bench.sink(OTHER, "slots/firmware/").expect("sink");
    bench.sent.frames.clear();
    bench
        .begin(SENDER, 1, "slots/firmware/mcu.bin")
        .expect("begin");
    assert_eq!(bench.took().0, OTHER);
    bench.begin(SENDER, 2, "slots/b.img").expect("begin");
    assert_eq!(bench.took().0, MANAGER);
}

#[test]
fn a_prefix_has_one_receiver() {
    let mut bench = Bench::new();
    bench.sink(MANAGER, "slots/").expect("sink");
    let (to, kind, payload) = bench.took();
    assert_eq!((to, kind), (MANAGER, kind::FILE_SINK_READY));
    assert_eq!(
        FileSinkReady::decode(&payload).expect("ready").request_seq,
        5
    );
    // The same again from the same program is the same receiver.
    bench.sink(MANAGER, "slots/").expect("again");
    assert_eq!(bench.sinks.sinks(), 1);
    assert_eq!(bench.sink(OTHER, "slots/"), Err(Error::AlreadyExists));
    // Not everything, and not upwards.
    assert_eq!(bench.sink(OTHER, ""), Err(Error::InvalidName));
    assert_eq!(bench.sink(OTHER, "a/../b/"), Err(Error::InvalidName));
    for index in 1..MAX_SINKS {
        bench.sink(OTHER, &format!("p{index}/")).expect("sink");
    }
    assert_eq!(bench.sink(OTHER, "one-more/"), Err(Error::Busy));
}

#[test]
fn only_the_receiver_answers_for_a_transfer() {
    let mut bench = Bench::with_sink();
    bench.begin(SENDER, 1, "slots/b.img").expect("begin");
    bench.sent.frames.clear();
    assert_eq!(
        bench.status(OTHER, 1, 50, FileState::Committed),
        Err(Error::NotFound)
    );
    assert_eq!(
        bench.status(MANAGER, 99, 0, FileState::Receiving),
        Err(Error::NotFound)
    );
    assert!(bench.sent.frames.is_empty());
    assert_eq!(bench.sinks.passing(), 1);
}

#[test]
fn a_sender_that_comes_back_in_another_session_is_the_one_told() {
    let mut bench = Bench::with_sink();
    bench.begin(SENDER, 1, "slots/b.img").expect("begin");
    bench.sinks.gone(SENDER, &mut bench.sent);
    assert_eq!(
        bench.sinks.passing(),
        1,
        "the transfer waits for its sender"
    );
    bench.sent.frames.clear();
    // It begins again from a new session: the receiver hears the begin and says where it is.
    bench.begin(OTHER, 1, "slots/b.img").expect("begin");
    assert_eq!(bench.took().0, MANAGER);
    bench
        .status(MANAGER, 1, 60, FileState::Receiving)
        .expect("status");
    let (to, _, payload) = bench.took();
    assert_eq!(to, OTHER);
    assert_eq!(FileStatus::decode(&payload).expect("status").received, 60);
}

#[test]
fn a_receiver_that_goes_ends_what_was_passing_through_it() {
    let mut bench = Bench::with_sink();
    bench.begin(SENDER, 1, "slots/b.img").expect("begin");
    bench.sent.frames.clear();
    bench.sinks.gone(MANAGER, &mut bench.sent);
    let (to, kind, payload) = bench.took();
    assert_eq!((to, kind), (SENDER, kind::FILE_STATUS));
    let status = FileStatus::decode(&payload).expect("status");
    assert_eq!(status.state, FileState::Rejected);
    assert_eq!(status.detail, "the program that receives it went away");
    assert_eq!((bench.sinks.sinks(), bench.sinks.passing()), (0, 0));
    // With no receiver the same destination is the store's again.
    assert_eq!(bench.begin(SENDER, 1, "slots/b.img"), Ok(false));
    bench.sinks.forget_sessions();
}

#[test]
fn a_rejection_ends_the_transfer_too() {
    let mut bench = Bench::with_sink();
    bench.begin(SENDER, 1, "slots/b.img").expect("begin");
    bench
        .status(MANAGER, 1, 0, FileState::Rejected)
        .expect("status");
    assert_eq!(bench.sinks.passing(), 0);
}

#[test]
fn a_receiver_that_names_a_chunk_size_is_sent_pieces_one_at_a_time() {
    let mut bench = Bench::new();
    bench
        .request(
            MANAGER,
            &FileSink {
                prefix: "devices/imu/",
                max_chunk: 100,
            },
        )
        .expect("sink");
    assert_eq!(bench.sinks.max_chunk("devices/imu/fw.bin"), Some(100));
    assert_eq!(bench.sinks.max_chunk("other/fw.bin"), None);
    bench.sent.frames.clear();

    assert_eq!(bench.begin(SENDER, 7, "devices/imu/fw.bin"), Ok(true));
    let (to, kind, _) = bench.took();
    assert_eq!((to, kind), (MANAGER, kind::FILE_BEGIN));
    // Two chunks in flight, one larger than the receiver takes: nothing goes until the
    // begin is answered.
    let data: Vec<u8> = (0..250_u32).map(|at| at as u8).collect();
    for (offset, bytes) in [(0, &data[..]), (250, &data[..10])] {
        assert_eq!(
            bench.pass(
                SENDER,
                &FileChunk {
                    transfer: 7,
                    offset,
                    data: Bytes(bytes),
                },
            ),
            Ok(true)
        );
    }
    assert!(bench.sent.frames.is_empty(), "sent before the answer");

    // Each answer lets the next piece go, and is passed back to the sender.
    let mut pieces = Vec::new();
    for received in [0, 100, 200, 250] {
        bench
            .status(MANAGER, 7, received, FileState::Receiving)
            .expect("status");
        let (to, kind, payload) = bench.took();
        assert_eq!((to, kind), (MANAGER, kind::FILE_CHUNK));
        let chunk = FileChunk::decode(&payload).expect("chunk");
        pieces.push((chunk.offset, chunk.data.0.to_vec()));
        let (to, kind, _) = bench.took();
        assert_eq!((to, kind), (SENDER, kind::FILE_STATUS));
        assert!(bench.sent.frames.is_empty(), "two frames at once");
    }
    assert_eq!(
        pieces,
        [
            (0, data[..100].to_vec()),
            (100, data[100..200].to_vec()),
            (200, data[200..].to_vec()),
            (250, data[..10].to_vec()),
        ]
    );

    // A sender that begins again is passed on at once, and what waited is dropped.
    bench
        .pass(
            SENDER,
            &FileChunk {
                transfer: 7,
                offset: 260,
                data: Bytes(&data[..10]),
            },
        )
        .expect("chunk");
    assert!(bench.sent.frames.is_empty(), "sent before the answer");
    assert_eq!(bench.begin(SENDER, 7, "devices/imu/fw.bin"), Ok(true));
    let (to, kind, _) = bench.took();
    assert_eq!((to, kind), (MANAGER, kind::FILE_BEGIN));
    bench
        .status(MANAGER, 7, 260, FileState::Receiving)
        .expect("status");
    let (to, kind, _) = bench.took();
    assert_eq!((to, kind), (SENDER, kind::FILE_STATUS));
    assert!(bench.sent.frames.is_empty(), "the dropped chunk was sent");
}
