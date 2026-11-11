//! Firmware to a microcontroller, end to end. An image is put on the board once, as any file
//! is, by an ordinary client. Running the board's `files.send` action sends it to the device:
//! the device's firmware layer asked for the files under its prefix, so the image passes
//! through the board to its spare slot, cut into chunks its receive buffer holds and sent one
//! frame at a time, and the run succeeds once the device has checked the image and put it
//! on trial.

use std::fs::File;
use std::io::{Cursor, ErrorKind, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use orion::actions::{ActionCall, Finished, Outcome, Target, Update};
use orion::files::FileState;
use orion::{Address, Orion};
use orion_device_core::{DeviceConfig, StreamDevice};
use orion_device_firmware::{Firmware, FlashError, Heard, RECORD_LEN, SlotFlash};
use orion_msg_files::FileSend;
use orion_stubs::Board;
use orion_wire::body::encode_fields;

mod common;
use common::{Logic, dir, pty, settings, until};

const CAPACITY: usize = 64 * 1024;

/// Flash in memory. Erased bytes are 0xFF.
struct Memory {
    slots: [Vec<u8>; 2],
    records: [[u8; RECORD_LEN]; 2],
}

impl SlotFlash for Memory {
    fn slot_capacity(&self) -> u32 {
        CAPACITY as u32
    }

    fn erase_slot(&mut self, slot: u8) -> Result<(), FlashError> {
        self.slots[usize::from(slot)].fill(0xFF);
        Ok(())
    }

    fn write_slot(&mut self, slot: u8, offset: u32, data: &[u8]) -> Result<(), FlashError> {
        let at = offset as usize;
        self.slots[usize::from(slot)]
            .get_mut(at..at + data.len())
            .ok_or(FlashError)?
            .copy_from_slice(data);
        Ok(())
    }

    fn read_slot(&self, slot: u8, offset: u32, out: &mut [u8]) -> Result<(), FlashError> {
        let at = offset as usize;
        out.copy_from_slice(
            self.slots[usize::from(slot)]
                .get(at..at + out.len())
                .ok_or(FlashError)?,
        );
        Ok(())
    }

    fn read_record(&self, copy: u8, out: &mut [u8; RECORD_LEN]) -> Result<(), FlashError> {
        *out = self.records[usize::from(copy)];
        Ok(())
    }

    fn write_record(&mut self, copy: u8, data: &[u8; RECORD_LEN]) -> Result<(), FlashError> {
        self.records[usize::from(copy)] = *data;
        Ok(())
    }
}

/// What the device did with an image that checked out: the slot, and the slot's bytes.
type Committed = Arc<Mutex<Option<(u8, Vec<u8>)>>>;

/// The firmware, in a thread of its own on the device's end of a pseudo-terminal, until
/// dropped.
struct Device {
    stop: Arc<AtomicBool>,
    /// Whether the board hands the device its files.
    ready: Arc<AtomicBool>,
    committed: Committed,
    largest_chunk: Arc<Mutex<usize>>,
    thread: Option<JoinHandle<()>>,
}

impl Device {
    fn start(port: File) -> Self {
        Self::losing(port, None)
    }

    /// One that loses the chunk with this number, as a noisy line would.
    fn losing(port: File, lose: Option<usize>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicBool::new(false));
        let committed = Committed::default();
        let largest_chunk = Arc::new(Mutex::new(0));
        let (stopping, readying, committing, largest) = (
            stop.clone(),
            ready.clone(),
            committed.clone(),
            largest_chunk.clone(),
        );
        let thread = std::thread::spawn(move || {
            run(port, lose, &stopping, &readying, &committing, &largest);
        });
        Self {
            stop,
            ready,
            committed,
            largest_chunk,
            thread: Some(thread),
        }
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A firmware's main loop with the firmware layer: feed, poll, send.
fn run(
    mut port: File,
    mut lose: Option<usize>,
    stop: &AtomicBool,
    ready: &AtomicBool,
    committed: &Committed,
    largest: &Mutex<usize>,
) {
    let mut device = StreamDevice::<512, 512>::new(DeviceConfig::new("imu-board"));
    let flash = Memory {
        slots: [vec![0xFF; CAPACITY], vec![0xFF; CAPACITY]],
        records: [[0xFF; RECORD_LEN]; 2],
    };
    let mut firmware = Firmware::new("devices/imu-board/firmware/", flash);
    let started = Instant::now();
    let (mut rx, mut tx) = ([0_u8; 256], [0_u8; 256]);
    while !stop.load(Ordering::Relaxed) {
        let now = u32::try_from(started.elapsed().as_millis()).unwrap_or(u32::MAX);
        match port.read(&mut rx) {
            Ok(n) => device.receive(&rx[..n], |_, frame| {
                if frame.kind() == orion_wire::kind::FILE_CHUNK {
                    let mut size = largest.lock().expect("lock");
                    *size = (*size).max(frame.payload().len());
                    match &mut lose {
                        Some(0) => {
                            lose = None;
                            return;
                        }
                        Some(left) => *left -= 1,
                        None => {}
                    }
                }
                if let Heard::Committed { slot } = firmware.handle(frame) {
                    let image = firmware.slots().flash().slots[usize::from(slot)].clone();
                    *committed.lock().expect("lock") = Some((slot, image));
                }
            }),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            Err(error) => panic!("the device's port: {error}"),
        }
        device.link.poll(now);
        while device.link.next_event().is_some() {}
        firmware.poll(&mut device.link, now);
        ready.store(firmware.ready(), Ordering::Relaxed);
        loop {
            let n = device.transmit(&mut tx);
            if n == 0 {
                break;
            }
            let mut out = &tx[..n];
            while !out.is_empty() {
                match port.write(out) {
                    Ok(wrote) => out = &out[wrote..],
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("the device's port: {error}"),
                }
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Puts a file on the board, once node logic is there to keep it.
fn put(client: &Orion, name: &str, bytes: &[u8]) {
    let pushed = until("node logic keeps the file", || {
        client
            .files()
            .push(name, &mut Cursor::new(bytes), |_, _| {})
            .ok()
    });
    assert_eq!(pushed.state, FileState::Committed, "{}", pushed.detail);
}

/// Runs the board's `files.send` and waits for how it ends.
fn send(client: &Orion, file: &str, to: &str) -> Finished {
    let mut args = vec![0_u8; 512];
    let used = encode_fields(
        &FileSend {
            file: file.parse().expect("name"),
            to: to.parse().expect("name"),
        },
        &mut args,
    )
    .expect("encode");
    let mut run = client
        .run(&ActionCall {
            name: "files.send",
            target: Target::Node("board".parse().expect("name")),
            args: &args[..used],
            deadline_ms: 60_000,
            detached: false,
        })
        .expect("run");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(Some(Update::Finished(finished))) = run.wait(Duration::from_millis(200)) {
            return finished;
        }
        assert!(Instant::now() < deadline, "the run of files.send ended");
    }
}

#[test]
fn an_image_on_the_board_reaches_a_device_through_an_action() {
    let dir = dir("firmware");
    let (port, link) = pty();
    let board = Board::start_for_node_with_links(&dir, &[link]);
    let _logic = Logic::start(settings(&dir));
    let device = Device::start(port);
    let address = Address::Local(board.socket().to_owned());
    let operator = Orion::connect(&address, "release").expect("connect");

    // The image, put on the board as any file is.
    let image: Vec<u8> = (0..40_000_u32)
        .map(|at| (at.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    put(&operator, "images/imu-board-2.bin", &image);

    // Nothing receives a destination no device asked for.
    let nowhere = send(
        &operator,
        "images/imu-board-2.bin",
        "devices/other/firmware/main.bin",
    );
    assert_eq!(nowhere.outcome, Outcome::Rejected, "{}", nowhere.detail);
    // A file the board does not keep is not sent.
    let missing = send(
        &operator,
        "images/none.bin",
        "devices/imu-board/firmware/main.bin",
    );
    assert_eq!(missing.outcome, Outcome::Rejected, "{}", missing.detail);

    // Once the device has asked for its prefix, the image goes to its spare slot.
    let finished = until("the device asks for its files", || {
        let finished = send(
            &operator,
            "images/imu-board-2.bin",
            "devices/imu-board/firmware/main.bin",
        );
        (finished.outcome != Outcome::Rejected).then_some(finished)
    });
    assert_eq!(finished.outcome, Outcome::Succeeded, "{}", finished.detail);
    let (slot, held) = device
        .committed
        .lock()
        .expect("lock")
        .clone()
        .expect("the device committed the image");
    assert_eq!(slot, 1, "the image goes to the slot that is not running");
    assert_eq!(&held[..image.len()], &image[..]);
    // Every chunk fit the device's receive buffer.
    let largest = *device.largest_chunk.lock().expect("lock");
    assert!(
        largest > 0 && largest <= 512 - 8,
        "a chunk of {largest} bytes"
    );

    // Sent again, the image is already there: the device says so at once.
    let again = send(
        &operator,
        "images/imu-board-2.bin",
        "devices/imu-board/firmware/main.bin",
    );
    assert_eq!(again.outcome, Outcome::Succeeded, "{}", again.detail);
}

#[test]
fn a_client_can_push_an_image_straight_to_a_device() {
    let dir = dir("firmware-push");
    let (port, link) = pty();
    let board = Board::start_for_node_with_links(&dir, &[link]);
    let _logic = Logic::start(settings(&dir));
    let device = Device::start(port);
    let address = Address::Local(board.socket().to_owned());
    let operator = Orion::connect(&address, "release").expect("connect");

    // The client sends chunks as large as its own frames allow, several in flight; the
    // board cuts them to what the device takes and sends them one at a time.
    let image: Vec<u8> = (0..50_000_u32).map(|at| (at % 251) as u8).collect();
    // Until the device has asked for its files, the destination is the board's own store's.
    until("the device asks for its files", || {
        device.ready.load(Ordering::Relaxed).then_some(())
    });
    let pushed = until("the device asks for its files", || {
        let pushed = operator
            .files()
            .push(
                "devices/imu-board/firmware/main.bin",
                &mut Cursor::new(&image),
                |_, _| {},
            )
            .ok()?;
        (pushed.state == FileState::Committed).then_some(pushed)
    });
    assert_eq!(pushed.received, image.len() as u64);
    let (_, held) = device
        .committed
        .lock()
        .expect("lock")
        .clone()
        .expect("the device committed the image");
    assert_eq!(&held[..image.len()], &image[..]);
    let largest = *device.largest_chunk.lock().expect("lock");
    assert!(largest <= 512 - 8, "a chunk of {largest} bytes");
}

#[test]
fn a_chunk_the_device_never_heard_is_sent_again() {
    let dir = dir("firmware-lost");
    let (port, link) = pty();
    let board = Board::start_for_node_with_links(&dir, &[link]);
    let _logic = Logic::start(settings(&dir));
    let device = Device::losing(port, Some(5));
    let address = Address::Local(board.socket().to_owned());
    let operator = Orion::connect(&address, "release").expect("connect");

    let image: Vec<u8> = (0..8_000_u32).map(|at| (at % 241) as u8).collect();
    put(&operator, "images/small.bin", &image);
    // The sixth chunk is lost and the device says nothing: the board begins again, the
    // device says where it is, and the image arrives whole.
    let started = Instant::now();
    let finished = until("the device asks for its files", || {
        let finished = send(
            &operator,
            "images/small.bin",
            "devices/imu-board/firmware/main.bin",
        );
        (finished.outcome != Outcome::Rejected).then_some(finished)
    });
    assert_eq!(finished.outcome, Outcome::Succeeded, "{}", finished.detail);
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "nothing was lost"
    );
    let (_, held) = device
        .committed
        .lock()
        .expect("lock")
        .clone()
        .expect("the device committed the image");
    assert_eq!(&held[..image.len()], &image[..]);
}
