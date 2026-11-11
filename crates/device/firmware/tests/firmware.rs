//! Firmware through the file kinds, against a host played by the test and a flash in
//! memory: an image arrives, is checked, runs on trial and stays once confirmed; a transfer
//! cut off goes on where it stopped; a bad image is never started.

use orion_device_core::Link;
use orion_device_firmware::{
    BootRecord, Firmware, FlashError, NO_SLOT, RECORD_LEN, Refused, ResetReason, Rollback,
    SlotFlash, boot, load,
};
use orion_device_host::{Drive, Rig, Sent};
use orion_msg_files::{
    FileBegin, FileChunk, FileCommit, FileSink, FileSinkReady, FileState, FileStatus,
};
use orion_wire::{Bytes, FrameView};
use sha2::{Digest, Sha256};

#[global_allocator]
static GLOBAL: orion_measure::CountingAlloc = orion_measure::CountingAlloc::system();

const TX: usize = 256;
const CAPACITY: usize = 4096;
type FirmwareRig = Rig<Firmware<Memory>, 256, TX>;

/// Flash in memory. Erased bytes are 0xFF. A record write can be made to fail half way.
#[derive(Clone, Debug)]
struct Memory {
    slots: [Vec<u8>; 2],
    records: [[u8; RECORD_LEN]; 2],
    /// The next record write stops after this many bytes and fails.
    tear_next_record: Option<usize>,
}

impl Memory {
    fn new() -> Self {
        Self {
            slots: [vec![0xFF; CAPACITY], vec![0xFF; CAPACITY]],
            records: [[0xFF; RECORD_LEN]; 2],
            tear_next_record: None,
        }
    }
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
        let to = self.slots[usize::from(slot)]
            .get_mut(at..at + data.len())
            .ok_or(FlashError)?;
        // Flash only clears bits: writing over unerased bytes is a bug in the caller.
        assert!(to.iter().all(|&b| b == 0xFF), "a write over unerased flash");
        to.copy_from_slice(data);
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
        let to = &mut self.records[usize::from(copy)];
        to.fill(0xFF);
        if let Some(n) = self.tear_next_record.take() {
            to[..n].copy_from_slice(&data[..n]);
            return Err(FlashError);
        }
        *to = *data;
        Ok(())
    }
}

const PREFIX: &str = "devices/imu/firmware";

fn poll(firmware: &mut Firmware<Memory>, link: &mut Link<TX>, now: u32) {
    firmware.poll(link, now);
}

fn handle(firmware: &mut Firmware<Memory>, _: &mut Link<TX>, frame: &FrameView<'_>) -> String {
    format!("{:?}", firmware.handle(frame))
}

fn rig_on(flash: Memory) -> FirmwareRig {
    let mut rig = Rig::new(Firmware::new(PREFIX, flash), Drive { poll, handle });
    let asked = rig.drain();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].body::<FileSink<'_>>().prefix, PREFIX);
    rig.send(&FileSinkReady {
        request_seq: asked[0].seq(),
    });
    assert_eq!(rig.last_heard(), Some("Ready"));
    rig
}

fn image(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31) ^ seed)
        .collect()
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn begin(
    rig: &mut FirmwareRig,
    transfer: u64,
    image: &[u8],
    digest: [u8; 32],
) -> FileStatus<'static> {
    rig.send(&FileBegin {
        transfer,
        destination: format!("{PREFIX}/main.bin").parse().expect("a name"),
        size: image.len() as u64,
        digest,
    });
    status(rig)
}

/// The status the device answered with.
fn status(rig: &mut FirmwareRig) -> FileStatus<'static> {
    let sent: Vec<Sent> = rig.drain();
    assert_eq!(sent.len(), 1, "one status");
    let status: FileStatus<'_> = sent[0].body();
    FileStatus {
        transfer: status.transfer,
        received: status.received,
        state: status.state,
        detail: Box::leak(status.detail.to_owned().into_boxed_str()),
    }
}

/// Sends `image[from..]` in chunks of 100 and returns the last status.
fn send_from(
    rig: &mut FirmwareRig,
    transfer: u64,
    image: &[u8],
    from: usize,
) -> FileStatus<'static> {
    let mut last = None;
    for (n, chunk) in image[from..].chunks(100).enumerate() {
        rig.send(&FileChunk {
            transfer,
            offset: (from + n * 100) as u64,
            data: Bytes(chunk),
        });
        let got = status(rig);
        assert_eq!(got.received, (from + n * 100 + chunk.len()) as u64);
        last = Some(got);
    }
    last.expect("a chunk")
}

fn commit(rig: &mut FirmwareRig, transfer: u64) -> FileStatus<'static> {
    rig.send(&FileCommit { transfer });
    status(rig)
}

/// The board resets: the bootloader runs, then the firmware starts again on the flash.
fn reset(flash: &mut Memory, reason: ResetReason) -> Option<u8> {
    boot(flash, reason)
}

#[test]
fn an_image_arrives_runs_on_trial_and_stays_once_confirmed() {
    let mut rig = rig_on(Memory::new());
    let new = image(1_000, 7);
    let got = begin(&mut rig, 5, &new, digest(&new));
    assert_eq!(
        (got.transfer, got.received, got.state),
        (5, 0, FileState::Receiving)
    );
    send_from(&mut rig, 5, &new, 0);
    let got = commit(&mut rig, 5);
    assert_eq!((got.received, got.state), (1_000, FileState::Committed));
    assert_eq!(rig.last_heard(), Some("Committed { slot: 1 }"));
    // A repeated commit gets the same answer.
    assert_eq!(commit(&mut rig, 5).state, FileState::Committed);

    let mut flash = rig.layer.slots().flash().clone();
    assert_eq!(&flash.slots[1][..1_000], &new[..]);
    // The trial starts once.
    assert_eq!(reset(&mut flash, ResetReason::Normal), Some(1));
    let mut firmware = Firmware::new(PREFIX, flash);
    assert_eq!(firmware.slots().record().trial, 1);
    firmware.confirm().expect("confirmed");
    let mut flash = firmware.slots().flash().clone();
    assert_eq!(load(&flash).1.trial, NO_SLOT);
    // Confirmed, it runs at every reset.
    assert_eq!(reset(&mut flash, ResetReason::Normal), Some(1));
    assert_eq!(reset(&mut flash, ResetReason::Fault), Some(1));
}

#[test]
fn a_transfer_cut_off_goes_on_where_it_stopped() {
    let mut rig = rig_on(Memory::new());
    let new = image(1_234, 3);
    begin(&mut rig, 9, &new, digest(&new));
    send_from(&mut rig, 9, &new[..600], 0);
    // The host goes quiet; the session ends and a new one starts.
    rig.quiet = true;
    rig.run(400);
    rig.quiet = false;
    rig.connect();
    let asked = rig.drain();
    rig.send(&FileSinkReady {
        request_seq: asked[0].seq(),
    });
    // The same transfer again: the bytes that are here stay, and the sender goes on.
    let got = begin(&mut rig, 9, &new, digest(&new));
    assert_eq!((got.received, got.state), (600, FileState::Receiving));
    send_from(&mut rig, 9, &new, 600);
    assert_eq!(commit(&mut rig, 9).state, FileState::Committed);
    assert_eq!(&rig.layer.slots().flash().slots[1][..1_234], &new[..]);
}

#[test]
fn a_chunk_that_is_not_next_is_not_written() {
    let mut rig = rig_on(Memory::new());
    let new = image(300, 1);
    begin(&mut rig, 2, &new, digest(&new));
    send_from(&mut rig, 2, &new[..100], 0);
    // Ahead of what is here: not written, and the status says where to go on from.
    rig.send(&FileChunk {
        transfer: 2,
        offset: 200,
        data: Bytes(&new[200..]),
    });
    assert_eq!(status(&mut rig).received, 100);
    // A repeat of what is here: not written again.
    rig.send(&FileChunk {
        transfer: 2,
        offset: 0,
        data: Bytes(&new[..100]),
    });
    assert_eq!(status(&mut rig).received, 100);
    // Another transfer's chunk is turned away.
    rig.send(&FileChunk {
        transfer: 3,
        offset: 100,
        data: Bytes(&new[100..200]),
    });
    let got = status(&mut rig);
    assert_eq!((got.transfer, got.state), (3, FileState::Rejected));
    // A commit before the end is turned away and changes nothing.
    assert_eq!(commit(&mut rig, 2).detail, Refused::Incomplete.detail());
    send_from(&mut rig, 2, &new, 100);
    assert_eq!(commit(&mut rig, 2).state, FileState::Committed);
}

#[test]
fn an_image_with_the_wrong_digest_never_starts() {
    let mut rig = rig_on(Memory::new());
    let new = image(500, 9);
    let mut wrong = digest(&new);
    wrong[0] ^= 1;
    begin(&mut rig, 4, &new, wrong);
    send_from(&mut rig, 4, &new, 0);
    let got = commit(&mut rig, 4);
    assert_eq!(
        (got.state, got.detail),
        (FileState::Rejected, Refused::BadDigest.detail())
    );
    let record = *rig.layer.slots().record();
    assert_eq!((record.active, record.trial), (0, NO_SLOT));
    let mut flash = rig.layer.slots().flash().clone();
    assert_eq!(reset(&mut flash, ResetReason::Normal), Some(0));
}

#[test]
fn an_image_that_changed_in_flash_is_given_up_before_it_starts() {
    let mut rig = rig_on(Memory::new());
    let new = image(800, 4);
    begin(&mut rig, 1, &new, digest(&new));
    send_from(&mut rig, 1, &new, 0);
    commit(&mut rig, 1);
    let mut flash = rig.layer.slots().flash().clone();
    flash.slots[1][17] ^= 0x10;
    assert_eq!(reset(&mut flash, ResetReason::Normal), Some(0));
    let record = load(&flash).1;
    assert_eq!(record.rollback, Some(Rollback::VerifyFailed));
    assert_eq!((record.active, record.trial), (0, NO_SLOT));
}

#[test]
fn a_trial_that_is_not_confirmed_goes_back() {
    for (reason, why) in [
        (ResetReason::Normal, Rollback::NotConfirmed),
        (ResetReason::Fault, Rollback::BootFailure),
    ] {
        let mut rig = rig_on(Memory::new());
        let new = image(700, 2);
        begin(&mut rig, 1, &new, digest(&new));
        send_from(&mut rig, 1, &new, 0);
        commit(&mut rig, 1);
        let mut flash = rig.layer.slots().flash().clone();
        assert_eq!(reset(&mut flash, ResetReason::Normal), Some(1));
        // Reset again before the application confirmed it.
        assert_eq!(reset(&mut flash, reason), Some(0));
        let record = load(&flash).1;
        assert_eq!(record.rollback, Some(why));
        assert_eq!(reset(&mut flash, ResetReason::Normal), Some(0));
    }
}

#[test]
fn a_new_image_waits_until_the_trial_is_settled() {
    let mut rig = rig_on(Memory::new());
    let new = image(100, 5);
    begin(&mut rig, 1, &new, digest(&new));
    send_from(&mut rig, 1, &new, 0);
    commit(&mut rig, 1);
    let got = begin(&mut rig, 2, &new, digest(&new));
    assert_eq!(
        (got.state, got.detail),
        (FileState::Rejected, Refused::TrialPending.detail())
    );
    rig.layer.confirm().expect("confirmed");
    // Now the next goes to the other slot, the one that is not running.
    assert_eq!(
        begin(&mut rig, 2, &new, digest(&new)).state,
        FileState::Receiving
    );
    send_from(&mut rig, 2, &new, 0);
    assert_eq!(commit(&mut rig, 2).state, FileState::Committed);
    assert_eq!(rig.last_heard(), Some("Committed { slot: 0 }"));
}

#[test]
fn a_record_torn_by_a_power_cut_leaves_the_one_before() {
    let mut rig = rig_on(Memory::new());
    let new = image(400, 6);
    begin(&mut rig, 1, &new, digest(&new));
    send_from(&mut rig, 1, &new, 0);
    let before = load(rig.layer.slots().flash()).1;
    rig.layer.slots_mut().flash_mut().tear_next_record = Some(30);
    let got = commit(&mut rig, 1);
    assert_eq!(
        (got.state, got.detail),
        (FileState::Rejected, Refused::Flash.detail())
    );
    let mut flash = rig.layer.slots().flash().clone();
    assert_eq!(load(&flash).1, before);
    assert_eq!(reset(&mut flash, ResetReason::Normal), Some(0));
}

#[test]
fn a_file_for_another_prefix_is_not_this_layers() {
    let mut rig = rig_on(Memory::new());
    rig.send(&FileBegin {
        transfer: 1,
        destination: "devices/other/firmware".parse().expect("a name"),
        size: 10,
        digest: [0; 32],
    });
    assert_eq!(rig.last_heard(), Some("Other"));
    assert!(rig.drain().is_empty());
}

#[test]
fn receiving_allocates_nothing() {
    let mut rig = rig_on(Memory::new());
    let new = image(2_000, 8);
    begin(&mut rig, 1, &new, digest(&new));
    let frames: Vec<Vec<u8>> = new
        .chunks(100)
        .enumerate()
        .map(|(n, chunk)| {
            rig.host.frame(&FileChunk {
                transfer: 1,
                offset: (n * 100) as u64,
                data: Bytes(chunk),
            })
        })
        .collect();
    let before = orion_measure::thread_counts().allocs;
    for frame in &frames {
        let layer = &mut rig.layer;
        rig.device.receive(frame, |_, frame| {
            layer.handle(frame);
        });
        layer.poll(&mut rig.device.link, rig.now);
        let mut out = [0_u8; 64];
        while rig.device.transmit(&mut out) != 0 {}
    }
    assert_eq!(orion_measure::thread_counts().allocs, before);
    assert_eq!(BootRecord::FACTORY.trial, NO_SLOT);
}
