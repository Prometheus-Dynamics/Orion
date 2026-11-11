//! New firmware through the C API (`--features ffi-slots`), over flash hooks defined here as
//! a C port defines them: an image sent in chunks the device takes is checked, put on trial,
//! started by the bootloader once, and kept when confirmed.

#![cfg(all(feature = "ffi-slots", not(feature = "ffi-can")))]

use std::sync::Mutex;

use orion_mcu_template::ffi::*;
use orion_msg_files::{FileBegin, FileChunk, FileCommit, FileState, FileStatus};
use orion_wire::{Bytes, kind};
use sha2::{Digest, Sha256};

mod common;
use common::{Peer, StreamReader, stream};

const CAPACITY: usize = 8192;

struct Flash {
    slots: [Vec<u8>; 2],
    records: [[u8; 96]; 2],
}

static FLASH: Mutex<Option<Flash>> = Mutex::new(None);

fn flash<R>(f: impl FnOnce(&mut Flash) -> R) -> R {
    let mut flash = FLASH.lock().expect("lock");
    f(flash.get_or_insert_with(|| Flash {
        slots: [vec![0xFF; CAPACITY], vec![0xFF; CAPACITY]],
        records: [[0xFF; 96]; 2],
    }))
}

#[unsafe(no_mangle)]
extern "C" fn orion_port_slot_capacity() -> u32 {
    CAPACITY as u32
}

#[unsafe(no_mangle)]
extern "C" fn orion_port_erase_slot(slot: u8) -> bool {
    flash(|flash| {
        flash
            .slots
            .get_mut(usize::from(slot))
            .map(|slot| slot.fill(0xFF))
    })
    .is_some()
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orion_port_write_slot(
    slot: u8,
    offset: u32,
    data: *const u8,
    len: usize,
) -> bool {
    // SAFETY: the library passes a slice's pointer and length.
    let data = unsafe { std::slice::from_raw_parts(data, len) };
    flash(|flash| {
        let at = offset as usize;
        flash
            .slots
            .get_mut(usize::from(slot))
            .and_then(|slot| slot.get_mut(at..at + len))
            .map(|to| to.copy_from_slice(data))
    })
    .is_some()
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orion_port_read_slot(slot: u8, offset: u32, out: *mut u8, len: usize) -> bool {
    // SAFETY: as above.
    let out = unsafe { std::slice::from_raw_parts_mut(out, len) };
    flash(|flash| {
        let at = offset as usize;
        flash
            .slots
            .get(usize::from(slot))
            .and_then(|slot| slot.get(at..at + len))
            .map(|from| out.copy_from_slice(from))
    })
    .is_some()
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orion_port_read_record(copy: u8, out: *mut u8) -> bool {
    // SAFETY: the library passes a whole record.
    let out = unsafe { std::slice::from_raw_parts_mut(out, 96) };
    flash(|flash| {
        flash
            .records
            .get(usize::from(copy))
            .map(|from| out.copy_from_slice(from))
    })
    .is_some()
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orion_port_write_record(copy: u8, data: *const u8) -> bool {
    // SAFETY: as above.
    let data = unsafe { std::slice::from_raw_parts(data, 96) };
    flash(|flash| {
        flash
            .records
            .get_mut(usize::from(copy))
            .map(|to| to.copy_from_slice(data))
    })
    .is_some()
}

/// Sends `frame` and passes until the device has answered it.
/// What `orion_poll` said meanwhile is added to `events`.
fn ask(
    peer: &mut Peer,
    reader: &mut StreamReader,
    now: &mut u32,
    events: &mut u32,
    frame: &[u8],
) -> FileStatus<'static> {
    let bytes = stream(frame);
    // SAFETY: valid for its length.
    unsafe { orion_rx(bytes.as_ptr(), bytes.len()) };
    for _ in 0..50 {
        *now += 1;
        *events |= pass(peer, reader, *now);
        if let Some(sent) = peer.take(kind::FILE_STATUS).pop() {
            let status: FileStatus = sent.body();
            let detail: &'static str = status.detail.to_owned().leak();
            return FileStatus { detail, ..status };
        }
    }
    panic!("no answer");
}

fn pass(peer: &mut Peer, reader: &mut StreamReader, now: u32) -> u32 {
    let events = orion_poll(now);
    let mut out = [0_u8; 64];
    loop {
        // SAFETY: valid for its length.
        let n = unsafe { orion_tx(out.as_mut_ptr(), out.len()) };
        if n == 0 {
            return events;
        }
        for sent in reader.read(&out[..n]) {
            for frame in peer.answer(sent) {
                let bytes = stream(&frame);
                // SAFETY: valid for its length.
                unsafe { orion_rx(bytes.as_ptr(), bytes.len()) };
            }
        }
    }
}

#[test]
fn new_firmware_goes_on_trial_and_is_kept_once_confirmed() {
    // A factory image runs from slot 0.
    assert_eq!(orion_boot(false), 0);
    // SAFETY: literals.
    unsafe {
        assert!(orion_init(c"fw-board".as_ptr()));
        assert_eq!(orion_firmware(c"devices/fw-board/firmware/".as_ptr()), 0);
    }
    let (mut peer, mut reader) = (Peer::default(), StreamReader::default());
    let mut now = 0;
    while peer.max_chunk.is_none() {
        now += 1;
        pass(&mut peer, &mut reader, now);
        assert!(now < 100, "the device asks for its prefix");
    }
    let max_chunk = peer.max_chunk.expect("asked") as usize;
    assert_eq!(max_chunk, orion_mcu_template::RX - 31);

    let mut events = 0;
    let image: Vec<u8> = (0..3_000_u32).map(|at| (at * 7 % 253) as u8).collect();
    let begin = peer.raw(&FileBegin {
        transfer: 9,
        destination: "devices/fw-board/firmware/main.bin".parse().expect("name"),
        size: image.len() as u64,
        digest: Sha256::digest(&image).into(),
    });
    let status = ask(&mut peer, &mut reader, &mut now, &mut events, &begin);
    assert_eq!((status.state, status.received), (FileState::Receiving, 0));
    assert_eq!(events & ORION_EVENT_FIRMWARE, 0);
    let mut received = 0;
    while received < image.len() {
        let end = (received + max_chunk).min(image.len());
        let chunk = peer.raw(&FileChunk {
            transfer: 9,
            offset: received as u64,
            data: Bytes(&image[received..end]),
        });
        let status = ask(&mut peer, &mut reader, &mut now, &mut events, &chunk);
        assert_eq!(status.state, FileState::Receiving, "{}", status.detail);
        assert_eq!(status.received, end as u64);
        received = end;
    }
    let commit = peer.raw(&FileCommit { transfer: 9 });
    let status = ask(&mut peer, &mut reader, &mut now, &mut events, &commit);
    assert_eq!(status.state, FileState::Committed, "{}", status.detail);
    assert_ne!(events & ORION_EVENT_FIRMWARE, 0);
    assert_eq!(flash(|flash| flash.slots[1][..image.len()].to_vec()), image);

    // The bootloader starts the trial; confirmed, it stays.
    assert_eq!(orion_boot(false), 1);
    assert_eq!(orion_firmware_confirm(), 0);
    assert_eq!(orion_boot(false), 1);
}
