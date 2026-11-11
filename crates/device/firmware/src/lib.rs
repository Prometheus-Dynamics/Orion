//! New firmware for a microcontroller, through the same file kinds as every other file.
//!
//! The device asks the host for every file under a prefix of its own (`FileSink`), and from
//! then on is sent `FileBegin`, `FileChunk` and `FileCommit` for an image and answers each
//! with `FileStatus`, as any receiver does. The image goes into the slot that is not running
//! ([`Slots`]); a commit reads it back and checks its SHA-256, and only an image that checks
//! out is put on trial. The bootloader ([`boot`]) starts a trial once; the application
//! confirms it ([`Firmware::confirm`]) or the next reset goes back to the slot before.
//!
//! - **Resumable.** A `FileBegin` for the transfer in progress answers with how many bytes
//!   are here, so a sender whose link dropped goes on from there. A chunk that is not next
//!   is not written, and the answer says where to go on from.
//! - **A bad image never starts.** It is never put on trial, and every boot checks the
//!   slot's stored bytes against the CRC recorded when it checked out.
//! - **No reset here.** A committed image waits for the firmware to reset when it is ready
//!   to; [`Heard::Committed`] says it may.
//!
//! The port supplies the flash as a [`SlotFlash`]: two slots and two copies of the record.

#![cfg_attr(not(test), no_std)]

mod boot;
mod flash;
mod record;
mod sha256;
mod slots;

pub use boot::{ResetReason, boot, boot_select};
pub use flash::{FlashError, RECORD_LEN, SlotFlash};
pub use orion_device_core::{RETRY_MS, Request, SendError};
pub use record::{BootRecord, NO_SLOT, Rollback, Slot, load, store};
pub use slots::{Refused, Slots};

use orion_device_core::codec::{Reader, Writer};
use orion_device_core::{Link, Requests};
use orion_wire::{FrameView, kind};

/// `FileState` on the wire.
mod state {
    pub const RECEIVING: u8 = 1;
    pub const COMMITTED: u8 = 3;
    pub const REJECTED: u8 = 4;
}

/// What a frame from the host meant to the firmware layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Heard {
    /// Not about firmware: for another layer.
    Other,
    /// The host hands this device its files from now on.
    Ready,
    /// The host refused to, with an error code.
    Refused {
        /// The error's code.
        code: u16,
    },
    /// Bytes of an image arrived, or one started: `received` are here in order.
    Receiving {
        /// The transfer.
        transfer: u64,
        /// Bytes here.
        received: u32,
    },
    /// An image checked out and is on trial in slot `slot`: reset when ready to start it.
    Committed {
        /// The slot.
        slot: u8,
    },
    /// A transfer was turned away.
    Rejected {
        /// The transfer.
        transfer: u64,
        /// Why.
        why: Refused,
    },
}

/// The status owed to the sender. Only the newest is kept: each says everything.
#[derive(Clone, Copy, Debug)]
struct Owed {
    transfer: u64,
    received: u32,
    state: u8,
    detail: &'static str,
}

/// The firmware layer of a device.
#[derive(Debug)]
pub struct Firmware<F> {
    prefix: &'static str,
    sink: Requests<1>,
    slots: Slots<F>,
    owed: Option<Owed>,
}

impl<F: SlotFlash> Firmware<F> {
    /// Takes files whose destination starts with `prefix`, such as `devices/imu/firmware`,
    /// into the slots of `flash`.
    pub fn new(prefix: &'static str, flash: F) -> Self {
        Self {
            prefix,
            sink: Requests::new(),
            slots: Slots::new(flash),
            owed: None,
        }
    }

    /// The slots.
    #[must_use]
    pub const fn slots(&self) -> &Slots<F> {
        &self.slots
    }

    /// The slots, mutably.
    pub fn slots_mut(&mut self) -> &mut Slots<F> {
        &mut self.slots
    }

    /// Whether the host hands this device its files in this session.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.sink.get(0) == Some(Request::Answered)
    }

    /// The image on trial works: keep it. See [`Slots::confirm`].
    ///
    /// # Errors
    ///
    /// [`Refused::Flash`].
    pub fn confirm(&mut self) -> Result<(), Refused> {
        self.slots.confirm()
    }

    /// Sends the status owed, then asks for the prefix if this session has not. One frame
    /// a call while the link has room. Call it after [`Link::poll`], with the same counter.
    pub fn poll<const TX: usize>(&mut self, link: &mut Link<TX>, now_ms: u32) {
        if link.session().is_none() {
            self.owed = None;
        }
        if let Some(owed) = self.owed
            && link
                .send_with(kind::FILE_STATUS, |out| {
                    Writer::new(out)
                        .varint(owed.transfer)
                        .varint(u64::from(owed.received))
                        .u8(owed.state)
                        .bytes(owed.detail.as_bytes())
                        .finish()
                })
                .is_ok()
        {
            self.owed = None;
        }
        let prefix = self.prefix;
        self.sink.poll(link, now_ms, kind::FILE_SINK, |_, out| {
            Writer::new(out).bytes(prefix.as_bytes()).finish()
        });
    }

    /// Reads a frame the session did not take.
    pub fn handle(&mut self, frame: &FrameView<'_>) -> Heard {
        let mut r = Reader::new(frame.payload());
        match frame.kind() {
            kind::FILE_SINK_READY => match r.u16().and_then(|seq| self.sink.answer(seq)) {
                Some(_) => Heard::Ready,
                None => Heard::Other,
            },
            kind::REFUSAL => {
                let (Some(seq), Some(code)) = (r.u16(), r.u16()) else {
                    return Heard::Other;
                };
                match self.sink.refuse(seq, code) {
                    Some(_) => Heard::Refused { code },
                    None => Heard::Other,
                }
            }
            kind::FILE_BEGIN => {
                let (Some(transfer), Some(destination), Some(size)) =
                    (r.varint(), r.bytes(), r.varint())
                else {
                    return Heard::Other;
                };
                let Some(digest) = r.raw(32).and_then(|d| <&[u8; 32]>::try_from(d).ok()) else {
                    return Heard::Other;
                };
                if !destination.starts_with(self.prefix.as_bytes()) {
                    return Heard::Other;
                }
                let result = self.slots.begin(transfer, size, digest);
                self.answer(transfer, result)
            }
            kind::FILE_CHUNK => {
                let (Some(transfer), Some(offset), Some(data)) =
                    (r.varint(), r.varint(), r.bytes())
                else {
                    return Heard::Other;
                };
                let result = self.slots.write(transfer, offset, data);
                self.answer(transfer, result)
            }
            kind::FILE_COMMIT => {
                let Some(transfer) = r.varint() else {
                    return Heard::Other;
                };
                match self.slots.commit(transfer) {
                    Ok(slot) => {
                        let received = self
                            .slots
                            .record()
                            .slots
                            .get(usize::from(slot))
                            .map_or(0, |held| held.len);
                        self.owe(transfer, received, state::COMMITTED, "");
                        Heard::Committed { slot }
                    }
                    Err(why) => self.reject(transfer, why),
                }
            }
            _ => Heard::Other,
        }
    }

    fn answer(&mut self, transfer: u64, result: Result<u32, Refused>) -> Heard {
        match result {
            Ok(received) => {
                self.owe(transfer, received, state::RECEIVING, "");
                Heard::Receiving { transfer, received }
            }
            Err(why) => self.reject(transfer, why),
        }
    }

    fn reject(&mut self, transfer: u64, why: Refused) -> Heard {
        self.owe(transfer, 0, state::REJECTED, why.detail());
        Heard::Rejected { transfer, why }
    }

    fn owe(&mut self, transfer: u64, received: u32, state: u8, detail: &'static str) {
        self.owed = Some(Owed {
            transfer,
            received,
            state,
            detail,
        });
    }
}
