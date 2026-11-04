//! The persistent boot record: which slot runs, the trial state, the versions and digests of the
//! two slot images, and the last rollback.
//!
//! The record is kept in two copies (the port's two record regions). Each copy carries a sequence
//! number and a CRC-32C. A write always goes to the copy that does not hold the newest valid
//! record, so a write torn by a power cut only damages the older copy: the reader takes the copy
//! with the highest sequence number whose CRC checks out.
//!
//! Layout (little-endian, `RECORD_LEN` bytes, the CRC last):
//!
//! | bytes | field |
//! |-------|-------|
//! | 0..4 | magic `"ORB1"` |
//! | 4..8 | sequence number |
//! | 8, 9 | active slot, previous slot |
//! | 10, 11 | trial slot (`NO_SLOT` if none), trial boots |
//! | 12..94 | two slots: version (1 + 32 bytes), image length, image CRC-32C (41 bytes each) |
//! | 94..162 | rollback: present, reason code, version (33 bytes), note (33 bytes) |
//! | 162..166 | CRC-32C of bytes 0..162 |

use orion_link::crc32c;
use orion_link::wire::RollbackReason;

use super::{FlashError, MAX_NOTE_BYTES, MAX_VERSION_BYTES, SLOTS, SlotFlash};

/// The trial slot field when no trial is pending.
pub const NO_SLOT: u8 = 0xFF;

const MAGIC: u32 = 0x4F52_4231; // "ORB1"
const SLOT_BYTES: usize = (1 + MAX_VERSION_BYTES) + 4 + 4;
const ROLLBACK_BYTES: usize = 1 + 1 + (1 + MAX_VERSION_BYTES) + (1 + MAX_NOTE_BYTES);
/// The record without its trailing CRC.
const BODY_LEN: usize = 4 + 4 + 4 + SLOTS as usize * SLOT_BYTES + ROLLBACK_BYTES;
/// Bytes of one stored copy of the record.
pub const RECORD_LEN: usize = BODY_LEN + 4;

/// A short string with a fixed capacity, stored inline (no allocator).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Text<const N: usize> {
    len: u8,
    bytes: [u8; N],
}

impl<const N: usize> Text<N> {
    /// The empty string.
    pub const EMPTY: Self = Self {
        len: 0,
        bytes: [0; N],
    };

    /// The text, or `None` if it is longer than `N` bytes.
    #[must_use]
    pub fn new(text: &str) -> Option<Self> {
        if text.len() > N {
            return None;
        }
        let mut bytes = [0; N];
        bytes
            .get_mut(..text.len())?
            .copy_from_slice(text.as_bytes());
        Some(Self {
            len: u8::try_from(text.len()).ok()?,
            bytes,
        })
    }

    /// The text (empty if the stored bytes are not UTF-8, which a valid record never has).
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.bytes
            .get(..usize::from(self.len))
            .and_then(|bytes| core::str::from_utf8(bytes).ok())
            .unwrap_or("")
    }
}

/// One image slot as the record describes it: the version and the digest the boot check verifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slot {
    /// The image's version (empty if the slot holds no image).
    pub version: Text<MAX_VERSION_BYTES>,
    /// The image length in bytes (0 if none).
    pub len: u32,
    /// CRC-32C of the image.
    pub crc32c: u32,
}

impl Slot {
    /// A slot with no image.
    pub const EMPTY: Self = Self {
        version: Text::EMPTY,
        len: 0,
        crc32c: 0,
    };
}

/// The last trial the bootloader reverted: the image that was not kept, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rollback {
    /// Version of the reverted image.
    pub version: Text<MAX_VERSION_BYTES>,
    /// Why it was reverted.
    pub reason: RollbackReason,
    /// Short note for the operator.
    pub note: Text<MAX_NOTE_BYTES>,
}

/// The state the bootloader and the firmware sink share. See the [module docs](self).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootRecord {
    /// The slot that runs (the trial slot while a trial is pending).
    pub active: u8,
    /// The slot a trial reverts to.
    pub previous: u8,
    /// The trial slot, or [`NO_SLOT`].
    pub trial: u8,
    /// Boots the pending trial has had (0: armed, not booted yet; the bootloader boots it once).
    pub trial_boots: u8,
    /// The two image slots.
    pub slots: [Slot; SLOTS as usize],
    /// The last rollback, cleared by the next transfer or activation.
    pub rollback: Option<Rollback>,
}

impl BootRecord {
    /// The record of a device with no image: slot 0 active and empty, no trial, no rollback.
    #[must_use]
    pub const fn factory() -> Self {
        Self {
            active: 0,
            previous: 0,
            trial: NO_SLOT,
            trial_boots: 0,
            slots: [Slot::EMPTY; SLOTS as usize],
            rollback: None,
        }
    }

    /// The version of `slot` (empty if out of range).
    #[must_use]
    pub fn version(&self, slot: u8) -> Text<MAX_VERSION_BYTES> {
        self.slots
            .get(usize::from(slot))
            .map_or(Text::EMPTY, |slot| slot.version)
    }

    /// Encodes the record as copy `seq` of the record (see the module docs).
    #[must_use]
    pub fn encode(&self, seq: u32) -> [u8; RECORD_LEN] {
        let mut out = [0u8; RECORD_LEN];
        {
            let mut put = Put {
                buf: &mut out[..BODY_LEN],
                at: 0,
            };
            put.u32(MAGIC);
            put.u32(seq);
            put.u8(self.active);
            put.u8(self.previous);
            put.u8(self.trial);
            put.u8(self.trial_boots);
            for slot in &self.slots {
                put.text(&slot.version);
                put.u32(slot.len);
                put.u32(slot.crc32c);
            }
            match &self.rollback {
                Some(rollback) => {
                    put.u8(1);
                    put.u8(rollback.reason.code());
                    put.text(&rollback.version);
                    put.text(&rollback.note);
                }
                None => {
                    put.u8(0);
                    put.u8(0);
                    put.text(&Text::<MAX_VERSION_BYTES>::EMPTY);
                    put.text(&Text::<MAX_NOTE_BYTES>::EMPTY);
                }
            }
        }
        let crc = crc32c(&out[..BODY_LEN]);
        out[BODY_LEN..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decodes one stored copy: its sequence number and the record. `None` if the copy is erased,
    /// torn, or not a record (bad magic, CRC, or field values).
    #[must_use]
    pub fn decode(buf: &[u8; RECORD_LEN]) -> Option<(u32, Self)> {
        let body = buf.get(..BODY_LEN)?;
        let stored = u32::from_le_bytes(buf.get(BODY_LEN..)?.try_into().ok()?);
        if crc32c(body) != stored {
            return None;
        }
        let mut get = Get { buf: body, at: 0 };
        if get.u32()? != MAGIC {
            return None;
        }
        let seq = get.u32()?;
        let active = get.u8()?;
        let previous = get.u8()?;
        let trial = get.u8()?;
        let trial_boots = get.u8()?;
        if active >= SLOTS || previous >= SLOTS || (trial != NO_SLOT && trial >= SLOTS) {
            return None;
        }
        let mut slots = [Slot::EMPTY; SLOTS as usize];
        for slot in &mut slots {
            slot.version = get.text()?;
            slot.len = get.u32()?;
            slot.crc32c = get.u32()?;
        }
        let present = get.u8()?;
        let reason = RollbackReason::from_code(get.u8()?);
        let version = get.text()?;
        let note = get.text()?;
        let rollback = match present {
            0 => None,
            1 => Some(Rollback {
                version,
                reason,
                note,
            }),
            _ => return None,
        };
        Some((
            seq,
            Self {
                active,
                previous,
                trial,
                trial_boots,
                slots,
                rollback,
            },
        ))
    }
}

/// Reads both copies and returns the newest valid record with its sequence number. A device with
/// no valid copy (never written, or both torn) gets the factory record at sequence 0.
pub fn load<F: SlotFlash>(flash: &F) -> (u32, BootRecord) {
    let mut newest: Option<(u32, BootRecord)> = None;
    for copy in 0..2 {
        let mut buf = [0u8; RECORD_LEN];
        if flash.read_record(copy, &mut buf).is_err() {
            continue;
        }
        if let Some((seq, record)) = BootRecord::decode(&buf)
            && newest.is_none_or(|(best, _)| seq > best)
        {
            newest = Some((seq, record));
        }
    }
    newest.unwrap_or((0, BootRecord::factory()))
}

/// Writes `record` as the next sequence number (`seq + 1`) into the copy that does not hold the
/// newest record, and returns that sequence number. A failed or torn write leaves the newest
/// valid copy intact.
pub fn store<F: SlotFlash>(
    flash: &mut F,
    seq: u32,
    record: &BootRecord,
) -> Result<u32, FlashError> {
    let next = seq.wrapping_add(1);
    flash.write_record(u8::from(next & 1 == 1), &record.encode(next))?;
    Ok(next)
}

/// Reads fields in order from a record body.
struct Get<'a> {
    buf: &'a [u8],
    at: usize,
}

impl Get<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let end = self.at.checked_add(n)?;
        let out = self.buf.get(self.at..end)?;
        self.at = end;
        Some(out)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1)?.first().copied()
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn text<const N: usize>(&mut self) -> Option<Text<N>> {
        let len = self.u8()?;
        let bytes = self.take(N)?;
        if usize::from(len) > N {
            return None;
        }
        let mut arr = [0u8; N];
        arr.copy_from_slice(bytes);
        Some(Text { len, bytes: arr })
    }
}

/// Writes fields in order into a record body of exactly the right size.
struct Put<'a> {
    buf: &'a mut [u8],
    at: usize,
}

impl Put<'_> {
    fn bytes(&mut self, src: &[u8]) {
        let end = self.at + src.len();
        self.buf[self.at..end].copy_from_slice(src);
        self.at = end;
    }

    fn u8(&mut self, value: u8) {
        self.bytes(&[value]);
    }

    fn u32(&mut self, value: u32) {
        self.bytes(&value.to_le_bytes());
    }

    fn text<const N: usize>(&mut self, text: &Text<N>) {
        self.u8(text.len);
        self.bytes(&text.bytes);
    }
}
