//! An in-memory flash for the slot logic. Programming refuses cells that are not erased, so a
//! sink that writes without erasing first fails the tests, as real NOR flash would.

use orion_link::crc32c;
use orion_link::wire::XferBeginView;
use orion_mcu_template::slots::{
    BootRecord, FlashError, RECORD_LEN, Slot, SlotFlash, Text, store_record,
};

pub const CAPACITY: u32 = 4096;

#[derive(Debug, Clone)]
pub struct MemFlash {
    slots: [Vec<u8>; 2],
    records: [[u8; RECORD_LEN]; 2],
    /// Set by `reset`; the test's power cycle clears it.
    pub reset_requested: bool,
    /// The next record write stops after this many bytes and fails (a power cut mid-write).
    pub tear_next_record_write: Option<usize>,
}

impl MemFlash {
    /// Erased slots and erased record copies.
    pub fn erased() -> Self {
        Self {
            slots: [vec![0xFF; CAPACITY as usize], vec![0xFF; CAPACITY as usize]],
            records: [[0xFF; RECORD_LEN]; 2],
            reset_requested: false,
            tear_next_record_write: None,
        }
    }

    /// A device flashed with `image` as `version` in slot 0, and the record that boots it.
    pub fn with_factory(version: &str, image: &[u8]) -> Self {
        let mut flash = Self::erased();
        flash.slots[0][..image.len()].copy_from_slice(image);
        let mut record = BootRecord::factory();
        record.slots[0] = Slot {
            version: Text::new(version).expect("short version"),
            len: u32::try_from(image.len()).expect("image length"),
            crc32c: crc32c(image),
        };
        store_record(&mut flash, 0, &record).expect("store the factory record");
        flash
    }

    /// Flips one byte of a slot's image.
    pub fn corrupt_slot(&mut self, slot: usize, at: usize) {
        self.slots[slot][at] ^= 0xFF;
    }

    /// Flips one byte of a record copy.
    pub fn corrupt_record_copy(&mut self, copy: usize, at: usize) {
        self.records[copy][at] ^= 0xFF;
    }
}

impl SlotFlash for MemFlash {
    fn slot_capacity(&self) -> u32 {
        CAPACITY
    }

    fn erase_slot(&mut self, slot: u8) -> Result<(), FlashError> {
        self.slots
            .get_mut(usize::from(slot))
            .ok_or(FlashError)?
            .fill(0xFF);
        Ok(())
    }

    fn write_slot(&mut self, slot: u8, offset: u32, data: &[u8]) -> Result<(), FlashError> {
        let image = self.slots.get_mut(usize::from(slot)).ok_or(FlashError)?;
        let start = usize::try_from(offset).map_err(|_| FlashError)?;
        let dst = image.get_mut(start..start + data.len()).ok_or(FlashError)?;
        if dst.iter().any(|&byte| byte != 0xFF) {
            return Err(FlashError); // programming needs erased cells
        }
        dst.copy_from_slice(data);
        Ok(())
    }

    fn read_slot(&self, slot: u8, offset: u32, out: &mut [u8]) -> Result<(), FlashError> {
        let image = self.slots.get(usize::from(slot)).ok_or(FlashError)?;
        let start = usize::try_from(offset).map_err(|_| FlashError)?;
        let src = image.get(start..start + out.len()).ok_or(FlashError)?;
        out.copy_from_slice(src);
        Ok(())
    }

    fn read_record(&self, copy: u8, out: &mut [u8; RECORD_LEN]) -> Result<(), FlashError> {
        *out = *self.records.get(usize::from(copy)).ok_or(FlashError)?;
        Ok(())
    }

    fn write_record(&mut self, copy: u8, data: &[u8; RECORD_LEN]) -> Result<(), FlashError> {
        let target = self.records.get_mut(usize::from(copy)).ok_or(FlashError)?;
        if let Some(torn) = self.tear_next_record_write.take() {
            let mut partial = [0xFF; RECORD_LEN];
            partial[..torn].copy_from_slice(&data[..torn]);
            *target = partial;
            return Err(FlashError);
        }
        *target = *data;
        Ok(())
    }

    fn reset(&mut self) {
        self.reset_requested = true;
    }
}

/// Deterministic test image of `len` bytes.
pub fn image(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

/// The `XferBegin` for `data` into `target`, one chunk.
pub fn begin_view<'a>(
    xfer_id: u32,
    version: &'a str,
    data: &[u8],
    target: u8,
) -> XferBeginView<'a> {
    XferBeginView {
        xfer_id,
        component: "motor-app",
        version,
        total_len: u32::try_from(data.len()).expect("image length"),
        chunk_len: 128,
        image_crc32c: crc32c(data),
        target_slot: target,
    }
}
