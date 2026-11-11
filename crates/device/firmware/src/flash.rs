//! The board's flash, as the slots need it. A port supplies it; nothing here names a chip.

/// Bytes in one copy of the boot record.
pub const RECORD_LEN: usize = 96;

/// A flash operation failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlashError;

/// Two image slots and two copies of the boot record.
///
/// A slot is a byte-addressed region of [`SlotFlash::slot_capacity`] bytes. The port hides
/// erase units and alignment: `erase_slot` leaves a whole slot erased, and `write_slot` may
/// be called at any offset of an erased slot, in order. Each record copy is its own region,
/// erased and programmed as a whole by `write_record`, so a power cut tears at most one.
pub trait SlotFlash {
    /// Bytes in one slot: the largest image.
    fn slot_capacity(&self) -> u32;

    /// Erases slot `slot` (0 or 1).
    ///
    /// # Errors
    ///
    /// [`FlashError`] if the erase failed.
    fn erase_slot(&mut self, slot: u8) -> Result<(), FlashError>;

    /// Programs `data` at `offset` of slot `slot`, which is erased there.
    ///
    /// # Errors
    ///
    /// [`FlashError`] if the write failed.
    fn write_slot(&mut self, slot: u8, offset: u32, data: &[u8]) -> Result<(), FlashError>;

    /// Reads `out.len()` bytes of slot `slot` at `offset`.
    ///
    /// # Errors
    ///
    /// [`FlashError`] if the read failed or leaves the slot.
    fn read_slot(&self, slot: u8, offset: u32, out: &mut [u8]) -> Result<(), FlashError>;

    /// Reads record copy `copy` (0 or 1). An erased or torn copy reads as whatever is there;
    /// the record's own check turns it away.
    ///
    /// # Errors
    ///
    /// [`FlashError`] if the read failed.
    fn read_record(&self, copy: u8, out: &mut [u8; RECORD_LEN]) -> Result<(), FlashError>;

    /// Erases and programs record copy `copy`.
    ///
    /// # Errors
    ///
    /// [`FlashError`] if the erase or the write failed. The copy may then be torn.
    fn write_record(&mut self, copy: u8, data: &[u8; RECORD_LEN]) -> Result<(), FlashError>;
}
