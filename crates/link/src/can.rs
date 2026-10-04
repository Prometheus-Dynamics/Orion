//! Conversions between link segments and [`embedded_can`] frames (feature `embedded-can`).

use embedded_can::{ExtendedId, Frame, Id, StandardId};

use crate::frame::FrameView;
use crate::packet::{CanLinkIds, PacketError, Reassembler, Segment};

impl CanLinkIds {
    fn id_for(&self, raw: u32) -> Option<Id> {
        if self.extended {
            ExtendedId::new(raw).map(Id::Extended)
        } else {
            u16::try_from(raw)
                .ok()
                .and_then(StandardId::new)
                .map(Id::Standard)
        }
    }

    /// [`CanLinkIds::device_to_host`] as an [`embedded_can::Id`], or `None` if out of range.
    #[must_use]
    pub fn device_to_host_id(&self) -> Option<Id> {
        self.id_for(self.device_to_host)
    }

    /// [`CanLinkIds::host_to_device`] as an [`embedded_can::Id`], or `None` if out of range.
    #[must_use]
    pub fn host_to_device_id(&self) -> Option<Id> {
        self.id_for(self.host_to_device)
    }

    /// Whether `id` is this link's device→host identifier (the host's receive filter).
    #[must_use]
    pub fn is_device_to_host(&self, id: Id) -> bool {
        self.device_to_host_id() == Some(id)
    }

    /// Whether `id` is this link's host→device identifier (the device's receive filter).
    #[must_use]
    pub fn is_host_to_device(&self, id: Id) -> bool {
        self.host_to_device_id() == Some(id)
    }
}

impl Segment {
    /// Builds a data frame carrying this segment. `None` if the frame type rejects the length
    /// (for example a classic-only frame type given a CAN FD segment).
    #[must_use]
    pub fn to_can_frame<F: Frame>(&self, id: Id) -> Option<F> {
        F::new(id, self.as_bytes())
    }
}

impl<const N: usize> Reassembler<N> {
    /// Feeds a received CAN frame. Remote frames are ignored. The caller filters by identifier,
    /// for example with [`CanLinkIds::is_device_to_host`].
    ///
    /// # Errors
    ///
    /// As [`Reassembler::push`].
    pub fn push_can_frame<F: Frame>(
        &mut self,
        frame: &F,
    ) -> Result<Option<FrameView<'_>>, PacketError> {
        if frame.is_remote_frame() {
            return Ok(None);
        }
        self.push(frame.data())
    }
}
