//! CAN identifier configuration for one device link.

/// The pair of CAN identifiers one device link uses.
///
/// The protocol does not fix identifiers. A deployment typically picks a base pair and adds the
/// device address, so many devices share one bus and lower addresses win arbitration.
///
/// ```
/// use orion_link::CanLinkIds;
/// let base = CanLinkIds::new(0x100, 0x180, false);
/// let ids = CanLinkIds::for_address(base, 5).unwrap();
/// assert_eq!((ids.device_to_host, ids.host_to_device), (0x105, 0x185));
/// assert!(CanLinkIds::for_address(base, 0x700).is_none()); // beyond 11 bits
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CanLinkIds {
    /// Identifier of frames the device sends.
    pub device_to_host: u32,
    /// Identifier of frames the host sends to the device.
    pub host_to_device: u32,
    /// 29-bit extended identifiers instead of 11-bit standard ones.
    pub extended: bool,
}

impl CanLinkIds {
    /// Largest 11-bit standard identifier.
    pub const MAX_STANDARD_ID: u32 = 0x7FF;
    /// Largest 29-bit extended identifier.
    pub const MAX_EXTENDED_ID: u32 = 0x1FFF_FFFF;

    /// A pair of identifiers (not validated; see [`CanLinkIds::is_valid`]).
    #[must_use]
    pub const fn new(device_to_host: u32, host_to_device: u32, extended: bool) -> Self {
        Self {
            device_to_host,
            host_to_device,
            extended,
        }
    }

    /// The identifiers for device `address`: both base identifiers plus `address`. Returns `None`
    /// if either result leaves the identifier range or the two would collide.
    #[must_use]
    pub const fn for_address(base: Self, address: u32) -> Option<Self> {
        let Some(device_to_host) = base.device_to_host.checked_add(address) else {
            return None;
        };
        let Some(host_to_device) = base.host_to_device.checked_add(address) else {
            return None;
        };
        let ids = Self::new(device_to_host, host_to_device, base.extended);
        if ids.is_valid() { Some(ids) } else { None }
    }

    /// Largest identifier allowed by [`CanLinkIds::extended`].
    #[must_use]
    pub const fn max_id(&self) -> u32 {
        if self.extended {
            Self::MAX_EXTENDED_ID
        } else {
            Self::MAX_STANDARD_ID
        }
    }

    /// Both identifiers are in range and distinct.
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        self.device_to_host <= self.max_id()
            && self.host_to_device <= self.max_id()
            && self.device_to_host != self.host_to_device
    }
}
