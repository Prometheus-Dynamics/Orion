/// One reading of [`Costs`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CostSnapshot {
    /// Messages handled.
    pub messages: u64,
    /// Bytes handled.
    pub bytes: u64,
    /// Times a sleeping reader or task was woken.
    pub wakeups: u64,
    /// Nanoseconds spent, as reported by the caller.
    pub nanos: u64,
}

impl CostSnapshot {
    /// What happened between `earlier` and this reading.
    #[must_use]
    pub const fn since(self, earlier: Self) -> Self {
        Self {
            messages: self.messages.wrapping_sub(earlier.messages),
            bytes: self.bytes.wrapping_sub(earlier.bytes),
            wakeups: self.wakeups.wrapping_sub(earlier.wakeups),
            nanos: self.nanos.wrapping_sub(earlier.nanos),
        }
    }

    /// Nanoseconds per message, or zero with no messages.
    #[must_use]
    pub const fn nanos_per_message(self) -> u64 {
        match self.messages {
            0 => 0,
            count => self.nanos / count,
        }
    }
}

/// Per-message costs and wake-ups for one component. Empty, and free, without feature `costs`.
#[derive(Debug, Default)]
pub struct Costs {
    #[cfg(feature = "costs")]
    messages: core::sync::atomic::AtomicU64,
    #[cfg(feature = "costs")]
    bytes: core::sync::atomic::AtomicU64,
    #[cfg(feature = "costs")]
    wakeups: core::sync::atomic::AtomicU64,
    #[cfg(feature = "costs")]
    nanos: core::sync::atomic::AtomicU64,
}

#[cfg(feature = "costs")]
impl Costs {
    /// Records one message of `bytes` bytes that took `nanos`.
    #[inline]
    pub fn message(&self, bytes: u64, nanos: u64) {
        use core::sync::atomic::Ordering::Relaxed;
        self.messages.fetch_add(1, Relaxed);
        self.bytes.fetch_add(bytes, Relaxed);
        self.nanos.fetch_add(nanos, Relaxed);
    }

    /// Records one wake-up.
    #[inline]
    pub fn wakeup(&self) {
        self.wakeups
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }

    /// The counts so far.
    #[must_use]
    pub fn snapshot(&self) -> CostSnapshot {
        use core::sync::atomic::Ordering::Relaxed;
        CostSnapshot {
            messages: self.messages.load(Relaxed),
            bytes: self.bytes.load(Relaxed),
            wakeups: self.wakeups.load(Relaxed),
            nanos: self.nanos.load(Relaxed),
        }
    }
}

#[cfg(not(feature = "costs"))]
impl Costs {
    /// Does nothing without feature `costs`.
    #[inline(always)]
    pub fn message(&self, _bytes: u64, _nanos: u64) {}

    /// Does nothing without feature `costs`.
    #[inline(always)]
    pub fn wakeup(&self) {}

    /// All zero without feature `costs`.
    #[must_use]
    #[inline(always)]
    pub fn snapshot(&self) -> CostSnapshot {
        CostSnapshot::default()
    }
}
