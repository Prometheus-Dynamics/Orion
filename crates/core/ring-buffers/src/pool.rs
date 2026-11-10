//! A pool of buffers: the publisher's, and a subscriber's view of it.

use std::os::fd::{AsFd, BorrowedFd, OwnedFd};

use orion_ring::{ReadMap, RingError, WriteMap, read_only, sealed_memfd};

use crate::map::Mapped;
use crate::{BufferIndex, MAX_BUFFERS, page};

/// Where one buffer is: a slice of one of the pool's descriptors. This is what crosses to the
/// comm core and to subscribers, next to the descriptors themselves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferDesc {
    /// Which of the pool's descriptors backs it.
    pub descriptor: u8,
    /// Where in that descriptor it starts.
    pub offset: u64,
    /// Its size in bytes.
    pub size: u64,
}

/// How much of each descriptor the buffers reach, after checking every buffer.
fn extents(descriptors: usize, buffers: &[BufferDesc]) -> Result<Vec<usize>, RingError> {
    if buffers.is_empty() || buffers.len() > MAX_BUFFERS as usize {
        return Err(RingError::Spec("a pool holds 1 to 1024 buffers"));
    }
    let mut extents = vec![0_usize; descriptors];
    for buffer in buffers {
        let extent =
            extents
                .get_mut(usize::from(buffer.descriptor))
                .ok_or(RingError::Malformed(
                    "a buffer names a descriptor that isn't there",
                ))?;
        let end = buffer
            .offset
            .checked_add(buffer.size)
            .and_then(|end| usize::try_from(end).ok())
            .ok_or(RingError::Malformed("a buffer's end is out of range"))?;
        *extent = (*extent).max(end);
    }
    Ok(extents)
}

fn map_all(
    fds: Vec<OwnedFd>,
    buffers: &[BufferDesc],
    writable: bool,
) -> Result<Vec<Mapped>, RingError> {
    let extents = extents(fds.len(), buffers)?;
    fds.into_iter()
        .zip(extents)
        .map(|(fd, extent)| Mapped::new(fd, extent, writable))
        .collect()
}

/// What a pool's buffers are in.
#[derive(Debug)]
enum Backing {
    /// Descriptors something else fills (a camera driver). The publisher only passes them on.
    Device(Vec<OwnedFd>),
    /// Memory this process fills, mapped for writing.
    Memory(Vec<Mapped>),
}

/// The publisher's pool: its buffers and the pool page it writes.
#[derive(Debug)]
pub struct Pool {
    generation: u32,
    page: WriteMap,
    backing: Backing,
    buffers: Vec<BufferDesc>,
}

impl Pool {
    /// A pool of `buffers`, each a slice of one of `fds`, which something else fills: a camera
    /// driver's dma-bufs. `generation` must be higher than that of any pool this topic had
    /// before.
    ///
    /// # Errors
    ///
    /// [`RingError`] if a buffer names a descriptor that isn't there or the pool page can't be
    /// made.
    pub fn new(
        generation: u32,
        fds: Vec<OwnedFd>,
        buffers: Vec<BufferDesc>,
    ) -> Result<Self, RingError> {
        extents(fds.len(), &buffers)?;
        Self::with(generation, Backing::Device(fds), buffers)
    }

    fn with(
        generation: u32,
        backing: Backing,
        buffers: Vec<BufferDesc>,
    ) -> Result<Self, RingError> {
        let count = buffers.len() as u32;
        let page = WriteMap::new(sealed_memfd(page::bytes(count))?)?;
        page::format(&page, generation, count)?;
        Ok(Self {
            generation,
            page,
            backing,
            buffers,
        })
    }

    /// A pool of `count` buffers of `size` bytes in one new memfd, mapped for writing: for a
    /// publisher that makes its own frames instead of receiving them from a device.
    ///
    /// # Errors
    ///
    /// [`RingError`] if the shape isn't allowed or the memory can't be made.
    pub fn in_memory(generation: u32, count: u32, size: usize) -> Result<Self, RingError> {
        let total = (count as usize)
            .checked_mul(size)
            .ok_or(RingError::Spec("pool is too large"))?;
        let buffers = (0..count as usize)
            .map(|index| BufferDesc {
                descriptor: 0,
                offset: (index * size) as u64,
                size: size as u64,
            })
            .collect::<Vec<_>>();
        let maps = map_all(vec![sealed_memfd(total)?], &buffers, true)?;
        Self::with(generation, Backing::Memory(maps), buffers)
    }

    /// The pool's generation.
    #[must_use]
    pub const fn generation(&self) -> u32 {
        self.generation
    }

    /// Where each buffer is. Sent to subscribers with the descriptors.
    #[must_use]
    pub fn buffers(&self) -> &[BufferDesc] {
        &self.buffers
    }

    /// The pool's descriptors, in the order [`BufferDesc::descriptor`] numbers them.
    pub fn descriptors(&self) -> impl ExactSizeIterator<Item = BorrowedFd<'_>> {
        let (fds, maps) = match &self.backing {
            Backing::Device(fds) => (fds.as_slice(), [].as_slice()),
            Backing::Memory(maps) => ([].as_slice(), maps.as_slice()),
        };
        fds.iter()
            .map(AsFd::as_fd)
            .chain(maps.iter().map(Mapped::fd))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// The pool page, read-only, to send to subscribers.
    ///
    /// # Errors
    ///
    /// If the descriptor can't be reopened, which needs `/proc`.
    pub fn page_fd(&self) -> Result<OwnedFd, RingError> {
        Ok(read_only(self.page.fd())?)
    }

    /// A subscriber's view of this pool, in this process.
    ///
    /// # Errors
    ///
    /// As [`PoolView::attach`].
    pub fn view(&self) -> Result<PoolView, RingError> {
        let fds = self
            .descriptors()
            .map(|fd| read_only(fd).or_else(|_| fd.try_clone_to_owned()))
            .collect::<Result<Vec<_>, _>>()?;
        PoolView::attach(self.page_fd()?, fds, &self.buffers)
    }

    /// The pool page, as the publisher writes it: tickets and cut-offs.
    #[must_use]
    pub const fn page(&self) -> &WriteMap {
        &self.page
    }

    /// The bytes of `buffer`, to fill, if this process fills the pool. The caller must have
    /// taken the buffer back.
    pub(crate) fn bytes_mut(&mut self, buffer: BufferIndex) -> Option<&mut [u8]> {
        let desc = self.buffers.get(buffer.0 as usize)?;
        let Backing::Memory(maps) = &mut self.backing else {
            return None;
        };
        maps.get_mut(usize::from(desc.descriptor))?
            .bytes_mut(desc.offset as usize, desc.size as usize)
    }
}

/// A subscriber's view of a pool: the same buffers, mapped read-only, and the pool page.
#[derive(Debug)]
pub struct PoolView {
    generation: u32,
    page: ReadMap,
    maps: Vec<Mapped>,
    buffers: Vec<BufferDesc>,
}

impl PoolView {
    /// Attaches to a pool handed over by its publisher: the pool page, the descriptors and where
    /// each buffer is. Nothing here is trusted: every buffer is checked against its descriptor,
    /// and a descriptor that could shrink is refused.
    ///
    /// # Errors
    ///
    /// [`RingError`] if the page isn't a pool page for these buffers, a buffer doesn't fit its
    /// descriptor, a descriptor could shrink, or the memory can't be mapped.
    pub fn attach(
        page: OwnedFd,
        fds: Vec<OwnedFd>,
        buffers: &[BufferDesc],
    ) -> Result<Self, RingError> {
        let page = ReadMap::new(page)?;
        let (generation, count) = page::read(&page)?;
        if count as usize != buffers.len() {
            return Err(RingError::Malformed("the pool page is for another pool"));
        }
        Ok(Self {
            generation,
            page,
            maps: map_all(fds, buffers, false)?,
            buffers: buffers.to_vec(),
        })
    }

    /// The pool's generation.
    #[must_use]
    pub const fn generation(&self) -> u32 {
        self.generation
    }

    /// How many buffers the pool has.
    #[must_use]
    pub fn len(&self) -> u32 {
        self.buffers.len() as u32
    }

    /// Whether the pool has no buffers. It never does.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty()
    }

    /// The descriptor behind `buffer` and where the buffer is in it, for a reader that hands
    /// the buffer to something else (a GPU import) instead of reading it.
    #[must_use]
    pub fn descriptor(&self, buffer: BufferIndex) -> Option<(BorrowedFd<'_>, BufferDesc)> {
        let desc = *self.buffers.get(buffer.0 as usize)?;
        Some((self.maps.get(usize::from(desc.descriptor))?.fd(), desc))
    }

    pub(crate) const fn page(&self) -> &ReadMap {
        &self.page
    }

    pub(crate) fn bytes(&self, buffer: BufferIndex) -> Option<&[u8]> {
        let desc = self.buffers.get(buffer.0 as usize)?;
        self.maps
            .get(usize::from(desc.descriptor))?
            .bytes(desc.offset as usize, desc.size as usize)
    }
}
