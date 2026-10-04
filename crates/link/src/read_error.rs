use core::fmt;

/// Failure while pulling a frame from a byte reader. Corrupt packets are not errors here: the
/// decoder skips them and counts them in its [`crate::StreamStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFrameError<E> {
    /// The reader failed.
    Io(E),
    /// The reader reached end of file before a complete frame.
    Eof,
}

impl<E: fmt::Debug> fmt::Display for ReadFrameError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "link read failed: {err:?}"),
            Self::Eof => f.write_str("link reader reached end of file"),
        }
    }
}

#[cfg(feature = "std")]
impl<E: fmt::Debug> std::error::Error for ReadFrameError<E> {}
