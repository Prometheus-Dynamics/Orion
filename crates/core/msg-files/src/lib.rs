//! File bodies. Files are sent in chunks, can resume, are checked against a digest, and go straight
//! to where they belong. A receiver resolves the destination, so the wire names no paths on disk.

#![no_std]

use orion_types::FileName;
use orion_wire::{Bytes, body, kind, open_enum};

open_enum! {
    /// Where a transfer is.
    pub enum FileState: u8 {
        /// More chunks are wanted.
        Receiving = 1,
        /// All bytes are here and the digest is being checked.
        Verifying = 2,
        /// It's in place.
        Committed = 3,
        /// It failed. See the detail.
        Rejected = 4,
    }
}

body! {
    /// Starts or resumes a transfer. Answered with [`FileStatus`], whose `received` says where
    /// to continue from.
    pub struct FileBegin = kind::FILE_BEGIN {
        /// Chosen by the sender. Reusing one resumes that transfer.
        pub transfer: u64,
        /// Where it should go.
        pub destination: FileName,
        /// Its total size.
        pub size: u64,
        /// Its digest.
        pub digest: [u8; 32],
    }
}

body! {
    /// Some bytes of a transfer.
    pub struct FileChunk<'a> = kind::FILE_CHUNK {
        /// The transfer.
        pub transfer: u64,
        /// Where these bytes go.
        pub offset: u64,
        /// The bytes.
        pub data: Bytes<'a>,
    }
}

body! {
    /// Every byte has been sent: check the digest and put it in place.
    pub struct FileCommit = kind::FILE_COMMIT {
        /// The transfer.
        pub transfer: u64,
    }
}

body! {
    /// Where a transfer is.
    pub struct FileStatus<'a> = kind::FILE_STATUS {
        /// The transfer.
        pub transfer: u64,
        /// Bytes received in order so far.
        pub received: u64,
        /// Where it is.
        pub state: FileState,
        /// A description for people if it was rejected.
        pub detail: &'a str,
    }
}

body! {
    /// Asks the receiver to fetch a file itself. Answered with [`FileStatus`].
    pub struct FilePull<'a> = kind::FILE_PULL {
        /// Chosen by the sender.
        pub transfer: u64,
        /// Where to fetch it from.
        pub url: &'a str,
        /// Where it should go.
        pub destination: FileName,
        /// Its total size.
        pub size: u64,
        /// Its digest.
        pub digest: [u8; 32],
    }
}

body! {
    /// Asks to be handed every file whose destination starts with `prefix`, instead of the
    /// node keeping it: the file then goes straight to where this program puts it, and is
    /// never held twice. Answered with [`FileSinkReady`]. From then on this program is sent
    /// [`FileBegin`], [`FileChunk`] and [`FileCommit`] for those files and answers each with
    /// [`FileStatus`], as a node would.
    pub struct FileSink<'a> = kind::FILE_SINK {
        /// What the destinations start with. Not empty.
        pub prefix: &'a str,
    }
}

body! {
    /// Answers a [`FileSink`]: files under its prefix now go to that program.
    pub struct FileSinkReady = kind::FILE_SINK_READY {
        /// The `seq` of the [`FileSink`].
        pub request_seq: u16,
    }
}
