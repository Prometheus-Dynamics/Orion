//! File bodies. Files are sent in chunks, can resume, are checked against a digest, and go straight
//! to where they belong. A receiver resolves the destination, so the wire names no paths on disk.

#![no_std]

use orion_types::FileName;
use orion_wire::{Bytes, List, body, kind, open_enum};

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
        /// The program giving a file stopped giving it, saying why in the detail. Only a giver
        /// says it; a receiver that does not know it ends the transfer as not given.
        Aborted = 5,
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
        ;
        /// The most bytes of a chunk this program takes, or 0 for any. A program that names
        /// a limit is also sent one frame at a time: each is sent once the one before is
        /// answered, so a receiver with room for one frame is never sent two.
        pub max_chunk: u32,
    }
}

body! {
    /// Answers a [`FileSink`]: files under its prefix now go to that program.
    pub struct FileSinkReady = kind::FILE_SINK_READY {
        /// The `seq` of the [`FileSink`].
        pub request_seq: u16,
    }
}

body! {
    /// The arguments of the node's `files.send` action: a file the node keeps is sent on to
    /// a destination that a receiver asked for with [`FileSink`], such as a device's
    /// firmware slot. The run succeeds when the receiver says committed.
    pub struct FileSend {
        /// The file in the node's store.
        pub file: FileName,
        /// Where it goes.
        pub to: FileName,
    }
}

/// The most chunks of a fetched file a program keeps on their way that the client has not
/// said it has. The comm core sizes the chunks it asks for so that this many fit in the
/// client's outbox twice over.
pub const FETCH_WINDOW: u32 = 8;

body! {
    /// Offers every file whose name starts with `prefix` to be fetched from this program: the
    /// comm core hands it each [`FileFetch`] for one. Answered with [`FileOffered`]. A prefix
    /// another program offers is refused as busy, naming it, until that program's session ends.
    pub struct FileOffer<'a> = kind::FILE_OFFER {
        /// What the names start with: one or more parts of a name, ending in `/`.
        pub prefix: &'a str,
    }
}

body! {
    /// Answers a [`FileOffer`]: fetches of names under its prefix now come to that program.
    pub struct FileOffered = kind::FILE_OFFERED {
        /// The `seq` of the [`FileOffer`].
        pub request_seq: u16,
    }
}

body! {
    /// Asks for the file `name`, from byte `from` on: what the client has already, so a fetch
    /// that broke goes on where it stopped. A client sends it to the comm core, which passes
    /// it to the program that offers the name with the transfer renumbered and `caller` said.
    /// Answered by that program with [`FileGive`], then [`FileGiveChunk`]s paced by the
    /// client's [`FileTaken`]; or with [`FileTaken`] rejected, saying why.
    pub struct FileFetch<'a> = kind::FILE_FETCH {
        /// Chosen by the client, and renumbered by the comm core for the program.
        pub transfer: u64,
        /// The file.
        pub name: FileName,
        /// The bytes the client has already, in order from the start.
        pub from: u64,
        /// Who asked, as the audit log names them. Empty from a client; the comm core says it.
        pub caller: &'a str,
        /// The most bytes of a chunk the client takes. Zero from a client; the comm core says
        /// it.
        pub max_chunk: u32,
    }
}

body! {
    /// The file asked for: its size and digest. The chunks from [`FileFetch::from`] follow.
    pub struct FileGive = kind::FILE_GIVE {
        /// The transfer.
        pub transfer: u64,
        /// Its total size.
        pub size: u64,
        /// Its SHA-256.
        pub digest: [u8; 32],
    }
}

body! {
    /// Some bytes of a file being fetched.
    pub struct FileGiveChunk<'a> = kind::FILE_GIVE_CHUNK {
        /// The transfer.
        pub transfer: u64,
        /// Where these bytes go.
        pub offset: u64,
        /// The bytes.
        pub data: Bytes<'a>,
    }
}

body! {
    /// Where a fetch is. From the client: the bytes it has, in order, while `Receiving`;
    /// `Committed` once the digest held, or `Rejected` with why. From the offering program, or
    /// the comm core: `Rejected`, with why the file is not given.
    pub struct FileTaken<'a> = kind::FILE_TAKEN {
        /// The transfer.
        pub transfer: u64,
        /// Bytes the client has, in order from the start.
        pub received: u64,
        /// Where it is.
        pub state: FileState,
        /// Why, if it was rejected.
        pub detail: &'a str,
    }
}

body! {
    /// Asks the comm core which prefixes are offered. Answered with [`FileOffersList`].
    pub struct FileOffers = kind::FILE_OFFERS {
        /// Reserved: zero.
        pub reserved: u8,
    }
}

open_enum! {
    /// What serves a prefix.
    pub enum OfferedBy: u8 {
        /// A program, for as long as its session lives.
        Program = 1,
        /// The comm core itself.
        CommCore = 2,
        /// The comm core, from its configuration, while no program offers the prefix.
        Configured = 3,
    }
}

body! {
    /// One prefix that is offered.
    pub struct OfferEntry<'a> {
        /// The prefix.
        pub prefix: &'a str,
        /// What serves it.
        pub by: OfferedBy,
        /// The name the program gave in its hello; empty for the comm core.
        pub name: &'a str,
        /// The program's principal, as the audit log names it; empty for the comm core.
        pub principal: &'a str,
        /// Since when, in milliseconds on the comm core's clock.
        pub since_ms: u64,
    }
}

body! {
    /// Answers a [`FileOffers`].
    pub struct FileOffersList<'a> = kind::FILE_OFFERS_LIST {
        /// The `seq` of the [`FileOffers`].
        pub request_seq: u16,
        /// Offers refused since the comm core started because another held the prefix.
        pub clashes: u64,
        /// Every prefix offered.
        pub offers: List<'a, OfferEntry<'a>>,
    }
}
