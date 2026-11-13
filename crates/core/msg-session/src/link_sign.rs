//! A signed session on a link: the handshake of `orion+tcp`, carried in link frames, and then
//! every frame under a MAC.
//!
//! A link's far end is a device of the board unless it proves a key. A tool on a USB cable or a
//! serial port that holds an operator's key starts with [`LinkSignHello`]; the board answers
//! [`LinkSignChallenge`], signed by its own key; the tool proves its key with
//! [`LinkSignProve`]; the board says [`LinkSignAnswer`]. The values and the signatures are
//! those of the network handshake (`orion-net-handshake`), so the two are one proof in two
//! carriers. From then on each frame, both ways, travels inside a [`LinkSealed`] with its MAC,
//! and the session is the operator's: what it may do is what the trust store grants that key.

use orion_wire::{Bytes, body, kind};

/// What sealing costs a frame on a link: the outer frame's header and check, the body's two
/// lengths, and the MAC. A signed link's frames are this much shorter than a plain one's.
pub const LINK_SEALED_OVERHEAD: usize = 32;

/// [`LinkSignAnswer::status`]: the key is taken, and sealed frames follow.
pub const LINK_SIGN_OK: u8 = 0;
/// [`LinkSignAnswer::status`]: the key, or the proof, was refused.
pub const LINK_SIGN_REFUSED: u8 = 2;
/// [`LinkSignAnswer::status`]: this link takes no signed sessions.
pub const LINK_SIGN_UNSUPPORTED: u8 = 3;

body! {
    /// From the tool: its fresh values and its key (`eph`, `nonce`, `key`, 32 bytes each).
    pub struct LinkSignHello<'a> = kind::LINK_SIGN_HELLO {
        /// The 96 bytes of the network handshake's client hello.
        pub hello: Bytes<'a>,
    }
}

body! {
    /// From the board: its fresh values and key, and its signature over both sides'.
    pub struct LinkSignChallenge<'a> = kind::LINK_SIGN_CHALLENGE {
        /// The 96 bytes of the network handshake's server hello.
        pub hello: Bytes<'a>,
        /// The board's signature (64 bytes).
        pub signature: Bytes<'a>,
    }
}

body! {
    /// From the tool: its signature over both sides' values.
    pub struct LinkSignProve<'a> = kind::LINK_SIGN_PROVE {
        /// The tool's signature (64 bytes).
        pub signature: Bytes<'a>,
    }
}

body! {
    /// From the board: whether the tool's key was taken.
    pub struct LinkSignAnswer<'a> = kind::LINK_SIGN_ANSWER {
        /// [`LINK_SIGN_OK`], [`LINK_SIGN_REFUSED`] or [`LINK_SIGN_UNSUPPORTED`].
        pub status: u8,
        /// Why, when it was not taken.
        pub why: &'a str,
    }
}

body! {
    /// One frame of a signed link session, and its MAC.
    pub struct LinkSealed<'a> = kind::LINK_SEALED {
        /// The frame, whole.
        pub frame: Bytes<'a>,
        /// Its MAC (16 bytes), under the sender's key and its count of frames sent.
        pub tag: Bytes<'a>,
    }
}
