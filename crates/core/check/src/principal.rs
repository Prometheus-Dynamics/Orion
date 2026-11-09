use orion_types::{DeviceName, KeyId, NodeName};

/// Who is asking.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Principal {
    /// Nothing is known about the caller.
    Anonymous,
    /// A program on this board, by its socket credentials.
    Local {
        /// User id.
        uid: u32,
        /// Group id.
        gid: u32,
    },
    /// A person's tool, by enrolled key.
    Operator(KeyId),
    /// Another node.
    Peer(NodeName),
    /// A device on a link.
    Device(DeviceName),
}

/// The classes a [`crate::Policy`] admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PrincipalClass {
    /// [`Principal::Anonymous`].
    Anonymous = 1 << 0,
    /// [`Principal::Local`] with user id 0.
    Root = 1 << 1,
    /// [`Principal::Local`] with any other user id.
    Local = 1 << 2,
    /// [`Principal::Operator`].
    Operator = 1 << 3,
    /// [`Principal::Peer`].
    Peer = 1 << 4,
    /// [`Principal::Device`].
    Device = 1 << 5,
}

impl Principal {
    /// The class this principal falls in.
    #[must_use]
    pub const fn class(&self) -> PrincipalClass {
        match self {
            Self::Anonymous => PrincipalClass::Anonymous,
            Self::Local { uid: 0, .. } => PrincipalClass::Root,
            Self::Local { .. } => PrincipalClass::Local,
            Self::Operator(_) => PrincipalClass::Operator,
            Self::Peer(_) => PrincipalClass::Peer,
            Self::Device(_) => PrincipalClass::Device,
        }
    }
}
