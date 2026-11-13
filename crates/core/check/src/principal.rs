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

/// As the audit log names the caller: `local:<uid>`, `operator:<key>`, `peer:<node>`,
/// `device:<name>` or `anonymous`.
impl core::fmt::Display for Principal {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Anonymous => f.write_str("anonymous"),
            Self::Local { uid, .. } => write!(f, "local:{uid}"),
            Self::Operator(key) => {
                f.write_str("operator:")?;
                key.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
            }
            Self::Peer(node) => write!(f, "peer:{node}"),
            Self::Device(device) => write!(f, "device:{device}"),
        }
    }
}
