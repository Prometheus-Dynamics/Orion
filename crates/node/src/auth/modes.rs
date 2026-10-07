use crate::NodeError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerAuthenticationMode {
    Disabled,
    Optional,
    Required,
}

impl PeerAuthenticationMode {
    /// Parses `ORION_NODE_PEER_AUTH` without panicking on invalid values.
    pub fn try_from_env() -> Result<Self, NodeError> {
        crate::config::parse_env_choice(
            "ORION_NODE_PEER_AUTH",
            "optional",
            &[
                ("disabled", Self::Disabled),
                ("optional", Self::Optional),
                ("required", Self::Required),
            ],
        )
    }
}

/// Which local IPC callers the node admits (`ORION_NODE_LOCAL_AUTH`), judged from the Unix
/// socket peer credentials. [`LocalAccessAllowList`](super::LocalAccessAllowList)
/// (`ORION_NODE_LOCAL_AUTH_ALLOW`) admits further users and groups in every mode but `Disabled`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalAuthenticationMode {
    /// Every caller that can connect to the socket.
    Disabled,
    /// Callers running as the node's user (the default).
    SameUser,
    /// Callers running as the node's user, or whose primary or supplementary groups include the
    /// node's primary group.
    SameUserOrGroup,
    /// `SameUserOrGroup`, plus root (uid 0), for appliances where operators use a root shell.
    SameUserOrGroupOrRoot,
}

impl LocalAuthenticationMode {
    /// Parses `ORION_NODE_LOCAL_AUTH` without panicking on invalid values.
    pub fn try_from_env() -> Result<Self, NodeError> {
        crate::config::parse_env_choice(
            "ORION_NODE_LOCAL_AUTH",
            "same-user",
            &[
                ("disabled", Self::Disabled),
                ("same-user", Self::SameUser),
                ("same-user-or-group", Self::SameUserOrGroup),
                ("same-user-or-group-or-root", Self::SameUserOrGroupOrRoot),
            ],
        )
    }
}
