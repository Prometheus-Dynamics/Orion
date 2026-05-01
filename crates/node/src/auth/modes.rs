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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalAuthenticationMode {
    Disabled,
    SameUser,
    SameUserOrGroup,
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
            ],
        )
    }
}
