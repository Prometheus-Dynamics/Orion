use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::PrincipalClass;

/// Which classes of caller a [`Policy`] lets through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Admits(u8);

impl Admits {
    /// Nobody.
    pub const NOBODY: Self = Self(0);
    /// Root on this board.
    pub const ROOT: Self = Self(PrincipalClass::Root as u8);
    /// Any program on this board.
    pub const LOCAL: Self = Self(PrincipalClass::Root as u8 | PrincipalClass::Local as u8);
    /// Enrolled operators.
    pub const OPERATOR: Self = Self(PrincipalClass::Operator as u8);
    /// Other nodes.
    pub const PEER: Self = Self(PrincipalClass::Peer as u8);
    /// Devices on links.
    pub const DEVICE: Self = Self(PrincipalClass::Device as u8);
    /// Everybody, including callers nothing is known about.
    pub const ANYONE: Self = Self(0x3F);

    /// Both sets.
    #[must_use]
    pub const fn and(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Whether `class` is in the set.
    #[must_use]
    pub const fn contains(self, class: PrincipalClass) -> bool {
        self.0 & class as u8 != 0
    }
}

/// Who may do something. Deliberately has no `Default`: whoever adds an action decides this.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    admits: Admits,
}

impl Policy {
    /// A policy admitting `admits`.
    #[must_use]
    pub const fn new(admits: Admits) -> Self {
        Self { admits }
    }

    /// Who it admits.
    #[must_use]
    pub const fn admits(&self) -> Admits {
        self.admits
    }
}

impl Serialize for Policy {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(self.admits.0)
    }
}

impl<'de> Deserialize<'de> for Policy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        u8::deserialize(deserializer).map(|bits| Self::new(Admits(bits)))
    }
}

/// Something registered together with its policy, such as an action. The only way to make one is
/// to supply the policy.
#[derive(Clone, Debug)]
pub struct Registered<D> {
    declaration: D,
    policy: Policy,
}

impl<D> Registered<D> {
    /// Registers `declaration` under `policy`.
    #[must_use]
    pub const fn new(declaration: D, policy: Policy) -> Self {
        Self {
            declaration,
            policy,
        }
    }

    /// What was registered.
    #[must_use]
    pub const fn declaration(&self) -> &D {
        &self.declaration
    }

    /// Who may use it.
    #[must_use]
    pub const fn policy(&self) -> &Policy {
        &self.policy
    }
}
