use crate::OrionError;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
#[rkyv(derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash))]
pub struct FeatureFlag(String);

impl FeatureFlag {
    pub fn try_new(value: impl Into<String>) -> Result<Self, OrionError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(OrionError::InvalidValue {
                type_name: "FeatureFlag",
                value,
            });
        }
        Ok(Self(value))
    }

    pub fn new(value: impl Into<String>) -> Self {
        Self::try_new(value).expect("FeatureFlag must not be empty")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum CompatibilityState {
    Preferred,
    Downgraded,
    Incompatible,
}
