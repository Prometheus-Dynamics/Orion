use crate::OrionError;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

macro_rules! type_name {
    ($name:ident) => {
        #[derive(
            Clone,
            Debug,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
            Archive,
            RkyvSerialize,
            RkyvDeserialize,
        )]
        #[serde(transparent)]
        #[rkyv(derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash))]
        pub struct $name(String);

        impl $name {
            pub fn try_new(value: impl Into<String>) -> Result<Self, OrionError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(OrionError::InvalidValue {
                        type_name: stringify!($name),
                        value,
                    });
                }
                Ok(Self(value))
            }

            pub fn new(value: impl Into<String>) -> Self {
                Self::try_new(value).expect(concat!(stringify!($name), " must not be empty"))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = OrionError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::try_new(s)
            }
        }
    };
}

type_name!(RuntimeType);
type_name!(ResourceType);
type_name!(ConfigSchemaId);
type_name!(CapabilityId);

pub trait RuntimeTypeDef {
    const TYPE: &'static str;
}

pub trait ResourceTypeDef {
    const TYPE: &'static str;
}

pub trait ConfigSchemaDef {
    const SCHEMA_ID: &'static str;
}

pub trait CapabilityDef {
    const CAPABILITY_ID: &'static str;
}

impl RuntimeType {
    pub fn of<T: RuntimeTypeDef>() -> Self {
        Self::new(T::TYPE)
    }
}

impl ResourceType {
    pub fn of<T: ResourceTypeDef>() -> Self {
        Self::new(T::TYPE)
    }
}

impl ConfigSchemaId {
    pub fn of<T: ConfigSchemaDef>() -> Self {
        Self::new(T::SCHEMA_ID)
    }
}

impl CapabilityId {
    pub fn of<T: CapabilityDef>() -> Self {
        Self::new(T::CAPABILITY_ID)
    }
}
