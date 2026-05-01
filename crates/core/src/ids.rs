use crate::OrionError;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, fmt, ops::Deref, str::FromStr};

macro_rules! string_newtype {
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
            /// Builds a checked identifier from runtime input.
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

            /// Builds an identifier from a known-good literal or invariant.
            ///
            /// Panics if the value is empty or whitespace-only. Use `try_new` for env, CLI,
            /// network, or other runtime input.
            #[track_caller]
            pub fn new(value: impl Into<String>) -> Self {
                Self::try_new(value).expect(concat!(stringify!($name), " must not be empty"))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl Deref for $name {
            type Target = str;

            fn deref(&self) -> &Self::Target {
                self.as_str()
            }
        }

        impl From<&'static str> for $name {
            fn from(value: &'static str) -> Self {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = OrionError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::try_new(value)
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

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.as_str() == other
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }
    };
}

string_newtype!(NodeId);
string_newtype!(WorkloadId);
string_newtype!(ResourceId);
string_newtype!(ArtifactId);
string_newtype!(ProviderId);
string_newtype!(ExecutorId);
string_newtype!(ClientName);
string_newtype!(SessionId);
string_newtype!(PeerBaseUrl);
string_newtype!(PublicKeyHex);
