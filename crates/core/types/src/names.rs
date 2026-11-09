use core::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

const fn name_byte_ok(byte: u8, slash: bool) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b'.' | b'_' | b'-' | b':')
        || (slash && byte == b'/')
}

macro_rules! name {
    ($(#[$doc:meta])* $name:ident, $max:literal, slash = $slash:literal) => {
        $(#[$doc])*
        ///
        /// Held inline, so it needs no allocator. Letters, digits and `. _ - :` are allowed.
        #[derive(Clone)]
        pub struct $name {
            len: u8,
            bytes: [u8; $max],
        }

        impl $name {
            /// The longest allowed name, in bytes.
            pub const MAX: usize = $max;

            /// Validates `value`.
            ///
            /// # Errors
            ///
            /// [`Error::EmptyName`], [`Error::NameTooLong`] or [`Error::InvalidName`].
            pub fn new(value: &str) -> Result<Self, Error> {
                let source = value.as_bytes();
                if source.is_empty() {
                    return Err(Error::EmptyName);
                }
                if source.len() > $max {
                    return Err(Error::NameTooLong);
                }
                if !source.iter().all(|&byte| name_byte_ok(byte, $slash)) {
                    return Err(Error::InvalidName);
                }
                if $slash && value.split('/').any(|part| part == ".." || part.is_empty()) {
                    return Err(Error::InvalidName);
                }
                let mut bytes = [0; $max];
                bytes[..source.len()].copy_from_slice(source);
                // `source.len()` is at most `$max`, which is below 256.
                Ok(Self { len: source.len() as u8, bytes })
            }

            /// The name as bytes.
            #[must_use]
            pub fn as_bytes(&self) -> &[u8] {
                &self.bytes[..usize::from(self.len)]
            }

            /// The name as text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                // Only ASCII is ever stored, so this always succeeds.
                core::str::from_utf8(self.as_bytes()).unwrap_or_default()
            }
        }

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                self.as_bytes() == other.as_bytes()
            }
        }

        impl Eq for $name {}

        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $name {
            fn cmp(&self, other: &Self) -> core::cmp::Ordering {
                self.as_bytes().cmp(other.as_bytes())
            }
        }

        impl core::hash::Hash for $name {
            fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
                self.as_bytes().hash(state);
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({:?})", stringify!($name), self.as_str())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl core::str::FromStr for $name {
            type Err = Error;

            fn from_str(value: &str) -> Result<Self, Error> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct NameVisitor;

                impl Visitor<'_> for NameVisitor {
                    type Value = $name;

                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str(stringify!($name))
                    }

                    fn visit_str<E: de::Error>(self, value: &str) -> Result<$name, E> {
                        $name::new(value).map_err(E::custom)
                    }
                }

                deserializer.deserialize_str(NameVisitor)
            }
        }
    };
}

name!(
    /// The name of a node.
    NodeName, 63, slash = false
);
name!(
    /// The name of a topic on its own node. Across boards see [`TopicRef`].
    TopicName, 63, slash = false
);
name!(
    /// The name of an action.
    ActionName, 63, slash = false
);
name!(
    /// The name of a workload.
    WorkloadName, 63, slash = false
);
name!(
    /// The kind of a workload, which picks the executor that runs it.
    WorkloadKind, 31, slash = false
);
name!(
    /// The name of a resource. May be a `/` path.
    ResourceName, 127, slash = true
);
name!(
    /// The name of a microcontroller or other device on a link.
    DeviceName, 63, slash = false
);
name!(
    /// A destination for a file, which the receiver resolves. May be a `/` path, never with `..`.
    FileName, 127, slash = true
);

/// A topic, optionally on another node: `<topic>` or `<node>/<topic>`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TopicRef {
    /// The node, or `None` for this board.
    pub node: Option<NodeName>,
    /// The topic.
    pub topic: TopicName,
}

impl TopicRef {
    /// A topic on this board.
    #[must_use]
    pub const fn local(topic: TopicName) -> Self {
        Self { node: None, topic }
    }

    /// Parses `<topic>` or `<node>/<topic>`.
    ///
    /// # Errors
    ///
    /// As [`NodeName::new`] and [`TopicName::new`].
    pub fn parse(value: &str) -> Result<Self, Error> {
        match value.split_once('/') {
            Some((node, topic)) => Ok(Self {
                node: Some(NodeName::new(node)?),
                topic: TopicName::new(topic)?,
            }),
            None => Ok(Self::local(TopicName::new(value)?)),
        }
    }
}

impl fmt::Display for TopicRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.node {
            Some(node) => write!(f, "{node}/{}", self.topic),
            None => write!(f, "{}", self.topic),
        }
    }
}

/// The identity of a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeyId(pub [u8; 32]);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_validated() {
        assert_eq!(NodeName::new("").unwrap_err(), Error::EmptyName);
        assert_eq!(NodeName::new("a b").unwrap_err(), Error::InvalidName);
        assert_eq!(NodeName::new("a/b").unwrap_err(), Error::InvalidName);
        assert_eq!(
            NodeName::new(core::str::from_utf8(&[b'a'; 64]).unwrap()).unwrap_err(),
            Error::NameTooLong
        );
        assert_eq!(
            NodeName::new(core::str::from_utf8(&[b'a'; 63]).unwrap())
                .unwrap()
                .as_bytes()
                .len(),
            63
        );
        assert_eq!(
            NodeName::new("cam-0.front_a:1").unwrap().as_str(),
            "cam-0.front_a:1"
        );
    }

    #[test]
    fn paths_cannot_climb_or_hold_empty_parts() {
        assert!(FileName::new("slots/boot.img").is_ok());
        assert_eq!(FileName::new("../etc").unwrap_err(), Error::InvalidName);
        assert_eq!(FileName::new("a/../b").unwrap_err(), Error::InvalidName);
        assert_eq!(FileName::new("/a").unwrap_err(), Error::InvalidName);
        assert_eq!(FileName::new("a//b").unwrap_err(), Error::InvalidName);
    }

    #[test]
    fn names_compare_by_text_whatever_their_length() {
        let (a, b) = (NodeName::new("abc").unwrap(), NodeName::new("b").unwrap());
        assert!(a < b);
    }

    #[test]
    fn topic_refs_parse_both_forms() {
        let local = TopicRef::parse("imu").unwrap();
        assert_eq!(local.node, None);
        let remote = TopicRef::parse("left/imu").unwrap();
        assert_eq!(remote.node.as_ref().unwrap().as_str(), "left");
        assert_eq!(remote.topic.as_str(), "imu");
        assert!(TopicRef::parse("a/b/c").is_err());
    }

    #[test]
    fn a_bad_name_from_the_wire_is_refused() {
        let mut buf = [0; 16];
        let good = postcard::to_slice("ok", &mut buf).unwrap();
        assert_eq!(
            postcard::from_bytes::<NodeName>(good).unwrap().as_str(),
            "ok"
        );
        let bad = postcard::to_slice("not ok", &mut buf).unwrap();
        assert!(postcard::from_bytes::<NodeName>(bad).is_err());
    }
}
