//! The full Orion records as device bodies (feature `alloc`): they encode through postcard itself
//! (into a [`Writer`]), so they are the reference the hand-written views are tested against.

use orion_control_plane::{ProviderRecord, ResourceRecord};
use serde::Serialize;

use super::codec::{Encode, Writer};
use super::views::{ProviderBody, ResourceBody, StatusBody};
use super::{RejectReason, Roles, sealed};
use crate::message::StatusEntry;

/// A postcard flavor writing into a [`Writer`] (which never fails).
struct WriterFlavor<'w, 'a>(&'w mut Writer<'a>);

impl postcard::ser_flavors::Flavor for WriterFlavor<'_, '_> {
    type Output = ();

    fn try_push(&mut self, data: u8) -> postcard::Result<()> {
        self.0.byte(data);
        Ok(())
    }

    fn try_extend(&mut self, data: &[u8]) -> postcard::Result<()> {
        self.0.raw(data);
        Ok(())
    }

    fn finalize(self) -> postcard::Result<()> {
        Ok(())
    }
}

/// Appends the postcard encoding of `value`.
fn encode_serde<T: Serialize + ?Sized>(value: &T, w: &mut Writer<'_>) {
    // Writing into a `Writer` cannot fail, and the record types serialize infallibly.
    let _ = postcard::serialize_with_flavor(value, WriterFlavor(w));
}

macro_rules! serde_body {
    ($($ty:ty => $marker:ident),+ $(,)?) => {$(
        impl Encode for $ty {
            fn encode(&self, w: &mut Writer<'_>) {
                encode_serde(self, w);
            }
        }
        impl sealed::Sealed for $ty {}
        impl $marker for $ty {}
    )+};
}

serde_body!(
    ProviderRecord => ProviderBody,
    ResourceRecord => ResourceBody,
    StatusEntry => StatusBody,
);

impl Serialize for Roles {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct("Roles", &self.0)
    }
}

impl<'de> serde::Deserialize<'de> for Roles {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(rename = "Roles")]
        struct Shadow(u8);
        Shadow::deserialize(deserializer).map(|Shadow(bits)| Self(bits))
    }
}

impl Serialize for RejectReason {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(self.code())
    }
}

impl<'de> serde::Deserialize<'de> for RejectReason {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        u8::deserialize(deserializer).map(Self::from_code)
    }
}
