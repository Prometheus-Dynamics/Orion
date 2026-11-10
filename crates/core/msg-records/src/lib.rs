//! Record bodies. A record is a named piece of state with a hybrid-clock stamp, so any two nodes
//! that have seen the same writes agree. Watches are always pushed.

#![no_std]

use orion_types::{HlcTimestamp, NodeName, ResourceName};
use orion_wire::{Bytes, Decode, List, body, kind, open_enum, types::Error};

open_enum! {
    /// What sort of record an entry is.
    pub enum RecordClass: u8 {
        /// A [`NodeRecord`].
        Node = 1,
        /// A [`ResourceRecord`].
        Resource = 2,
        /// A [`LeaseRecord`].
        Lease = 3,
    }
}

open_enum! {
    /// Whether a resource can be used.
    pub enum ResourceState: u8 {
        /// Declared, not yet ready.
        Pending = 0,
        /// Ready.
        Ready = 1,
        /// Present, not working.
        Failed = 2,
        /// Its provider went away.
        Lost = 3,
    }
}

body! {
    /// One `key=value` label.
    pub struct Label<'a> {
        /// The key.
        pub key: &'a str,
        /// The value.
        pub value: &'a str,
    }
}

body! {
    /// One record on the wire. `value` holds the typed record, so a store can keep and merge
    /// records of a class it doesn't otherwise understand.
    pub struct RecordEntry<'a> {
        /// The sort of record.
        pub class: RecordClass,
        /// Its name, unique within its class.
        pub key: &'a str,
        /// When it was written.
        pub stamp: HlcTimestamp,
        /// Whether this entry says the record is gone.
        pub deleted: bool,
        /// The typed record.
        pub value: Bytes<'a>,
    }
}

impl<'a> RecordEntry<'a> {
    /// Decodes [`value`](Self::value) as a `T`.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`].
    pub fn decode_value<T: Decode<'a>>(&self) -> Result<T, Error> {
        T::decode(self.value.0)
    }
}

body! {
    /// A node, as the record store holds it.
    pub struct NodeRecord<'a> {
        /// Its name.
        pub name: NodeName,
        /// Where peers reach it.
        pub address: &'a str,
        /// Its labels.
        pub labels: List<'a, Label<'a>>,
    }
}

body! {
    /// A resource: something a provider offers. Orion doesn't know what kinds exist.
    pub struct ResourceRecord<'a> {
        /// Its name.
        pub name: ResourceName,
        /// Its kind, chosen by the provider.
        pub kind: &'a str,
        /// The node it's on.
        pub node: NodeName,
        /// Whether it can be used.
        pub state: ResourceState,
        /// Whatever the provider says about it.
        pub attributes: Bytes<'a>,
    }
}

body! {
    /// The name of one topic or action a resource offers.
    pub struct Offered<'a> {
        /// The full name: the resource's name, a dot, and the topic's or action's own.
        pub name: &'a str,
    }
}

body! {
    /// What a resource offers, as providers write it in [`ResourceRecord::attributes`]. With
    /// this a program that has never heard of the resource's kind can find its topics, read
    /// their declarations, and run its actions.
    pub struct ResourceDescription<'a> {
        /// Its topics.
        pub topics: List<'a, Offered<'a>>,
        /// Its actions.
        pub actions: List<'a, Offered<'a>>,
        /// Whatever else its provider says about it.
        pub labels: List<'a, Label<'a>>,
    }
}

body! {
    /// Who holds a resource.
    pub struct LeaseRecord<'a> {
        /// The resource.
        pub resource: ResourceName,
        /// Its holder.
        pub holder: &'a str,
        /// When the lease ends, in hybrid-clock milliseconds.
        pub expires_ms: u64,
    }
}

body! {
    /// Writes records.
    pub struct Put<'a> = kind::PUT {
        /// The entries.
        pub records: List<'a, RecordEntry<'a>>,
    }
}

body! {
    /// Reads records once. Answered with [`Changed`].
    pub struct Get<'a> = kind::GET {
        /// The class to read.
        pub class: RecordClass,
        /// Keys starting with this. Empty for all.
        pub prefix: &'a str,
    }
}

body! {
    /// Asks for every change to matching records from now on, pushed as [`Changed`].
    pub struct Watch<'a> = kind::WATCH {
        /// The class to watch.
        pub class: RecordClass,
        /// Keys starting with this. Empty for all.
        pub prefix: &'a str,
        /// Send everything newer than this first. Zero for the whole current state.
        pub after: HlcTimestamp,
    }
}

body! {
    /// Records that are new or changed: the answer to a [`Get`], or a push for a [`Watch`].
    pub struct Changed<'a> = kind::CHANGED {
        /// The `seq` of the request.
        pub request_seq: u16,
        /// The entries.
        pub records: List<'a, RecordEntry<'a>>,
        /// Whether more follow for this request.
        pub more: bool,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_wire::body::encode_fields;

    #[test]
    fn a_typed_record_rides_inside_an_entry() {
        let labels = [Label {
            key: "zone",
            value: "a",
        }];
        let node = NodeRecord {
            name: NodeName::new("left").unwrap(),
            address: "10.0.0.1:9200",
            labels: List::new(&labels),
        };
        let mut value = [0; 64];
        let len = encode_fields(&node, &mut value).unwrap();
        let entries = [RecordEntry {
            class: RecordClass::Node,
            key: "left",
            stamp: HlcTimestamp::new(5, 1, 9),
            deleted: false,
            value: Bytes(&value[..len]),
        }];
        let put = Put {
            records: List::new(&entries),
        };
        let mut buf = [0; 128];
        let len = encode_fields(&put, &mut buf).unwrap();
        let decoded = Put::decode(&buf[..len]).unwrap();
        let entry = decoded.records.iter().next().unwrap().unwrap();
        assert_eq!(entry.stamp, HlcTimestamp::new(5, 1, 9));
        assert_eq!(entry.decode_value::<NodeRecord<'_>>().unwrap(), node);
    }

    #[test]
    fn a_bad_name_inside_a_record_is_refused() {
        // A name holding a space, then an empty address and no labels.
        assert_eq!(
            NodeRecord::decode(&[3, b'a', b' ', b'b', 0, 0]).unwrap_err(),
            Error::Malformed
        );
    }
}
