//! The client's provider role.
//!
//! A resource is a thing that exists: a camera, a fan, an arm. A provider is the program
//! that speaks for it. It says the resource is there, declares the topics that carry what the
//! resource reports, and registers the actions that make it do something.
//!
//! What is written down about a resource is enough for a program that has never heard of its
//! kind. The record names the resource's topics and actions; a topic's declaration names its
//! fields, their types and their units; an action has a description and a policy. So a
//! dashboard, a tool or another board shows the resource, reads it and drives it with no code
//! for that kind of device. [`resources`] is that reading side.
//!
//! A resource's topics and actions are named after it: `fan.left` has the topic
//! `fan.left.speed` and the action `fan.left.set`.

use std::fmt;

use orion_actions::{ActionDecl, Actions, Policy, Registration, Target};
use orion_connect::{Client, ClientError};
use orion_msg_records::{Label, Offered, ResourceDescription, ResourceRecord};
use orion_msg_topics::{Field, OnSubscribe};
use orion_records::{Record, RecordClass, RecordEntry, Records};
use orion_topics::{Publisher, TopicDecl, TopicError, Topics};
use orion_types::{HlcTimestamp, NodeName, ResourceName};
use orion_wire::body::encode_fields;
use orion_wire::{Bytes, Decode, List};

pub use orion_msg_records::ResourceState;

/// A provider on one session.
#[derive(Clone)]
pub struct Provider {
    node: NodeName,
    records: Records,
    topics: Topics,
    actions: Actions,
}

impl fmt::Debug for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Provider({})", self.node)
    }
}

impl Provider {
    /// A provider on `client`'s session, using its records, topics and actions.
    #[must_use]
    pub fn new(client: &Client, records: &Records, topics: &Topics, actions: &Actions) -> Self {
        Self {
            node: client.session(|session| session.welcomed().node.clone()),
            records: records.clone(),
            topics: topics.clone(),
            actions: actions.clone(),
        }
    }

    /// Starts describing the resource `name`, of the kind `kind`. The kind is the
    /// provider's own word: Orion does not know what kinds exist.
    ///
    /// # Errors
    ///
    /// If the name is not one.
    pub fn resource(&self, name: &str, kind: &str) -> Result<Describing, TopicError> {
        Ok(Describing {
            provider: self.clone(),
            name: name
                .parse()
                .map_err(|error| TopicError::Invalid(format!("{name:?}: {error}")))?,
            kind: kind.to_owned(),
            topics: Vec::new(),
            actions: Vec::new(),
            labels: Vec::new(),
            requests: None,
        })
    }
}

/// A resource being described, before anyone can see it.
pub struct Describing {
    provider: Provider,
    name: ResourceName,
    kind: String,
    topics: Vec<String>,
    actions: Vec<String>,
    labels: Vec<(String, String)>,
    requests: Option<Registration>,
}

impl fmt::Debug for Describing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Describing({})", self.name)
    }
}

impl Describing {
    /// Says something else about the resource: where it is mounted, its serial number.
    #[must_use]
    pub fn label(mut self, key: &str, value: &str) -> Self {
        self.labels.push((key.to_owned(), value.to_owned()));
        self
    }

    /// Declares a topic of the resource, `<resource>.<name>`, and returns its publisher. Its
    /// values never go stale, and a subscriber sent samples as messages starts from now: for
    /// anything else, [`Describing::topic_with`].
    ///
    /// # Errors
    ///
    /// As [`Topics::declare`].
    pub fn topic(
        &mut self,
        name: &str,
        fields: &[Field<'_>],
        history: u32,
        max_payload: u32,
    ) -> Result<Publisher, TopicError> {
        self.topic_with(&TopicDecl {
            name,
            fields,
            history,
            expiry_ms: 0,
            max_payload,
            on_subscribe: OnSubscribe::FromNow,
        })
    }

    /// Declares a topic of the resource as `topic` says, named `<resource>.<topic.name>`, and
    /// returns its publisher. A value that changes rarely, a fan's mode say, is declared with
    /// [`OnSubscribe::Latest`], so a subscriber on another board or in a browser is given it
    /// when it starts instead of waiting for it to change.
    ///
    /// # Errors
    ///
    /// As [`Topics::declare`]: a node too old to give a late subscriber the newest values
    /// refuses [`OnSubscribe::Latest`] rather than quietly starting it from now.
    pub fn topic_with(&mut self, topic: &TopicDecl<'_>) -> Result<Publisher, TopicError> {
        let full = format!("{}.{}", self.name, topic.name);
        let publisher = self.provider.topics.declare(&TopicDecl {
            name: &full,
            ..*topic
        })?;
        self.topics.push(full);
        Ok(publisher)
    }

    /// Registers an action of the resource, `<resource>.<name>`, with who may run it. Its
    /// runs arrive at [`Resource::requests`].
    ///
    /// # Errors
    ///
    /// As [`Actions::register`].
    pub fn action(
        &mut self,
        name: &str,
        description: &str,
        policy: Policy,
    ) -> Result<(), ClientError> {
        let full = format!("{}.{name}", self.name);
        let registration = self.provider.actions.register(
            &ActionDecl {
                name: &full,
                target: Target::Resource(self.name.clone()),
                description,
            },
            policy,
        )?;
        self.requests = Some(registration);
        self.actions.push(full);
        Ok(())
    }

    /// Writes the resource down, in `state`, with everything declared for it. From now on it
    /// can be found.
    ///
    /// # Errors
    ///
    /// If the store refuses the record, or the connection broke.
    pub fn publish(self, state: ResourceState) -> Result<Resource, ClientError> {
        let mut resource = Resource {
            provider: self.provider,
            name: self.name,
            kind: self.kind,
            topics: self.topics,
            actions: self.actions,
            labels: self.labels,
            requests: self.requests,
        };
        resource.write(state, false)?;
        Ok(resource)
    }
}

/// A resource that has been written down.
pub struct Resource {
    provider: Provider,
    name: ResourceName,
    kind: String,
    topics: Vec<String>,
    actions: Vec<String>,
    labels: Vec<(String, String)>,
    requests: Option<Registration>,
}

impl fmt::Debug for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Resource({})", self.name)
    }
}

impl Resource {
    fn write(&mut self, state: ResourceState, deleted: bool) -> Result<(), ClientError> {
        let topics: Vec<Offered<'_>> = self.topics.iter().map(|name| Offered { name }).collect();
        let actions: Vec<Offered<'_>> = self.actions.iter().map(|name| Offered { name }).collect();
        let labels: Vec<Label<'_>> = self
            .labels
            .iter()
            .map(|(key, value)| Label { key, value })
            .collect();
        let description = ResourceDescription {
            topics: List::new(&topics),
            actions: List::new(&actions),
            labels: List::new(&labels),
        };
        let too_long = |_| ClientError::TooLarge;
        let mut attributes = vec![0_u8; 4096];
        let len = encode_fields(&description, &mut attributes).map_err(too_long)?;
        let record = ResourceRecord {
            name: self.name.clone(),
            kind: &self.kind,
            node: self.provider.node.clone(),
            state,
            attributes: Bytes(&attributes[..len]),
        };
        let mut value = vec![0_u8; len + 512];
        let len = encode_fields(&record, &mut value).map_err(too_long)?;
        self.provider.records.put(&[RecordEntry {
            class: RecordClass::Resource,
            key: self.name.as_str(),
            stamp: HlcTimestamp::ZERO,
            deleted,
            value: Bytes(&value[..len]),
        }])?;
        Ok(())
    }

    /// The resource's name.
    #[must_use]
    pub const fn name(&self) -> &ResourceName {
        &self.name
    }

    /// Says whether the resource can be used now.
    ///
    /// # Errors
    ///
    /// If the store refuses the record, or the connection broke.
    pub fn set_state(&mut self, state: ResourceState) -> Result<(), ClientError> {
        self.write(state, false)
    }

    /// Where runs of the resource's actions arrive. `None` if it has no actions.
    pub fn requests(&mut self) -> Option<&mut Registration> {
        self.requests.as_mut()
    }

    /// Says the resource is gone, and withdraws its topics: their subscribers are told the
    /// publisher is gone, and they are not declared again after a reconnect. Its actions stay
    /// registered until their registration is dropped.
    ///
    /// # Errors
    ///
    /// If the comm core or the store refuses, or the connection broke.
    pub fn withdraw(mut self) -> Result<(), ClientError> {
        for topic in &self.topics {
            self.provider.topics.withdraw(topic)?;
        }
        self.write(ResourceState::Lost, true)
    }
}

/// A resource as anyone finds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    /// Its name.
    pub name: String,
    /// Its kind, in its provider's words.
    pub kind: String,
    /// The node it is on.
    pub node: String,
    /// Whether it can be used.
    pub state: ResourceState,
    /// Its topics, by full name. Subscribe to one and its declaration says what is in it.
    pub topics: Vec<String>,
    /// Its actions, by full name.
    pub actions: Vec<String>,
    /// Whatever else its provider said about it.
    pub labels: Vec<(String, String)>,
    /// Why what its provider wrote about it does not decode, if it does not: then its
    /// topics, actions and labels are empty because they could not be read, not because it
    /// has none. `None` for a resource with no description, and for one that decodes.
    pub broken: Option<String>,
}

/// Reads a resource's record. `None` if it is not one, or is one that is gone. A resource
/// whose provider wrote nothing this build understands about what it offers is still found,
/// with no topics and no actions, and [`Found::broken`] saying why.
#[must_use]
pub fn describe(record: &Record) -> Option<Found> {
    if record.class != RecordClass::Resource || record.deleted {
        return None;
    }
    let resource = ResourceRecord::decode(&record.value).ok()?;
    let names = |list: List<'_, Offered<'_>>| {
        list.iter()
            .flatten()
            .map(|offered| offered.name.to_owned())
            .collect()
    };
    let mut broken = None;
    let (topics, actions, labels) = match ResourceDescription::decode(resource.attributes.0) {
        // Nothing written is no description, not a broken one.
        _ if resource.attributes.0.is_empty() => (Vec::new(), Vec::new(), Vec::new()),
        Ok(description) => (
            names(description.topics),
            names(description.actions),
            description
                .labels
                .iter()
                .flatten()
                .map(|label| (label.key.to_owned(), label.value.to_owned()))
                .collect(),
        ),
        Err(error) => {
            broken = Some(format!("description does not decode: {error}"));
            (Vec::new(), Vec::new(), Vec::new())
        }
    };
    Some(Found {
        name: resource.name.to_string(),
        kind: resource.kind.to_owned(),
        node: resource.node.to_string(),
        state: resource.state,
        topics,
        actions,
        labels,
        broken,
    })
}

/// Every resource whose name starts with `prefix`, with what each offers. An empty prefix is
/// all of them.
///
/// # Errors
///
/// If the store refuses, or the connection broke.
pub fn resources(records: &Records, prefix: &str) -> Result<Vec<Found>, ClientError> {
    Ok(records
        .get(RecordClass::Resource, prefix)?
        .iter()
        .filter_map(describe)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_msg_records::ResourceState;

    fn record(attributes: &[u8]) -> Record {
        let resource = ResourceRecord {
            name: "arm".parse().expect("name"),
            kind: "servo",
            node: "board".parse().expect("node"),
            state: ResourceState::Ready,
            attributes: Bytes(attributes),
        };
        let mut value = vec![0_u8; 512];
        let len = encode_fields(&resource, &mut value).expect("encode");
        value.truncate(len);
        Record {
            class: RecordClass::Resource,
            key: "arm".into(),
            stamp: HlcTimestamp::default(),
            deleted: false,
            value,
        }
    }

    #[test]
    fn a_description_that_does_not_decode_is_told_from_none() {
        let none = describe(&record(&[])).expect("found");
        assert_eq!(none.broken, None);
        assert!(none.topics.is_empty() && none.labels.is_empty());

        let broken = describe(&record(&[0xff; 7])).expect("found");
        let why = broken.broken.as_deref().expect("broken");
        assert!(why.starts_with("description does not decode: "), "{why}");
        assert!(broken.topics.is_empty() && broken.actions.is_empty());

        let mut attributes = vec![0_u8; 256];
        let description = ResourceDescription {
            topics: List::new(&[]),
            actions: List::new(&[]),
            labels: List::new(&[Label {
                key: "bus",
                value: "can0",
            }]),
        };
        let len = encode_fields(&description, &mut attributes).expect("encode");
        let read = describe(&record(&attributes[..len])).expect("found");
        assert_eq!(read.broken, None);
        assert_eq!(read.labels, [("bus".to_owned(), "can0".to_owned())]);
    }
}
