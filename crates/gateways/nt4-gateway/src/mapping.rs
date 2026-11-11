//! Which topics go to NT4 and which come back, and under what names and types. All of it is
//! configuration: nothing here knows what any name means.

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use orion_config::{ConfigError, Source};
use orion_msg_topics::FieldType;
use orion_types::TopicName;

/// One field of one topic and the NT4 topic it is: `topic:field=type@name`.
///
/// `type` is the NT4 type string that is announced, such as `double`, `int[]` or
/// `struct:Pose2d`. `name` is the NT4 topic's name and runs to the end, so it may hold
/// anything but a comma.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The topic here.
    pub topic: TopicName,
    /// Its field, by name.
    pub field: String,
    /// The NT4 type string.
    pub nt_type: String,
    /// The NT4 topic's name.
    pub nt_name: String,
}

impl FromStr for Entry {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let shape = || format!("{text:?} is not topic:field=type@name");
        let (ours, theirs) = text.split_once('=').ok_or_else(shape)?;
        let (topic, field) = ours.split_once(':').ok_or_else(shape)?;
        let (nt_type, nt_name) = theirs.split_once('@').ok_or_else(shape)?;
        let (field, nt_type, nt_name) = (field.trim(), nt_type.trim(), nt_name.trim());
        if field.is_empty() || nt_name.is_empty() {
            return Err(shape());
        }
        if orion_nt4::type_id_for_name(nt_type).is_none() {
            return Err(format!("{nt_type:?} is not an NT4 type"));
        }
        Ok(Self {
            topic: topic
                .trim()
                .parse()
                .map_err(|_| format!("{topic:?} is not a topic name"))?,
            field: field.to_owned(),
            nt_type: nt_type.to_owned(),
            nt_name: nt_name.to_owned(),
        })
    }
}

/// The type a field has here when its values come from an NT4 topic of type `nt_type`.
/// `None` for the two NT4 types that have no like here: `boolean[]` and `string[]`.
#[must_use]
pub fn field_type(nt_type: &str) -> Option<FieldType> {
    use orion_nt4::value as nt;
    Some(match orion_nt4::type_id_for_name(nt_type)? {
        nt::BOOLEAN => FieldType::Bool,
        nt::INT => FieldType::Int,
        nt::DOUBLE | nt::FLOAT => FieldType::Float,
        nt::STRING => FieldType::Text,
        nt::RAW => FieldType::Bytes,
        nt::DOUBLE_ARRAY | nt::FLOAT_ARRAY => FieldType::FloatArray,
        nt::INT_ARRAY => FieldType::IntArray,
        _ => return None,
    })
}

/// The mapping, and how often it is looked at: the same for both forms of the gateway.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mapping {
    /// `nt4.period_ms`: how often topics are read for NT4. NT4 carries the latest value, so
    /// what was published and replaced within one period is never sent.
    pub period: Duration,
    /// `nt4.publish`: the fields that go to NT4.
    pub publish: Vec<Entry>,
    /// `nt4.accept`: the NT4 topics whose values are published here.
    pub accept: Vec<Entry>,
    /// `nt4.max_payload`: the longest text, bytes or array taken from NT4, in bytes.
    pub max_payload: u32,
}

/// What the server form of the gateway is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Nt4Config {
    /// `nt4.listen`: where NT4 clients connect.
    pub listen: SocketAddr,
    /// The mapping.
    pub mapping: Mapping,
}

impl Nt4Config {
    /// The gateway's settings, or `None` if `nt4.listen` is not set: no gateway of this
    /// form.
    ///
    /// # Errors
    ///
    /// As [`Mapping::from_source`].
    pub fn from_source(source: &mut Source) -> Result<Option<Self>, ConfigError> {
        let listen: Option<SocketAddr> = source.optional("nt4.listen")?;
        let mapping = Mapping::from_source(source)?;
        Ok(listen.map(|listen| Self { listen, mapping }))
    }
}

impl Mapping {
    /// The mapping as the settings give it.
    ///
    /// # Errors
    ///
    /// An entry that does not parse, an NT4 name used twice, or an accepted type that has
    /// no like here.
    pub fn from_source(source: &mut Source) -> Result<Self, ConfigError> {
        let mapping = Self {
            period: Duration::from_millis(source.get("nt4.period_ms", 20_u64)?.max(1)),
            publish: source.list("nt4.publish")?,
            accept: source.list("nt4.accept")?,
            max_payload: source.get("nt4.max_payload", 1024_u32)?,
        };
        mapping.check()?;
        Ok(mapping)
    }

    /// Whether the mapping holds together.
    ///
    /// # Errors
    ///
    /// As [`Mapping::from_source`].
    pub fn check(&self) -> Result<(), ConfigError> {
        let wrong = |key: &str, message: String| ConfigError {
            key: key.into(),
            message,
        };
        let mut names: Vec<&str> = Vec::new();
        for entry in self.publish.iter().chain(&self.accept) {
            if names.contains(&entry.nt_name.as_str()) {
                return Err(wrong(
                    "nt4.publish",
                    format!("the NT4 topic {:?} is mapped twice", entry.nt_name),
                ));
            }
            names.push(&entry.nt_name);
        }
        for (at, entry) in self.accept.iter().enumerate() {
            if field_type(&entry.nt_type).is_none() {
                return Err(wrong(
                    "nt4.accept",
                    format!("{:?} has no type here to be published as", entry.nt_type),
                ));
            }
            let again = self.accept[..at]
                .iter()
                .any(|other| other.topic == entry.topic && other.field == entry.field);
            if again {
                return Err(wrong(
                    "nt4.accept",
                    format!(
                        "{}:{} is written by two NT4 topics",
                        entry.topic, entry.field
                    ),
                ));
            }
            // A topic that comes from NT4 is the gateway's to publish: it cannot also be
            // one that goes there.
            if self.publish.iter().any(|other| other.topic == entry.topic) {
                return Err(wrong(
                    "nt4.accept",
                    format!("{} both goes to NT4 and comes from it", entry.topic),
                ));
            }
        }
        Ok(())
    }
}
