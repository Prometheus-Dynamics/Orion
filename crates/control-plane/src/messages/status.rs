//! Volatile status lane: latest value per `(subject, key)`, held in node memory only.
//!
//! Status entries are small typed values (temperature, frame rate, queue depth, last error) that
//! change too often to be worth a durable write and are worthless after a restart. A node keeps
//! the newest value per `(subject, key)` until its time-to-live runs out; nothing is persisted or
//! replicated to peers. Durable facts (existence, health, availability, workload phase) stay in
//! the observed records.

use crate::TypedConfigValue;
use alloc::{string::String, vec::Vec};
use core::{fmt, str::FromStr};
use orion_core::{ExecutorId, NodeId, ProviderId, ResourceId, WorkloadId};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// What a status entry describes. Publishers may only publish for subjects they own: their
/// provider or executor, the resources of their provider (or realized by their executor), and the
/// workloads their executor runs. [`StatusSubject::Node`] entries are published by the node
/// itself (host metrics, see `docs/host-facts.md`); clients cannot publish them.
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
pub enum StatusSubject {
    Provider(ProviderId),
    Executor(ExecutorId),
    Resource(ResourceId),
    Workload(WorkloadId),
    /// The node itself; only the node publishes for it.
    Node(NodeId),
}

impl StatusSubject {
    /// The subject kind as used in `kind/id` text form: `provider`, `executor`, `resource`,
    /// `workload`, or `node`.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Provider(_) => "provider",
            Self::Executor(_) => "executor",
            Self::Resource(_) => "resource",
            Self::Workload(_) => "workload",
            Self::Node(_) => "node",
        }
    }

    /// The subject's identifier.
    pub fn id(&self) -> &str {
        match self {
            Self::Provider(id) => id.as_str(),
            Self::Executor(id) => id.as_str(),
            Self::Resource(id) => id.as_str(),
            Self::Workload(id) => id.as_str(),
            Self::Node(id) => id.as_str(),
        }
    }
}

impl fmt::Display for StatusSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.kind_name(), self.id())
    }
}

/// Error for a malformed `kind/id` subject.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusSubjectParseError(pub String);

impl fmt::Display for StatusSubjectParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid status subject `{}`; expected provider/<id>, executor/<id>, resource/<id>, workload/<id>, or node/<id>",
            self.0
        )
    }
}

impl FromStr for StatusSubject {
    type Err = StatusSubjectParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let error = || StatusSubjectParseError(value.into());
        let (kind, id) = value.split_once('/').ok_or_else(error)?;
        if id.trim().is_empty() {
            return Err(error());
        }
        Ok(match kind {
            "provider" => Self::Provider(ProviderId::new(id)),
            "executor" => Self::Executor(ExecutorId::new(id)),
            "resource" => Self::Resource(ResourceId::new(id)),
            "workload" => Self::Workload(WorkloadId::new(id)),
            "node" => Self::Node(NodeId::new(id)),
            _ => return Err(error()),
        })
    }
}

/// One status value.
///
/// On publish, `ttl_ms` is the requested time-to-live (`0` asks for the node maximum; larger
/// values are capped at it) and `published_at_ms` is ignored. Entries returned by the node carry
/// the effective TTL and the node's receive time (Unix milliseconds).
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StatusEntry {
    pub subject: StatusSubject,
    pub key: String,
    pub value: TypedConfigValue,
    pub ttl_ms: u64,
    pub published_at_ms: u64,
}

impl StatusEntry {
    /// An entry with the node's maximum TTL.
    pub fn new(subject: StatusSubject, key: impl Into<String>, value: TypedConfigValue) -> Self {
        Self {
            subject,
            key: key.into(),
            value,
            ttl_ms: 0,
            published_at_ms: 0,
        }
    }

    /// Requests a time-to-live (capped by the node maximum).
    pub fn with_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.ttl_ms = ttl_ms;
        self
    }

    /// Unix milliseconds after which the node drops the entry.
    pub fn expires_at_ms(&self) -> u64 {
        self.published_at_ms.saturating_add(self.ttl_ms)
    }

    /// The entry's `(subject, key)` identity.
    pub fn status_key(&self) -> StatusKey {
        StatusKey {
            subject: self.subject.clone(),
            key: self.key.clone(),
        }
    }
}

/// Identity of a status entry.
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
pub struct StatusKey {
    pub subject: StatusSubject,
    pub key: String,
}

/// Selects status entries. Empty filters match everything.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct StatusQuery {
    /// Only entries of this subject.
    pub subject: Option<StatusSubject>,
    /// Only entries whose key starts with this prefix.
    pub key_prefix: Option<String>,
}

impl StatusQuery {
    /// Every entry.
    pub fn all() -> Self {
        Self::default()
    }

    /// Entries of one subject.
    pub fn subject(subject: StatusSubject) -> Self {
        Self {
            subject: Some(subject),
            key_prefix: None,
        }
    }

    /// Restricts the query to keys starting with `prefix`.
    pub fn with_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = Some(prefix.into());
        self
    }

    /// Whether `(subject, key)` matches.
    pub fn matches(&self, subject: &StatusSubject, key: &str) -> bool {
        self.subject.as_ref().is_none_or(|wanted| wanted == subject)
            && self
                .key_prefix
                .as_deref()
                .is_none_or(|prefix| key.starts_with(prefix))
    }
}

/// A coalesced status watch event: entries published (newest value per key) and entries that
/// expired since the watcher's previous event.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct StatusChange {
    /// `true` for the first event of a watch, which carries every matching entry.
    pub bootstrap: bool,
    pub updated: Vec<StatusEntry>,
    pub expired: Vec<StatusKey>,
}

impl StatusChange {
    pub fn is_empty(&self) -> bool {
        self.updated.is_empty() && self.expired.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subjects_round_trip_through_text() {
        for text in [
            "provider/provider.camera",
            "executor/executor.a",
            "resource/resource.camera/front",
            "workload/workload.pose",
            "node/node-a",
        ] {
            let subject: StatusSubject = text.parse().expect("subject parses");
            assert_eq!(alloc::format!("{subject}"), text);
        }
        assert!("camera".parse::<StatusSubject>().is_err());
        assert!("host/a".parse::<StatusSubject>().is_err());
        assert!("provider/ ".parse::<StatusSubject>().is_err());
    }

    #[test]
    fn query_filters_by_subject_and_key_prefix() {
        let camera = StatusSubject::Provider(ProviderId::new("provider.camera"));
        let other = StatusSubject::Provider(ProviderId::new("provider.other"));
        let query = StatusQuery::subject(camera.clone()).with_key_prefix("fps");
        assert!(query.matches(&camera, "fps.current"));
        assert!(!query.matches(&camera, "temperature"));
        assert!(!query.matches(&other, "fps.current"));
        assert!(StatusQuery::all().matches(&other, "anything"));
    }
}
