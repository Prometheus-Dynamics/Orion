//! The in-memory latest-value store behind the volatile status lane.
//!
//! Pure data structure: no locks, no clock, no I/O. Callers pass the current Unix time in
//! milliseconds. Memory is bounded by the node-wide and per-publisher entry caps plus the fixed
//! per-entry key and value size limits.

use orion::control_plane::{StatusEntry, StatusKey, StatusQuery, TypedConfigValue};
use std::collections::{BTreeMap, BTreeSet};

/// Longest status key, in bytes.
pub(crate) const MAX_STATUS_KEY_BYTES: usize = 128;
/// Longest string or byte value, in bytes.
pub(crate) const MAX_STATUS_VALUE_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StatusLimits {
    pub(crate) max_entries: usize,
    pub(crate) max_entries_per_publisher: usize,
    pub(crate) max_ttl_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StatusPublishError {
    Invalid(String),
    NodeFull { needed: usize, max: usize },
    PublisherFull { needed: usize, max: usize },
}

impl std::fmt::Display for StatusPublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid status entry: {reason}"),
            Self::NodeFull { needed, max } => write!(
                f,
                "status lane is full: batch needs {needed} entries, node cap is {max}"
            ),
            Self::PublisherFull { needed, max } => write!(
                f,
                "publisher status cap reached: batch needs {needed} entries, cap is {max}"
            ),
        }
    }
}

#[derive(Debug)]
struct Stored {
    entry: StatusEntry,
    publisher: String,
}

/// Result of an accepted publish.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PublishOutcome {
    /// Entries whose value is new or changed (watchers are told about these).
    pub(crate) changed: Vec<StatusEntry>,
    /// Entries accepted in total (including TTL refreshes of unchanged values).
    pub(crate) accepted: usize,
}

#[derive(Debug, Default)]
pub(crate) struct StatusStore {
    entries: BTreeMap<StatusKey, Stored>,
    per_publisher: BTreeMap<String, usize>,
    expiries: BTreeSet<(u64, StatusKey)>,
}

impl StatusStore {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn publishers(&self) -> usize {
        self.per_publisher.len()
    }

    /// Earliest expiry time, if any entry is held.
    pub(crate) fn next_expiry_ms(&self) -> Option<u64> {
        self.expiries.first().map(|(at, _)| *at)
    }

    /// Applies a batch atomically: either every entry is stored or none is.
    pub(crate) fn publish(
        &mut self,
        publisher: &str,
        entries: Vec<StatusEntry>,
        now_ms: u64,
        limits: StatusLimits,
    ) -> Result<PublishOutcome, StatusPublishError> {
        // Later entries for the same key win within one batch.
        let mut batch: BTreeMap<StatusKey, StatusEntry> = BTreeMap::new();
        for entry in entries {
            validate(&entry)?;
            batch.insert(entry.status_key(), entry);
        }
        let new_global = batch
            .keys()
            .filter(|key| !self.entries.contains_key(*key))
            .count();
        let needed = self.entries.len().saturating_add(new_global);
        if needed > limits.max_entries {
            return Err(StatusPublishError::NodeFull {
                needed,
                max: limits.max_entries,
            });
        }
        let new_for_publisher = batch
            .keys()
            .filter(|key| {
                self.entries
                    .get(*key)
                    .is_none_or(|stored| stored.publisher != publisher)
            })
            .count();
        let held = self.per_publisher.get(publisher).copied().unwrap_or(0);
        let needed = held.saturating_add(new_for_publisher);
        if needed > limits.max_entries_per_publisher {
            return Err(StatusPublishError::PublisherFull {
                needed,
                max: limits.max_entries_per_publisher,
            });
        }

        let mut outcome = PublishOutcome::default();
        for (key, mut entry) in batch {
            entry.ttl_ms = if entry.ttl_ms == 0 {
                limits.max_ttl_ms
            } else {
                entry.ttl_ms.min(limits.max_ttl_ms)
            };
            entry.published_at_ms = now_ms;
            let changed = match self.remove(&key) {
                Some(previous) => previous.entry.value != entry.value,
                None => true,
            };
            self.expiries.insert((entry.expires_at_ms(), key.clone()));
            *self.per_publisher.entry(publisher.to_owned()).or_insert(0) += 1;
            if changed {
                outcome.changed.push(entry.clone());
            }
            outcome.accepted += 1;
            self.entries.insert(
                key,
                Stored {
                    entry,
                    publisher: publisher.to_owned(),
                },
            );
        }
        Ok(outcome)
    }

    /// Drops every entry whose TTL ran out at `now_ms` and returns their keys.
    pub(crate) fn expire(&mut self, now_ms: u64) -> Vec<StatusKey> {
        let mut expired = Vec::new();
        while let Some((at, key)) = self.expiries.first().cloned() {
            if at > now_ms {
                break;
            }
            self.remove(&key);
            expired.push(key);
        }
        expired
    }

    /// Live entries matching `query`, ordered by subject then key.
    pub(crate) fn query(&self, query: &StatusQuery, now_ms: u64) -> Vec<StatusEntry> {
        self.entries
            .iter()
            .filter(|(key, stored)| {
                stored.entry.expires_at_ms() > now_ms && query.matches(&key.subject, &key.key)
            })
            .map(|(_, stored)| stored.entry.clone())
            .collect()
    }

    fn remove(&mut self, key: &StatusKey) -> Option<Stored> {
        let stored = self.entries.remove(key)?;
        self.expiries
            .remove(&(stored.entry.expires_at_ms(), key.clone()));
        if let Some(count) = self.per_publisher.get_mut(&stored.publisher) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.per_publisher.remove(&stored.publisher);
            }
        }
        Some(stored)
    }
}

fn validate(entry: &StatusEntry) -> Result<(), StatusPublishError> {
    if entry.key.trim().is_empty() {
        return Err(StatusPublishError::Invalid("empty key".into()));
    }
    if entry.key.len() > MAX_STATUS_KEY_BYTES {
        return Err(StatusPublishError::Invalid(format!(
            "key `{}...` is longer than {MAX_STATUS_KEY_BYTES} bytes",
            entry.key.chars().take(16).collect::<String>()
        )));
    }
    let value_len = match &entry.value {
        TypedConfigValue::String(value) => value.len(),
        TypedConfigValue::Bytes(value) => value.len(),
        TypedConfigValue::Bool(_) | TypedConfigValue::Int(_) | TypedConfigValue::UInt(_) => 0,
    };
    if value_len > MAX_STATUS_VALUE_BYTES {
        return Err(StatusPublishError::Invalid(format!(
            "value of `{}` is {value_len} bytes, limit is {MAX_STATUS_VALUE_BYTES}",
            entry.key
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion::{ProviderId, control_plane::StatusSubject};

    const LIMITS: StatusLimits = StatusLimits {
        max_entries: 4,
        max_entries_per_publisher: 3,
        max_ttl_ms: 1_000,
    };

    fn entry(provider: &str, key: &str, value: u64) -> StatusEntry {
        StatusEntry::new(
            StatusSubject::Provider(ProviderId::new(provider)),
            key,
            TypedConfigValue::UInt(value),
        )
    }

    #[test]
    fn latest_value_wins_and_unchanged_values_only_refresh() {
        let mut store = StatusStore::default();
        let first = store
            .publish("a", vec![entry("p", "fps", 30)], 10, LIMITS)
            .expect("publish");
        assert_eq!(first.changed.len(), 1);
        assert_eq!(first.changed[0].ttl_ms, 1_000);
        assert_eq!(first.changed[0].published_at_ms, 10);

        let same = store
            .publish("a", vec![entry("p", "fps", 30)], 20, LIMITS)
            .expect("publish");
        assert!(same.changed.is_empty());
        assert_eq!(same.accepted, 1);

        let newer = store
            .publish(
                "a",
                vec![entry("p", "fps", 29).with_ttl_ms(5_000)],
                30,
                LIMITS,
            )
            .expect("publish");
        assert_eq!(newer.changed.len(), 1);
        let all = store.query(&StatusQuery::all(), 30);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].value, TypedConfigValue::UInt(29));
        assert_eq!(all[0].ttl_ms, 1_000, "TTL is capped at the node maximum");
    }

    #[test]
    fn entries_expire_after_their_ttl() {
        let mut store = StatusStore::default();
        store
            .publish(
                "a",
                vec![
                    entry("p", "short", 1).with_ttl_ms(100),
                    entry("p", "long", 2),
                ],
                0,
                LIMITS,
            )
            .expect("publish");
        assert_eq!(store.next_expiry_ms(), Some(100));
        assert!(store.expire(99).is_empty());
        assert_eq!(store.query(&StatusQuery::all(), 100).len(), 1);
        let expired = store.expire(100);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key, "short");
        assert_eq!(store.len(), 1);
        assert_eq!(store.next_expiry_ms(), Some(1_000));
        assert_eq!(store.expire(5_000).len(), 1);
        assert_eq!(store.len(), 0);
        assert_eq!(store.publishers(), 0);
    }

    #[test]
    fn caps_reject_whole_batches() {
        let mut store = StatusStore::default();
        let batch = |publisher: &str, keys: &[&str]| {
            keys.iter()
                .map(|key| entry(publisher, key, 1))
                .collect::<Vec<_>>()
        };
        store
            .publish("a", batch("a", &["1", "2", "3"]), 0, LIMITS)
            .expect("three entries fit the publisher cap");
        assert!(matches!(
            store.publish("a", batch("a", &["4"]), 0, LIMITS),
            Err(StatusPublishError::PublisherFull { needed: 4, max: 3 })
        ));
        // Updating existing keys never needs new capacity.
        store
            .publish("a", batch("a", &["1", "2", "3"]), 0, LIMITS)
            .expect("updates fit");
        store
            .publish("b", batch("b", &["1"]), 0, LIMITS)
            .expect("fourth entry fits the node cap");
        assert!(matches!(
            store.publish("b", batch("b", &["2"]), 0, LIMITS),
            Err(StatusPublishError::NodeFull { needed: 5, max: 4 })
        ));
        assert_eq!(store.len(), 4);
    }

    #[test]
    fn invalid_entries_reject_the_batch() {
        let mut store = StatusStore::default();
        let long_key = "k".repeat(MAX_STATUS_KEY_BYTES + 1);
        assert!(matches!(
            store.publish(
                "a",
                vec![entry("p", "ok", 1), entry("p", &long_key, 1)],
                0,
                LIMITS
            ),
            Err(StatusPublishError::Invalid(_))
        ));
        let big = StatusEntry::new(
            StatusSubject::Provider(ProviderId::new("p")),
            "blob",
            TypedConfigValue::Bytes(vec![0; MAX_STATUS_VALUE_BYTES + 1]),
        );
        assert!(store.publish("a", vec![big], 0, LIMITS).is_err());
        assert_eq!(store.len(), 0);
    }
}
