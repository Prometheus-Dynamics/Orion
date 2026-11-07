//! The in-memory latest-value store behind the volatile status lane.
//!
//! Pure data structure: no locks, no clock, no I/O. Callers pass the current Unix time in
//! milliseconds. Memory is bounded by the node-wide and per-publisher entry caps plus the fixed
//! per-entry key and value size limits.

use orion::control_plane::{StatusEntry, StatusKey, StatusQuery, TypedConfigValue};
use std::collections::BTreeMap;

/// Longest status key, in bytes.
pub(crate) const MAX_STATUS_KEY_BYTES: usize = 128;
/// Hard ceiling for string or byte values, in bytes. `ORION_NODE_STATUS_MAX_VALUE_BYTES` cannot
/// raise the cap above it (64 KiB, one SPI transaction, like `Bytes` action values).
pub(crate) const STATUS_VALUE_CEILING_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StatusLimits {
    pub(crate) max_entries: usize,
    pub(crate) max_entries_per_publisher: usize,
    pub(crate) max_ttl_ms: u64,
    /// Longest string or byte value accepted.
    pub(crate) max_value_bytes: usize,
    /// Bytes of string and byte values held node-wide (the memory bound behind the entry caps).
    pub(crate) max_total_value_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StatusPublishError {
    Invalid(String),
    NodeFull { needed: usize, max: usize },
    PublisherFull { needed: usize, max: usize },
    ValueBytesFull { needed: usize, max: usize },
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
            Self::ValueBytesFull { needed, max } => write!(
                f,
                "status value budget reached: batch needs {needed} value bytes, budget is {max}"
            ),
        }
    }
}

#[derive(Debug)]
struct Stored {
    entry: StatusEntry,
    publisher: String,
    /// The entry's place in `expiries`.
    expiry: (u64, u64),
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
    /// `(expires_at_ms, id)` to key: removing an entry's expiry needs no copy of its key.
    expiries: BTreeMap<(u64, u64), StatusKey>,
    next_expiry_id: u64,
    /// Sum of the value sizes of every held entry (kept exact by `remove` and `publish`).
    value_bytes: usize,
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
        self.expiries.first_key_value().map(|((at, _), _)| *at)
    }

    /// Applies a batch atomically: either every entry is stored or none is.
    pub(crate) fn publish(
        &mut self,
        publisher: &str,
        entries: Vec<StatusEntry>,
        now_ms: u64,
        limits: StatusLimits,
    ) -> Result<PublishOutcome, StatusPublishError> {
        // Later entries for the same key win within one batch. One vector, sorted by key (a stable
        // sort keeps the batch order within a key, so the last of each run wins), instead of a map
        // node per entry: the common batch is one entry.
        let mut batch: Vec<(StatusKey, StatusEntry)> = Vec::with_capacity(entries.len());
        for entry in entries {
            validate(&entry, limits.max_value_bytes)?;
            batch.push((entry.status_key(), entry));
        }
        if batch.len() > 1 {
            batch.sort_by(|a, b| a.0.cmp(&b.0));
            let mut kept: Vec<(StatusKey, StatusEntry)> = Vec::with_capacity(batch.len());
            for item in batch {
                match kept.last_mut() {
                    Some(last) if last.0 == item.0 => *last = item,
                    _ => kept.push(item),
                }
            }
            batch = kept;
        }
        let keys = || batch.iter().map(|(key, _)| key);
        let new_global = keys()
            .filter(|key| !self.entries.contains_key(*key))
            .count();
        let needed = self.entries.len().saturating_add(new_global);
        if needed > limits.max_entries {
            return Err(StatusPublishError::NodeFull {
                needed,
                max: limits.max_entries,
            });
        }
        let new_for_publisher = keys()
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

        let batch_value_bytes: usize = batch.iter().map(|(_, e)| value_len(&e.value)).sum();
        let replaced_value_bytes: usize = keys()
            .filter_map(|key| self.entries.get(key))
            .map(|stored| value_len(&stored.entry.value))
            .sum();
        let needed_value_bytes = self
            .value_bytes
            .saturating_sub(replaced_value_bytes)
            .saturating_add(batch_value_bytes);
        if needed_value_bytes > limits.max_total_value_bytes {
            return Err(StatusPublishError::ValueBytesFull {
                needed: needed_value_bytes,
                max: limits.max_total_value_bytes,
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
            let previous = self.take(&key);
            let changed = previous
                .as_ref()
                .is_none_or(|previous| previous.entry.value != entry.value);
            let owner = match previous {
                // The same publisher replacing its entry: its count and name stay, no new string.
                Some(previous) if previous.publisher == publisher => previous.publisher,
                Some(previous) => {
                    self.release(&previous.publisher);
                    self.count(publisher)
                }
                None => self.count(publisher),
            };
            self.next_expiry_id = self.next_expiry_id.wrapping_add(1);
            let expiry = (entry.expires_at_ms(), self.next_expiry_id);
            self.expiries.insert(expiry, key.clone());
            self.value_bytes = self.value_bytes.saturating_add(value_len(&entry.value));
            if changed {
                outcome.changed.push(entry.clone());
            }
            outcome.accepted += 1;
            self.entries.insert(
                key,
                Stored {
                    entry,
                    publisher: owner,
                    expiry,
                },
            );
        }
        Ok(outcome)
    }

    /// Drops every entry whose TTL ran out at `now_ms` and returns their keys.
    pub(crate) fn expire(&mut self, now_ms: u64) -> Vec<StatusKey> {
        let mut expired = Vec::new();
        while let Some(entry) = self.expiries.first_entry() {
            if entry.key().0 > now_ms {
                break;
            }
            let key = entry.remove();
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
        let stored = self.take(key)?;
        self.release(&stored.publisher);
        Some(stored)
    }

    /// Removes the entry and its expiry and value bytes, but not its publisher's count.
    fn take(&mut self, key: &StatusKey) -> Option<Stored> {
        let stored = self.entries.remove(key)?;
        self.expiries.remove(&stored.expiry);
        self.value_bytes = self
            .value_bytes
            .saturating_sub(value_len(&stored.entry.value));
        Some(stored)
    }

    /// One entry more for `publisher`; returns its name for the stored entry.
    fn count(&mut self, publisher: &str) -> String {
        match self.per_publisher.get_mut(publisher) {
            Some(count) => *count += 1,
            None => {
                self.per_publisher.insert(publisher.to_owned(), 1);
            }
        }
        publisher.to_owned()
    }

    /// One entry less for `publisher`.
    fn release(&mut self, publisher: &str) {
        if let Some(count) = self.per_publisher.get_mut(publisher) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.per_publisher.remove(publisher);
            }
        }
    }
}

/// Size of a value's payload: string and byte values count their bytes, scalars count nothing.
fn value_len(value: &TypedConfigValue) -> usize {
    match value {
        TypedConfigValue::String(value) => value.len(),
        TypedConfigValue::Bytes(value) => value.len(),
        TypedConfigValue::Bool(_)
        | TypedConfigValue::Int(_)
        | TypedConfigValue::UInt(_)
        | TypedConfigValue::F64(_) => 0,
    }
}

fn validate(entry: &StatusEntry, max_value_bytes: usize) -> Result<(), StatusPublishError> {
    if entry.key.trim().is_empty() {
        return Err(StatusPublishError::Invalid("empty key".into()));
    }
    if entry.key.len() > MAX_STATUS_KEY_BYTES {
        return Err(StatusPublishError::Invalid(format!(
            "key `{}...` is longer than {MAX_STATUS_KEY_BYTES} bytes",
            entry.key.chars().take(16).collect::<String>()
        )));
    }
    let len = value_len(&entry.value);
    if len > max_value_bytes {
        return Err(StatusPublishError::Invalid(format!(
            "value of `{}` is {len} bytes, limit is {max_value_bytes}",
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
        max_value_bytes: 1024,
        max_total_value_bytes: 4096,
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
            TypedConfigValue::Bytes(vec![0; LIMITS.max_value_bytes + 1]),
        );
        assert!(store.publish("a", vec![big], 0, LIMITS).is_err());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn value_budget_is_exact_and_refreshes_free_it() {
        let limits = StatusLimits {
            max_entries: 16,
            max_entries_per_publisher: 16,
            max_ttl_ms: 1_000,
            max_value_bytes: 1024,
            max_total_value_bytes: 100,
        };
        let text = |key: &str, len: usize| {
            StatusEntry::new(
                StatusSubject::Provider(ProviderId::new("p")),
                key,
                TypedConfigValue::String("x".repeat(len)),
            )
        };
        let mut store = StatusStore::default();
        store
            .publish("a", vec![text("a", 60)], 0, limits)
            .expect("60 of 100 bytes");
        // 60 + 50 is over budget: the whole batch is refused and nothing changes.
        assert!(matches!(
            store.publish("a", vec![text("b", 50)], 0, limits),
            Err(StatusPublishError::ValueBytesFull {
                needed: 110,
                max: 100
            })
        ));
        assert_eq!(store.len(), 1);
        // Replacing the held value frees its bytes first: 30 (replacement) fits.
        store
            .publish("a", vec![text("a", 30)], 0, limits)
            .expect("replacement needs only its own bytes");
        store
            .publish("a", vec![text("b", 70)], 0, limits)
            .expect("30 + 70 fits exactly");
        // Expiry releases the bytes too.
        assert_eq!(store.expire(2_000).len(), 2);
        store
            .publish("a", vec![text("c", 100)], 2_000, limits)
            .expect("expired entries no longer count");
    }
}
