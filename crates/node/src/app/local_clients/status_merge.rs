//! Merging a status change into the one still queued for a watcher, in place: entries and expired
//! keys stay sorted by key, so a merge looks keys up instead of cloning them into a map.

use orion::control_plane::{StatusChange, StatusEntry, StatusSubject};

/// `change` as [`merge_status_change`] keeps it; as it is when it already is (the lane's changes
/// are).
pub(super) fn normalized(change: StatusChange) -> StatusChange {
    if is_normal_status_change(&change) {
        return change;
    }
    let mut normal = StatusChange {
        bootstrap: false,
        updated: Vec::new(),
        expired: Vec::new(),
    };
    merge_status_change(&mut normal, change);
    normal
}

fn entry_order(entry: &StatusEntry, subject: &StatusSubject, key: &str) -> std::cmp::Ordering {
    (&entry.subject, entry.key.as_str()).cmp(&(subject, key))
}

/// Entries and expired keys strictly sorted by key, and no key both updated and expired: what
/// [`merge_status_change`] makes.
fn is_normal_status_change(change: &StatusChange) -> bool {
    change
        .updated
        .is_sorted_by(|a, b| entry_order(a, &b.subject, &b.key) == std::cmp::Ordering::Less)
        && change.expired.is_sorted_by(|a, b| a < b)
        && change.expired.iter().all(|key| {
            change
                .updated
                .binary_search_by(|entry| entry_order(entry, &key.subject, &key.key))
                .is_err()
        })
}

/// Applies `change` to the normal `into`: expiries first, then updates (an entry both updated
/// and expired in `change` stays updated).
pub(super) fn merge_status_change(into: &mut StatusChange, change: StatusChange) {
    into.bootstrap |= change.bootstrap;
    for key in change.expired {
        if let Ok(index) = into
            .updated
            .binary_search_by(|entry| entry_order(entry, &key.subject, &key.key))
        {
            into.updated.remove(index);
        }
        if let Err(index) = into.expired.binary_search(&key) {
            into.expired.insert(index, key);
        }
    }
    for entry in change.updated {
        if let Ok(index) = into.expired.binary_search_by(|key| {
            (&key.subject, key.key.as_str()).cmp(&(&entry.subject, entry.key.as_str()))
        }) {
            into.expired.remove(index);
        }
        match into
            .updated
            .binary_search_by(|queued| entry_order(queued, &entry.subject, &entry.key))
        {
            Ok(index) => into.updated[index] = entry,
            Err(index) => into.updated.insert(index, entry),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion::ProviderId;
    use orion::control_plane::{StatusKey, TypedConfigValue};
    use std::collections::BTreeMap;

    fn entry(provider: &str, key: &str, value: u64) -> StatusEntry {
        StatusEntry::new(
            StatusSubject::Provider(ProviderId::new(provider)),
            key,
            TypedConfigValue::UInt(value),
        )
    }

    fn key(provider: &str, key: &str) -> StatusKey {
        entry(provider, key, 0).status_key()
    }

    /// What the map-based merge produced.
    fn reference(changes: &[StatusChange]) -> StatusChange {
        let mut updated = BTreeMap::new();
        let mut expired = BTreeMap::new();
        let mut bootstrap = false;
        for change in changes {
            bootstrap |= change.bootstrap;
            for key in &change.expired {
                updated.remove(key);
                expired.insert(key.clone(), ());
            }
            for entry in &change.updated {
                expired.remove(&entry.status_key());
                updated.insert(entry.status_key(), entry.clone());
            }
        }
        StatusChange {
            bootstrap,
            updated: updated.into_values().collect(),
            expired: expired.into_keys().collect(),
        }
    }

    #[test]
    fn merging_matches_the_map_merge() {
        let changes = [
            StatusChange {
                bootstrap: true,
                updated: vec![entry("b", "x", 1), entry("a", "y", 2), entry("a", "y", 3)],
                expired: vec![key("c", "z"), key("a", "y")],
            },
            StatusChange {
                bootstrap: false,
                updated: vec![entry("c", "z", 4), entry("a", "a", 5)],
                expired: vec![key("b", "x"), key("d", "q")],
            },
            StatusChange {
                bootstrap: false,
                updated: vec![entry("b", "x", 6)],
                expired: vec![key("a", "a")],
            },
        ];
        let mut merged = normalized(changes[0].clone());
        assert_eq!(merged, reference(&changes[..1]));
        for (n, change) in changes.iter().enumerate().skip(1) {
            merge_status_change(&mut merged, change.clone());
            assert_eq!(merged, reference(&changes[..=n]), "after change {n}");
        }
    }

    #[test]
    fn a_normal_change_is_kept_as_it_is() {
        let change = StatusChange {
            bootstrap: false,
            updated: vec![entry("a", "k", 1), entry("a", "l", 2)],
            expired: vec![key("b", "k")],
        };
        let pointer = change.updated.as_ptr();
        let kept = normalized(change);
        assert_eq!(kept.updated.as_ptr(), pointer);
    }
}
