//! The merge: the rules dev's tests held it to, and convergence under random histories.

use orion_msg_records::{RecordClass, RecordEntry};
use orion_store::{Change, Merged, Record, Scope, Store, StoreConfig, Version};
use orion_types::{HlcTimestamp, NodeName};
use orion_wire::Bytes;

const R: RecordClass = RecordClass::Resource;
const DAY: u64 = 24 * 60 * 60 * 1000;
/// A wall clock in the present.
const NOW: u64 = 1_800_000_000_000;

fn name(text: &str) -> NodeName {
    text.parse().expect("name")
}

fn store(node: &str) -> Store {
    Store::new(name(node), StoreConfig::default())
}

/// Sends every version `from` has to `to`, as peer sync does.
fn sync(from: &Store, to: &mut Store, wall_ms: u64) -> Vec<Merged> {
    let all: Vec<Record> = from.all().collect();
    all.iter()
        .map(|record| to.merge(&record.entry(), wall_ms))
        .collect()
}

fn live(store: &Store) -> Vec<(String, Vec<u8>)> {
    store
        .list(R, "")
        .map(|record| (record.key, record.version.value))
        .collect()
}

fn whole(store: &Store) -> Vec<Record> {
    store.all().collect()
}

#[test]
fn a_local_write_is_stamped_after_everything_the_node_has_seen() {
    let mut a = store("node-a");
    let first = a.put(R, "camera", b"one", NOW);
    let second = a.put(R, "camera", b"two", NOW);
    assert!(
        second > first,
        "two writes in one millisecond are still in order"
    );
    assert_eq!(
        a.get(R, "camera").map(|version| version.value.clone()),
        Some(b"two".to_vec())
    );
    // The wall clock steps back an hour: this node's stamps still go up.
    let third = a.put(R, "camera", b"three", NOW - 3_600_000);
    assert!(third > second);
    // A version from a peer with a later clock is accepted, and the next local write orders
    // after it although this node's own clock is behind.
    let mut b = store("node-b");
    b.put(R, "fan", b"b's", NOW + 60_000);
    assert_eq!(sync(&b, &mut a, NOW), [Merged::Applied]);
    let fourth = a.put(R, "fan", b"a's", NOW);
    assert!(fourth > b.newest());
    assert_eq!(
        sync(&a, &mut b, NOW + 60_000)
            .iter()
            .filter(|merged| **merged == Merged::Applied)
            .count(),
        2
    );
    assert_eq!(
        b.get(R, "fan").map(|version| version.value.clone()),
        Some(b"a's".to_vec())
    );
}

#[test]
fn concurrent_writes_converge_to_the_last_writer_for_every_interleaving() {
    // Three nodes write the same record without having heard of each other.
    let writes = [
        ("node-a", b"from a".as_slice(), NOW + 5),
        ("node-b", b"from b".as_slice(), NOW + 9),
        ("node-c", b"from c".as_slice(), NOW + 7),
    ];
    let versions: Vec<Record> = writes
        .iter()
        .map(|(node, value, at)| {
            let mut writer = store(node);
            writer.put(R, "camera", value, *at);
            writer.all().next().expect("the write")
        })
        .collect();
    // Every order of arrival, with repeats, ends with the latest write.
    let orders = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for order in orders {
        let mut reader = store("node-d");
        for index in order.iter().chain(order.iter().rev()) {
            reader.merge(&versions[*index].entry(), NOW + 20);
        }
        assert_eq!(
            live(&reader),
            [("camera".to_owned(), b"from b".to_vec())],
            "{order:?}"
        );
        assert_eq!(
            reader.counters().remote_applied + reader.counters().remote_stale,
            6
        );
    }
}

#[test]
fn on_equal_stamps_a_delete_beats_a_write_and_then_the_larger_value_wins() {
    let stamp = HlcTimestamp::new(NOW, 3, 77);
    let version = |deleted: bool, value: &'static [u8]| RecordEntry {
        class: R,
        key: "camera",
        stamp,
        deleted,
        value: Bytes(value),
    };
    // Either way round, the same winner.
    for flip in [false, true] {
        let mut reader = store("node-a");
        let (first, second) = if flip {
            (version(false, b"bb"), version(false, b"a"))
        } else {
            (version(false, b"a"), version(false, b"bb"))
        };
        reader.merge(&first, NOW);
        reader.merge(&second, NOW);
        assert_eq!(live(&reader), [("camera".to_owned(), b"bb".to_vec())]);
        let mut reader = store("node-a");
        let (first, second) = if flip {
            (version(true, b""), version(false, b"zz"))
        } else {
            (version(false, b"zz"), version(true, b""))
        };
        reader.merge(&first, NOW);
        reader.merge(&second, NOW);
        assert!(live(&reader).is_empty(), "the delete wins");
    }
    // The same version again is not news.
    let mut reader = store("node-a");
    assert_eq!(reader.merge(&version(false, b"a"), NOW), Merged::Applied);
    assert_eq!(reader.merge(&version(false, b"a"), NOW), Merged::Stale);
}

#[test]
fn a_delete_is_a_version_that_travels_and_a_later_write_brings_the_record_back() {
    let (mut a, mut b) = (store("node-a"), store("node-b"));
    a.put(R, "camera", b"here", NOW);
    sync(&a, &mut b, NOW);
    assert_eq!(live(&b).len(), 1);
    // Deleted on a: b learns of it, and does not hand the record back to a.
    assert!(a.delete(R, "camera", NOW + 10).is_some());
    assert!(
        a.delete(R, "camera", NOW + 11).is_none(),
        "there is nothing left to delete"
    );
    assert_eq!(a.len(), 1, "the tombstone is kept");
    assert_eq!(
        sync(&b, &mut a, NOW + 20),
        [Merged::Stale],
        "the old version cannot resurrect it"
    );
    assert_eq!(sync(&a, &mut b, NOW + 20), [Merged::Applied]);
    assert!(live(&a).is_empty() && live(&b).is_empty());
    assert_eq!(a.fingerprint(), b.fingerprint());
    // Written again later, anywhere: it is back everywhere.
    b.put(R, "camera", b"again", NOW + 30);
    sync(&b, &mut a, NOW + 40);
    assert_eq!(live(&a), [("camera".to_owned(), b"again".to_vec())]);
}

#[test]
fn tombstones_are_collected_after_retention_and_expired_deletes_are_not_reimported() {
    let config = StoreConfig {
        tombstone_retention_ms: DAY,
        ..StoreConfig::default()
    };
    let mut a = Store::new(name("node-a"), config);
    let mut b = Store::new(name("node-b"), config);
    a.put(R, "camera", b"here", NOW);
    a.put(R, "fan", b"here", NOW);
    sync(&a, &mut b, NOW);
    a.delete(R, "camera", NOW + 10);
    sync(&a, &mut b, NOW + 20);

    // Within retention nothing is collected.
    assert_eq!(a.collect_tombstones(NOW + DAY - 1000), 0);
    // Past it, a forgets the delete. Collection is not a write: nobody is told.
    a.take_changes();
    assert_eq!(a.collect_tombstones(NOW + DAY + 1000), 1);
    assert_eq!(a.take_changes(), Some(Vec::new()));
    assert_eq!((a.len(), a.counters().collected), (1, 1));
    // b still has the tombstone and sends it: a has nothing to delete and does not keep it.
    let merged = sync(&b, &mut a, NOW + DAY + 2000);
    assert!(merged.contains(&Merged::Expired), "{merged:?}");
    assert_eq!(a.len(), 1);
    assert_eq!(b.collect_tombstones(NOW + DAY + 2000), 1);
    assert_eq!(a.fingerprint(), b.fingerprint());
    // A delete within retention that this node never had the record for is kept: a node
    // that still has the record may yet send it.
    let mut c = Store::new(name("node-c"), config);
    c.put(R, "arm", b"here", NOW + DAY);
    c.delete(R, "arm", NOW + DAY + 5);
    let merged = sync(&c, &mut a, NOW + DAY + 2000);
    assert_eq!(merged, [Merged::Applied]);
    assert_eq!(a.len(), 2);
}

#[test]
fn a_stamp_too_far_ahead_is_refused_and_changes_nothing() {
    let mut a = store("node-a");
    a.put(R, "camera", b"mine", NOW);
    let before = (whole(&a), a.now());
    // A node whose clock is a day ahead writes the same record.
    let mut wrong = store("node-b");
    wrong.put(R, "camera", b"from the future", NOW + DAY);
    let merged = sync(&wrong, &mut a, NOW);
    assert!(
        matches!(merged.as_slice(), [Merged::Skewed(skew)] if skew.ahead_by_ms() >= DAY - 10),
        "{merged:?}"
    );
    assert_eq!(
        (whole(&a), a.now()),
        before,
        "neither the record nor the clock moved"
    );
    assert_eq!(a.counters().skewed, 1);
    // Within the allowed drift it is taken.
    let mut near = store("node-c");
    near.put(R, "camera", b"a little ahead", NOW + 200_000);
    assert_eq!(sync(&near, &mut a, NOW), [Merged::Applied]);
}

#[test]
fn a_store_that_restarts_writes_after_what_it_wrote_before() {
    let mut a = store("node-a");
    a.put(R, "camera", b"one", NOW);
    a.put(R, "fan", b"one", NOW);
    a.delete(R, "fan", NOW + 1);
    let kept = whole(&a);
    // It comes back with its clock reset to years ago.
    let mut back = Store::restored(name("node-a"), StoreConfig::default(), kept.clone());
    assert_eq!(whole(&back), kept);
    assert_eq!(back.fingerprint(), a.fingerprint());
    let stamp = back.put(R, "camera", b"two", 1_000);
    assert!(
        stamp > a.newest(),
        "a write after the restart orders after the ones before it"
    );
    // And a peer that has the old version takes the new one.
    assert_eq!(
        sync(&back, &mut a, NOW + 10)
            .iter()
            .filter(|merged| **merged == Merged::Applied)
            .count(),
        1
    );
    assert_eq!(
        a.get(R, "camera").map(|version| version.value.clone()),
        Some(b"two".to_vec())
    );
}

#[test]
fn reads_follow_class_prefix_and_time() {
    let mut a = store("node-a");
    let first = a.put(R, "camera.front", b"1", NOW);
    a.put(R, "camera.rear", b"2", NOW + 1);
    a.put(R, "arm.left", b"3", NOW + 2);
    a.put(RecordClass::Node, "camera.node", b"4", NOW + 3);
    a.delete(R, "camera.rear", NOW + 4);
    let keys = |records: Vec<Record>| -> Vec<String> {
        records.into_iter().map(|record| record.key).collect()
    };
    assert_eq!(keys(a.list(R, "").collect()), ["arm.left", "camera.front"]);
    assert_eq!(keys(a.list(R, "camera.").collect()), ["camera.front"]);
    assert_eq!(
        keys(a.list(RecordClass::Node, "").collect()),
        ["camera.node"]
    );
    assert!(a.list(RecordClass::Lease, "").next().is_none());
    // Since a stamp: what changed after it, deletes included, oldest first.
    let newer = a.since(R, "", first);
    assert_eq!(keys(newer.clone()), ["arm.left", "camera.rear"]);
    assert!(newer[1].version.deleted);
    assert!(a.since(R, "", a.newest()).is_empty());
    assert_eq!(a.since(R, "camera.", HlcTimestamp::ZERO).len(), 2);
}

#[test]
fn every_change_is_handed_to_whoever_follows_them_in_order() {
    let mut a = store("node-a");
    assert_eq!(a.take_changes(), Some(Vec::new()));
    a.put(R, "camera", b"1", NOW);
    a.delete(R, "camera", NOW + 1);
    let mut b = store("node-b");
    b.put(R, "fan", b"2", NOW + 2);
    sync(&b, &mut a, NOW + 3);
    // A version that loses changes nothing and is not a change.
    sync(&b, &mut a, NOW + 3);
    let changes = a.take_changes().expect("kept");
    let seen: Vec<(&str, bool)> = changes
        .iter()
        .map(|change| (change.record.key.as_str(), change.record.version.deleted))
        .collect();
    assert_eq!(seen, [("camera", false), ("camera", true), ("fan", false)]);
    assert!(changes.iter().all(|change| change.scope == Scope::Desired));
    assert_eq!(a.take_changes(), Some(Vec::new()), "taken once");
    // Left untaken for too long, they are dropped and the follower is told to read again.
    for index in 0..5000 {
        a.put(R, &format!("item.{index}"), b"x", NOW + 10);
    }
    assert_eq!(a.take_changes(), None);
    a.put(R, "after", b"y", NOW + 11);
    assert_eq!(a.take_changes().map(|changes| changes.len()), Some(1));
}

#[test]
fn what_a_node_observes_is_its_own_and_goes_when_it_does() {
    let mut a = store("node-a");
    let b_name = name("node-b");
    a.observe(R, "camera", b"seen by a", NOW);
    let entry = |key: &'static str, value: &'static [u8], at: u64| RecordEntry {
        class: R,
        key,
        stamp: HlcTimestamp::new(at, 0, 2),
        deleted: false,
        value: Bytes(value),
    };
    a.observed_from(
        &b_name,
        &[
            entry("camera", b"seen by b", NOW),
            entry("fan", b"spinning", NOW),
        ],
    );
    let everywhere: Vec<(String, String)> = a
        .observed_everywhere(R, "")
        .map(|(node, record)| (node.to_string(), record.key))
        .collect();
    assert_eq!(
        everywhere,
        [
            ("node-a".to_owned(), "camera".to_owned()),
            ("node-b".to_owned(), "camera".to_owned()),
            ("node-b".to_owned(), "fan".to_owned())
        ]
    );
    // Nothing observed is desired state, and nothing is merged between nodes.
    assert!(a.is_empty());
    assert_eq!(
        a.observed(&name("node-a"), R, "")
            .next()
            .map(|record| record.version.value),
        Some(b"seen by a".to_vec())
    );
    a.take_changes();

    // b's next slice replaces its last: what it no longer sees is gone, what changed is new.
    a.observed_from(&b_name, &[entry("fan", b"stopped", NOW + 5)]);
    let changes: Vec<Change> = a.take_changes().expect("kept");
    let seen: Vec<(String, bool, Vec<u8>)> = changes
        .iter()
        .map(|change| {
            (
                change.record.key.clone(),
                change.record.version.deleted,
                change.record.version.value.clone(),
            )
        })
        .collect();
    assert_eq!(
        seen,
        [
            ("camera".to_owned(), true, Vec::new()),
            ("fan".to_owned(), false, b"stopped".to_vec())
        ]
    );
    assert!(
        changes
            .iter()
            .all(|change| change.scope == Scope::Observed(b_name.clone()))
    );
    // The same slice again is no change.
    a.observed_from(&b_name, &[entry("fan", b"stopped", NOW + 5)]);
    assert_eq!(a.take_changes(), Some(Vec::new()));
    // A peer cannot speak for this node's slice.
    a.observed_from(&name("node-a"), &[]);
    assert_eq!(a.observed(&name("node-a"), R, "").count(), 1);

    // b goes away: everything it saw goes with it. This node's own stays until it says so.
    a.drop_observed(&b_name);
    assert_eq!(a.observed_everywhere(R, "").count(), 1);
    assert!(a.unobserve(R, "camera", NOW + 9));
    assert!(!a.unobserve(R, "camera", NOW + 9));
    assert_eq!(a.observed_everywhere(R, "").count(), 0);
}

/// A small generator, the same every run.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, limit: u64) -> u64 {
        self.next() % limit
    }
}

#[test]
fn any_history_of_writes_deletes_and_gossip_converges() {
    for seed in 1..=40_u64 {
        let mut random = Random(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let nodes = 2 + random.below(4) as usize;
        let mut stores: Vec<Store> = (0..nodes)
            .map(|index| store(&format!("node-{index}")))
            .collect();
        // Every version ever written, as it was when written: what "everything" is.
        let mut written: Vec<Record> = Vec::new();
        for step in 0..600_u64 {
            let at = random.below(nodes as u64) as usize;
            // Wall clocks that disagree by up to a minute and sometimes step back.
            let wall = NOW + step * 3 + random.below(60_000)
                - if random.below(10) == 0 { 30_000 } else { 0 };
            let key = format!("key.{}", random.below(12));
            match random.below(10) {
                0..=4 => {
                    let value = random.next().to_le_bytes();
                    stores[at].put(R, &key, &value[..1 + random.below(8) as usize], wall);
                }
                5 | 6 => {
                    stores[at].delete(R, &key, wall);
                }
                // Gossip: some of what one node has, to another, in any order, often twice.
                _ => {
                    let to = random.below(nodes as u64) as usize;
                    let mut some: Vec<Record> =
                        stores[at].all().filter(|_| random.below(3) > 0).collect();
                    for index in (1..some.len()).rev() {
                        some.swap(index, random.below(index as u64 + 1) as usize);
                    }
                    for record in some.iter().chain(some.iter().take(3)) {
                        let merged = stores[to].merge(&record.entry(), NOW + step * 3 + 60_000);
                        assert!(
                            !matches!(merged, Merged::Skewed(_) | Merged::Expired),
                            "{merged:?}"
                        );
                    }
                }
            }
            if let Some(last) = stores[at].all().max_by_key(|record| record.version.stamp)
                && !written.contains(&last)
            {
                written.push(last);
            }
        }
        // Until everyone has heard from everyone: two rounds of everyone to everyone.
        let end = NOW + 10_000_000;
        for _ in 0..2 {
            for from in 0..nodes {
                let all: Vec<Record> = stores[from].all().collect();
                for (to, target) in stores.iter_mut().enumerate() {
                    if to != from {
                        for record in &all {
                            target.merge(&record.entry(), end);
                        }
                    }
                }
            }
        }
        // They agree: on what is there, what is deleted, and at which stamps.
        let reference = whole(&stores[0]);
        for (index, other) in stores.iter().enumerate() {
            assert_eq!(whole(other), reference, "seed {seed}: node {index} differs");
            assert_eq!(other.fingerprint(), stores[0].fingerprint(), "seed {seed}");
        }
        // And what they agree on is the newest version of each record ever written, as one
        // node that was handed every version in a scrambled order would hold it.
        let mut scrambled = written.clone();
        for index in (1..scrambled.len()).rev() {
            scrambled.swap(index, random.below(index as u64 + 1) as usize);
        }
        let mut oracle = store("oracle");
        for record in &scrambled {
            oracle.merge(&record.entry(), end);
        }
        let newest = |records: Vec<Record>| -> Vec<(String, Version)> {
            records
                .into_iter()
                .map(|record| (record.key, record.version))
                .collect()
        };
        assert_eq!(
            newest(whole(&oracle)),
            newest(reference),
            "seed {seed}: not the newest of everything written"
        );
    }
}
