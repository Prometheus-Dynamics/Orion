//! Three nodes, a network that can be cut anywhere, and the wire in between: whatever is
//! written where, and whatever is cut when, the nodes end up with the same records.

use std::collections::{BTreeSet, VecDeque};

use orion_msg_records::RecordClass;
use orion_store::{Record, Store, StoreConfig};
use orion_sync::{Counters, Out, PeerStatus, Received, Sync, SyncConfig};
use orion_types::{Error, HlcTimestamp, NodeName};
use orion_wire::kind;

const NAMES: [&str; 3] = ["a", "b", "c"];
const R: RecordClass = RecordClass::Resource;

fn name(index: usize) -> NodeName {
    NAMES[index].parse().expect("name")
}

struct Message {
    from: usize,
    to: usize,
    kind: u8,
    payload: Vec<u8>,
}

/// Collects what one node sends.
struct Sending<'a> {
    from: usize,
    queue: &'a mut VecDeque<Message>,
}

impl Out for Sending<'_> {
    fn frame(&mut self, peer: &NodeName, kind: u8, payload: &[u8]) {
        let to = NAMES
            .iter()
            .position(|name| *name == peer.as_str())
            .expect("a known peer");
        self.queue.push_back(Message {
            from: self.from,
            to,
            kind,
            payload: payload.to_vec(),
        });
    }
}

struct Net {
    stores: Vec<Store>,
    syncs: Vec<Sync>,
    /// The pairs that can talk, lower index first.
    links: BTreeSet<(usize, usize)>,
    queue: VecDeque<Message>,
    now: u64,
    delivered: usize,
}

fn pair(one: usize, other: usize) -> (usize, usize) {
    (one.min(other), one.max(other))
}

impl Net {
    fn new() -> Self {
        Self::with(SyncConfig::default())
    }

    fn with(config: SyncConfig) -> Self {
        Self {
            stores: (0..3)
                .map(|index| Store::new(name(index), StoreConfig::default()))
                .collect(),
            syncs: (0..3).map(|index| Sync::new(name(index), config)).collect(),
            links: BTreeSet::new(),
            queue: VecDeque::new(),
            now: 1_700_000_000_000,
            delivered: 0,
        }
    }

    /// Every pair connected.
    fn meshed() -> Self {
        let mut net = Self::new();
        for (one, other) in [(0, 1), (0, 2), (1, 2)] {
            net.connect(one, other);
        }
        net.settle();
        net
    }

    fn connect(&mut self, one: usize, other: usize) {
        if !self.links.insert(pair(one, other)) {
            return;
        }
        for (this, that) in [(one, other), (other, one)] {
            let mut out = Sending {
                from: this,
                queue: &mut self.queue,
            };
            self.syncs[this].peer_up(&name(that), &self.stores[this], self.now, &mut out);
        }
    }

    fn cut(&mut self, one: usize, other: usize) {
        if !self.links.remove(&pair(one, other)) {
            return;
        }
        // What was on the way is lost with the connection.
        self.queue
            .retain(|message| pair(message.from, message.to) != pair(one, other));
        for (this, that) in [(one, other), (other, one)] {
            self.syncs[this].peer_down(&name(that), &mut self.stores[this], self.now);
            self.publish(this);
        }
    }

    /// Sends on what changed in a node's store.
    fn publish(&mut self, node: usize) {
        let mut out = Sending {
            from: node,
            queue: &mut self.queue,
        };
        match self.stores[node].take_changes() {
            Some(changes) => {
                self.syncs[node].changed(&self.stores[node], &changes, self.now, &mut out);
            }
            None => self.syncs[node].resync(&self.stores[node], &mut out),
        }
    }

    fn put(&mut self, node: usize, key: &str, value: &[u8]) -> HlcTimestamp {
        self.now += 1;
        let stamp = self.stores[node].put(R, key, value, self.now);
        self.publish(node);
        stamp
    }

    fn delete(&mut self, node: usize, key: &str) {
        self.now += 1;
        self.stores[node].delete(R, key, self.now);
        self.publish(node);
    }

    fn deliver(&mut self) -> Option<Result<Received, Error>> {
        let message = self.queue.pop_front()?;
        self.delivered += 1;
        let mut out = Sending {
            from: message.to,
            queue: &mut self.queue,
        };
        let result = self.syncs[message.to].receive(
            &mut self.stores[message.to],
            &name(message.from),
            message.kind,
            &message.payload,
            self.now,
            &mut out,
        );
        self.publish(message.to);
        Some(result)
    }

    /// Delivers until nobody has anything more to say. Returns how many messages that took.
    fn settle(&mut self) -> usize {
        let before = self.delivered;
        while self.deliver().is_some() {
            assert!(
                self.delivered - before < 100_000,
                "sync does not come to rest"
            );
        }
        self.delivered - before
    }

    fn records(&self, node: usize) -> Vec<Record> {
        self.stores[node].all().collect()
    }

    fn value(&self, node: usize, key: &str) -> Option<Vec<u8>> {
        self.stores[node]
            .get(R, key)
            .map(|version| version.value.clone())
    }

    #[track_caller]
    fn assert_same(&self) {
        let first = self.records(0);
        for node in 1..3 {
            assert_eq!(self.records(node), first, "node {} differs", name(node));
        }
    }

    fn counters(&self, node: usize) -> Counters {
        self.syncs[node].counters()
    }

    fn fulls(&self) -> u64 {
        (0..3).map(|node| self.counters(node).fulls_sent).sum()
    }
}

#[test]
fn a_write_on_one_node_reaches_the_others() {
    let mut net = Net::meshed();
    net.put(0, "fan", b"on");
    net.settle();
    net.assert_same();
    assert_eq!(net.value(2, "fan").as_deref(), Some(&b"on"[..]));
    net.delete(1, "fan");
    net.settle();
    net.assert_same();
    assert_eq!(net.value(0, "fan"), None);
    assert_eq!(net.records(0).len(), 1, "the delete is kept");
    assert_eq!(net.fulls(), 0, "changes travel as changes");
    for node in 0..3 {
        for peer in (0..3).filter(|peer| *peer != node) {
            assert_eq!(net.syncs[node].status(&name(peer)), PeerStatus::InSync);
        }
    }
}

#[test]
fn nodes_at_rest_say_nothing() {
    let mut net = Net::meshed();
    net.put(0, "fan", b"on");
    net.settle();
    assert_eq!(net.settle(), 0);
    assert!(net.queue.is_empty());
    assert!(net.syncs.iter().all(|sync| sync.next_deadline().is_none()));
}

#[test]
fn three_nodes_converge_after_a_partition() {
    let mut net = Net::meshed();
    net.put(0, "shared", b"before");
    net.put(1, "doomed", b"x");
    net.settle();

    // A is cut off from B and C.
    net.cut(0, 1);
    net.cut(0, 2);
    net.put(0, "only-a", b"1");
    net.put(1, "only-b", b"2");
    net.put(2, "only-c", b"3");
    // The same record written on both sides: the later write wins everywhere.
    net.put(0, "shared", b"from a");
    net.put(1, "shared", b"from b");
    // Deleted on one side, written later on the other: the write wins.
    net.delete(1, "doomed");
    net.put(0, "doomed", b"back");
    // Written on one side, deleted later on the other: the delete wins.
    net.put(0, "brief", b"y");
    net.settle();
    assert_ne!(net.records(0), net.records(1));
    assert_eq!(net.records(1), net.records(2), "b and c could still talk");

    net.connect(0, 1);
    net.connect(0, 2);
    net.settle();
    net.assert_same();
    for node in 0..3 {
        assert_eq!(net.value(node, "only-a").as_deref(), Some(&b"1"[..]));
        assert_eq!(net.value(node, "only-b").as_deref(), Some(&b"2"[..]));
        assert_eq!(net.value(node, "only-c").as_deref(), Some(&b"3"[..]));
        assert_eq!(net.value(node, "shared").as_deref(), Some(&b"from b"[..]));
        assert_eq!(net.value(node, "doomed").as_deref(), Some(&b"back"[..]));
        for peer in (0..3).filter(|peer| *peer != node) {
            assert_eq!(net.syncs[node].status(&name(peer)), PeerStatus::InSync);
        }
    }
    net.delete(2, "brief");
    net.settle();
    net.assert_same();
    assert_eq!(net.value(0, "brief"), None);
}

#[test]
fn a_difference_in_the_past_is_found() {
    // B has an old record A lacks, and A's newest stamp is newer than anything B has: no
    // stamp tells B that A is missing it. Only the digest does.
    let mut net = Net::new();
    net.put(1, "old", b"b's");
    net.put(0, "new", b"a's");
    net.connect(0, 1);
    net.settle();
    assert_eq!(net.records(0), net.records(1));
    assert_eq!(net.records(0).len(), 2);
    assert!(net.fulls() <= 1, "everything is sent once at most");
}

#[test]
fn a_node_that_starts_empty_is_filled() {
    let mut net = Net::new();
    net.connect(0, 1);
    for index in 0..500 {
        net.put(0, &format!("part.{index:03}"), &[7; 200]);
    }
    net.settle();
    net.connect(1, 2);
    net.settle();
    assert_eq!(net.records(2).len(), 500);
    assert_eq!(net.records(2), net.records(0));
    assert!(
        net.counters(1).data_sent >= 4,
        "100 KiB goes in several messages"
    );
    assert_eq!(
        net.fulls(),
        0,
        "a node that is merely behind is sent what is newer"
    );
}

#[test]
fn a_node_between_two_that_never_meet_passes_everything_on() {
    let mut net = Net::new();
    net.connect(0, 1);
    net.connect(1, 2);
    net.settle();
    net.put(0, "from-a", b"1");
    net.put(2, "from-c", b"2");
    net.settle();
    net.assert_same();
    assert_eq!(net.records(0).len(), 2);
    assert_eq!(net.syncs[0].status(&name(2)), PeerStatus::Down);
}

#[test]
fn a_version_is_not_sent_back_to_where_it_came_from() {
    let mut net = Net::new();
    net.connect(0, 1);
    net.settle();
    let before = net.counters(1).versions_sent;
    net.put(0, "fan", b"on");
    net.settle();
    assert_eq!(net.counters(1).versions_sent, before);
    assert_eq!(net.counters(1).applied, 1);
}

struct Rng(u64);

impl Rng {
    fn next(&mut self, below: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % below
    }
}

#[test]
fn any_history_of_writes_and_cuts_converges() {
    for seed in 1..=300_u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut net = Net::meshed();
        for _ in 0..120 {
            let node = rng.next(3) as usize;
            let key = format!("k{}", rng.next(6));
            match rng.next(10) {
                0..=4 => {
                    let value = [rng.next(256) as u8; 3];
                    net.put(node, &key, &value);
                }
                5 => net.delete(node, &key),
                6 => {
                    let other = (node + 1 + rng.next(2) as usize) % 3;
                    net.cut(node, other);
                }
                7 => {
                    let other = (node + 1 + rng.next(2) as usize) % 3;
                    net.connect(node, other);
                }
                // Some of what is on the way arrives, in order.
                _ => {
                    for _ in 0..rng.next(8) {
                        net.deliver();
                    }
                }
            }
        }
        for (one, other) in [(0, 1), (0, 2), (1, 2)] {
            net.connect(one, other);
        }
        net.settle();
        // Whatever was held back by the interval is sent when it is due.
        while let Some(due) = net.syncs.iter().filter_map(Sync::next_deadline).min() {
            net.now = net.now.max(due);
            for node in 0..3 {
                let mut out = Sending {
                    from: node,
                    queue: &mut net.queue,
                };
                net.syncs[node].tick(&net.stores[node], net.now, &mut out);
            }
            net.settle();
        }
        let first = net.records(0);
        for node in 1..3 {
            assert_eq!(
                net.records(node),
                first,
                "seed {seed}: {} differs",
                name(node)
            );
        }
        for node in 0..3 {
            for peer in (0..3).filter(|peer| *peer != node) {
                assert_eq!(
                    net.syncs[node].status(&name(peer)),
                    PeerStatus::InSync,
                    "seed {seed}"
                );
            }
        }
    }
}

#[test]
fn what_a_node_sees_is_told_to_its_peers_and_forgotten_when_it_goes() {
    let mut net = Net::new();
    net.now += 1;
    net.stores[0].observe(R, "cam.left", b"ready", net.now);
    net.connect(0, 1);
    net.connect(1, 2);
    net.settle();
    let seen = |net: &Net, node: usize| -> Vec<String> {
        net.stores[node]
            .observed_everywhere(R, "")
            .map(|(by, record)| format!("{by}:{}", record.key))
            .collect()
    };
    assert_eq!(seen(&net, 1), ["a:cam.left"]);
    assert!(
        seen(&net, 2).is_empty(),
        "a slice is told by its own node only"
    );

    // It changes: the peers have the new slice, whole.
    net.now += 1;
    net.stores[0].observe(R, "cam.right", b"ready", net.now);
    net.publish(0);
    net.settle();
    assert_eq!(seen(&net, 1), ["a:cam.left", "a:cam.right"]);
    net.now += 1;
    assert!(net.stores[0].unobserve(R, "cam.left", net.now));
    net.publish(0);
    net.settle();
    assert_eq!(seen(&net, 1), ["a:cam.right"]);

    // The node goes: nobody knows what it sees.
    net.cut(0, 1);
    assert!(seen(&net, 1).is_empty());
    assert_eq!(seen(&net, 0), ["a:cam.right"], "its own slice is its own");
}

#[test]
fn a_message_under_another_nodes_name_is_refused() {
    let mut net = Net::meshed();
    net.put(2, "fan", b"on");
    // What C sent to A, handed to B's sync as if it came from A.
    let message = net
        .queue
        .iter()
        .find(|message| message.from == 2 && message.kind == kind::PEER_SYNC)
        .expect("a message from c");
    let mut sink = VecDeque::new();
    let mut out = Sending {
        from: 1,
        queue: &mut sink,
    };
    let result = net.syncs[1].receive(
        &mut net.stores[1],
        &name(0),
        message.kind,
        &message.payload,
        net.now,
        &mut out,
    );
    assert_eq!(result, Err(Error::Denied));
    assert_eq!(
        net.syncs[1].receive(&mut net.stores[1], &name(0), 0x7F, &[], net.now, &mut out),
        Err(Error::Unsupported)
    );
    assert_eq!(
        net.syncs[1].receive(
            &mut net.stores[1],
            &name(0),
            kind::PEER_SYNC,
            &[0xFF; 3],
            net.now,
            &mut out
        ),
        Err(Error::Malformed)
    );
}

#[test]
fn a_record_from_a_clock_far_ahead_is_refused_and_sync_still_comes_to_rest() {
    let mut net = Net::new();
    // A's clock is ten minutes ahead of everyone's.
    net.stores[0].put(R, "future", b"x", net.now + 600_000);
    net.publish(0);
    net.put(1, "present", b"y");
    net.connect(0, 1);
    let messages = net.settle();
    assert!(messages < 40, "{messages} messages");
    assert_eq!(net.value(1, "future"), None);
    assert_eq!(net.value(0, "present").as_deref(), Some(&b"y"[..]));
    assert!(net.counters(1).skewed >= 1);
    assert_eq!(net.syncs[1].status(&name(0)), PeerStatus::Syncing);
    // Each further write costs a bounded number of messages, and not everything again.
    let fulls = net.fulls();
    for index in 0..20 {
        net.put(1, &format!("more.{index}"), b"z");
        assert!(net.settle() < 12);
    }
    assert_eq!(
        net.fulls(),
        fulls,
        "everything is not sent again inside the interval"
    );
    // When time catches up with the stamp, it is taken.
    net.now += 600_000;
    net.cut(0, 1);
    net.connect(0, 1);
    net.settle();
    assert_eq!(net.value(1, "future").as_deref(), Some(&b"x"[..]));
    assert_eq!(net.syncs[1].status(&name(0)), PeerStatus::InSync);
}

#[test]
fn everything_held_back_by_the_interval_is_sent_when_it_is_due() {
    let mut net = Net::new();
    // Each has an old record the other lacks, twice over, so everything is needed twice.
    net.put(1, "old-b", b"1");
    net.put(0, "new-a", b"2");
    net.connect(0, 1);
    net.settle();
    assert_eq!(net.records(0), net.records(1));
    let fulls = net.fulls();
    assert!(fulls >= 1);
    net.cut(0, 1);
    net.put(1, "old-b2", b"3");
    net.put(0, "new-a2", b"4");
    net.connect(0, 1);
    net.settle();
    if net.records(0) != net.records(1) {
        // Held back: due at the interval, and then they agree.
        let due = (0..2)
            .filter_map(|node| net.syncs[node].next_deadline())
            .min()
            .expect("a send is due");
        assert!(due > net.now && due <= net.now + 10_000);
        net.now = due;
        for node in 0..2 {
            let mut out = Sending {
                from: node,
                queue: &mut net.queue,
            };
            net.syncs[node].tick(&net.stores[node], net.now, &mut out);
        }
        net.settle();
    }
    assert_eq!(net.records(0), net.records(1));
    assert!(net.syncs.iter().all(|sync| sync.next_deadline().is_none()));
}

#[test]
fn a_peer_away_longer_than_deletes_are_remembered_is_not_synced_with() {
    let config = SyncConfig {
        stale_after_ms: 1_000_000,
        ..SyncConfig::default()
    };
    let mut net = Net::with(config);
    net.connect(0, 1);
    net.put(0, "kept", b"1");
    net.put(0, "deleted", b"2");
    net.settle();

    // Connected and agreeing for a long time, then a short break: not stale.
    net.now += 5_000_000;
    net.cut(0, 1);
    net.now += 10;
    net.connect(0, 1);
    net.settle();
    assert_eq!(net.syncs[0].status(&name(1)), PeerStatus::InSync);

    // B is away past the limit. Meanwhile A deletes a record and forgets the delete.
    net.cut(0, 1);
    net.delete(0, "deleted");
    net.now += 2_000_000;
    net.connect(0, 1);
    net.settle();
    assert_eq!(net.syncs[0].status(&name(1)), PeerStatus::Stale);
    assert_eq!(net.syncs[1].status(&name(0)), PeerStatus::Stale);
    assert!(
        net.counters(0).refused_stale + net.counters(1).refused_stale == 0
            || net.value(0, "deleted").is_none()
    );
    assert_eq!(net.value(0, "deleted"), None, "it did not come back");
    net.put(0, "after", b"3");
    net.settle();
    assert_eq!(net.value(1, "after"), None, "nothing is sent to it either");

    // Someone says it may. They sync.
    for (this, that) in [(0, 1), (1, 0)] {
        let mut out = Sending {
            from: this,
            queue: &mut net.queue,
        };
        net.syncs[this].forgive(&name(that), &net.stores[this], net.now, &mut out);
    }
    net.settle();
    assert_eq!(net.records(0), net.records(1));
    assert_eq!(net.syncs[0].status(&name(1)), PeerStatus::InSync);
}

#[test]
fn when_peers_last_agreed_is_kept_across_a_restart() {
    let mut net = Net::meshed();
    net.put(0, "fan", b"on");
    net.settle();
    let kept: Vec<(NodeName, u64)> = net.syncs[0]
        .agreed_at()
        .map(|(peer, at)| (peer.clone(), at))
        .collect();
    assert_eq!(kept.len(), 2);
    let config = SyncConfig {
        stale_after_ms: 1_000,
        ..SyncConfig::default()
    };
    let mut again = Sync::new(name(0), config);
    for (peer, at) in &kept {
        again.restore_agreed(peer, *at);
    }
    let mut sink = VecDeque::new();
    let mut out = Sending {
        from: 0,
        queue: &mut sink,
    };
    again.peer_up(&name(1), &net.stores[0], net.now + 5_000, &mut out);
    assert_eq!(again.status(&name(1)), PeerStatus::Stale);
}
