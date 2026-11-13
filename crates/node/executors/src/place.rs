//! Which node runs a workload that names none, worked out from the store.
//!
//! - **Who is there.** A node that observes its own node record is live: its slice is
//!   forgotten when the connection to it goes (E19). That record lists the kinds its
//!   executors run, under [`LABEL_KIND`], and the labels it reports of itself; the node's
//!   record in the store says labels over those. [`LABEL_SCHEDULABLE`] `false` keeps new work
//!   off a node.
//! - **Whose it is.** The node that runs a workload says so by observing it. If two did, for
//!   a moment, the one placement scores higher for it keeps it and the other lets it go.

use std::collections::{BTreeMap, BTreeSet};

use orion_msg_records::{Label, NodeRecord, RecordClass, ResourceRecord};
use orion_msg_workloads::WorkloadPut;
use orion_placement::{Candidate, Needs, Step, View, merge_labels, rendezvous_choice};
use orion_reply::Out;
use orion_store::Store;
use orion_types::{NodeName, WorkloadKind, WorkloadName};
use orion_wire::body::encode_fields;
use orion_wire::{Decode, List};
use orion_workloads::Workload;
use tracing::info;

/// The label a node's observed record lists each kind its executors run under.
pub const LABEL_KIND: &str = "orion.kind";

/// A node label that, set to `false`, keeps new work off the node.
pub const LABEL_SCHEDULABLE: &str = "orion.schedulable";

/// This node's own observed node record, listing `kinds`.
pub(crate) fn own_record(me: &NodeName, kinds: &BTreeSet<WorkloadKind>) -> Vec<u8> {
    let labels: Vec<Label<'_>> = kinds
        .iter()
        .map(|kind| Label {
            key: LABEL_KIND,
            value: kind.as_str(),
        })
        .collect();
    let mut value = vec![0_u8; 256 + 64 * labels.len()];
    let len = encode_fields(
        &NodeRecord {
            name: me.clone(),
            address: "",
            labels: List::new(&labels),
        },
        &mut value,
    )
    .unwrap_or(0);
    value.truncate(len);
    value
}

/// The cluster as the store has it.
pub(crate) fn view(store: &Store) -> View {
    let mut desired: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for record in store.list(RecordClass::Node, "") {
        if let Ok(node) = NodeRecord::decode(&record.version.value) {
            let labels = node
                .labels
                .iter()
                .flatten()
                .map(|label| (label.key.to_owned(), label.value.to_owned()))
                .collect();
            desired.insert(node.name.as_str().to_owned(), labels);
        }
    }
    let mut view = View::default();
    for (node, record) in store.observed_everywhere(RecordClass::Node, "") {
        if record.key != node.as_str() || record.version.deleted {
            continue;
        }
        let Ok(reported) = NodeRecord::decode(&record.version.value) else {
            continue;
        };
        let mut kinds = BTreeSet::new();
        let mut labels = BTreeMap::new();
        for label in reported.labels.iter().flatten() {
            if label.key == LABEL_KIND {
                if let Ok(kind) = label.value.parse() {
                    kinds.insert(kind);
                }
            } else {
                labels.insert(label.key.to_owned(), label.value.to_owned());
            }
        }
        let none = BTreeMap::new();
        let labels = merge_labels(&labels, desired.get(node.as_str()).unwrap_or(&none));
        let schedulable = labels
            .get(LABEL_SCHEDULABLE)
            .is_none_or(|value| value != "false");
        view.nodes.insert(
            node.clone(),
            Candidate {
                labels,
                live: true,
                schedulable,
                kinds,
            },
        );
    }
    for record in store.list(RecordClass::Resource, "") {
        if let Ok(resource) = ResourceRecord::decode(&record.version.value) {
            view.resource_hosts
                .insert(resource.name.as_str().to_owned(), resource.node.clone());
        }
    }
    view
}

/// The node that runs the workload `needs` describes: of the nodes that say so, one that can
/// still run it before one that cannot, and of those the one placement scores highest.
pub(crate) fn assigned(store: &Store, view: &View, needs: &Needs) -> Option<NodeName> {
    let name = needs.name.as_str();
    let claimers: Vec<NodeName> = store
        .observed_everywhere(RecordClass::Workload, name)
        .filter(|(_, record)| record.key == name && !record.version.deleted)
        .map(|(node, _)| node.clone())
        .collect();
    let able: Vec<String> = claimers
        .iter()
        .filter(|node| view.eligible(node, needs))
        .map(|node| node.as_str().to_owned())
        .collect();
    let pool = if able.is_empty() {
        claimers
            .iter()
            .map(|node| node.as_str().to_owned())
            .collect()
    } else {
        able
    };
    rendezvous_choice(name, pool.iter()).and_then(|node| node.parse().ok())
}

impl crate::Workloads {
    /// Whether placement should look again: something it depends on changed.
    #[must_use]
    pub const fn place_due(&self) -> bool {
        self.place_due
    }

    /// Says that the records placement reads changed.
    pub fn records_changed(&mut self) {
        self.place_due = true;
    }

    /// How long a node that would take over a workload waits for its node to come back.
    pub fn set_placement_grace(&mut self, grace_ms: u64) {
        self.placement = orion_placement::Coordinator::new(self.me.clone(), grace_ms);
    }

    /// When placement next has something to do even if nothing changes: a grace period
    /// ends. On the clock `now_ms` is on.
    #[must_use]
    pub fn placement_deadline(&self) -> Option<u64> {
        self.placement.next_deadline()
    }

    /// The kinds the executors here run, as this node's observed node record says them.
    fn say_kinds(&mut self, store: &mut Store, wall_ms: u64) {
        let kinds: BTreeSet<WorkloadKind> = self
            .executors
            .values()
            .filter(|offering| offering.up)
            .flat_map(|offering| offering.kinds.iter().cloned())
            .collect();
        if self.said_kinds.as_ref() == Some(&kinds) {
            return;
        }
        let value = own_record(&self.me, &kinds);
        store.observe(RecordClass::Node, self.me.as_str(), &value, wall_ms);
        self.said_kinds = Some(kinds);
    }

    /// Works out which workloads are this node's to run, from the records, and starts or
    /// lets go of them. A workload that names no node is taken by the node placement
    /// chooses, which observes it to say so.
    pub fn place(
        &mut self,
        store: &mut Store,
        board: &dyn crate::Board,
        now_ms: u64,
        wall_ms: u64,
        out: &mut dyn Out,
    ) {
        self.place_due = false;
        self.say_kinds(store, wall_ms);
        let view = view(store);
        let records: Vec<Workload> = store
            .list(RecordClass::Workload, "")
            .filter_map(|record| {
                WorkloadPut::decode(&record.version.value)
                    .and_then(|put| Workload::from_put(&put))
                    .ok()
            })
            .collect();
        let mut seen = BTreeSet::new();
        for workload in records {
            let name = workload.name.clone();
            seen.insert(name.clone());
            let needs = Needs::of(&workload);
            let mut owner = assigned(store, &view, &needs);
            if let Step::Take(reason) = self.placement.step(&view, &needs, owner.as_ref(), now_ms) {
                info!("workload {name} is this node's to run: {reason:?}");
                store.observe(RecordClass::Workload, name.as_str(), &[], wall_ms);
                owner = assigned(store, &view, &needs);
            }
            let chosen = view.choose(&needs);
            let here = match &workload.node {
                Some(node) => *node == self.me,
                None => match &owner {
                    Some(owner) => *owner == self.me,
                    // Nobody runs it and nobody can: it waits here as anywhere, Pending.
                    None => chosen.is_none(),
                },
            };
            if owner.as_ref() != Some(&self.me) {
                // Another node has it, or nobody: this node does not say it does.
                store.unobserve(RecordClass::Workload, name.as_str(), wall_ms);
            }
            if !here {
                if self.tracker.get(&name).is_some() {
                    // It runs on another node now. Its record stays.
                    self.released.insert(name.clone());
                    let _ = self.tracker.remove(&name, false, now_ms);
                }
                continue;
            }
            self.released.remove(&name);
            let mut why = crate::missing(&workload, board);
            let has_executor = self
                .executors
                .values()
                .any(|offering| offering.up && offering.kinds.contains(&workload.kind));
            if why.is_none() && workload.node.is_none() && owner.is_none() && has_executor {
                // An executor is here but this node is not one placement may choose.
                why = Some("no node has what it asks for".to_owned());
            }
            self.tracker.hold(&name, why.as_deref());
            if self.tracker.get(&name).map(|tracked| &tracked.workload) != Some(&workload) {
                self.tracker.put(workload, now_ms);
            }
        }
        // A record that is gone, from here or another node: its run goes too.
        let gone: Vec<WorkloadName> = self
            .tracker
            .iter()
            .map(|(name, _)| name.clone())
            .filter(|name| !seen.contains(name))
            .collect();
        for name in gone {
            self.placement.forget(&name);
            let _ = self.tracker.remove(&name, false, now_ms);
        }
        self.carry(store, wall_ms, out);
    }
}
