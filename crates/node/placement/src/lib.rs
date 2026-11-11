//! Leaderless placement.
//!
//! Which node runs a workload is not decided by one node for the others. Every node holds the
//! same records and works out the same answer from them, and only the node the answer names
//! acts on it, by saying "this one is mine". Nothing here talks to anything: it is handed a
//! [`View`] of the cluster and what a workload [`Needs`], and says what the local node should
//! do.
//!
//! - **Eligible.** A node can run a workload if it is live, takes new work, has an executor
//!   for the workload's kind, has every label the workload asks for, and, if the workload is
//!   to sit beside a resource, is the node that resource is on.
//! - **The choice** among eligible nodes is by rendezvous hashing: the node with the highest
//!   score of (workload, node). Every node that sees the same view chooses the same node.
//!   Taking a node away only moves the workloads it had; adding one only moves the workloads
//!   it wins. The hash is the one `dev` used and does not change.
//! - **Only the chosen node writes**, and only "mine". A node never assigns work to another.
//! - **A workload that runs stays.** While its node is eligible it never moves, whoever would
//!   win it now. When its node stops being eligible, the node that would take it waits out a
//!   grace period first; if the old node is back within it, nothing moves.
//! - **A workload told a node** runs there and nowhere else, eligible or not.
//!
//! Resources across nodes use the same hash: a workload that needs "a camera" is given the
//! free, ready one with the highest score of (workload, resource), on whatever live node.

use std::collections::{BTreeMap, BTreeSet};

use orion_types::{NodeName, WorkloadKind, WorkloadName};
use orion_workloads::Workload;

/// The label key by which a workload names a resource it must hold. Not a node label.
pub const LABEL_RESOURCE: &str = "orion.resource";

/// The label key by which a workload names a resource whose node it must run on. Not a node
/// label.
pub const LABEL_COLOCATE: &str = "orion.colocate";

/// How long a node that would take over a workload waits for its node to come back.
pub const DEFAULT_GRACE_MS: u64 = 10_000;

/// The score of `node` for `key`. FNV-1a over the key, `0xff` and the node, finished with the
/// SplitMix64 mixer. It is the same on every platform and in every release.
#[must_use]
pub fn rendezvous_score(key: &str, node: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key.bytes().chain([0xff]).chain(node.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

/// The candidate with the highest score for `key`. Equal scores go to the smaller name.
pub fn rendezvous_choice<'a, T>(
    key: &str,
    candidates: impl IntoIterator<Item = &'a T>,
) -> Option<&'a T>
where
    T: AsRef<str> + Ord + 'a,
{
    candidates.into_iter().max_by(|left, right| {
        rendezvous_score(key, left.as_ref())
            .cmp(&rendezvous_score(key, right.as_ref()))
            .then_with(|| right.cmp(left))
    })
}

/// A node's labels: what the node reports of itself, with what its record in the store says
/// over it, key by key.
#[must_use]
pub fn merge_labels(
    reported: &BTreeMap<String, String>,
    desired: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut labels = reported.clone();
    labels.extend(
        desired
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    labels
}

/// A node, as placement sees it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Candidate {
    /// Its labels.
    pub labels: BTreeMap<String, String>,
    /// Whether this node has heard from it lately. The local node always is.
    pub live: bool,
    /// Whether it takes new work.
    pub schedulable: bool,
    /// The workload kinds an executor on it runs.
    pub kinds: BTreeSet<WorkloadKind>,
}

/// The cluster, as one node sees it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct View {
    /// Every node it knows.
    pub nodes: BTreeMap<NodeName, Candidate>,
    /// Which node each resource is on.
    pub resource_hosts: BTreeMap<String, NodeName>,
}

/// What a workload needs of the node that runs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Needs {
    /// The workload.
    pub name: WorkloadName,
    /// Its kind: the node must have an executor for it.
    pub kind: WorkloadKind,
    /// The node it was told to run on, if it was told one.
    pub node: Option<NodeName>,
    /// Labels the node must have. An empty value asks only that the label is there.
    pub labels: Vec<(String, String)>,
    /// A resource whose node it must run on.
    pub colocate: Option<String>,
    /// Resources it must hold, wherever they are.
    pub resources: Vec<String>,
}

impl Needs {
    /// What `workload` needs. Its labels under [`LABEL_RESOURCE`] and [`LABEL_COLOCATE`] name
    /// resources; the rest are labels a node must have.
    #[must_use]
    pub fn of(workload: &Workload) -> Self {
        let mut needs = Self {
            name: workload.name.clone(),
            kind: workload.kind.clone(),
            node: workload.node.clone(),
            labels: Vec::new(),
            colocate: None,
            resources: Vec::new(),
        };
        for (key, value) in &workload.labels {
            match key.as_str() {
                LABEL_RESOURCE => needs.resources.push(value.clone()),
                LABEL_COLOCATE => needs.colocate = Some(value.clone()),
                _ => needs.labels.push((key.clone(), value.clone())),
            }
        }
        needs
    }
}

impl View {
    /// Whether `node` can run what `needs` describes.
    #[must_use]
    pub fn eligible(&self, node: &NodeName, needs: &Needs) -> bool {
        let Some(candidate) = self.nodes.get(node) else {
            return false;
        };
        candidate.live
            && candidate.schedulable
            && candidate.kinds.contains(&needs.kind)
            && needs.labels.iter().all(|(key, value)| {
                candidate
                    .labels
                    .get(key)
                    .is_some_and(|has| value.is_empty() || has == value)
            })
            && needs
                .colocate
                .as_ref()
                .is_none_or(|resource| self.resource_hosts.get(resource) == Some(node))
    }

    /// Every node that can run it, by name.
    #[must_use]
    pub fn eligible_nodes(&self, needs: &Needs) -> Vec<NodeName> {
        self.nodes
            .keys()
            .filter(|node| self.eligible(node, needs))
            .cloned()
            .collect()
    }

    /// The node every observer of this view chooses, if any can run it.
    #[must_use]
    pub fn choose(&self, needs: &Needs) -> Option<NodeName> {
        let key = needs.name.as_str();
        self.eligible_nodes(needs)
            .into_iter()
            .max_by(|left, right| {
                rendezvous_score(key, left.as_str())
                    .cmp(&rendezvous_score(key, right.as_str()))
                    .then_with(|| right.cmp(left))
            })
    }
}

/// Why the local node takes a workload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reason {
    /// Nobody had it.
    Placed,
    /// The node that had it is gone or can no longer run it.
    Failover {
        /// The node that had it.
        from: NodeName,
    },
    /// The workload names this node.
    Told,
}

/// What the local node should do about a workload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Nothing: it is where it should be, it is another node's to take, or it is too soon.
    Nothing,
    /// Say "this one is mine": write the assignment to the local node.
    Take(Reason),
}

/// Placement on one node: the rule, and what it has to remember between looks, which is
/// since when each workload's node has not been able to run it.
#[derive(Debug)]
pub struct Coordinator {
    local: NodeName,
    grace_ms: u64,
    /// Since when a workload's node has been ineligible, on the caller's clock.
    ineligible_since: BTreeMap<WorkloadName, u64>,
}

impl Coordinator {
    /// Placement on the node `local`, waiting `grace_ms` before a takeover.
    #[must_use]
    pub const fn new(local: NodeName, grace_ms: u64) -> Self {
        Self {
            local,
            grace_ms,
            ineligible_since: BTreeMap::new(),
        }
    }

    /// When the next grace period ends: when to look again even if nothing changes.
    #[must_use]
    pub fn next_deadline(&self) -> Option<u64> {
        self.ineligible_since
            .values()
            .map(|since| since.saturating_add(self.grace_ms))
            .min()
    }

    /// Forgets a workload that is gone.
    pub fn forget(&mut self, workload: &WorkloadName) {
        self.ineligible_since.remove(workload);
    }

    /// Looks at one workload: where it is assigned now (`None` for nowhere), what it needs,
    /// and the cluster as this node sees it at `now_ms`.
    pub fn step(
        &mut self,
        view: &View,
        needs: &Needs,
        assigned: Option<&NodeName>,
        now_ms: u64,
    ) -> Step {
        if let Some(told) = &needs.node {
            // It names its node. That node runs it, whatever the node is like; nobody else
            // ever does.
            self.ineligible_since.remove(&needs.name);
            return if *told == self.local && assigned != Some(told) {
                Step::Take(Reason::Told)
            } else {
                Step::Nothing
            };
        }
        let Some(assignee) = assigned else {
            self.ineligible_since.remove(&needs.name);
            return if view.choose(needs).as_ref() == Some(&self.local) {
                Step::Take(Reason::Placed)
            } else {
                Step::Nothing
            };
        };
        if view.eligible(assignee, needs) {
            // It stays, whoever would win it now.
            self.ineligible_since.remove(&needs.name);
            return Step::Nothing;
        }
        let since = *self
            .ineligible_since
            .entry(needs.name.clone())
            .or_insert(now_ms);
        if now_ms.saturating_sub(since) < self.grace_ms {
            return Step::Nothing;
        }
        if view.choose(needs).as_ref() == Some(&self.local) {
            self.ineligible_since.remove(&needs.name);
            Step::Take(Reason::Failover {
                from: assignee.clone(),
            })
        } else {
            Step::Nothing
        }
    }
}

/// A resource a workload could be given.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Offer {
    /// The resource.
    pub name: String,
    /// The node it is on.
    pub node: NodeName,
    /// Whether it is ready.
    pub ready: bool,
    /// Who holds it now, if anyone.
    pub holder: Option<String>,
}

/// The name a workload on a node holds resources under.
#[must_use]
pub fn holder_name(node: &NodeName, workload: &WorkloadName) -> String {
    format!("{node}/{workload}")
}

/// The node and workload in a holder's name, if it is a workload's.
#[must_use]
pub fn holder_parts(holder: &str) -> Option<(NodeName, WorkloadName)> {
    let (node, workload) = holder.split_once('/')?;
    Some((node.parse().ok()?, workload.parse().ok()?))
}

/// Which of `offers` the workload held under `holder` is given: one it already holds, or
/// else the ready, free one on a live node with the highest score. Workloads spread over
/// equal resources, and every node gives the same answer.
#[must_use]
pub fn choose_resource<'a>(
    workload: &WorkloadName,
    holder: &str,
    offers: &'a [Offer],
    view: &View,
) -> Option<&'a Offer> {
    if let Some(held) = offers
        .iter()
        .find(|offer| offer.holder.as_deref() == Some(holder))
    {
        return Some(held);
    }
    offers
        .iter()
        .filter(|offer| {
            offer.ready
                && offer.holder.is_none()
                && view.nodes.get(&offer.node).is_some_and(|node| node.live)
        })
        .max_by(|left, right| {
            rendezvous_score(workload.as_str(), &left.name)
                .cmp(&rendezvous_score(workload.as_str(), &right.name))
                .then_with(|| right.name.cmp(&left.name))
        })
}

/// The resources on the local node whose lease is to be dropped: held by a workload that is
/// not assigned to the node the lease names. The node a resource is on arbitrates it.
/// `assigned` says where a workload is assigned now.
#[must_use]
pub fn stale_leases<'a>(
    local: &NodeName,
    offers: &'a [Offer],
    assigned: impl Fn(&WorkloadName) -> Option<NodeName>,
) -> Vec<&'a str> {
    offers
        .iter()
        .filter(|offer| offer.node == *local)
        .filter(|offer| {
            offer
                .holder
                .as_deref()
                .and_then(holder_parts)
                .is_some_and(|(node, workload)| assigned(&workload).as_ref() != Some(&node))
        })
        .map(|offer| offer.name.as_str())
        .collect()
}
