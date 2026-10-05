use super::leases::{LeaseEdit, arbitrate_owned_leases, remote_candidates, with_holder};
use super::*;
use alloc::{
    collections::{BTreeMap, BTreeSet},
    format,
    string::String,
    vec,
    vec::Vec,
};
use orion_control_plane::{
    AvailabilityState, DesiredClusterState, DesiredState, ExecutorRecord, LabelRequirement,
    LeaseHolder, NodeRecord, ObservedClusterState, PlacementReason, ProviderRecord,
    ResourceBinding, ResourceOwnershipMode, ResourceRecord, WorkloadObservedState,
    WorkloadPlacement, WorkloadRecord, WorkloadRequirement,
};
use orion_core::{NodeId, ResourceId, WorkloadId};

const RUNTIME: &str = "graph.exec.v1";

fn node(labels: &[&str]) -> NodeCandidate {
    NodeCandidate {
        labels: labels.iter().map(|label| String::from(*label)).collect(),
        schedulable: true,
        live: true,
        runtime_types: [RUNTIME.into()].into_iter().collect(),
    }
}

fn view(nodes: &[(&str, &[&str])]) -> ClusterView {
    ClusterView {
        nodes: nodes
            .iter()
            .map(|(id, labels)| (NodeId::new(*id), node(labels)))
            .collect(),
        resource_hosts: BTreeMap::new(),
    }
}

fn workload(id: &str, placement: WorkloadPlacement) -> WorkloadRecord {
    WorkloadRecord::builder(WorkloadId::new(id), RUNTIME, "artifact.x")
        .desired_state(DesiredState::Running)
        .placement(placement)
        .build()
}

/// SplitMix64, so the property test is reproducible from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

#[test]
fn selector_filters_by_label_equality_and_existence() {
    let view = view(&[
        ("node-a", &["zone=north"]),
        ("node-b", &["zone=south", "gpu"]),
        ("node-c", &["zone=south"]),
    ]);
    let gpu_south = workload(
        "workload.gpu",
        WorkloadPlacement::any()
            .require_label(LabelRequirement::equals("zone", "south"))
            .require_label(LabelRequirement::exists("gpu")),
    );
    assert_eq!(eligible_nodes(&gpu_south, &view), [NodeId::new("node-b")]);
    assert_eq!(choose_node(&gpu_south, &view), Some(NodeId::new("node-b")));
    assert_eq!(
        eligibility(&gpu_south, &NodeId::new("node-a"), &view),
        Err(Ineligibility::SelectorMismatch(LabelRequirement::equals(
            "zone", "south"
        )))
    );
}

#[test]
fn unreachable_unschedulable_and_unsupported_nodes_are_ineligible() {
    let mut view = view(&[("node-a", &[]), ("node-b", &[]), ("node-c", &[])]);
    view.nodes.get_mut(&NodeId::new("node-a")).unwrap().live = false;
    view.nodes
        .get_mut(&NodeId::new("node-b"))
        .unwrap()
        .schedulable = false;
    view.nodes
        .get_mut(&NodeId::new("node-c"))
        .unwrap()
        .runtime_types
        .clear();
    let any = workload("workload.any", WorkloadPlacement::any());
    assert!(eligible_nodes(&any, &view).is_empty());
    assert_eq!(choose_node(&any, &view), None);
}

#[test]
fn colocation_follows_the_resource_host() {
    let mut desired = DesiredClusterState::default();
    for id in ["node-a", "node-b"] {
        desired.put_executor(
            ExecutorRecord::builder(
                orion_core::ExecutorId::new(format!("executor.{id}")),
                NodeId::new(id),
            )
            .runtime_type(RUNTIME)
            .build(),
        );
    }
    desired.put_provider(ProviderRecord::builder("provider.cam", "node-b").build());
    let mut observed = ObservedClusterState::default();
    observed
        .put_resource(ResourceRecord::builder("resource.cam", "camera", "provider.cam").build());
    let view = ClusterView::from_state(&NodeId::new("node-a"), &desired, &observed, true, |_| true);
    let colocated = workload(
        "workload.colocated",
        WorkloadPlacement::any().colocate_with("resource.cam"),
    );
    assert_eq!(choose_node(&colocated, &view), Some(NodeId::new("node-b")));

    // The provider moves to node-a: the workload follows.
    desired.put_provider(ProviderRecord::builder("provider.cam", "node-a").build());
    let view = ClusterView::from_state(&NodeId::new("node-a"), &desired, &observed, true, |_| true);
    assert_eq!(choose_node(&colocated, &view), Some(NodeId::new("node-a")));
}

#[test]
fn desired_labels_override_reported_labels_per_key() {
    let mut desired = DesiredClusterState::default();
    desired.put_node(NodeRecord::builder("node-a").label("zone=south").build());
    let mut observed = ObservedClusterState::default();
    observed.put_node(
        NodeRecord::builder("node-a")
            .label("zone=north")
            .label("gpu")
            .build(),
    );
    let view = ClusterView::from_state(&NodeId::new("node-a"), &desired, &observed, true, |_| true);
    assert_eq!(
        view.nodes[&NodeId::new("node-a")].labels,
        ["gpu", "zone=south"]
    );
}

#[test]
fn rendezvous_choice_is_deterministic_and_minimally_disruptive_property() {
    let mut rng = Rng(0x0510_7a11);
    for _ in 0..300 {
        let count = 1 + rng.below(12) as usize;
        let ids: BTreeSet<String> = (0..count)
            .map(|_| format!("node-{}", rng.below(40)))
            .collect();
        let labels = ["zone=a", "zone=b"];
        let nodes: Vec<(String, &str)> = ids
            .iter()
            .map(|id| (id.clone(), labels[rng.below(2) as usize]))
            .collect();
        let build = |order: &[(String, &str)]| ClusterView {
            nodes: order
                .iter()
                .map(|(id, label)| (NodeId::new(id.as_str()), node(&[label])))
                .collect(),
            resource_hosts: BTreeMap::new(),
        };
        let forward = build(&nodes);
        let mut reversed = nodes.clone();
        reversed.reverse();
        let backward = build(&reversed);
        for index in 0..8 {
            let selector = if index % 2 == 0 {
                WorkloadPlacement::any()
            } else {
                WorkloadPlacement::any().require_label(LabelRequirement::equals("zone", "a"))
            };
            let workload = workload(&format!("workload.{}", rng.next()), selector);
            let chosen = choose_node(&workload, &forward);
            // Same answer from every observer, whatever order it learned the nodes in.
            assert_eq!(chosen, choose_node(&workload, &backward));
            // The answer is an eligible node, and exists whenever any node is eligible.
            let eligible = eligible_nodes(&workload, &forward);
            assert_eq!(chosen.is_some(), !eligible.is_empty());
            if let Some(chosen) = chosen {
                assert!(eligible.contains(&chosen));
                // Removing a node that was not chosen never changes the choice.
                for other in eligible.iter().filter(|id| **id != chosen) {
                    let mut without = forward.clone();
                    without.nodes.remove(other);
                    assert_eq!(choose_node(&workload, &without).as_ref(), Some(&chosen));
                }
                // Adding a node either keeps the choice or picks the new node.
                let mut with = forward.clone();
                let newcomer = NodeId::new(format!("node-new-{}", rng.next()));
                with.nodes.insert(newcomer.clone(), node(&["zone=a"]));
                let after = choose_node(&workload, &with).expect("still eligible");
                assert!(after == chosen || after == newcomer);
            }
        }
    }
}

fn coordinator_state(nodes: &[&str]) -> DesiredClusterState {
    let mut desired = DesiredClusterState::default();
    for id in nodes {
        desired.put_executor(
            ExecutorRecord::builder(
                orion_core::ExecutorId::new(format!("executor.{id}")),
                NodeId::new(*id),
            )
            .runtime_type(RUNTIME)
            .build(),
        );
    }
    desired
}

fn live_view(desired: &DesiredClusterState, local: &str, live: &[&str]) -> ClusterView {
    let live: BTreeSet<NodeId> = live.iter().map(|id| NodeId::new(*id)).collect();
    ClusterView::from_state(
        &NodeId::new(local),
        desired,
        &ObservedClusterState::default(),
        true,
        |node_id| live.contains(node_id),
    )
}

#[test]
fn only_the_chosen_node_writes_and_running_workloads_do_not_flap() {
    let mut desired = coordinator_state(&["node-a", "node-b"]);
    let any = workload("workload.any", WorkloadPlacement::any());
    desired.put_workload(any.clone());
    let view = live_view(&desired, "node-a", &["node-b"]);
    let chosen = choose_node(&any, &view).expect("a node is eligible");

    let mut writers = Vec::new();
    for local in ["node-a", "node-b"] {
        let view = live_view(&desired, local, &["node-a", "node-b"]);
        let mut coordinator = ClusterCoordinator::new(NodeId::new(local), 1_000);
        writers.extend(coordinator.plan(&desired, &view, 0));
    }
    assert_eq!(writers.len(), 1, "exactly one node writes the assignment");
    assert_eq!(writers[0].assigned_node_id.as_ref(), Some(&chosen));
    desired.put_workload(writers[0].clone());

    // A third node joins and would win the hash: nothing moves while the assignee is eligible.
    for joiner in ["node-c", "node-d", "node-e", "node-f"] {
        desired.put_executor(
            ExecutorRecord::builder(
                orion_core::ExecutorId::new(format!("executor.{joiner}")),
                NodeId::new(joiner),
            )
            .runtime_type(RUNTIME)
            .build(),
        );
    }
    let everyone = ["node-a", "node-b", "node-c", "node-d", "node-e", "node-f"];
    for local in everyone {
        let view = live_view(&desired, local, &everyone);
        let mut coordinator = ClusterCoordinator::new(NodeId::new(local), 0);
        assert!(coordinator.plan(&desired, &view, 0).is_empty());
    }
}

#[test]
fn failover_waits_for_the_grace_period_and_explicit_assignment_wins() {
    let mut desired = coordinator_state(&["node-a", "node-b"]);
    let mut placed = workload("workload.placed", WorkloadPlacement::any());
    placed = ClusterCoordinator::new(NodeId::new("node-b"), 0)
        .assign_in_place(placed, PlacementReason::Placed);
    desired.put_workload(placed);
    let explicit = WorkloadRecord::builder("workload.pinned", RUNTIME, "artifact.x")
        .desired_state(DesiredState::Running)
        .placement(WorkloadPlacement::any())
        .assigned_to("node-b")
        .build();
    desired.put_workload(explicit);

    // node-b is gone from node-a's point of view.
    let view = live_view(&desired, "node-a", &[]);
    let mut coordinator = ClusterCoordinator::new(NodeId::new("node-a"), 500);
    assert!(coordinator.plan(&desired, &view, 1_000).is_empty());
    assert!(coordinator.plan(&desired, &view, 1_400).is_empty());
    let writes = coordinator.plan(&desired, &view, 1_500);
    assert_eq!(writes.len(), 1, "only the placement-managed workload moves");
    assert_eq!(writes[0].workload_id, WorkloadId::new("workload.placed"));
    assert_eq!(writes[0].assigned_node_id, Some(NodeId::new("node-a")));
    assert_eq!(
        writes[0]
            .placement
            .as_ref()
            .unwrap()
            .decision
            .as_ref()
            .unwrap()
            .reason,
        PlacementReason::Failover {
            from: NodeId::new("node-b")
        }
    );

    // node-b returning inside the grace period resets the clock.
    let mut coordinator = ClusterCoordinator::new(NodeId::new("node-a"), 500);
    assert!(coordinator.plan(&desired, &view, 0).is_empty());
    let back = live_view(&desired, "node-a", &["node-b"]);
    assert!(coordinator.plan(&desired, &back, 300).is_empty());
    assert!(coordinator.plan(&desired, &view, 600).is_empty());
    assert_eq!(coordinator.plan(&desired, &view, 1_100).len(), 1);
}

fn binding_state() -> (DesiredClusterState, ObservedClusterState) {
    let mut desired = coordinator_state(&["node-a", "node-b", "node-c"]);
    desired.put_provider(ProviderRecord::builder("provider.cam", "node-b").build());
    for (id, node) in [
        ("workload.a", "node-a"),
        ("workload.c", "node-c"),
        ("workload.b", "node-b"),
    ] {
        desired.put_workload(
            WorkloadRecord::builder(WorkloadId::new(id), RUNTIME, "artifact.x")
                .desired_state(DesiredState::Running)
                .assigned_to(node)
                .require_resource_with_ownership("camera", 1, ResourceOwnershipMode::Exclusive)
                .build(),
        );
    }
    let mut observed = ObservedClusterState::default();
    observed.put_resource(
        ResourceRecord::builder("resource.cam", "camera", "provider.cam")
            .ownership_mode(ResourceOwnershipMode::Exclusive)
            .availability(AvailabilityState::Available)
            .build(),
    );
    (desired, observed)
}

fn requirement() -> WorkloadRequirement {
    WorkloadRequirement::new("camera", 1).with_ownership_mode(ResourceOwnershipMode::Exclusive)
}

#[test]
fn exclusive_remote_resource_admits_one_holder_and_owner_evicts_overflow() {
    let (mut desired, mut observed) = binding_state();
    let resource_id = ResourceId::new("resource.cam");
    let a = desired.workloads[&WorkloadId::new("workload.a")].clone();
    let candidates = remote_candidates(
        &NodeId::new("node-a"),
        &a,
        &requirement(),
        &desired,
        &observed,
        |_| true,
        &BTreeSet::new(),
    );
    assert_eq!(candidates, [(resource_id.clone(), NodeId::new("node-b"))]);
    // Unreachable owners are not candidates.
    assert!(
        remote_candidates(
            &NodeId::new("node-a"),
            &a,
            &requirement(),
            &desired,
            &observed,
            |_| false,
            &BTreeSet::new()
        )
        .is_empty()
    );

    desired.put_lease(with_holder(
        &resource_id,
        None,
        LeaseHolder::new("node-a", "workload.a"),
    ));
    let c = desired.workloads[&WorkloadId::new("workload.c")].clone();
    assert!(
        remote_candidates(
            &NodeId::new("node-c"),
            &c,
            &requirement(),
            &desired,
            &observed,
            |_| true,
            &BTreeSet::new()
        )
        .is_empty(),
        "a full exclusive lease admits nobody else"
    );

    // A concurrent write left two holders: the owner keeps the first in sorted order.
    let both = with_holder(
        &resource_id,
        desired.leases.get(&resource_id),
        LeaseHolder::new("node-c", "workload.c"),
    );
    desired.put_lease(both);
    let edits = arbitrate_owned_leases(&NodeId::new("node-b"), &desired, &observed);
    assert_eq!(
        edits,
        [LeaseEdit::Put(orion_control_plane::LeaseRecord::held_by(
            resource_id.clone(),
            vec![LeaseHolder::new("node-a", "workload.a")]
        ))]
    );
    // Non-owners never arbitrate.
    assert!(arbitrate_owned_leases(&NodeId::new("node-a"), &desired, &observed).is_empty());

    // The owner's own running workload binds the resource locally: it counts first.
    let mut local = desired.workloads[&WorkloadId::new("workload.b")].clone();
    local.observed_state = WorkloadObservedState::Running;
    local.resource_bindings = vec![ResourceBinding::new("resource.cam", "node-b")];
    observed.put_workload(local);
    let edits = arbitrate_owned_leases(&NodeId::new("node-b"), &desired, &observed);
    assert_eq!(edits, [LeaseEdit::Remove(resource_id)]);
}
