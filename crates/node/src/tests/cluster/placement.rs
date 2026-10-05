//! Leaderless placement over `orion+tcp` (`docs/placement.md`).

use super::fixtures::*;
use super::*;
use orion::{
    cluster::choose_node,
    control_plane::{LabelRequirement, PlacementReason, WorkloadPlacement},
};

const FAST: Duration = Duration::from_millis(300);
/// Liveness and grace well above a round on a loaded machine, for tests where nothing may move.
const STEADY: Duration = Duration::from_millis(3_000);

fn decision_reason(node: &ClusterNode, workload_id: &str) -> Option<PlacementReason> {
    node.workload(workload_id)?
        .placement?
        .decision
        .map(|decision| decision.reason)
}

#[tokio::test]
async fn selector_placement_converges_to_the_same_node_everywhere() {
    let a = tcp_cluster_node("node-a", "zone=north", FAST, FAST).await;
    let b = tcp_cluster_node("node-b", "zone=south,gpu", FAST, FAST).await;
    let c = tcp_cluster_node("node-c", "zone=south", FAST, FAST).await;
    let all = [&a, &b, &c];
    mesh(&all);
    rounds(&all, 2).await;

    let south = WorkloadPlacement::any().require_label(LabelRequirement::equals("zone", "south"));
    let gpu = WorkloadPlacement::any().require_label(LabelRequirement::exists("gpu"));
    write_local(
        &a.app,
        DesiredStateMutation::PutWorkload(placed_workload("workload.south", south.clone())),
    );
    write_local(
        &c.app,
        DesiredStateMutation::PutWorkload(placed_workload("workload.gpu", gpu)),
    );
    rounds_until(&all, Duration::from_secs(10), "placement", || {
        all.iter().all(|node| {
            node.assignee("workload.south").is_some() && node.assignee("workload.gpu").is_some()
        })
    })
    .await;
    rounds(&all, 2).await;

    let south_node = agreed_assignee(&all, "workload.south").expect("placed");
    assert!(
        south_node == b.id() || south_node == c.id(),
        "selector respected"
    );
    // Exactly the node every observer computes from the converged view.
    let expected = choose_node(
        &placed_workload("workload.south", south),
        &a.app.cluster_view(),
    );
    assert_eq!(Some(south_node.clone()), expected);
    assert_eq!(agreed_assignee(&all, "workload.gpu"), Some(b.id()));
    assert_eq!(
        decision_reason(&a, "workload.gpu"),
        Some(PlacementReason::Placed)
    );

    // The chosen node runs it, nobody else does.
    for node in all {
        assert_eq!(
            node.executor
                .running()
                .contains_key(&WorkloadId::new("workload.south")),
            node.id() == south_node
        );
    }
    for node in [a, b, c] {
        node.shutdown().await;
    }
}

#[tokio::test]
async fn colocation_follows_the_resource() {
    let grace = Duration::from_millis(600);
    let a = tcp_cluster_node("node-a", "", FAST, grace).await;
    let b = tcp_cluster_node("node-b", "", FAST, grace).await;
    let c = tcp_cluster_node("node-c", "", FAST, grace).await;
    let all = [&a, &b, &c];
    let provider_b = ListProvider::new("provider.cam-b", "node-b");
    let provider_c = ListProvider::new("provider.cam-c", "node-c");
    provider_b.set(vec![camera(
        "resource.cam",
        "provider.cam-b",
        "tcp://10.0.0.2:5000",
    )]);
    b.app
        .register_provider(provider_b.clone())
        .expect("provider registers");
    c.app
        .register_provider(provider_c.clone())
        .expect("provider registers");
    mesh(&all);
    rounds(&all, 2).await;

    write_local(
        &a.app,
        DesiredStateMutation::PutWorkload(placed_workload(
            "workload.colocated",
            WorkloadPlacement::any().colocate_with("resource.cam"),
        )),
    );
    rounds_until(
        &all,
        Duration::from_secs(10),
        "co-located placement",
        || {
            all.iter()
                .all(|node| node.assignee("workload.colocated") == Some(b.id()))
        },
    )
    .await;
    assert!(
        b.executor
            .running()
            .contains_key(&WorkloadId::new("workload.colocated"))
    );

    // The resource moves to node-c: after the grace period the workload follows it.
    provider_b.set(Vec::new());
    provider_c.set(vec![camera(
        "resource.cam",
        "provider.cam-c",
        "tcp://10.0.0.3:5000",
    )]);
    rounds_until(
        &all,
        Duration::from_secs(15),
        "workload to follow the resource",
        || {
            all.iter()
                .all(|node| node.assignee("workload.colocated") == Some(c.id()))
        },
    )
    .await;
    assert_eq!(
        decision_reason(&a, "workload.colocated"),
        Some(PlacementReason::Failover { from: b.id() })
    );
    rounds(&all, 2).await;
    assert!(
        c.executor
            .running()
            .contains_key(&WorkloadId::new("workload.colocated")),
        "c starts={:?} running={:?} wl={:?} b={:?}",
        c.executor.starts.lock().unwrap().len(),
        c.executor.running().keys().collect::<Vec<_>>(),
        c.workload("workload.colocated"),
        b.executor.running().keys().collect::<Vec<_>>()
    );
    assert!(
        !b.executor
            .running()
            .contains_key(&WorkloadId::new("workload.colocated"))
    );
    for node in [a, b, c] {
        node.shutdown().await;
    }
}

#[tokio::test]
async fn failover_after_the_assignee_disappears_respects_the_grace_period() {
    let grace = Duration::from_millis(1_500);
    let a = tcp_cluster_node("node-a", "", FAST, grace).await;
    let b = tcp_cluster_node("node-b", "", FAST, grace).await;
    let c = tcp_cluster_node("node-c", "", FAST, grace).await;
    let all = [&a, &b, &c];
    mesh(&all);
    rounds(&all, 2).await;
    write_local(
        &a.app,
        DesiredStateMutation::PutWorkload(placed_workload(
            "workload.any",
            WorkloadPlacement::any(),
        )),
    );
    rounds_until(&all, Duration::from_secs(10), "placement", || {
        all.iter()
            .all(|node| node.assignee("workload.any").is_some())
    })
    .await;
    let first = agreed_assignee(&all, "workload.any").expect("placed");
    let (gone, survivors): (Vec<&ClusterNode>, Vec<&ClusterNode>) =
        all.iter().copied().partition(|node| node.id() == first);
    let gone = gone[0];

    // The assignee is partitioned away. Until liveness timeout plus grace have passed, the
    // survivors keep the assignment.
    let partitioned_at = std::time::Instant::now();
    while partitioned_at.elapsed() < Duration::from_millis(1_000) {
        round(&survivors).await;
        for node in &survivors {
            assert_eq!(
                node.assignee("workload.any"),
                Some(first.clone()),
                "moved inside the grace period"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    rounds_until(&survivors, Duration::from_secs(15), "failover", || {
        survivors
            .iter()
            .all(|node| node.assignee("workload.any").is_some_and(|id| id != first))
    })
    .await;
    assert!(
        partitioned_at.elapsed() >= FAST + grace,
        "failover waited for the grace period"
    );
    rounds(&survivors, 2).await;
    let second = agreed_assignee(&survivors, "workload.any").expect("re-placed");
    assert_eq!(
        decision_reason(survivors[0], "workload.any"),
        Some(PlacementReason::Failover {
            from: first.clone()
        })
    );
    let runner = survivors
        .iter()
        .find(|node| node.id() == second)
        .expect("survivor");
    assert!(
        runner
            .executor
            .running()
            .contains_key(&WorkloadId::new("workload.any"))
    );

    // The old assignee comes back: it learns the new assignment and stops its copy.
    rounds(&all, 3).await;
    assert_eq!(agreed_assignee(&all, "workload.any"), Some(second));
    assert!(
        !gone
            .executor
            .running()
            .contains_key(&WorkloadId::new("workload.any"))
    );
    for node in [a, b, c] {
        node.shutdown().await;
    }
}

#[tokio::test]
async fn running_workloads_do_not_move_when_nodes_join() {
    let a = tcp_cluster_node("node-a", "", STEADY, STEADY).await;
    let b = tcp_cluster_node("node-b", "", STEADY, STEADY).await;
    mesh(&[&a, &b]);
    rounds(&[&a, &b], 2).await;
    let ids: Vec<String> = (0..12)
        .map(|index| format!("workload.join-{index}"))
        .collect();
    for id in &ids {
        write_local(
            &a.app,
            DesiredStateMutation::PutWorkload(placed_workload(id, WorkloadPlacement::any())),
        );
    }
    rounds_until(&[&a, &b], Duration::from_secs(10), "placement", || {
        ids.iter()
            .all(|id| a.assignee(id).is_some() && b.assignee(id).is_some())
    })
    .await;
    let before: Vec<_> = ids
        .iter()
        .map(|id| agreed_assignee(&[&a, &b], id))
        .collect();

    let c = tcp_cluster_node("node-c", "", STEADY, STEADY).await;
    let d = tcp_cluster_node("node-d", "", STEADY, STEADY).await;
    let e = tcp_cluster_node("node-e", "", STEADY, STEADY).await;
    let all = [&a, &b, &c, &d, &e];
    mesh(&all);
    rounds(&all, 4).await;
    // Keep the cluster busy for longer than the grace period.
    let joined = std::time::Instant::now();
    while joined.elapsed() < STEADY + FAST {
        round(&all).await;
    }
    rounds(&all, 2).await;

    let after: Vec<_> = ids.iter().map(|id| agreed_assignee(&all, id)).collect();
    assert_eq!(before, after, "no running workload moved");
    // Without hysteresis some of them would have moved to a newcomer.
    let view = a.app.cluster_view();
    let would_move = ids
        .iter()
        .zip(&after)
        .filter(|(id, assigned)| {
            choose_node(&placed_workload(id, WorkloadPlacement::any()), &view) != **assigned
        })
        .count();
    assert!(
        would_move > 0,
        "test should include workloads a newcomer would win"
    );
    for node in [a, b, c, d, e] {
        node.shutdown().await;
    }
}

#[tokio::test]
async fn explicit_assignment_wins_over_placement() {
    let a = tcp_cluster_node("node-a", "", FAST, FAST).await;
    let b = tcp_cluster_node("node-b", "", FAST, FAST).await;
    let all = [&a, &b];
    mesh(&all);
    rounds(&all, 2).await;
    let pinned = WorkloadRecord::builder(WorkloadId::new("workload.pinned"), RUNTIME, "artifact.x")
        .desired_state(DesiredState::Running)
        .placement(WorkloadPlacement::any().require_label(LabelRequirement::exists("never")))
        .assigned_to("node-b")
        .build();
    write_local(&a.app, DesiredStateMutation::PutWorkload(pinned.clone()));
    rounds(&all, 3).await;
    assert_eq!(agreed_assignee(&all, "workload.pinned"), Some(b.id()));
    assert!(
        b.executor
            .running()
            .contains_key(&WorkloadId::new("workload.pinned"))
    );

    // node-b disappears for well over liveness timeout plus grace: still pinned.
    let started = std::time::Instant::now();
    while started.elapsed() < FAST * 4 {
        round(&[&a]).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(a.assignee("workload.pinned"), Some(b.id()));
    assert_eq!(a.workload("workload.pinned"), Some(pinned));
    assert!(a.executor.running().is_empty());
    for node in [a, b] {
        node.shutdown().await;
    }
}
