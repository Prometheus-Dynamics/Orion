//! Cross-node binding over `orion+tcp` (`docs/placement.md`, "Cross-node binding").

use super::fixtures::*;
use super::*;
use orion::control_plane::{LeaseHolder, ResourceOwnershipMode};

const FAST: Duration = Duration::from_millis(300);

fn camera_consumer(id: &str, node: &str) -> WorkloadRecord {
    WorkloadRecord::builder(WorkloadId::new(id), RUNTIME, "artifact.consumer")
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new(node))
        .require_resource_with_ownership("camera", 1, ResourceOwnershipMode::Exclusive)
        .build()
}

fn holders(node: &ClusterNode, resource_id: &str) -> Vec<LeaseHolder> {
    node.lease(resource_id)
        .map(|lease| lease.holders)
        .unwrap_or_default()
}

/// A cluster where only node-b has a camera.
async fn camera_cluster(grace: Duration) -> (ClusterNode, ClusterNode, ClusterNode, ListProvider) {
    let a = tcp_cluster_node("node-a", "", FAST, grace).await;
    let b = tcp_cluster_node("node-b", "", FAST, grace).await;
    let c = tcp_cluster_node("node-c", "", FAST, grace).await;
    let provider = ListProvider::new("provider.cam-b", "node-b");
    provider.set(vec![camera(
        "resource.cam-b",
        "provider.cam-b",
        "tcp://10.0.0.2:5000",
    )]);
    b.app
        .register_provider(provider.clone())
        .expect("provider registers");
    mesh(&[&a, &b, &c]);
    rounds(&[&a, &b, &c], 2).await;
    (a, b, c, provider)
}

#[tokio::test]
async fn workload_binds_a_remote_resource_and_both_sides_see_the_lease() {
    let (a, b, c, _provider) = camera_cluster(Duration::from_secs(5)).await;
    let all = [&a, &b, &c];
    write_local(
        &a.app,
        DesiredStateMutation::PutWorkload(camera_consumer("workload.viewer", "node-a")),
    );
    rounds_until(&all, Duration::from_secs(10), "remote binding", || {
        a.executor
            .bindings("workload.viewer")
            .is_some_and(|b| !b.is_empty())
    })
    .await;
    rounds(&all, 2).await;

    // The lease is ordinary desired state: every node agrees on it.
    let expected = vec![LeaseHolder::new("node-a", "workload.viewer")];
    for node in all {
        assert_eq!(holders(node, "resource.cam-b"), expected, "{}", node.id());
    }

    // The executor on node-a sees the remote resource with its owner and endpoints.
    let bindings = a.executor.bindings("workload.viewer").expect("running");
    assert_eq!(bindings.len(), 1);
    let binding = &bindings[0];
    assert_eq!(binding.resource_id.as_str(), "resource.cam-b");
    assert_eq!(binding.node_id, b.id());
    assert!(binding.is_remote() && binding.is_available());
    assert_eq!(
        binding
            .remote
            .as_ref()
            .map(|remote| remote.endpoints.clone()),
        Some(vec!["tcp://10.0.0.2:5000".to_string()])
    );
    // The executor watch delivers the same binding.
    let delivered = a
        .app
        .current_executor_workloads(&ExecutorId::new("executor.node-a"))
        .expect("executor known");
    assert_eq!(delivered[0].resource_bindings, bindings);

    // The provider on node-b sees the lease held by the remote workload.
    let leases = b
        .app
        .current_provider_leases(&ProviderId::new("provider.cam-b"));
    assert_eq!(leases.len(), 1);
    assert!(leases[0].is_held_by(&a.id(), &WorkloadId::new("workload.viewer")));

    // Stopping the workload releases the lease everywhere.
    let mut stopped = camera_consumer("workload.viewer", "node-a");
    stopped.desired_state = DesiredState::Stopped;
    write_local(&a.app, DesiredStateMutation::PutWorkload(stopped));
    rounds_until(&all, Duration::from_secs(10), "lease release", || {
        all.iter()
            .all(|node| node.lease("resource.cam-b").is_none())
    })
    .await;
    for node in [a, b, c] {
        node.shutdown().await;
    }
}

#[tokio::test]
async fn competing_workloads_for_an_exclusive_resource_resolve_deterministically() {
    let (a, b, c, _provider) = camera_cluster(Duration::from_secs(5)).await;
    let all = [&a, &b, &c];
    write_local(
        &a.app,
        DesiredStateMutation::PutWorkload(camera_consumer("workload.on-a", "node-a")),
    );
    write_local(
        &c.app,
        DesiredStateMutation::PutWorkload(camera_consumer("workload.on-c", "node-c")),
    );
    // Both nodes lease the camera before they hear of each other's lease.
    a.app.tick_async().await.expect("reconcile");
    c.app.tick_async().await.expect("reconcile");
    assert_eq!(holders(&a, "resource.cam-b").len(), 1);
    assert_eq!(holders(&c, "resource.cam-b").len(), 1);

    rounds(&all, 4).await;
    let winner = holders(&a, "resource.cam-b");
    assert_eq!(winner.len(), 1, "an exclusive resource has one holder");
    for node in all {
        assert_eq!(holders(node, "resource.cam-b"), winner, "{}", node.id());
    }
    let (holder_node, loser_node, holder_workload, loser_workload) = if winner[0].node_id == a.id()
    {
        (&a, &c, "workload.on-a", "workload.on-c")
    } else {
        (&c, &a, "workload.on-c", "workload.on-a")
    };
    assert_eq!(winner[0].workload_id.as_str(), holder_workload);
    assert!(holder_node.executor.bindings(holder_workload).is_some());
    assert!(
        loser_node.executor.bindings(loser_workload).is_none(),
        "the losing workload never binds the camera"
    );
    for node in [a, b, c] {
        node.shutdown().await;
    }
}

#[tokio::test]
async fn binding_becomes_unavailable_when_the_owner_disappears_and_recovers() {
    let (a, b, c, _provider) = camera_cluster(Duration::from_secs(30)).await;
    let all = [&a, &b, &c];
    write_local(
        &a.app,
        DesiredStateMutation::PutWorkload(camera_consumer("workload.viewer", "node-a")),
    );
    rounds_until(&all, Duration::from_secs(10), "remote binding", || {
        a.executor
            .bindings("workload.viewer")
            .is_some_and(|bindings| bindings.iter().any(|b| b.is_available()))
    })
    .await;

    // node-b is partitioned away: after the liveness timeout the binding is unavailable, but
    // the lease is kept for the (long) grace period.
    rounds_until(
        &[&a, &c],
        Duration::from_secs(10),
        "binding to become unavailable",
        || {
            a.executor
                .bindings("workload.viewer")
                .is_some_and(|bindings| bindings.len() == 1 && !bindings[0].is_available())
        },
    )
    .await;
    assert_eq!(
        holders(&a, "resource.cam-b"),
        vec![LeaseHolder::new("node-a", "workload.viewer")]
    );
    assert!(a.app.unreachable_peers().contains_key(&b.id()));

    // node-b is back: the binding recovers without a new lease.
    rounds_until(&all, Duration::from_secs(10), "binding to recover", || {
        a.executor
            .bindings("workload.viewer")
            .is_some_and(|bindings| bindings.len() == 1 && bindings[0].is_available())
    })
    .await;
    assert_eq!(
        holders(&b, "resource.cam-b"),
        vec![LeaseHolder::new("node-a", "workload.viewer")]
    );
    for node in [a, b, c] {
        node.shutdown().await;
    }
}

#[tokio::test]
async fn binding_moves_to_another_owner_after_the_grace_period() {
    let grace = Duration::from_millis(600);
    let (a, b, c, _provider) = camera_cluster(grace).await;
    let provider_c = ListProvider::new("provider.cam-c", "node-c");
    provider_c.set(vec![camera(
        "resource.cam-c",
        "provider.cam-c",
        "tcp://10.0.0.3:5000",
    )]);
    c.app
        .register_provider(provider_c)
        .expect("provider registers");
    let all = [&a, &b, &c];
    rounds(&all, 2).await;
    write_local(
        &a.app,
        DesiredStateMutation::PutWorkload(camera_consumer("workload.viewer", "node-a")),
    );
    rounds_until(&all, Duration::from_secs(10), "remote binding", || {
        a.executor
            .bindings("workload.viewer")
            .is_some_and(|b| b.len() == 1)
    })
    .await;
    let first = a.executor.bindings("workload.viewer").expect("bound")[0].clone();
    let (gone, survivor) = if first.node_id == b.id() {
        (&b, &c)
    } else {
        (&c, &b)
    };
    let _ = gone;

    // The owner disappears for longer than the grace period: the lease is released and the
    // workload re-resolves to the other camera.
    rounds_until(
        &[&a, survivor],
        Duration::from_secs(15),
        "re-resolution",
        || {
            a.executor
                .bindings("workload.viewer")
                .is_some_and(|bindings| {
                    bindings.len() == 1
                        && bindings[0].node_id == survivor.id()
                        && bindings[0].is_available()
                })
        },
    )
    .await;
    assert!(
        a.lease(first.resource_id.as_str()).is_none(),
        "the stale lease was released"
    );
    for node in [a, b, c] {
        node.shutdown().await;
    }
}
