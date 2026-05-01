mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use support::docker_cluster::{ClusterOptions, DockerCluster, stopped_workload};

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_flapping_node_catches_latest_state() {
    let cluster = DockerCluster::start("flapping-node").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster.stop_service("node-b");
    cluster
        .put_workload("node-a", stopped_workload("workload.during-flap", "node-a"))
        .await;

    cluster.start_service("node-b");
    cluster.wait_for_http("node-b").await;
    cluster
        .wait_for_workload("node-b", "workload.during-flap", Duration::from_secs(20))
        .await;

    cluster.stop_service("node-b");
    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.after-second-flap", "node-a"),
        )
        .await;

    cluster.start_service("node-b");
    cluster.wait_for_http("node-b").await;
    cluster
        .wait_for_workload(
            "node-b",
            "workload.after-second-flap",
            Duration::from_secs(20),
        )
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_can_shrink_to_one_node_and_rebuild() {
    let cluster = DockerCluster::start("shrink-rebuild").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster.stop_service("node-b");
    cluster.stop_service("node-c");

    for workload_id in [
        "workload.one-node.1",
        "workload.one-node.2",
        "workload.one-node.3",
    ] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    cluster.start_service("node-b");
    cluster.wait_for_http("node-b").await;
    cluster.start_service("node-c");
    cluster.wait_for_http("node-c").await;

    for node in ["node-b", "node-c"] {
        for workload_id in [
            "workload.one-node.1",
            "workload.one-node.2",
            "workload.one-node.3",
        ] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_rolling_restart_preserves_state() {
    let cluster = DockerCluster::start("rolling-restart").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload("node-a", stopped_workload("workload.before-roll", "node-a"))
        .await;

    for node in ["node-a", "node-b", "node-c"] {
        cluster.restart_service(node);
        cluster.wait_for_http(node).await;
        for target in ["node-a", "node-b", "node-c"] {
            cluster
                .wait_for_workload(target, "workload.before-roll", Duration::from_secs(20))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_two_nodes_down_then_rejoin_to_latest() {
    let cluster = DockerCluster::start("two-down-rejoin").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster.stop_service("node-b");
    cluster.stop_service("node-c");

    for workload_id in ["workload.rejoin.1", "workload.rejoin.2"] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    cluster.start_service("node-b");
    cluster.wait_for_http("node-b").await;
    cluster.start_service("node-c");
    cluster.wait_for_http("node-c").await;

    for node in ["node-b", "node-c"] {
        for workload_id in ["workload.rejoin.1", "workload.rejoin.2"] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_node_replacement_cycles_do_not_lose_state() {
    let cluster = DockerCluster::start("replacement-cycles").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.replacement.base", "node-a"),
        )
        .await;

    cluster.restart_service("node-b");
    cluster.wait_for_http("node-b").await;
    cluster
        .wait_for_workload(
            "node-b",
            "workload.replacement.base",
            Duration::from_secs(20),
        )
        .await;

    cluster.restart_service("node-c");
    cluster.wait_for_http("node-c").await;
    cluster
        .wait_for_workload(
            "node-c",
            "workload.replacement.base",
            Duration::from_secs(20),
        )
        .await;

    cluster.restart_service("node-a");
    cluster.wait_for_http("node-a").await;

    for node in ["node-a", "node-b", "node-c"] {
        cluster
            .wait_for_workload(node, "workload.replacement.base", Duration::from_secs(20))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_all_nodes_flap_in_sequence_and_recover() {
    let cluster = DockerCluster::start("sequential-flap").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload("node-a", stopped_workload("workload.pre-flap", "node-a"))
        .await;

    for node in ["node-a", "node-b", "node-c"] {
        cluster.stop_service(node);
        tokio::time::sleep(Duration::from_secs(1)).await;
        cluster.start_service(node);
        cluster.wait_for_http(node).await;
    }

    for node in ["node-a", "node-b", "node-c"] {
        cluster
            .wait_for_workload(node, "workload.pre-flap", Duration::from_secs(20))
            .await;
    }

    cluster
        .put_workload("node-c", stopped_workload("workload.post-flap", "node-c"))
        .await;

    for node in ["node-a", "node-b", "node-c"] {
        cluster
            .wait_for_workload(node, "workload.post-flap", Duration::from_secs(20))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_five_nodes_converge_and_survive_total_node_replacement() {
    let cluster = DockerCluster::start_with_options(
        "five-node-replacement",
        ClusterOptions::with_node_count(5),
    )
    .await;

    cluster.wait_for_all_http().await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.five-node.base", "node-a"),
        )
        .await;

    for node in cluster.nodes().iter().map(String::as_str) {
        cluster
            .wait_for_workload(node, "workload.five-node.base", Duration::from_secs(30))
            .await;
    }

    for node in ["node-b", "node-c", "node-d", "node-e", "node-a"] {
        cluster.stop_service(node);
        tokio::time::sleep(Duration::from_secs(1)).await;
        cluster.start_service(node);
        cluster.wait_for_http(node).await;
        for target in cluster.nodes().iter().map(String::as_str) {
            cluster
                .wait_for_workload(target, "workload.five-node.base", Duration::from_secs(30))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_four_nodes_basic_convergence() {
    let cluster =
        DockerCluster::start_with_options("four-node-converge", ClusterOptions::with_node_count(4))
            .await;

    cluster.wait_for_all_http().await;
    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.four-node.base", "node-a"),
        )
        .await;

    for node in cluster.nodes().iter().map(String::as_str) {
        cluster
            .wait_for_workload(node, "workload.four-node.base", Duration::from_secs(30))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_six_nodes_basic_convergence() {
    let cluster =
        DockerCluster::start_with_options("six-node-converge", ClusterOptions::with_node_count(6))
            .await;

    cluster.wait_for_all_http().await;
    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.six-node.base", "node-a"),
        )
        .await;

    for node in cluster.nodes().iter().map(String::as_str) {
        cluster
            .wait_for_workload(node, "workload.six-node.base", Duration::from_secs(40))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_five_node_ring_topology_converges() {
    let cluster = DockerCluster::start_with_options(
        "five-node-ring",
        ClusterOptions {
            node_ids: vec![
                "node-a".into(),
                "node-b".into(),
                "node-c".into(),
                "node-d".into(),
                "node-e".into(),
            ],
            extra_peers: BTreeMap::new(),
            explicit_peers: BTreeMap::from([
                (
                    "node-a".into(),
                    vec![
                        "node-b=http://node-b:9100".into(),
                        "node-e=http://node-e:9100".into(),
                    ],
                ),
                (
                    "node-b".into(),
                    vec![
                        "node-a=http://node-a:9100".into(),
                        "node-c=http://node-c:9100".into(),
                    ],
                ),
                (
                    "node-c".into(),
                    vec![
                        "node-b=http://node-b:9100".into(),
                        "node-d=http://node-d:9100".into(),
                    ],
                ),
                (
                    "node-d".into(),
                    vec![
                        "node-c=http://node-c:9100".into(),
                        "node-e=http://node-e:9100".into(),
                    ],
                ),
                (
                    "node-e".into(),
                    vec![
                        "node-d=http://node-d:9100".into(),
                        "node-a=http://node-a:9100".into(),
                    ],
                ),
            ]),
            reconcile_ms_by_node: BTreeMap::new(),
            peer_sync_mode_by_node: BTreeMap::new(),
            peer_sync_max_in_flight_by_node: BTreeMap::new(),
        },
    )
    .await;

    cluster.wait_for_all_http().await;
    cluster
        .put_workload("node-a", stopped_workload("workload.ring.base", "node-a"))
        .await;

    for node in cluster.nodes().iter().map(String::as_str) {
        cluster
            .wait_for_workload(node, "workload.ring.base", Duration::from_secs(40))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_five_node_star_topology_converges() {
    let cluster = DockerCluster::start_with_options(
        "five-node-star",
        ClusterOptions {
            node_ids: vec![
                "node-a".into(),
                "node-b".into(),
                "node-c".into(),
                "node-d".into(),
                "node-e".into(),
            ],
            extra_peers: BTreeMap::new(),
            explicit_peers: BTreeMap::from([
                (
                    "node-a".into(),
                    vec![
                        "node-b=http://node-b:9100".into(),
                        "node-c=http://node-c:9100".into(),
                        "node-d=http://node-d:9100".into(),
                        "node-e=http://node-e:9100".into(),
                    ],
                ),
                ("node-b".into(), vec!["node-a=http://node-a:9100".into()]),
                ("node-c".into(), vec!["node-a=http://node-a:9100".into()]),
                ("node-d".into(), vec!["node-a=http://node-a:9100".into()]),
                ("node-e".into(), vec!["node-a=http://node-a:9100".into()]),
            ]),
            reconcile_ms_by_node: BTreeMap::new(),
            peer_sync_mode_by_node: BTreeMap::new(),
            peer_sync_max_in_flight_by_node: BTreeMap::new(),
        },
    )
    .await;

    cluster.wait_for_all_http().await;
    cluster
        .put_workload("node-a", stopped_workload("workload.star.base", "node-a"))
        .await;

    for node in cluster.nodes().iter().map(String::as_str) {
        cluster
            .wait_for_workload(node, "workload.star.base", Duration::from_secs(40))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_six_node_asymmetric_topology_converges() {
    let cluster = DockerCluster::start_with_options(
        "six-node-asymmetric",
        ClusterOptions {
            node_ids: vec![
                "node-a".into(),
                "node-b".into(),
                "node-c".into(),
                "node-d".into(),
                "node-e".into(),
                "node-f".into(),
            ],
            extra_peers: BTreeMap::new(),
            explicit_peers: BTreeMap::from([
                (
                    "node-a".into(),
                    vec![
                        "node-b=http://node-b:9100".into(),
                        "node-c=http://node-c:9100".into(),
                    ],
                ),
                (
                    "node-b".into(),
                    vec![
                        "node-a=http://node-a:9100".into(),
                        "node-d=http://node-d:9100".into(),
                    ],
                ),
                (
                    "node-c".into(),
                    vec![
                        "node-a=http://node-a:9100".into(),
                        "node-e=http://node-e:9100".into(),
                    ],
                ),
                (
                    "node-d".into(),
                    vec![
                        "node-b=http://node-b:9100".into(),
                        "node-f=http://node-f:9100".into(),
                    ],
                ),
                (
                    "node-e".into(),
                    vec![
                        "node-c=http://node-c:9100".into(),
                        "node-f=http://node-f:9100".into(),
                    ],
                ),
                (
                    "node-f".into(),
                    vec![
                        "node-d=http://node-d:9100".into(),
                        "node-e=http://node-e:9100".into(),
                    ],
                ),
            ]),
            reconcile_ms_by_node: BTreeMap::new(),
            peer_sync_mode_by_node: BTreeMap::new(),
            peer_sync_max_in_flight_by_node: BTreeMap::new(),
        },
    )
    .await;

    cluster.wait_for_all_http().await;
    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.asymmetric.base", "node-a"),
        )
        .await;

    for node in cluster.nodes().iter().map(String::as_str) {
        cluster
            .wait_for_workload(node, "workload.asymmetric.base", Duration::from_secs(45))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_five_nodes_accept_multi_origin_updates() {
    let cluster = DockerCluster::start_with_options(
        "five-node-multi-origin",
        ClusterOptions::with_node_count(5),
    )
    .await;

    cluster.wait_for_all_http().await;

    for (origin, workload_id) in [
        ("node-a", "workload.origin.a"),
        ("node-b", "workload.origin.b"),
        ("node-c", "workload.origin.c"),
        ("node-d", "workload.origin.d"),
        ("node-e", "workload.origin.e"),
    ] {
        cluster
            .put_workload(origin, stopped_workload(workload_id, origin))
            .await;
    }

    for node in cluster.nodes().iter().map(String::as_str) {
        for workload_id in [
            "workload.origin.a",
            "workload.origin.b",
            "workload.origin.c",
            "workload.origin.d",
            "workload.origin.e",
        ] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(30))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_five_nodes_multi_wave_topology_churn_propagates_latest_wave() {
    let cluster = DockerCluster::start_with_options(
        "five-node-multi-wave",
        ClusterOptions::with_node_count(5),
    )
    .await;

    cluster.wait_for_all_http().await;

    for workload_id in ["workload.wave1.1", "workload.wave1.2", "workload.wave1.3"] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }
    for node in cluster.nodes().iter().map(String::as_str) {
        for workload_id in ["workload.wave1.1", "workload.wave1.2", "workload.wave1.3"] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(30))
                .await;
        }
    }

    cluster.restart_service("node-b");
    cluster.wait_for_http("node-b").await;
    cluster.restart_service("node-d");
    cluster.wait_for_http("node-d").await;

    for workload_id in ["workload.wave1.1", "workload.wave1.2", "workload.wave1.3"] {
        cluster.delete_workload("node-b", workload_id).await;
    }
    for workload_id in [
        "workload.wave2.1",
        "workload.wave2.2",
        "workload.wave2.3",
        "workload.wave2.4",
    ] {
        cluster
            .put_workload("node-c", stopped_workload(workload_id, "node-c"))
            .await;
    }

    for node in cluster.nodes().iter().map(String::as_str) {
        for workload_id in [
            "workload.wave2.1",
            "workload.wave2.2",
            "workload.wave2.3",
            "workload.wave2.4",
        ] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(30))
                .await;
        }
    }

    cluster.restart_service("node-a");
    cluster.wait_for_http("node-a").await;
    cluster.restart_service("node-e");
    cluster.wait_for_http("node-e").await;

    for workload_id in [
        "workload.wave2.1",
        "workload.wave2.2",
        "workload.wave2.3",
        "workload.wave2.4",
    ] {
        cluster.delete_workload("node-a", workload_id).await;
    }
    for workload_id in ["workload.wave3.1", "workload.wave3.2"] {
        cluster
            .put_workload("node-e", stopped_workload(workload_id, "node-e"))
            .await;
    }

    for node in cluster.nodes().iter().map(String::as_str) {
        for workload_id in ["workload.wave3.1", "workload.wave3.2"] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(30))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_slow_node_flapping_node_and_restarting_node_still_converge() {
    let cluster = DockerCluster::start_with_options(
        "five-node-mixed-churn",
        ClusterOptions {
            node_ids: vec![
                "node-a".into(),
                "node-b".into(),
                "node-c".into(),
                "node-d".into(),
                "node-e".into(),
            ],
            extra_peers: BTreeMap::new(),
            explicit_peers: BTreeMap::new(),
            reconcile_ms_by_node: BTreeMap::from([("node-e".into(), 750)]),
            peer_sync_mode_by_node: BTreeMap::new(),
            peer_sync_max_in_flight_by_node: BTreeMap::new(),
        },
    )
    .await;

    cluster.wait_for_all_http().await;

    cluster
        .put_workload("node-a", stopped_workload("workload.mixed.base", "node-a"))
        .await;

    for node in cluster.nodes().iter().map(String::as_str) {
        cluster
            .wait_for_workload(node, "workload.mixed.base", Duration::from_secs(40))
            .await;
    }

    cluster.stop_service("node-b");
    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.mixed.while-flap", "node-a"),
        )
        .await;

    cluster.restart_service("node-d");
    cluster.wait_for_http("node-d").await;

    cluster.start_service("node-b");
    cluster.wait_for_http("node-b").await;

    cluster
        .put_workload(
            "node-c",
            stopped_workload("workload.mixed.after-restart", "node-c"),
        )
        .await;

    for node in cluster.nodes().iter().map(String::as_str) {
        cluster
            .wait_for_workload(node, "workload.mixed.while-flap", Duration::from_secs(50))
            .await;
        cluster
            .wait_for_workload(
                node,
                "workload.mixed.after-restart",
                Duration::from_secs(50),
            )
            .await;
    }
}
