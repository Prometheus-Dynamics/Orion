mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use orion::{
    ArtifactId, NodeId, RuntimeType, WorkloadId,
    control_plane::{
        ControlMessage, DesiredState, DesiredStateMutation, MutationBatch, WorkloadRecord,
    },
    transport::http::HttpRequestPayload,
};
use support::docker_cluster::{ClusterOptions, DockerCluster, stopped_workload};

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_falls_back_to_snapshot_when_history_is_missing() {
    let cluster = DockerCluster::start("snapshot-fallback").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload("node-a", stopped_workload("workload.base", "node-a"))
        .await;
    cluster
        .wait_for_workload("node-b", "workload.base", Duration::from_secs(20))
        .await;

    cluster.stop_service("node-b");

    cluster
        .put_workload("node-a", stopped_workload("workload.new", "node-a"))
        .await;

    cluster.exec_service(
        "node-a",
        ["sh", "-lc", "rm -f /var/lib/orion/mutation-history.rkyv"],
    );
    cluster.restart_service("node-a");
    cluster.wait_for_http("node-a").await;

    cluster.start_service("node-b");
    cluster.wait_for_http("node-b").await;
    cluster
        .wait_for_workload("node-b", "workload.new", Duration::from_secs(20))
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_rejects_stale_mutation_batch_without_crashing() {
    let cluster = DockerCluster::start("stale-mutation").await;

    cluster.wait_for_http("node-a").await;
    let snapshot = cluster.snapshot("node-a").await;

    cluster
        .put_workload(
            "node-a",
            WorkloadRecord::builder(
                WorkloadId::new("workload.live"),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new("artifact.live"),
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        )
        .await;

    let stale = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![DesiredStateMutation::PutWorkload(
            WorkloadRecord::builder(
                WorkloadId::new("workload.stale"),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new("artifact.stale"),
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        )],
    };

    let response = cluster
        .client("node-a")
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(stale),
        )))
        .await;
    assert!(response.is_err());

    cluster.wait_for_http("node-a").await;
    cluster
        .wait_for_workload("node-a", "workload.live", Duration::from_secs(10))
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_with_bad_peer_config_keeps_healthy_nodes_working() {
    let cluster = DockerCluster::start_with_options(
        "bad-peer",
        ClusterOptions {
            node_ids: Vec::new(),
            extra_peers: BTreeMap::from([(
                "node-a".to_owned(),
                vec!["node-bad=http://no-such-peer:9100".to_owned()],
            )]),
            explicit_peers: BTreeMap::new(),
            reconcile_ms_by_node: BTreeMap::new(),
            peer_sync_mode_by_node: BTreeMap::new(),
            peer_sync_max_in_flight_by_node: BTreeMap::new(),
        },
    )
    .await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.with-bad-peer", "node-a"),
        )
        .await;

    cluster
        .wait_for_workload("node-b", "workload.with-bad-peer", Duration::from_secs(20))
        .await;
    cluster
        .wait_for_workload("node-c", "workload.with-bad-peer", Duration::from_secs(20))
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_five_nodes_partial_partition_rejoins_to_latest() {
    let cluster = DockerCluster::start_with_options(
        "five-node-partition",
        ClusterOptions::with_node_count(5),
    )
    .await;

    cluster.wait_for_all_http().await;

    for node in ["node-d", "node-e"] {
        cluster.stop_service(node);
    }

    for workload_id in [
        "workload.partition.1",
        "workload.partition.2",
        "workload.partition.3",
    ] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    for node in ["node-b", "node-c"] {
        for workload_id in [
            "workload.partition.1",
            "workload.partition.2",
            "workload.partition.3",
        ] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(30))
                .await;
        }
    }

    for node in ["node-d", "node-e"] {
        cluster.start_service(node);
        cluster.wait_for_http(node).await;
    }

    for node in cluster.nodes().iter().map(String::as_str) {
        for workload_id in [
            "workload.partition.1",
            "workload.partition.2",
            "workload.partition.3",
        ] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(30))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_five_nodes_corrupt_persistence_recovers_from_peer_snapshot() {
    let cluster = DockerCluster::start_with_options(
        "five-node-corrupt-persistence",
        ClusterOptions::with_node_count(5),
    )
    .await;

    cluster.wait_for_all_http().await;

    for workload_id in [
        "workload.persist.1",
        "workload.persist.2",
        "workload.persist.3",
    ] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    for node in ["node-b", "node-c", "node-d", "node-e"] {
        for workload_id in [
            "workload.persist.1",
            "workload.persist.2",
            "workload.persist.3",
        ] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(30))
                .await;
        }
    }

    cluster.exec_service(
        "node-c",
        [
            "sh",
            "-lc",
            "printf 'not-rkyv' > /var/lib/orion/snapshot-manifest.rkyv",
        ],
    );
    cluster.restart_service("node-c");
    cluster.wait_for_http("node-c").await;

    for workload_id in [
        "workload.persist.1",
        "workload.persist.2",
        "workload.persist.3",
    ] {
        cluster
            .wait_for_workload("node-c", workload_id, Duration::from_secs(30))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_five_nodes_history_loss_on_leader_still_recovers_cluster() {
    let cluster = DockerCluster::start_with_options(
        "five-node-history-loss",
        ClusterOptions::with_node_count(5),
    )
    .await;

    cluster.wait_for_all_http().await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.history.base", "node-a"),
        )
        .await;
    for node in cluster.nodes().iter().map(String::as_str) {
        cluster
            .wait_for_workload(node, "workload.history.base", Duration::from_secs(30))
            .await;
    }

    cluster.stop_service("node-d");
    cluster.stop_service("node-e");

    cluster
        .put_workload("node-a", stopped_workload("workload.history.new", "node-a"))
        .await;

    cluster.exec_service(
        "node-a",
        ["sh", "-lc", "rm -f /var/lib/orion/mutation-history.rkyv"],
    );
    cluster.restart_service("node-a");
    cluster.wait_for_http("node-a").await;

    for node in ["node-d", "node-e"] {
        cluster.start_service(node);
        cluster.wait_for_http(node).await;
    }

    for node in cluster.nodes().iter().map(String::as_str) {
        for workload_id in ["workload.history.base", "workload.history.new"] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(30))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_recovers_from_truncated_mutation_history_file() {
    let cluster = DockerCluster::start("truncated-history").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.history.truncated", "node-a"),
        )
        .await;
    cluster
        .wait_for_workload(
            "node-b",
            "workload.history.truncated",
            Duration::from_secs(20),
        )
        .await;

    cluster.stop_service("node-b");
    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.history.after-truncate", "node-a"),
        )
        .await;

    cluster.exec_service(
        "node-a",
        [
            "sh",
            "-lc",
            "printf 'not-rkyv' > /var/lib/orion/mutation-history.rkyv",
        ],
    );
    cluster.restart_service("node-a");
    cluster.wait_for_http("node-a").await;

    cluster.start_service("node-b");
    cluster.wait_for_http("node-b").await;
    cluster
        .wait_for_workload(
            "node-b",
            "workload.history.after-truncate",
            Duration::from_secs(20),
        )
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_recovers_when_snapshot_file_is_missing_but_peers_remain_healthy() {
    let cluster = DockerCluster::start("missing-snapshot").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.snapshot.missing", "node-a"),
        )
        .await;
    cluster
        .wait_for_workload(
            "node-b",
            "workload.snapshot.missing",
            Duration::from_secs(20),
        )
        .await;

    cluster.exec_service(
        "node-b",
        ["sh", "-lc", "rm -f /var/lib/orion/snapshot-manifest.rkyv"],
    );
    cluster.restart_service("node-b");
    cluster.wait_for_http("node-b").await;
    cluster
        .wait_for_workload(
            "node-b",
            "workload.snapshot.missing",
            Duration::from_secs(20),
        )
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_recovers_when_both_snapshot_and_history_are_missing() {
    let cluster = DockerCluster::start("missing-both-state-files").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.missing-both.base", "node-a"),
        )
        .await;
    cluster
        .wait_for_workload(
            "node-b",
            "workload.missing-both.base",
            Duration::from_secs(20),
        )
        .await;

    cluster.exec_service(
        "node-b",
        [
            "sh",
            "-lc",
            "rm -f /var/lib/orion/snapshot-manifest.rkyv /var/lib/orion/mutation-history.rkyv",
        ],
    );
    cluster.restart_service("node-b");
    cluster.wait_for_http("node-b").await;
    cluster
        .wait_for_workload(
            "node-b",
            "workload.missing-both.base",
            Duration::from_secs(20),
        )
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_recovers_when_snapshot_is_corrupt_but_history_remains_valid() {
    let cluster = DockerCluster::start("corrupt-snapshot-valid-history").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.corrupt-snapshot.base", "node-a"),
        )
        .await;
    cluster
        .wait_for_workload(
            "node-b",
            "workload.corrupt-snapshot.base",
            Duration::from_secs(20),
        )
        .await;

    cluster.exec_service(
        "node-b",
        [
            "sh",
            "-lc",
            "printf 'not-rkyv' > /var/lib/orion/snapshot-manifest.rkyv",
        ],
    );
    cluster.restart_service("node-b");
    cluster.wait_for_http("node-b").await;

    cluster
        .wait_for_workload(
            "node-b",
            "workload.corrupt-snapshot.base",
            Duration::from_secs(20),
        )
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_recovers_when_history_is_corrupt_but_snapshot_remains_valid() {
    let cluster = DockerCluster::start("corrupt-history-valid-snapshot").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.corrupt-history.base", "node-a"),
        )
        .await;
    cluster
        .wait_for_workload(
            "node-b",
            "workload.corrupt-history.base",
            Duration::from_secs(20),
        )
        .await;

    cluster.exec_service(
        "node-b",
        [
            "sh",
            "-lc",
            "printf 'not-rkyv' > /var/lib/orion/mutation-history.rkyv",
        ],
    );
    cluster.restart_service("node-b");
    cluster.wait_for_http("node-b").await;

    cluster
        .wait_for_workload(
            "node-b",
            "workload.corrupt-history.base",
            Duration::from_secs(20),
        )
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_missing_artifact_payload_file_does_not_block_restart_or_convergence() {
    let cluster = DockerCluster::start("missing-artifact-payload").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster.exec_service(
        "node-b",
        [
            "sh",
            "-lc",
            "mkdir -p /var/lib/orion/artifacts/artifact.missing.payload && printf 'not-rkyv' > /var/lib/orion/artifacts/artifact.missing.payload/metadata.rkyv && printf 'payload' > /var/lib/orion/artifacts/artifact.missing.payload/payload.bin && rm -f /var/lib/orion/artifacts/artifact.missing.payload/payload.bin",
        ],
    );

    cluster.restart_service("node-b");
    cluster.wait_for_http("node-b").await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.after-missing-payload", "node-a"),
        )
        .await;

    for node in ["node-b", "node-c"] {
        cluster
            .wait_for_workload(
                node,
                "workload.after-missing-payload",
                Duration::from_secs(20),
            )
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_missing_artifact_metadata_file_does_not_block_restart_or_convergence() {
    let cluster = DockerCluster::start("missing-artifact-metadata").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster.exec_service(
        "node-c",
        [
            "sh",
            "-lc",
            "mkdir -p /var/lib/orion/artifacts/artifact.missing.metadata && printf 'not-rkyv' > /var/lib/orion/artifacts/artifact.missing.metadata/metadata.rkyv && printf 'payload' > /var/lib/orion/artifacts/artifact.missing.metadata/payload.bin && rm -f /var/lib/orion/artifacts/artifact.missing.metadata/metadata.rkyv",
        ],
    );

    cluster.restart_service("node-c");
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.after-missing-metadata", "node-a"),
        )
        .await;

    for node in ["node-b", "node-c"] {
        cluster
            .wait_for_workload(
                node,
                "workload.after-missing-metadata",
                Duration::from_secs(20),
            )
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_restart_during_persistence_replay_reaches_latest_state() {
    let cluster = DockerCluster::start("restart-during-replay").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    for workload_id in [
        "workload.replay.1",
        "workload.replay.2",
        "workload.replay.3",
    ] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }
    cluster
        .wait_for_workload("node-b", "workload.replay.3", Duration::from_secs(20))
        .await;

    cluster.stop_service("node-b");

    for workload_id in ["workload.replay.4", "workload.replay.5"] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    cluster.start_service("node-b");
    tokio::time::sleep(Duration::from_millis(200)).await;
    cluster.restart_service("node-b");
    cluster.wait_for_http("node-b").await;

    for workload_id in [
        "workload.replay.1",
        "workload.replay.2",
        "workload.replay.3",
        "workload.replay.4",
        "workload.replay.5",
    ] {
        cluster
            .wait_for_workload("node-b", workload_id, Duration::from_secs(20))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_repeated_partition_heal_cycles_reach_latest_state() {
    let cluster = DockerCluster::start("partition-heal-cycles").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    for round in 0..3 {
        let isolated = if round % 2 == 0 { "node-b" } else { "node-c" };
        cluster.stop_service(isolated);

        let workload_id = format!("workload.partition-cycle.{round}");
        cluster
            .put_workload("node-a", stopped_workload(&workload_id, "node-a"))
            .await;

        let healthy_peer = if isolated == "node-b" {
            "node-c"
        } else {
            "node-b"
        };
        cluster
            .wait_for_workload(healthy_peer, &workload_id, Duration::from_secs(40))
            .await;

        cluster.start_service(isolated);
        cluster.wait_for_http(isolated).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        cluster
            .wait_for_workload(isolated, &workload_id, Duration::from_secs(40))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_split_brain_conflicting_updates_same_workload_id() {
    let cluster = DockerCluster::start("split-brain-conflict").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster.stop_service("node-b");
    cluster.stop_service("node-c");

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.split.conflict", "node-a"),
        )
        .await;

    cluster.start_service("node-b");
    cluster.wait_for_http("node-b").await;
    cluster
        .put_workload(
            "node-b",
            stopped_workload("workload.split.conflict", "node-b"),
        )
        .await;

    cluster.start_service("node-c");
    cluster.wait_for_http("node-c").await;

    for node in ["node-a", "node-b", "node-c"] {
        cluster
            .wait_for_workload(node, "workload.split.conflict", Duration::from_secs(30))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_split_brain_delete_vs_readd_same_workload_id() {
    let cluster = DockerCluster::start("split-brain-delete-readd").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload("node-a", stopped_workload("workload.split.readd", "node-a"))
        .await;
    cluster
        .wait_for_workload("node-b", "workload.split.readd", Duration::from_secs(20))
        .await;

    cluster.stop_service("node-c");
    cluster
        .delete_workload("node-a", "workload.split.readd")
        .await;
    cluster
        .wait_for_workload_absent("node-b", "workload.split.readd", Duration::from_secs(20))
        .await;

    cluster.start_service("node-c");
    cluster.wait_for_http("node-c").await;
    cluster
        .put_workload("node-c", stopped_workload("workload.split.readd", "node-c"))
        .await;

    for node in ["node-a", "node-b", "node-c"] {
        cluster
            .wait_for_workload(node, "workload.split.readd", Duration::from_secs(30))
            .await;
    }
}
