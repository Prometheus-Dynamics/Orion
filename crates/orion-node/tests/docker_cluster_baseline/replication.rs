use std::time::Duration;

use crate::support::docker_cluster::{DockerCluster, stopped_workload};

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_converges_workload_state_across_three_nodes() {
    let cluster = DockerCluster::start("converges").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload("node-a", stopped_workload("workload.pose", "node-a"))
        .await;

    cluster
        .wait_for_workload("node-b", "workload.pose", Duration::from_secs(20))
        .await;
    cluster
        .wait_for_workload("node-c", "workload.pose", Duration::from_secs(20))
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_propagates_multiple_workloads_and_artifacts() {
    let cluster = DockerCluster::start("multi-workloads").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    for workload_id in ["workload.one", "workload.two", "workload.three"] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    for node in ["node-b", "node-c"] {
        for workload_id in ["workload.one", "workload.two", "workload.three"] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_propagates_removal_of_workload_state() {
    let cluster = DockerCluster::start("removal").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload("node-a", stopped_workload("workload.remove", "node-a"))
        .await;

    cluster
        .wait_for_workload("node-b", "workload.remove", Duration::from_secs(20))
        .await;

    cluster.delete_workload("node-a", "workload.remove").await;

    cluster
        .wait_for_workload_absent("node-b", "workload.remove", Duration::from_secs(20))
        .await;
    cluster
        .wait_for_workload_absent("node-c", "workload.remove", Duration::from_secs(20))
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_accepts_updates_from_non_leader_peer() {
    let cluster = DockerCluster::start("peer-origin").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload("node-b", stopped_workload("workload.from-b", "node-b"))
        .await;

    cluster
        .wait_for_workload("node-a", "workload.from-b", Duration::from_secs(20))
        .await;
    cluster
        .wait_for_workload("node-c", "workload.from-b", Duration::from_secs(20))
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_restart_rejoins_and_catches_up() {
    let cluster = DockerCluster::start("restart").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload("node-a", stopped_workload("workload.initial", "node-a"))
        .await;

    cluster
        .wait_for_workload("node-b", "workload.initial", Duration::from_secs(20))
        .await;

    cluster.restart_service("node-b");
    cluster.wait_for_http("node-b").await;

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.after-restart", "node-a"),
        )
        .await;

    cluster
        .wait_for_workload("node-b", "workload.after-restart", Duration::from_secs(20))
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_readds_same_workload_after_removal() {
    let cluster = DockerCluster::start("readd-same-workload").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload("node-a", stopped_workload("workload.readd", "node-a"))
        .await;
    cluster
        .wait_for_workload("node-b", "workload.readd", Duration::from_secs(20))
        .await;

    cluster.delete_workload("node-a", "workload.readd").await;
    for node in ["node-b", "node-c"] {
        cluster
            .wait_for_workload_absent(node, "workload.readd", Duration::from_secs(20))
            .await;
    }

    cluster
        .put_workload("node-a", stopped_workload("workload.readd", "node-a"))
        .await;
    for node in ["node-b", "node-c"] {
        cluster
            .wait_for_workload(node, "workload.readd", Duration::from_secs(20))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_removes_all_but_one_workload() {
    let cluster = DockerCluster::start("remove-all-but-one").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    for workload_id in [
        "workload.keep",
        "workload.drop.1",
        "workload.drop.2",
        "workload.drop.3",
    ] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    for node in ["node-b", "node-c"] {
        for workload_id in [
            "workload.keep",
            "workload.drop.1",
            "workload.drop.2",
            "workload.drop.3",
        ] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }

    for workload_id in ["workload.drop.1", "workload.drop.2", "workload.drop.3"] {
        cluster.delete_workload("node-a", workload_id).await;
    }

    for node in ["node-b", "node-c"] {
        cluster
            .wait_for_workload(node, "workload.keep", Duration::from_secs(20))
            .await;
        for workload_id in ["workload.drop.1", "workload.drop.2", "workload.drop.3"] {
            cluster
                .wait_for_workload_absent(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_replaces_every_workload_with_smaller_set() {
    let cluster = DockerCluster::start("replace-smaller").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    for workload_id in [
        "workload.old.1",
        "workload.old.2",
        "workload.old.3",
        "workload.old.4",
    ] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    for workload_id in [
        "workload.old.1",
        "workload.old.2",
        "workload.old.3",
        "workload.old.4",
    ] {
        cluster.delete_workload("node-a", workload_id).await;
    }
    for workload_id in ["workload.new.1", "workload.new.2"] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    for node in ["node-b", "node-c"] {
        for workload_id in ["workload.new.1", "workload.new.2"] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(20))
                .await;
        }
        for workload_id in [
            "workload.old.1",
            "workload.old.2",
            "workload.old.3",
            "workload.old.4",
        ] {
            cluster
                .wait_for_workload_absent(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_replaces_every_workload_with_larger_set() {
    let cluster = DockerCluster::start("replace-larger").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    for workload_id in ["workload.seed.1", "workload.seed.2"] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    for workload_id in ["workload.seed.1", "workload.seed.2"] {
        cluster.delete_workload("node-a", workload_id).await;
    }
    for workload_id in [
        "workload.grow.1",
        "workload.grow.2",
        "workload.grow.3",
        "workload.grow.4",
        "workload.grow.5",
    ] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    for node in ["node-b", "node-c"] {
        for workload_id in [
            "workload.grow.1",
            "workload.grow.2",
            "workload.grow.3",
            "workload.grow.4",
            "workload.grow.5",
        ] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(20))
                .await;
        }
        for workload_id in ["workload.seed.1", "workload.seed.2"] {
            cluster
                .wait_for_workload_absent(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_replaces_workloads_one_by_one() {
    let cluster = DockerCluster::start("replace-one-by-one").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    for workload_id in ["workload.stage.1", "workload.stage.2", "workload.stage.3"] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    for (remove_id, add_id) in [
        ("workload.stage.1", "workload.next.1"),
        ("workload.stage.2", "workload.next.2"),
        ("workload.stage.3", "workload.next.3"),
    ] {
        cluster.delete_workload("node-a", remove_id).await;
        cluster
            .put_workload("node-a", stopped_workload(add_id, "node-a"))
            .await;
    }

    for node in ["node-b", "node-c"] {
        for workload_id in ["workload.next.1", "workload.next.2", "workload.next.3"] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(20))
                .await;
        }
        for workload_id in ["workload.stage.1", "workload.stage.2", "workload.stage.3"] {
            cluster
                .wait_for_workload_absent(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_repeated_add_remove_cycles_for_many_waves() {
    let cluster = DockerCluster::start("many-waves").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    for wave in 0..8 {
        let add_id = format!("workload.wave.{wave}");
        cluster
            .put_workload("node-a", stopped_workload(&add_id, "node-a"))
            .await;

        for node in ["node-b", "node-c"] {
            cluster
                .wait_for_workload(node, &add_id, Duration::from_secs(20))
                .await;
        }

        if wave > 0 {
            let remove_id = format!("workload.wave.{}", wave - 1);
            cluster.delete_workload("node-a", &remove_id).await;

            for node in ["node-b", "node-c"] {
                cluster
                    .wait_for_workload_absent(node, &remove_id, Duration::from_secs(20))
                    .await;
            }
        }
    }

    for node in ["node-a", "node-b", "node-c"] {
        cluster
            .wait_for_workload(node, "workload.wave.7", Duration::from_secs(20))
            .await;
        for old_wave in 0..7 {
            cluster
                .wait_for_workload_absent(
                    node,
                    &format!("workload.wave.{old_wave}"),
                    Duration::from_secs(20),
                )
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_full_wipe_then_rebuild() {
    let cluster = DockerCluster::start("wipe-rebuild").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    for workload_id in [
        "workload.seed.1",
        "workload.seed.2",
        "workload.seed.3",
        "workload.seed.4",
    ] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    for node in ["node-b", "node-c"] {
        for workload_id in [
            "workload.seed.1",
            "workload.seed.2",
            "workload.seed.3",
            "workload.seed.4",
        ] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }

    for workload_id in [
        "workload.seed.1",
        "workload.seed.2",
        "workload.seed.3",
        "workload.seed.4",
    ] {
        cluster.delete_workload("node-a", workload_id).await;
    }

    for node in ["node-a", "node-b", "node-c"] {
        for workload_id in [
            "workload.seed.1",
            "workload.seed.2",
            "workload.seed.3",
            "workload.seed.4",
        ] {
            cluster
                .wait_for_workload_absent(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }

    for workload_id in [
        "workload.rebuild.1",
        "workload.rebuild.2",
        "workload.rebuild.3",
    ] {
        cluster
            .put_workload("node-a", stopped_workload(workload_id, "node-a"))
            .await;
    }

    for node in ["node-a", "node-b", "node-c"] {
        for workload_id in [
            "workload.rebuild.1",
            "workload.rebuild.2",
            "workload.rebuild.3",
        ] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_alternating_origin_node_every_mutation_converges() {
    let cluster = DockerCluster::start("alternating-origin").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    for (origin, workload_id) in [
        ("node-a", "workload.origin.1"),
        ("node-b", "workload.origin.2"),
        ("node-c", "workload.origin.3"),
        ("node-a", "workload.origin.4"),
        ("node-b", "workload.origin.5"),
        ("node-c", "workload.origin.6"),
    ] {
        cluster
            .put_workload(origin, stopped_workload(workload_id, origin))
            .await;

        for node in ["node-a", "node-b", "node-c"] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(20))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_repeated_restart_rejoin_loops_over_many_cycles() {
    let cluster = DockerCluster::start("restart-rejoin-loops").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    cluster
        .put_workload("node-a", stopped_workload("workload.loop.base", "node-a"))
        .await;

    for node in ["node-b", "node-c"] {
        cluster
            .wait_for_workload(node, "workload.loop.base", Duration::from_secs(20))
            .await;
    }

    for cycle in 0..6 {
        let service = if cycle % 2 == 0 { "node-b" } else { "node-c" };
        cluster.restart_service(service);
        cluster.wait_for_http(service).await;

        let workload_id = format!("workload.loop.{cycle}");
        cluster
            .put_workload("node-a", stopped_workload(&workload_id, "node-a"))
            .await;

        for node in ["node-a", "node-b", "node-c"] {
            cluster
                .wait_for_workload(node, &workload_id, Duration::from_secs(20))
                .await;
        }
    }
}
