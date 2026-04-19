mod support;

use std::time::Duration;

use support::docker_cluster::{ClusterOptions, DockerCluster, stopped_workload};

#[tokio::test]
#[ignore = "requires docker compose; long-running soak"]
async fn docker_cluster_three_nodes_repeated_churn_soak() {
    let cluster = DockerCluster::start("three-node-soak").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    for round in 0..10 {
        let add_id = format!("workload.soak.add.{round}");
        cluster
            .put_workload("node-a", stopped_workload(&add_id, "node-a"))
            .await;

        for node in ["node-b", "node-c"] {
            cluster
                .wait_for_workload(node, &add_id, Duration::from_secs(20))
                .await;
        }

        if round > 0 {
            let remove_id = format!("workload.soak.add.{}", round - 1);
            cluster.delete_workload("node-b", &remove_id).await;
            for node in ["node-a", "node-b", "node-c"] {
                cluster
                    .wait_for_workload_absent(node, &remove_id, Duration::from_secs(20))
                    .await;
            }
        }

        let flap = if round % 2 == 0 { "node-b" } else { "node-c" };
        cluster.restart_service(flap);
        cluster.wait_for_http(flap).await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose; long-running soak"]
async fn docker_cluster_five_nodes_restart_and_partition_soak() {
    let cluster =
        DockerCluster::start_with_options("five-node-soak", ClusterOptions::with_node_count(5))
            .await;

    cluster.wait_for_all_http().await;

    for round in 0..6 {
        let workload_id = format!("workload.five-soak.{round}");
        let origin = match round % 5 {
            0 => "node-a",
            1 => "node-b",
            2 => "node-c",
            3 => "node-d",
            _ => "node-e",
        };

        cluster
            .put_workload(origin, stopped_workload(&workload_id, origin))
            .await;

        for node in cluster.nodes().iter().map(String::as_str) {
            cluster
                .wait_for_workload(node, &workload_id, Duration::from_secs(30))
                .await;
        }

        let stopped = if round % 2 == 0 { "node-d" } else { "node-e" };
        cluster.stop_service(stopped);

        let next_id = format!("workload.five-soak.extra.{round}");
        cluster
            .put_workload("node-a", stopped_workload(&next_id, "node-a"))
            .await;

        for node in ["node-a", "node-b", "node-c"] {
            cluster
                .wait_for_workload(node, &next_id, Duration::from_secs(30))
                .await;
        }

        cluster.start_service(stopped);
        cluster.wait_for_http(stopped).await;
        cluster
            .wait_for_workload(stopped, &next_id, Duration::from_secs(30))
            .await;
    }
}
