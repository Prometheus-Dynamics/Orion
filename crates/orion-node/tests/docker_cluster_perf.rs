mod support;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use support::docker_cluster::{ClusterOptions, DockerCluster, stopped_workload};

#[tokio::test]
#[ignore = "perf baseline harness; requires docker compose"]
async fn docker_cluster_reports_sync_time_by_node_count() {
    for node_count in [3usize, 5, 7] {
        let elapsed = measure_cluster_sync_waves(
            node_count,
            "baseline",
            ClusterOptions::with_node_count(node_count),
            5,
        )
        .await;
        println!(
            "cluster_sync_time node_count={} waves=5 total_ms={} avg_ms={}",
            node_count,
            elapsed.as_secs_f64() * 1000.0,
            elapsed.as_secs_f64() * 1000.0 / 5.0,
        );
    }
}

#[tokio::test]
#[ignore = "perf baseline harness; requires docker compose"]
async fn docker_cluster_compares_serial_vs_parallel_peer_sync() {
    for node_count in [3usize, 5, 7] {
        let serial_elapsed = measure_cluster_sync_waves(
            node_count,
            "serial",
            ClusterOptions {
                node_ids: Vec::new(),
                extra_peers: BTreeMap::new(),
                explicit_peers: BTreeMap::new(),
                reconcile_ms_by_node: BTreeMap::new(),
                peer_sync_mode_by_node: (0..node_count)
                    .map(|index| {
                        (
                            format!("node-{}", (b'a' + index as u8) as char),
                            "serial".to_owned(),
                        )
                    })
                    .collect(),
                peer_sync_max_in_flight_by_node: BTreeMap::new(),
            },
            5,
        )
        .await;

        let parallel_elapsed = measure_cluster_sync_waves(
            node_count,
            "parallel",
            ClusterOptions {
                node_ids: Vec::new(),
                extra_peers: BTreeMap::new(),
                explicit_peers: BTreeMap::new(),
                reconcile_ms_by_node: BTreeMap::new(),
                peer_sync_mode_by_node: (0..node_count)
                    .map(|index| {
                        (
                            format!("node-{}", (b'a' + index as u8) as char),
                            "parallel".to_owned(),
                        )
                    })
                    .collect(),
                peer_sync_max_in_flight_by_node: (0..node_count)
                    .map(|index| (format!("node-{}", (b'a' + index as u8) as char), 4usize))
                    .collect(),
            },
            5,
        )
        .await;

        println!(
            "cluster_sync_compare node_count={} waves=5 serial_ms={} parallel_ms={} delta_ms={} improvement_pct={}",
            node_count,
            serial_elapsed.as_secs_f64() * 1000.0,
            parallel_elapsed.as_secs_f64() * 1000.0,
            serial_elapsed
                .saturating_sub(parallel_elapsed)
                .as_secs_f64()
                * 1000.0,
            if serial_elapsed.is_zero() {
                0.0
            } else {
                ((serial_elapsed.as_secs_f64() - parallel_elapsed.as_secs_f64())
                    / serial_elapsed.as_secs_f64())
                    * 100.0
            }
        );
    }
}

async fn measure_cluster_sync_waves(
    node_count: usize,
    mode: &str,
    options: ClusterOptions,
    waves: usize,
) -> Duration {
    let cluster = DockerCluster::start_with_options(
        &format!("perf-sync-{mode}-{node_count}"),
        ClusterOptions {
            node_ids: if options.node_ids.is_empty() {
                (0..node_count)
                    .map(|index| format!("node-{}", (b'a' + index as u8) as char))
                    .collect()
            } else {
                options.node_ids
            },
            extra_peers: options.extra_peers,
            explicit_peers: options.explicit_peers,
            reconcile_ms_by_node: options.reconcile_ms_by_node,
            peer_sync_mode_by_node: options.peer_sync_mode_by_node,
            peer_sync_max_in_flight_by_node: options.peer_sync_max_in_flight_by_node,
        },
    )
    .await;

    cluster.wait_for_all_http().await;

    let start = Instant::now();
    for wave in 0..waves {
        let workload_id = format!("workload.sync.compare.{mode}.{node_count}.{wave}");
        cluster
            .put_workload("node-a", stopped_workload(&workload_id, "node-a"))
            .await;

        for node in cluster.nodes().iter().map(String::as_str) {
            cluster
                .wait_for_workloads(node, [&workload_id], Duration::from_secs(60))
                .await;
        }
    }

    start.elapsed()
}
