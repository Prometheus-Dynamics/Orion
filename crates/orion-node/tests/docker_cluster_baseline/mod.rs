use std::time::Duration;

use crate::support::docker_cluster::DockerCluster;

mod replication;
mod resources;

async fn wait_for_snapshot_condition<F>(
    cluster: &DockerCluster,
    node: &str,
    timeout: Duration,
    predicate: F,
) where
    F: Fn(&orion::control_plane::StateSnapshot) -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let snapshot = cluster.snapshot(node).await;
        if predicate(&snapshot) {
            return;
        }

        if std::time::Instant::now() >= deadline {
            panic!(
                "node {node} did not satisfy snapshot condition in time\n{}",
                cluster.compose_logs()
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
