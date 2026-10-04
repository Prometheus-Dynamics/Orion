//! Link gateway tests: config parsing, serial end-to-end over pseudo-terminals, and CAN bridging
//! over an in-memory bus (plus an ignored test on a real `vcan0`).

mod can;
mod config;
mod serial;
mod status;

use crate::{NodeApp, NodeConfig};
use orion::control_plane::{
    AvailabilityState, HealthState, LeaseRecord, ProviderRecord, ResourceRecord,
};
use std::time::Duration;

pub(super) const NODE: &str = "node-link";
pub(super) const HEARTBEAT_MS: u32 = 100;

pub(super) fn test_app(name: &str) -> NodeApp {
    let socket = std::env::temp_dir().join(format!("orion-lg-{name}-{}.sock", std::process::id()));
    NodeApp::try_new(NodeConfig::for_local_node(NODE).with_ipc_socket_path(socket))
        .expect("node app should build")
}

/// The provider a device named `device` publishes. Its `node_id` is deliberately wrong: the
/// gateway owns it.
pub(super) fn device_provider(provider_id: &str) -> ProviderRecord {
    ProviderRecord::builder(orion::ProviderId::new(provider_id), "unassigned")
        .resource_type("imu.sample_source")
        .build()
}

pub(super) fn device_resource(resource_id: &str, provider_id: &str, label: &str) -> ResourceRecord {
    ResourceRecord::builder(
        orion::ResourceId::new(resource_id),
        "imu.sample_source",
        orion::ProviderId::new(provider_id),
    )
    .health(HealthState::Healthy)
    .availability(AvailabilityState::Available)
    .label(label)
    .build()
}

pub(super) fn lease(resource_id: &str, workload: &str) -> LeaseRecord {
    LeaseRecord::builder(orion::ResourceId::new(resource_id))
        .holder_node(NODE)
        .holder_workload(orion::WorkloadId::new(workload))
        .build()
}

/// Adds `lease` to desired state (as `orionctl`/a control plane would).
pub(super) fn assign_lease(app: &NodeApp, lease: LeaseRecord) {
    let mut desired = app.state_snapshot().state.desired;
    desired.put_lease(lease);
    app.replace_desired_tracked(desired)
        .expect("lease should be accepted");
}

pub(super) fn observed_resource(app: &NodeApp, resource_id: &str) -> Option<ResourceRecord> {
    app.state_snapshot()
        .state
        .observed
        .resources
        .get(&orion::ResourceId::new(resource_id))
        .cloned()
}

pub(super) fn has_provider(app: &NodeApp, provider_id: &str) -> bool {
    app.state_snapshot()
        .state
        .desired
        .providers
        .get(&orion::ProviderId::new(provider_id))
        .is_some_and(|provider| provider.node_id.as_str() == NODE)
}

/// Polls `condition` every 10 ms until it holds or `timeout` passes.
pub(super) async fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub(super) fn fast_device(name: &str) -> orion_link::device::DeviceConfig {
    let mut config = orion_link::device::DeviceConfig::provider(name);
    config.hello_retry_min_ms = 20;
    config.hello_retry_max_ms = 200;
    config.reject_retry_min_ms = 200;
    config.reject_retry_max_ms = 400;
    config.state_retry_min_ms = 30;
    config
}
