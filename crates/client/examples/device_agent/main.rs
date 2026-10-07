//! Example device agent (`docs/device-agent.md`) with a fake updater.
//!
//! ```text
//! cargo run -p orion-client --example device_agent -- <control socket> <stream socket> <node id>
//! orionctl action run node/<node id> update --arg image_url=http://host/image-2.0.img.xz \
//!     --arg sha256=string:<64 hex digits> --arg size=1048576 --wait
//! orionctl get status --subject node/<node id> --key-prefix update.
//! ```
//!
//! A real agent replaces `FakeUpdater` with one that runs the device package's writer.

use std::sync::Arc;
use std::time::Duration;

use orion_client::{LocalNodeRuntime, LocalProviderService, LocalServiceRetryPolicy};
use orion_control_plane::ProviderRecord;
use orion_core::{NodeId, ProviderId};

#[path = "../support/common.rs"]
mod common;

#[allow(dead_code)] // test hooks of the fake updater, used by crates/node/tests/device_agent.rs
mod agent;

#[tokio::main]
async fn main() {
    common::exit_on_error(run()).await;
}

async fn run() -> Result<(), common::ExampleError> {
    let [socket_path, stream_socket_path, node_id] = common::read_exact_args::<3>()?;
    let runtime = LocalNodeRuntime::new(&socket_path, &stream_socket_path);
    // The agent claims node actions; it registers no provider, so the record only names it.
    let service = LocalProviderService::new(
        runtime,
        "device-agent",
        ProviderRecord::builder(ProviderId::new("device-agent"), NodeId::new(node_id)).build(),
    )
    // Wait for orion-node indefinitely (it may start after the agent, or restart).
    .with_retry_policy(LocalServiceRetryPolicy::fixed_delay(Duration::from_secs(1)));
    let updater = Arc::new(agent::FakeUpdater::new(
        agent::UpdaterStatus {
            state: "idle".into(),
            slot_active: Some("A".into()),
            version_active: Some("1.0".into()),
            ..agent::UpdaterStatus::default()
        },
        10,
        Duration::from_millis(200),
    ));
    println!("device agent: claiming {:?}", agent::CLAIMED_ACTIONS);
    agent::run(&service, updater, agent::REPUBLISH_INTERVAL).await?;
    Ok(())
}
