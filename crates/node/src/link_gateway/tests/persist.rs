//! Link-owned providers across a restart: a node that crashed (no shutdown, no device loss) marks
//! the providers its devices published unavailable when it starts again, a reconnecting device
//! makes them available, and a link removed from the config is forgotten.

use super::*;
use crate::link_gateway::LinkConfig;
use std::path::{Path, PathBuf};

const LINK: &str = "serial:/dev/orion-persist-missing";
const PROVIDER: &str = "imu.provider";
const RESOURCE: &str = "imu.sample";

fn state_dir(name: &str) -> PathBuf {
    crate::test_temp::dir().join(format!(
        "orion-lg-persist-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos()
    ))
}

/// A node with its state in `dir`. Building it again on the same directory is a restart.
fn persisted_app(dir: &Path) -> NodeApp {
    let socket =
        crate::test_temp::dir().join(format!("orion-lg-persist-{}.sock", std::process::id()));
    let config = NodeConfig::for_local_node(NODE)
        .with_ipc_socket_path(socket)
        .with_state_dir(dir);
    NodeApp::try_new(config).expect("node app should build")
}

fn link(name: &str) -> LinkConfig {
    LinkConfig::parse(name).expect("link should parse")
}

/// A device snapshot through the gateway's accepted path. `false` when the node rejects it.
fn publish_as(app: &NodeApp, link: &str, device: &str) -> bool {
    app.link_apply_device_state(
        link,
        device,
        device_provider(PROVIDER),
        vec![device_resource(RESOURCE, PROVIDER, "imu")],
    )
    .is_ok()
}

fn resource_availability(app: &NodeApp) -> Option<AvailabilityState> {
    observed_resource(app, RESOURCE).map(|resource| resource.availability)
}

/// Publishes from `imu-board` on [`LINK`], then drops the node without any shutdown.
fn crash_after_publish(dir: &Path) {
    let app = persisted_app(dir);
    // Starts with no links: the state of a fresh directory is restored (nothing), and the
    // gateway has no tasks to outlive the node.
    drop(
        app.start_link_gateway(Vec::new())
            .expect("gateway should start"),
    );
    assert!(publish_as(&app, LINK, "imu-board"), "device should publish");
    assert_eq!(
        resource_availability(&app),
        Some(AvailabilityState::Available)
    );
    drop(app);
}

#[tokio::test]
async fn crash_then_restart_marks_link_providers_unavailable() {
    let dir = state_dir("crash");
    crash_after_publish(&dir);

    let app = persisted_app(&dir);
    assert_eq!(
        resource_availability(&app),
        None,
        "observed state is not persisted, so the restart starts without it"
    );
    let handle = app
        .start_link_gateway(vec![link(LINK)])
        .expect("gateway should start");

    let resource = observed_resource(&app, RESOURCE).expect("the restored resource is reported");
    assert_eq!(resource.availability, AvailabilityState::Unavailable);
    assert_eq!(resource.health, HealthState::Unknown);
    assert!(has_provider(&app, PROVIDER));
    assert!(
        !publish_as(&app, LINK, "other-board"),
        "the provider stays reserved for the device that owned it"
    );
    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn reconnect_after_restart_makes_link_providers_available() {
    let dir = state_dir("reconnect");
    crash_after_publish(&dir);

    let app = persisted_app(&dir);
    let handle = app
        .start_link_gateway(vec![link(LINK)])
        .expect("gateway should start");
    assert_eq!(
        resource_availability(&app),
        Some(AvailabilityState::Unavailable)
    );

    assert!(
        publish_as(&app, LINK, "imu-board"),
        "device should reconnect"
    );
    assert_eq!(
        resource_availability(&app),
        Some(AvailabilityState::Available)
    );
    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn link_removed_from_config_is_forgotten() {
    let dir = state_dir("removed");
    crash_after_publish(&dir);

    // The link is gone from the config: its resources stay unavailable, and the provider id is
    // no longer reserved.
    let app = persisted_app(&dir);
    let handle = app
        .start_link_gateway(Vec::new())
        .expect("gateway should start");
    assert_eq!(
        resource_availability(&app),
        Some(AvailabilityState::Unavailable)
    );
    handle.shutdown().await;
    drop(app);

    // Restarted with the link configured again: nothing was persisted for it, so nothing is
    // restored and another device may publish the provider.
    let app = persisted_app(&dir);
    let handle = app
        .start_link_gateway(vec![link(LINK)])
        .expect("gateway should start");
    assert_eq!(resource_availability(&app), None);
    assert!(publish_as(&app, LINK, "spare-board"));
    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

/// `orion-node`'s startup order: the restore runs before the IPC servers accept clients. A local
/// provider that publishes the persisted id right at startup is refused, and the device keeps it.
#[tokio::test]
async fn an_ipc_provider_cannot_claim_a_persisted_id_at_startup() {
    use orion::control_plane::{ClientHello, ClientRole, ControlMessage, ProviderStateUpdate};
    use orion::transport::ipc::{ControlEnvelope, LocalAddress, UnixControlClient};

    let dir = state_dir("ipc-startup");
    crash_after_publish(&dir);

    let app = persisted_app(&dir);
    app.restore_link_devices(&[link(LINK)])
        .expect("the restore should succeed before the IPC servers start");
    let socket =
        crate::test_temp::dir().join(format!("orion-lg-startup-{}.sock", std::process::id()));
    let (_path, server) = app
        .start_ipc_server_graceful(&socket)
        .await
        .expect("the IPC server should start");

    let client = UnixControlClient::new(&socket);
    let send = |message: ControlMessage| {
        let envelope = ControlEnvelope {
            source: LocalAddress::new("local-provider"),
            destination: LocalAddress::new("orion"),
            message,
        };
        let client = client.clone();
        async move {
            client
                .send(envelope)
                .await
                .expect("the IPC exchange should complete")
        }
    };
    send(ControlMessage::ClientHello(ClientHello {
        client_name: "local-provider".into(),
        role: ClientRole::Provider,
    }))
    .await;
    let provider = ProviderRecord::builder(orion::ProviderId::new(PROVIDER), NODE)
        .resource_type("imu.sample_source")
        .build();
    let response = send(ControlMessage::ProviderState(ProviderStateUpdate {
        provider,
        resources: vec![device_resource(RESOURCE, PROVIDER, "imu")],
    }))
    .await;
    assert!(
        matches!(response.message, ControlMessage::Rejected(_)),
        "a local provider may not publish a link device's persisted id, got {:?}",
        response.message
    );
    assert_eq!(
        resource_availability(&app),
        Some(AvailabilityState::Unavailable),
        "the refused publish leaves the restored resource unavailable"
    );

    // The device still reconnects to its id.
    assert!(
        publish_as(&app, LINK, "imu-board"),
        "the device should reconnect"
    );
    assert_eq!(
        resource_availability(&app),
        Some(AvailabilityState::Available)
    );
    server.shutdown().await.expect("the IPC server should stop");
    let _ = std::fs::remove_dir_all(dir);
}
