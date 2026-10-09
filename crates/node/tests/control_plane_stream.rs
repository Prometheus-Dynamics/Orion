//! `ControlPlaneEventStream` against real IPC servers: observed-state changes reach
//! `subscribe_state_and_observed` watchers (and not desired-only watchers), and a client that
//! reconnects under the same local address can subscribe again (events queued meanwhile are
//! resumed, not mistaken for responses) and is not detached by the old connection's teardown.

#![cfg(unix)]

use orion::ResourceType;
use orion::client::{ControlPlaneEventStream, LocalNodeRuntime, LocalProviderService};
use orion::control_plane::{
    AvailabilityState, ClientEvent, ClientEventKind, HealthState, HostFacts, HostMetricsSample,
    ProviderRecord, ResourceRecord, StateSnapshot, StatusQuery, StatusSubject,
};
use orion_node::{HostFactsSource, NodeApp, NodeConfig, NodeId};
use std::path::PathBuf;
use std::time::Duration;

fn temp_socket(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("orion-cps-{label}-{}.sock", std::process::id()))
}

struct Node {
    app: NodeApp,
    runtime: LocalNodeRuntime,
    socket: PathBuf,
    stream: PathBuf,
    servers: tokio::task::JoinHandle<()>,
}

async fn start_node(label: &str) -> Node {
    let socket = temp_socket(&format!("{label}-unary"));
    let stream = temp_socket(&format!("{label}-stream"));
    let app = NodeApp::try_new(
        NodeConfig::for_local_node(NodeId::new("node-a")).with_ipc_socket_path(socket.clone()),
    )
    .expect("node app should build");
    let (_, unary) = app
        .start_ipc_server_graceful(&socket)
        .await
        .expect("ipc server should start");
    let (_, stream_server) = app
        .start_ipc_stream_server_graceful(&stream)
        .await
        .expect("ipc stream server should start");
    Node {
        runtime: LocalNodeRuntime::new(&socket, &stream),
        app,
        socket,
        stream,
        servers: tokio::spawn(async move {
            let _servers = (unary, stream_server);
            std::future::pending::<()>().await;
        }),
    }
}

impl Node {
    async fn stop(self) {
        self.servers.abort();
        let _ = std::fs::remove_file(self.socket);
        let _ = std::fs::remove_file(self.stream);
    }

    async fn publish_camera(&self, health: HealthState) {
        let provider = ProviderRecord::builder(orion::ProviderId::new("provider.camera"), "node-a")
            .resource_type(ResourceType::new("camera.frame"))
            .build();
        let service = LocalProviderService::new(self.runtime.clone(), "camera", provider.clone());
        service.register().await.expect("register provider");
        self.runtime
            .provider("camera", provider)
            .expect("provider app")
            .publish_resource(
                ResourceRecord::builder("camera.front", "camera.frame", "provider.camera")
                    .health(health)
                    .availability(AvailabilityState::Available)
                    .build(),
            )
            .await
            .expect("publish resource");
    }
}

fn snapshots(events: Vec<ClientEvent>) -> Vec<StateSnapshot> {
    events
        .into_iter()
        .filter_map(|event| match event.event {
            ClientEventKind::StateSnapshot(snapshot) => Some(*snapshot),
            _ => None,
        })
        .collect()
}

/// Waits for a snapshot that satisfies `done`.
async fn next_snapshot_where(
    stream: &mut ControlPlaneEventStream,
    done: impl Fn(&StateSnapshot) -> bool,
) -> StateSnapshot {
    loop {
        let events = tokio::time::timeout(Duration::from_secs(5), stream.next_events())
            .await
            .expect("a snapshot should arrive")
            .expect("events");
        if let Some(snapshot) = snapshots(events).into_iter().rev().find(|s| done(s)) {
            return snapshot;
        }
    }
}

fn camera_health(snapshot: &StateSnapshot) -> Option<HealthState> {
    snapshot
        .state
        .observed
        .resources
        .get(&orion::ResourceId::new("camera.front"))
        .map(|resource| resource.health)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_changes_reach_observed_state_watchers() {
    let node = start_node("observed").await;
    let mut observed = ControlPlaneEventStream::connect_at(&node.stream, "observer")
        .await
        .expect("stream");
    observed
        .subscribe_state_and_observed(orion::Revision::ZERO)
        .await
        .expect("subscribe");
    // A bootstrap snapshot even though nothing changed yet.
    next_snapshot_where(&mut observed, |_| true).await;

    // Publishing a resource is an observed change (registering the provider also writes desired
    // state).
    node.publish_camera(HealthState::Healthy).await;
    let snapshot = next_snapshot_where(&mut observed, |s| camera_health(s).is_some()).await;
    assert_eq!(camera_health(&snapshot), Some(HealthState::Healthy));

    // A desired-only watcher, up to date with the desired revision.
    let mut desired_only = ControlPlaneEventStream::connect_at(&node.stream, "desired-only")
        .await
        .expect("stream");
    desired_only
        .subscribe_state(node.app.state_snapshot().state.desired.revision)
        .await
        .expect("subscribe");

    // A health change is an observed change only: the observed watcher hears it.
    node.publish_camera(HealthState::Degraded).await;
    let snapshot = next_snapshot_where(&mut observed, |s| {
        camera_health(s) == Some(HealthState::Degraded)
    })
    .await;
    assert_eq!(camera_health(&snapshot), Some(HealthState::Degraded));

    // The desired-only watcher does not.
    if let Ok(Ok(events)) =
        tokio::time::timeout(Duration::from_millis(300), desired_only.next_events()).await
    {
        assert!(
            snapshots(events).is_empty(),
            "desired-only watchers are not woken by observed changes"
        );
    }
    node.stop().await;
}

struct BusyHost(u32);

impl HostFactsSource for BusyHost {
    fn sample(&self) -> HostFacts {
        HostFacts {
            metrics: HostMetricsSample {
                cpu_busy_milli: Some(self.0),
                ..HostMetricsSample::default()
            },
            ..HostFacts::default()
        }
    }
}

fn host_status_query() -> StatusQuery {
    StatusQuery::subject(StatusSubject::Node(NodeId::new("node-a"))).with_key_prefix("host.")
}

async fn connect_fixed(node: &Node) -> ControlPlaneEventStream {
    ControlPlaneEventStream::connect_at_with_local_address(&node.stream, "helios-api", "helios-api")
        .await
        .expect("stream")
}

async fn subscribe_all(stream: &mut ControlPlaneEventStream) {
    stream
        .subscribe_state_and_observed(orion::Revision::ZERO)
        .await
        .expect("state subscription after reconnect");
    stream
        .subscribe_status(host_status_query())
        .await
        .expect("status subscription after reconnect");
}

/// Waits until a status event carries `host.cpu_busy_milli = expected`.
async fn wait_cpu(stream: &mut ControlPlaneEventStream, expected: u64) {
    loop {
        let events = tokio::time::timeout(Duration::from_secs(5), stream.next_events())
            .await
            .expect("status should arrive")
            .expect("events");
        for event in events {
            if let ClientEventKind::Status(change) = event.event
                && change.updated.iter().any(|entry| {
                    entry.key == "host.cpu_busy_milli"
                        && entry.value == orion::control_plane::TypedConfigValue::UInt(expected)
                })
            {
                return;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnecting_under_the_same_local_address_keeps_working() {
    let node = start_node("reconnect").await;

    // First connection: subscribed, then gone while events keep queueing for its address.
    let mut first = connect_fixed(&node).await;
    subscribe_all(&mut first).await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;
    node.app.refresh_host_facts_from(&BusyHost(100));
    node.publish_camera(HealthState::Healthy).await;

    // A restarted service reconnects under the same address: its subscriptions are accepted
    // even though queued events reach the stream first, and it receives new changes.
    let mut second = connect_fixed(&node).await;
    subscribe_all(&mut second).await;
    // Events queued for the address while it was disconnected are resumed (within the session
    // TTL) instead of breaking the subscriptions above.
    wait_cpu(&mut second, 100).await;
    node.app.refresh_host_facts_from(&BusyHost(200));
    wait_cpu(&mut second, 200).await;

    // A third connection arrives while the second is still open (a reconnect racing the old
    // connection's teardown); the old connection closing afterwards must not detach it.
    let mut third = connect_fixed(&node).await;
    subscribe_all(&mut third).await;
    drop(second);
    tokio::time::sleep(Duration::from_millis(200)).await;
    node.app.refresh_host_facts_from(&BusyHost(300));
    wait_cpu(&mut third, 300).await;
    node.publish_camera(HealthState::Degraded).await;
    next_snapshot_where(&mut third, |s| {
        camera_health(s) == Some(HealthState::Degraded)
    })
    .await;
    node.stop().await;
}
