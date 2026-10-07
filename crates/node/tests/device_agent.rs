//! The device-agent contract (`docs/device-agent.md`) against real IPC servers: the example
//! agent (`crates/client/examples/device_agent/agent.rs`) with a fake updater claims `update`,
//! `reboot` and `locate`, reports progress, ends `update` with "staged and apply issued",
//! republishes the `update.*` keys after a simulated reboot, and fails in-flight actions when it
//! disconnects. Also covers the claim-scoped status keys and `ControlPlaneEventStream` status
//! subscriptions.

#![cfg(unix)]

#[path = "../../client/examples/device_agent/agent.rs"]
#[allow(dead_code)]
mod agent;

use agent::{FakeUpdater, UpdaterStatus};
use orion::client::{
    ControlPlaneEventStream, LocalControlPlaneClient, LocalNodeRuntime, LocalProviderService,
    LocalServiceRetryPolicy,
};
use orion::control_plane::{
    ActionRequest, ActionState, ActionTarget, ClientEventKind, HostFacts, HostMetricsSample,
    ProviderRecord, StatusEntry, StatusQuery, StatusSubject, TypedConfigValue, update_action,
};
use orion_node::{HostFactsSource, NodeApp, NodeConfig, NodeId};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const SHA: &str = "4f0d6a2c9b8e7d6c5b4a39281706f5e4d3c2b1a09f8e7d6c5b4a392817065f4e";

fn temp_socket(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("orion-agent-{label}-{}.sock", std::process::id()))
}

struct Node {
    app: NodeApp,
    runtime: LocalNodeRuntime,
    socket: PathBuf,
    stream: PathBuf,
    // Keeps the IPC servers' handles alive until the test stops the node.
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

    fn service(&self, name: &str) -> LocalProviderService {
        // Node action claims need no registered provider; the record only names the client.
        LocalProviderService::new(
            self.runtime.clone(),
            name,
            ProviderRecord::builder(orion::ProviderId::new(name), "node-a").build(),
        )
        .with_retry_policy(
            LocalServiceRetryPolicy::fixed_delay(Duration::from_millis(20)).with_max_attempts(10),
        )
    }

    fn node_status(&self, prefix: &str) -> Vec<StatusEntry> {
        self.app.query_status(
            &StatusQuery::subject(StatusSubject::Node(NodeId::new("node-a")))
                .with_key_prefix(prefix),
        )
    }

    fn node_value(&self, key: &str) -> Option<TypedConfigValue> {
        self.node_status(key)
            .into_iter()
            .find(|entry| entry.key == key)
            .map(|entry| entry.value)
    }

    async fn wait_value(&self, key: &str, expected: TypedConfigValue) {
        for _ in 0..500 {
            if self.node_value(key).as_ref() == Some(&expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "status key {key} never became {expected:?} (now {:?})",
            self.node_value(key)
        );
    }
}

fn text(value: &str) -> TypedConfigValue {
    TypedConfigValue::String(value.into())
}

fn update_request(id: &str, image_url: &str) -> ActionRequest {
    ActionRequest::new(id, ActionTarget::Node(NodeId::new("node-a")), "update")
        .with_arg(update_action::ARG_IMAGE_URL, text(image_url))
        .with_arg(update_action::ARG_SHA256, text(SHA))
        .with_arg(update_action::ARG_SIZE, TypedConfigValue::UInt(1 << 20))
        .with_deadline_ms(60_000)
}

async fn wait_final(
    client: &LocalControlPlaneClient,
    action_id: &str,
) -> orion::control_plane::ActionResult {
    client
        .wait_for_action(
            action_id,
            Duration::from_millis(10),
            Duration::from_secs(10),
        )
        .await
        .expect("wait for action")
}

fn spawn_agent(
    service: LocalProviderService,
    updater: Arc<FakeUpdater>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = agent::run(&service, updater, Duration::from_millis(100)).await;
    })
}

fn booted(version: &str, slot: &str, state: &str, boot: &str) -> UpdaterStatus {
    UpdaterStatus {
        state: state.into(),
        slot_active: Some(slot.into()),
        version_active: Some(version.into()),
        boot_id: Some(boot.into()),
        ..UpdaterStatus::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_agent_updates_reboots_and_republishes_after_boot() {
    let node = start_node("contract").await;
    let operator =
        LocalControlPlaneClient::connect_at(&node.socket, "operator").expect("operator client");

    // Boot 1: the agent claims its actions and publishes the updater's state.
    let updater = Arc::new(FakeUpdater::new(
        booted("1.0", "A", "idle", "boot-1"),
        4,
        Duration::from_millis(20),
    ));
    let agent_task = spawn_agent(node.service("device-agent"), updater.clone());
    node.wait_value(update_action::KEY_STATE, text("idle"))
        .await;
    assert_eq!(
        node.node_value(update_action::KEY_VERSION_ACTIVE),
        Some(text("1.0"))
    );
    assert_eq!(
        node.node_value(update_action::KEY_BOOT_ID),
        Some(text("boot-1"))
    );

    // Invalid arguments are rejected by the agent, not run.
    let mut bad = update_request("update-bad", "http://atlas/image-2.0.img.xz");
    bad.args.remove(update_action::ARG_SHA256);
    operator.run_action(bad).await.expect("run");
    assert!(matches!(
        wait_final(&operator, "update-bad").await.state,
        ActionState::Rejected { reason } if reason.contains("sha256")
    ));

    // A good update: progress while staging, then "staged and apply issued".
    let accepted = operator
        .run_action(update_request("update-1", "http://atlas/image-2.0.img.xz"))
        .await
        .expect("run");
    assert_eq!(accepted.state, ActionState::Accepted);
    let done = wait_final(&operator, "update-1").await;
    assert_eq!(done.state, ActionState::Succeeded, "{done:?}");
    assert_eq!(
        done.output[update_action::OUTPUT_PHASE],
        text(update_action::PHASE_REBOOTING)
    );
    assert_eq!(
        done.output[update_action::OUTPUT_VERSION_STAGED],
        text("2.0")
    );
    node.wait_value("action.update-1.state", text("succeeded"))
        .await;
    assert_eq!(
        node.node_value("action.update-1.progress"),
        Some(TypedConfigValue::UInt(1000))
    );
    node.wait_value(update_action::KEY_VERSION_STAGED, text("2.0"))
        .await;
    for _ in 0..200 {
        if updater.calls().contains(&"apply".to_owned()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        updater.calls(),
        vec![
            "stage http://atlas/image-2.0.img.xz".to_owned(),
            "apply".to_owned()
        ]
    );

    // A failing stage fails the action and leaves the error in the durable keys.
    operator
        .run_action(update_request("update-2", "http://atlas/corrupt.img.xz"))
        .await
        .expect("run");
    assert_eq!(
        wait_final(&operator, "update-2").await.state,
        ActionState::Failed {
            reason: "sha256 mismatch".into()
        }
    );
    node.wait_value(update_action::KEY_ERROR, text("sha256 mismatch"))
        .await;

    // Locate and reboot.
    operator
        .run_action(
            ActionRequest::new(
                "locate-1",
                ActionTarget::Node(NodeId::new("node-a")),
                "locate",
            )
            .with_arg("duration_ms", TypedConfigValue::UInt(500)),
        )
        .await
        .expect("run");
    assert_eq!(
        wait_final(&operator, "locate-1").await.state,
        ActionState::Succeeded
    );
    operator
        .run_action(ActionRequest::new(
            "reboot-1",
            ActionTarget::Node(NodeId::new("node-a")),
            "reboot",
        ))
        .await
        .expect("run");
    assert_eq!(
        wait_final(&operator, "reboot-1").await.state,
        ActionState::Succeeded
    );

    // "Reboot": the agent goes away (its claims are released) and comes back on the new boot,
    // where the updater confirmed the trial slot. The keys describe the new boot.
    agent_task.abort();
    let _ = agent_task.await;
    let updater = Arc::new(FakeUpdater::new(
        booted("2.0", "B", "confirmed", "boot-2"),
        4,
        Duration::from_millis(20),
    ));
    let agent_task = spawn_agent(node.service("device-agent"), updater.clone());
    node.wait_value(update_action::KEY_STATE, text("confirmed"))
        .await;
    assert_eq!(
        node.node_value(update_action::KEY_VERSION_ACTIVE),
        Some(text("2.0"))
    );
    assert_eq!(
        node.node_value(update_action::KEY_SLOT_ACTIVE),
        Some(text("B"))
    );
    assert_eq!(
        node.node_value(update_action::KEY_BOOT_ID),
        Some(text("boot-2"))
    );
    // The keys are republished periodically, so a changed updater state shows up unprompted.
    updater.set_status(booted("2.0", "B", "idle", "boot-2"));
    node.wait_value(update_action::KEY_STATE, text("idle"))
        .await;

    // Disconnect with an update in flight: the action fails, the claim is released.
    let hold = updater.hold_staging();
    operator
        .run_action(update_request("update-3", "http://atlas/image-3.0.img.xz"))
        .await
        .expect("run");
    node.wait_value("action.update-3.state", text("running"))
        .await;
    agent_task.abort();
    let _ = agent_task.await;
    let failed = wait_final(&operator, "update-3").await;
    assert_eq!(
        failed.state,
        ActionState::Failed {
            reason: "handler disconnected".into()
        }
    );
    hold.notify_one();
    // Its status entries stay until their TTL; the next agent republishes them.
    assert!(node.node_value(update_action::KEY_STATE).is_some());
    let rejected = operator
        .run_action(update_request("update-4", "http://atlas/image-3.0.img.xz"))
        .await
        .expect("run");
    assert!(
        matches!(&rejected.state, ActionState::Rejected { .. }),
        "without a claimant `update` is rejected: {rejected:?}"
    );

    node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_status_keys_follow_the_claimed_action_names() {
    let node = start_node("keys").await;
    let tester = node.service("self-tester");
    let watch = tester
        .claim_node_actions(["self-test"])
        .await
        .expect("claim self-test");
    assert_eq!(watch.node_id().as_str(), "node-a");
    watch
        .publish_status([
            watch.node_status_entry("self-test.result", text("pass")),
            watch.node_status_entry("action.t1.state", text("succeeded")),
        ])
        .await
        .expect("keys of a claimed name and action keys are allowed");
    assert_eq!(node.node_value("self-test.result"), Some(text("pass")));
    for key in [
        "update.state",
        "self-testing",
        "self-test.",
        "host.load1_milli",
    ] {
        let err = watch
            .publish_status([watch.node_status_entry(key, text("x"))])
            .await
            .expect_err(key);
        assert!(format!("{err}").contains("claimed"), "{key}: {err}");
    }

    // Without any claim, nothing may be published for the node subject.
    let stranger = node
        .service("stranger")
        .with_retry_policy(LocalServiceRetryPolicy::no_retry());
    stranger.register().await.expect("register");
    let runtime_client = node
        .runtime
        .provider("stranger", stranger.provider().clone())
        .expect("provider client");
    assert!(
        runtime_client
            .publish_status([watch.node_status_entry("action.t2.state", text("x"))])
            .await
            .is_err()
    );
    drop(watch);
    node.stop().await;
}

struct BusyHost;

impl HostFactsSource for BusyHost {
    fn sample(&self) -> HostFacts {
        HostFacts {
            metrics: HostMetricsSample {
                cpu_busy_milli: Some(420),
                cpu_core_busy_milli: vec![400, 440],
                ..HostMetricsSample::default()
            },
            ..HostFacts::default()
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_event_stream_follows_host_metrics_on_the_status_lane() {
    let node = start_node("host-watch").await;
    let mut events = ControlPlaneEventStream::connect_at(&node.stream, "helios-api")
        .await
        .expect("event stream");
    assert_eq!(events.node_id().as_str(), "node-a");
    events
        .subscribe_status(
            StatusQuery::subject(StatusSubject::Node(NodeId::new("node-a")))
                .with_key_prefix("host."),
        )
        .await
        .expect("subscribe");
    node.app.refresh_host_facts_from(&BusyHost);
    let mut seen = std::collections::BTreeMap::new();
    for _ in 0..20 {
        let batch = tokio::time::timeout(Duration::from_secs(5), events.next_events())
            .await
            .expect("events should arrive")
            .expect("events");
        for event in batch {
            if let ClientEventKind::Status(change) = event.event {
                for entry in change.updated {
                    seen.insert(entry.key, entry.value);
                }
            }
        }
        if seen.contains_key("host.cpu1_busy_milli") {
            break;
        }
    }
    assert_eq!(seen["host.cpu_busy_milli"], TypedConfigValue::UInt(420));
    assert_eq!(seen["host.cpu0_busy_milli"], TypedConfigValue::UInt(400));
    assert_eq!(seen["host.cpu1_busy_milli"], TypedConfigValue::UInt(440));

    // The observability snapshot carries live CPU and temperature figures where the host has
    // them, else the host-facts sample's.
    let host = node.app.observability_snapshot().host;
    if std::path::Path::new("/proc/stat").exists() {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let host = node.app.observability_snapshot().host;
        assert!(host.cpu_busy_milli.is_some_and(|milli| milli <= 1000));
        assert!(host.cpu_window_ms.is_some_and(|ms| ms >= 250));
        assert!(!host.cpu_core_busy_milli.is_empty());
    } else {
        assert_eq!(host.cpu_busy_milli, Some(420));
    }
    node.stop().await;
}
