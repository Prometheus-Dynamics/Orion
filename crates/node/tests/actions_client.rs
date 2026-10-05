//! orion-client action APIs against real IPC servers: a provider handles resource actions, an
//! out-of-process device manager claims node actions, claims are released when the handler's
//! stream disconnects, and conflicting claims are refused (`docs/actions.md`).

#![cfg(unix)]

use orion::ResourceType;
use orion::client::{
    ActionWatch, LocalControlPlaneClient, LocalNodeRuntime, LocalProviderService,
    LocalServiceRetryPolicy,
};
use orion::control_plane::{
    ActionQuery, ActionRequest, ActionState, ActionTarget, AvailabilityState, HealthState,
    ProviderRecord, ResourceRecord, TypedConfigValue,
};
use orion_node::{NodeApp, NodeConfig, NodeId};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

fn temp_socket(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("orion-act-{label}-{}.sock", std::process::id()))
}

fn service(runtime: &LocalNodeRuntime, name: &str, provider: &str) -> LocalProviderService {
    LocalProviderService::new(
        runtime.clone(),
        name,
        ProviderRecord::builder(orion::ProviderId::new(provider), "node-a")
            .resource_type(ResourceType::new("camera.frame"))
            .build(),
    )
    .with_retry_policy(
        LocalServiceRetryPolicy::fixed_delay(Duration::from_millis(20)).with_max_attempts(10),
    )
}

async fn wait_state(
    client: &LocalControlPlaneClient,
    action_id: &str,
    done: impl Fn(&ActionState) -> bool,
) -> ActionState {
    for _ in 0..500 {
        let state = client
            .query_actions(ActionQuery::action(action_id))
            .await
            .expect("query")
            .pop()
            .expect("tracked")
            .state;
        if done(&state) {
            return state;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("action {action_id} did not reach the expected state");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn providers_and_device_managers_handle_actions_over_ipc() {
    let socket_path = temp_socket("unary");
    let stream_path = temp_socket("stream");
    let app = NodeApp::try_new(
        NodeConfig::for_local_node(NodeId::new("node-a")).with_ipc_socket_path(socket_path.clone()),
    )
    .expect("node app should build");
    let (_, unary) = app
        .start_ipc_server_graceful(&socket_path)
        .await
        .expect("ipc server should start");
    let (_, stream) = app
        .start_ipc_stream_server_graceful(&stream_path)
        .await
        .expect("ipc stream server should start");
    let runtime = LocalNodeRuntime::new(&socket_path, &stream_path);
    let operator =
        LocalControlPlaneClient::connect_at(&socket_path, "operator").expect("operator client");

    // A provider handles actions on its resource.
    let camera = service(&runtime, "camera", "provider.camera");
    camera.register().await.expect("register");
    runtime
        .provider("camera", camera.provider().clone())
        .expect("provider app")
        .publish_resource(
            ResourceRecord::builder("camera.front", "camera.frame", "provider.camera")
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .build(),
        )
        .await
        .expect("publish resource");
    let mut requests = camera.watch_action_requests().await.expect("watch");
    let mut results = ActionWatch::connect_at(&stream_path, "operator-watch", ActionQuery::all())
        .await
        .expect("action watch");

    let accepted = operator
        .run_action(
            ActionRequest::new(
                "locate-1",
                ActionTarget::Resource("camera.front".into()),
                "locate",
            )
            .with_arg("duration_ms", TypedConfigValue::UInt(500)),
        )
        .await
        .expect("run");
    assert_eq!(accepted.state, ActionState::Accepted);
    let request = tokio::time::timeout(Duration::from_secs(5), requests.next())
        .await
        .expect("request should arrive")
        .expect("request");
    assert_eq!(request.action_id, "locate-1");
    requests
        .progress("locate-1", Some(500))
        .await
        .expect("progress");
    requests
        .succeed(
            "locate-1",
            BTreeMap::from([("blinks".to_owned(), TypedConfigValue::UInt(5))]),
        )
        .await
        .expect("succeed");
    let done = operator
        .wait_for_action(
            "locate-1",
            Duration::from_millis(10),
            Duration::from_secs(5),
        )
        .await
        .expect("wait");
    assert_eq!(done.state, ActionState::Succeeded);
    assert_eq!(done.output["blinks"], TypedConfigValue::UInt(5));
    let mut seen_final = false;
    for _ in 0..10 {
        let batch = tokio::time::timeout(Duration::from_secs(5), results.next())
            .await
            .expect("results should arrive")
            .expect("results");
        if batch
            .iter()
            .any(|result| result.action_id == "locate-1" && result.state.is_terminal())
        {
            seen_final = true;
            break;
        }
    }
    assert!(seen_final, "the watch reports the final result");

    // A device manager claims node actions; a second claim of the same name is refused.
    let manager = service(&runtime, "device-manager", "provider.device-manager");
    manager.register().await.expect("register");
    let mut node_actions = manager
        .claim_node_actions(["update", "reboot"])
        .await
        .expect("claim");
    let rival = service(&runtime, "rival", "provider.rival")
        .with_retry_policy(LocalServiceRetryPolicy::no_retry());
    rival.register().await.expect("register");
    assert!(rival.claim_node_actions(["update"]).await.is_err());

    operator
        .run_action(
            ActionRequest::new(
                "update-1",
                ActionTarget::Node(NodeId::new("node-a")),
                "update",
            )
            .with_arg("transfer_id", TypedConfigValue::String("t-1".into())),
        )
        .await
        .expect("run");
    let request = tokio::time::timeout(Duration::from_secs(5), node_actions.next())
        .await
        .expect("request should arrive")
        .expect("request");
    assert_eq!(request.name, "update");
    node_actions
        .succeed(
            "update-1",
            BTreeMap::from([(
                "phase".to_owned(),
                TypedConfigValue::String("rebooting".into()),
            )]),
        )
        .await
        .expect("succeed");
    assert_eq!(
        wait_state(&operator, "update-1", ActionState::is_terminal).await,
        ActionState::Succeeded
    );

    // The manager disconnects with an action in flight: it fails and the claim is released.
    operator
        .run_action(ActionRequest::new(
            "reboot-1",
            ActionTarget::Node(NodeId::new("node-a")),
            "reboot",
        ))
        .await
        .expect("run");
    let _ = tokio::time::timeout(Duration::from_secs(5), node_actions.next())
        .await
        .expect("request should arrive");
    drop(node_actions);
    assert_eq!(
        wait_state(&operator, "reboot-1", ActionState::is_terminal).await,
        ActionState::Failed {
            reason: "handler disconnected".into()
        }
    );
    let _rival_watch = rival
        .claim_node_actions(["update"])
        .await
        .expect("the released name can be claimed");

    drop(requests);
    drop(results);
    let _ = unary.shutdown().await;
    let _ = stream.shutdown().await;
    let _ = std::fs::remove_file(socket_path);
    let _ = std::fs::remove_file(stream_path);
}
