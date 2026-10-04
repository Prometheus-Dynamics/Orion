//! The orion-client status API (`publish_status`, `query_status`, `watch_status`) against real
//! IPC servers.

#![cfg(unix)]

use orion::ResourceType;
use orion::client::{LocalNodeRuntime, LocalProviderService, LocalServiceRetryPolicy};
use orion::control_plane::{ProviderRecord, StatusQuery, StatusSubject, TypedConfigValue};
use orion_node::{NodeApp, NodeConfig, NodeId};
use std::path::PathBuf;
use std::time::Duration;

fn temp_socket(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("orion-sl-{label}-{}.sock", std::process::id()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_service_publishes_queries_and_watches_status() {
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
    let service = LocalProviderService::new(
        runtime,
        "status-camera",
        ProviderRecord::builder("provider.status-camera", "node-a")
            .resource_type(ResourceType::new("camera.frame"))
            .build(),
    )
    .with_retry_policy(
        LocalServiceRetryPolicy::fixed_delay(Duration::from_millis(20)).with_max_attempts(10),
    );

    // Publishing before the provider is registered is refused: the client owns nothing yet.
    let refused = LocalProviderService::new(
        service.runtime().clone(),
        "status-camera",
        service.provider().clone(),
    )
    .publish_status([service.status_entry("fps", TypedConfigValue::UInt(30))])
    .await;
    assert!(refused.is_err(), "unregistered publisher: {refused:?}");

    service.register().await.expect("provider should register");
    let mut watch = service
        .watch_status(StatusQuery::subject(StatusSubject::Provider(
            service.provider().provider_id.clone(),
        )))
        .await
        .expect("watch should start");
    let bootstrap = watch.next().await.expect("bootstrap change");
    assert!(bootstrap.bootstrap);
    assert!(bootstrap.updated.is_empty());

    service
        .publish_status([
            service.status_entry("fps", TypedConfigValue::UInt(30)),
            service
                .status_entry("mode", TypedConfigValue::String("streaming".into()))
                .with_ttl_ms(60_000),
        ])
        .await
        .expect("registered provider publishes its own status");

    let change = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("change should arrive")
        .expect("change should decode");
    assert!(!change.bootstrap);
    assert_eq!(change.updated.len(), 2);

    let entries = service
        .query_status(StatusQuery::all().with_key_prefix("mo"))
        .await
        .expect("query should succeed");
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].value,
        TypedConfigValue::String("streaming".into())
    );
    assert_eq!(entries[0].ttl_ms, 60_000);

    drop(watch);
    let _ = unary.shutdown().await;
    let _ = stream.shutdown().await;
    let _ = std::fs::remove_file(socket_path);
    let _ = std::fs::remove_file(stream_path);
}
