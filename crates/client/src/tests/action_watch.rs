//! An action watch outlives the node it is connected to: after `orion-node` restarts, the watch
//! reconnects and re-registers with backoff, even with the default (no retry) service policy.

use super::local_services::{unique_socket_path, write_welcome};
use crate::prelude::*;
use orion_control_plane::{
    ActionRequest, ActionTarget, ClientEvent, ClientEventKind, ControlMessage, ProviderRecord,
};
use orion_transport_ipc::{ControlEnvelope, LocalAddress, read_control_frame, write_control_frame};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::net::UnixListener;

#[tokio::test]
async fn action_watch_resumes_after_the_node_restarts_with_the_default_policy() {
    let unary_socket_path = unique_socket_path("action-watch-unary");
    let stream_socket_path = unique_socket_path("action-watch-stream");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
    let stream_listener =
        UnixListener::bind(&stream_socket_path).expect("stream listener should bind");

    let provider = ProviderRecord::builder("provider.actions", NodeId::new("node-a"))
        .resource_type(ResourceType::new("camera.frame"))
        .build();
    let provider_id = provider.provider_id.clone();
    let node_task = tokio::spawn(serve_node_restart(
        stream_listener,
        stream_socket_path.clone(),
        provider_id.clone(),
    ));

    // The default policy: the first registration and the reconnects get no retries.
    let runtime = LocalNodeRuntime::new(&unary_socket_path, &stream_socket_path);
    let service = LocalProviderService::new(runtime, "actions", provider);
    let mut watch = service
        .watch_action_requests()
        .await
        .expect("watch should connect");

    let first = watch
        .next()
        .await
        .expect("first request before the restart");
    assert_eq!(first.action_id, "action.before-restart");
    assert_eq!(watch.reconnects(), 0);

    let second = watch
        .next()
        .await
        .expect("the watch should resume after the restart");
    assert_eq!(second.action_id, "action.after-restart");
    assert_eq!(second.target, ActionTarget::Provider(provider_id));
    assert_eq!(watch.reconnects(), 1);

    node_task.await.expect("node task should complete");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
}

#[tokio::test]
async fn action_watch_ends_with_the_error_when_the_restarted_node_refuses_it() {
    let unary_socket_path = unique_socket_path("action-watch-refused-unary");
    let stream_socket_path = unique_socket_path("action-watch-refused-stream");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
    let stream_listener =
        UnixListener::bind(&stream_socket_path).expect("stream listener should bind");

    let provider = ProviderRecord::builder("provider.refused", NodeId::new("node-a"))
        .resource_type(ResourceType::new("camera.frame"))
        .build();
    let provider_id = provider.provider_id.clone();
    let node_task = tokio::spawn(serve_node_refusing_reconnect(
        stream_listener,
        stream_socket_path.clone(),
        provider_id.clone(),
    ));

    let runtime = LocalNodeRuntime::new(&unary_socket_path, &stream_socket_path);
    let service = LocalProviderService::new(runtime, "actions", provider);
    let mut watch = service
        .watch_action_requests()
        .await
        .expect("watch should connect");
    let first = watch
        .next()
        .await
        .expect("first request before the restart");
    assert_eq!(first.action_id, "action.before-refusal");

    // The restarted node refuses the registration (say, the claim is held by another client). A
    // retry cannot fix that, so the watch ends with the refusal instead of reconnecting forever.
    let error = watch
        .next()
        .await
        .expect_err("a refused reconnect should end the watch");
    assert_eq!(
        error,
        ClientError::Rejected("another client already watches these providers".into())
    );

    node_task.await.expect("node task should complete");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
}

/// A node that serves one request, goes down, and comes back refusing the registration.
async fn serve_node_refusing_reconnect(
    listener: UnixListener,
    path: PathBuf,
    provider_id: ProviderId,
) {
    serve_one_action_watch(&listener, &provider_id, "action.before-refusal").await;
    drop(listener);
    tokio::time::sleep(Duration::from_millis(300)).await;
    remove_socket(&path).await;
    let listener = UnixListener::bind(&path).expect("restarted node should bind its socket");
    let (mut stream, _) = listener.accept().await.expect("watch accept should work");
    let hello = read_control_frame(&mut stream)
        .await
        .expect("hello frame should decode")
        .expect("hello frame should exist");
    write_welcome(
        &mut stream,
        hello.source,
        ClientRole::Provider,
        "actions-actions",
    )
    .await;
    let request = read_control_frame(&mut stream)
        .await
        .expect("watch request should decode")
        .expect("watch request should exist");
    write_control_frame(
        &mut stream,
        &ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: request.source,
            message: ControlMessage::Rejected(
                "another client already watches these providers".into(),
            ),
        },
    )
    .await
    .expect("rejection should send");
}

/// A node that serves one request, goes down (its listener closes) for a while, and comes back.
async fn serve_node_restart(listener: UnixListener, path: PathBuf, provider_id: ProviderId) {
    serve_one_action_watch(&listener, &provider_id, "action.before-restart").await;
    drop(listener);
    // The watch's reconnects are refused while the node is down.
    tokio::time::sleep(Duration::from_millis(300)).await;
    remove_socket(&path).await;
    let listener = UnixListener::bind(&path).expect("restarted node should bind its socket");
    serve_one_action_watch(&listener, &provider_id, "action.after-restart").await;
}

async fn serve_one_action_watch(
    listener: &UnixListener,
    provider_id: &ProviderId,
    action_id: &str,
) {
    let (mut stream, _) = listener.accept().await.expect("watch accept should work");
    let hello = read_control_frame(&mut stream)
        .await
        .expect("hello frame should decode")
        .expect("hello frame should exist");
    write_welcome(
        &mut stream,
        hello.source,
        ClientRole::Provider,
        "actions-actions",
    )
    .await;
    let request = read_control_frame(&mut stream)
        .await
        .expect("watch request should decode")
        .expect("watch request should exist");
    assert!(matches!(
        &request.message,
        ControlMessage::WatchActionRequests(targets)
            if targets == &[ActionTarget::Provider(provider_id.clone())]
    ));
    let action = ActionRequest::new(
        action_id,
        ActionTarget::Provider(provider_id.clone()),
        "noop",
    );
    for message in [
        ControlMessage::Accepted,
        ControlMessage::ClientEvents(vec![ClientEvent {
            sequence: 1,
            event: ClientEventKind::ActionRequest(Box::new(action)),
        }]),
    ] {
        write_control_frame(
            &mut stream,
            &ControlEnvelope {
                source: LocalAddress::new("orion"),
                destination: request.source.clone(),
                message,
            },
        )
        .await
        .expect("frame should send");
    }
    // Dropping the stream closes the connection, as a node restart does.
}

async fn remove_socket(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}
