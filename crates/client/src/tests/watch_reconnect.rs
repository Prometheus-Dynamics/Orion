//! Status and workload watches outlive the node they are connected to, like action watches (see
//! `app/reconnect.rs`): after `orion-node` restarts they reconnect with backoff, even with the
//! default (no retry) service policy.

use super::local_services::{
    serve_executor_bootstrap_query, serve_executor_watch_stream, unique_socket_path, write_welcome,
};
use crate::prelude::*;
use orion_control_plane::{
    ClientEvent, ClientEventKind, ClientRole, ControlMessage, ExecutorRecord, ProviderRecord,
    StatusChange, StatusEntry, StatusQuery, StatusSubject, TypedConfigValue, WorkloadRecord,
};
use orion_transport_ipc::{ControlEnvelope, LocalAddress, read_control_frame, write_control_frame};
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::UnixListener;

#[tokio::test]
async fn status_watch_resumes_after_the_node_restarts_with_the_default_policy() {
    let stream_socket_path = unique_socket_path("status-watch-stream");
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
    let stream_listener =
        UnixListener::bind(&stream_socket_path).expect("stream listener should bind");

    let provider = ProviderRecord::builder("provider.status", NodeId::new("node-a"))
        .resource_type(ResourceType::new("camera.frame"))
        .build();
    let subject = StatusSubject::Provider(provider.provider_id.clone());
    let before = StatusEntry::new(
        subject.clone(),
        "before.restart",
        TypedConfigValue::Bool(true),
    );
    let after = StatusEntry::new(subject, "after.restart", TypedConfigValue::Bool(false));
    let node_task = tokio::spawn(serve_status_restart(
        stream_listener,
        stream_socket_path.clone(),
        before.clone(),
        after.clone(),
    ));

    let runtime = LocalNodeRuntime::new(
        unique_socket_path("status-watch-unary"),
        &stream_socket_path,
    );
    let service = LocalProviderService::new(runtime, "status", provider);
    let mut watch = service
        .watch_status(StatusQuery::all())
        .await
        .expect("status watch should connect");

    let first = watch.next().await.expect("bootstrap before the restart");
    assert!(first.bootstrap);
    assert_eq!(first.updated, vec![before]);

    let second = watch
        .next()
        .await
        .expect("the status watch should resume after the restart");
    assert!(second.bootstrap, "a reconnect starts with a new bootstrap");
    assert_eq!(second.updated, vec![after]);

    node_task.await.expect("node task should complete");
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
}

#[tokio::test]
async fn workload_watch_resumes_after_the_node_restarts_with_the_default_policy() {
    let unary_socket_path = unique_socket_path("workload-watch-unary");
    let stream_socket_path = unique_socket_path("workload-watch-stream");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
    let unary_listener =
        UnixListener::bind(&unary_socket_path).expect("unary listener should bind");
    let stream_listener =
        UnixListener::bind(&stream_socket_path).expect("stream listener should bind");

    let executor = ExecutorRecord {
        executor_id: ExecutorId::new("executor.restart"),
        node_id: NodeId::new("node-a"),
        runtime_types: vec![RuntimeType::new("graph.exec.v1")],
    };
    let workload = |id: &str| {
        WorkloadRecord::builder(
            WorkloadId::new(id),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.restart"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-a"))
        .build()
    };
    let (before, after) = (workload("workload.before"), workload("workload.after"));

    let unary_task = tokio::spawn(serve_executor_bootstrap_query(
        unary_listener,
        executor.clone(),
        Vec::new(),
    ));
    let node_task = tokio::spawn(serve_workload_restart(
        stream_listener,
        stream_socket_path.clone(),
        executor.clone(),
        vec![before.clone()],
        vec![after.clone()],
    ));

    let runtime = LocalNodeRuntime::new(&unary_socket_path, &stream_socket_path);
    let service = LocalExecutorService::new(runtime, "restart", executor);
    let mut subscription = service
        .subscribe_workloads()
        .await
        .expect("executor subscription should connect");
    assert_eq!(
        subscription.next_event().await.expect("bootstrap event"),
        LocalExecutorEvent::Bootstrap(Vec::new())
    );
    assert_eq!(
        subscription
            .next_event()
            .await
            .expect("event before the restart"),
        LocalExecutorEvent::WorkloadsChanged {
            sequence: 7,
            workloads: vec![before],
        }
    );
    assert_eq!(
        subscription
            .next_event()
            .await
            .expect("the workload watch should resume after the restart"),
        LocalExecutorEvent::WorkloadsChanged {
            sequence: 8,
            workloads: vec![after],
        }
    );

    unary_task.await.expect("unary task should complete");
    node_task.await.expect("node task should complete");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
}

/// A node that serves one status watch, goes down for a while, and comes back.
async fn serve_status_restart(
    listener: UnixListener,
    path: PathBuf,
    before: StatusEntry,
    after: StatusEntry,
) {
    serve_status_session(&listener, before).await;
    drop(listener);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = tokio::fs::remove_file(&path).await;
    let listener = UnixListener::bind(&path).expect("restarted node should bind its socket");
    serve_status_session(&listener, after).await;
}

async fn serve_status_session(listener: &UnixListener, entry: StatusEntry) {
    let (mut stream, _) = listener.accept().await.expect("status accept should work");
    let hello = read_control_frame(&mut stream)
        .await
        .expect("hello frame should decode")
        .expect("hello frame should exist");
    write_welcome(
        &mut stream,
        hello.source,
        ClientRole::Provider,
        "status-status",
    )
    .await;
    let request = read_control_frame(&mut stream)
        .await
        .expect("watch request should decode")
        .expect("watch request should exist");
    assert!(matches!(request.message, ControlMessage::WatchStatus(_)));
    let destination = request.source;
    for message in [
        ControlMessage::Accepted,
        ControlMessage::ClientEvents(vec![ClientEvent {
            sequence: 1,
            event: ClientEventKind::Status(StatusChange {
                bootstrap: true,
                updated: vec![entry],
                expired: Vec::new(),
            }),
        }]),
    ] {
        write_control_frame(
            &mut stream,
            &ControlEnvelope {
                source: LocalAddress::new("orion"),
                destination: destination.clone(),
                message,
            },
        )
        .await
        .expect("frame should send");
    }
    // Dropping the stream closes the connection, as a node restart does.
}

/// A node that serves one workload watch, goes down, and comes back with a new workload.
async fn serve_workload_restart(
    listener: UnixListener,
    path: PathBuf,
    executor: ExecutorRecord,
    before: Vec<WorkloadRecord>,
    after: Vec<WorkloadRecord>,
) {
    serve_executor_watch_stream(listener, executor.clone(), before, 7).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = tokio::fs::remove_file(&path).await;
    let listener = UnixListener::bind(&path).expect("restarted node should bind its socket");
    serve_executor_watch_stream(listener, executor, after, 8).await;
}
