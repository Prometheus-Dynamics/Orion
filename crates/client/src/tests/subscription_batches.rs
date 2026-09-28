use super::local_services::{serve_executor_bootstrap_query, unique_socket_path, write_welcome};
use crate::LocalExecutorEvent;
use crate::prelude::*;
use orion_control_plane::{ClientEvent, ClientEventKind, ControlMessage};
use orion_transport_ipc::{ControlEnvelope, LocalAddress, read_control_frame, write_control_frame};
use std::time::Duration;
use tokio::net::UnixListener;

fn workload(id: &str) -> WorkloadRecord {
    WorkloadRecord::builder(
        WorkloadId::new(id),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.batch"),
    )
    .assigned_to(NodeId::new("node-a"))
    .build()
}

#[tokio::test]
async fn executor_subscription_delivers_every_event_in_a_batch() {
    let unary_socket_path = unique_socket_path("batch-unary");
    let stream_socket_path = unique_socket_path("batch-stream");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
    let unary_listener =
        UnixListener::bind(&unary_socket_path).expect("unary listener should bind");
    let stream_listener =
        UnixListener::bind(&stream_socket_path).expect("stream listener should bind");

    let executor =
        ExecutorRecord::builder(ExecutorId::new("executor.batch"), NodeId::new("node-a"))
            .runtime_type(RuntimeType::new("graph.exec.v1"))
            .build();
    let unary_task = tokio::spawn(serve_executor_bootstrap_query(
        unary_listener,
        executor.clone(),
        Vec::new(),
    ));
    let executor_id = executor.executor_id.clone();
    let stream_task = tokio::spawn(async move {
        let (mut stream, _) = stream_listener.accept().await.expect("accept should work");
        let hello = read_control_frame(&mut stream)
            .await
            .expect("hello should decode")
            .expect("hello should exist");
        write_welcome(&mut stream, hello.source, ClientRole::Executor, "batch").await;
        let request = read_control_frame(&mut stream)
            .await
            .expect("watch request should decode")
            .expect("watch request should exist");
        let batch = [(1, "workload.first"), (2, "workload.second")]
            .into_iter()
            .map(|(sequence, id)| ClientEvent {
                sequence,
                event: ClientEventKind::ExecutorWorkloads {
                    executor_id: executor_id.clone(),
                    workloads: vec![workload(id)],
                },
            })
            .collect();
        for message in [
            ControlMessage::Accepted,
            ControlMessage::ClientEvents(batch),
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let runtime = LocalNodeRuntime::new(&unary_socket_path, &stream_socket_path);
    let service = LocalExecutorService::new(runtime, "batch", executor);
    let mut subscription = service
        .subscribe_workloads()
        .await
        .expect("subscription should connect");

    assert!(matches!(
        subscription.next_event().await.expect("bootstrap"),
        LocalExecutorEvent::Bootstrap(_)
    ));
    for (expected_sequence, expected_id) in [(1, "workload.first"), (2, "workload.second")] {
        match subscription.next_event().await.expect("batched event") {
            LocalExecutorEvent::WorkloadsChanged {
                sequence,
                workloads,
            } => {
                assert_eq!(sequence, expected_sequence);
                assert_eq!(workloads[0].workload_id, WorkloadId::new(expected_id));
            }
            other => panic!("unexpected event {other:?}"),
        }
    }

    unary_task.await.expect("unary task should complete");
    stream_task.await.expect("stream task should complete");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
}
