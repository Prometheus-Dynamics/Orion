//! Regression coverage for `LocalProviderSubscription::next_event` cancel safety.
//!
//! The subscription multiplexes a lease stream and a state stream. If a frame read on one stream
//! is cancelled mid-frame because the other stream produced events first, the partially consumed
//! bytes must not be lost: the lease stream must still deliver its event without reconnecting.

use super::local_services::{serve_provider_bootstrap_queries, unique_socket_path, write_welcome};
use crate::prelude::*;
use orion_control_plane::{
    ClientEvent, ClientEventKind, ClientHello, ControlMessage, LeaseRecord, ProviderLeaseQuery,
    StateSnapshot, StateWatch,
};
use orion_core::encode_to_vec;
use orion_transport_ipc::{ControlEnvelope, LocalAddress, read_control_frame, write_control_frame};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::time::{sleep, timeout};

fn snapshot_at(revision: u64) -> StateSnapshot {
    StateSnapshot {
        state: orion_control_plane::ClusterStateEnvelope::new(
            orion_control_plane::DesiredClusterState {
                revision: Revision::new(revision),
                ..Default::default()
            },
            orion_control_plane::ObservedClusterState::default(),
            orion_control_plane::AppliedClusterState::default(),
        ),
    }
}

fn envelope(destination: &LocalAddress, message: ControlMessage) -> ControlEnvelope {
    ControlEnvelope {
        source: LocalAddress::new("orion"),
        destination: destination.clone(),
        message,
    }
}

async fn write_frame_in_two_chunks(stream: &mut UnixStream, envelope: &ControlEnvelope) {
    let payload = encode_to_vec(envelope).expect("frame should encode");
    let mut frame = Vec::with_capacity(payload.len() + 4);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload);
    // Split inside the payload so the reader has consumed the length prefix and part of the body
    // when it gets cancelled.
    let split = 4 + payload.len() / 2;
    stream
        .write_all(&frame[..split])
        .await
        .expect("first chunk should write");
    stream.flush().await.expect("first chunk should flush");
    sleep(Duration::from_millis(300)).await;
    stream
        .write_all(&frame[split..])
        .await
        .expect("second chunk should write");
    stream.flush().await.expect("second chunk should flush");
}

struct SplitFrameServer {
    provider: ProviderRecord,
    lease: LeaseRecord,
    lease_connections: AtomicUsize,
    state_connections: AtomicUsize,
}

impl SplitFrameServer {
    async fn serve(self: Arc<Self>, listener: UnixListener) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(Arc::clone(&self).handle(stream));
        }
    }

    async fn handle(self: Arc<Self>, mut stream: UnixStream) {
        let Ok(Some(hello)) = read_control_frame(&mut stream).await else {
            return;
        };
        let ControlMessage::ClientHello(ClientHello {
            role, client_name, ..
        }) = &hello.message
        else {
            panic!("unexpected stream hello: {:?}", hello.message);
        };
        let client_name = client_name.as_str().to_owned();
        write_welcome(&mut stream, hello.source, role.clone(), &client_name).await;

        let Ok(Some(request)) = read_control_frame(&mut stream).await else {
            return;
        };
        let peer = request.source.clone();
        match request.message {
            ControlMessage::QueryStateSnapshot => {
                let _ = write_control_frame(
                    &mut stream,
                    &envelope(&peer, ControlMessage::Snapshot(snapshot_at(0))),
                )
                .await;
            }
            ControlMessage::WatchProviderLeases(ProviderLeaseQuery { provider_id }) => {
                assert_eq!(provider_id, self.provider.provider_id);
                let attempt = self.lease_connections.fetch_add(1, Ordering::SeqCst);
                write_control_frame(&mut stream, &envelope(&peer, ControlMessage::Accepted))
                    .await
                    .expect("accepted should send");
                if attempt == 0 {
                    let events = ControlMessage::ClientEvents(vec![ClientEvent {
                        sequence: 7,
                        event: ClientEventKind::ProviderLeases {
                            provider_id,
                            leases: vec![self.lease.clone()],
                        },
                    }]);
                    // Give the client time to start polling both streams.
                    sleep(Duration::from_millis(100)).await;
                    write_frame_in_two_chunks(&mut stream, &envelope(&peer, events)).await;
                }
                sleep(Duration::from_secs(5)).await;
            }
            ControlMessage::WatchState(StateWatch { .. }) => {
                let attempt = self.state_connections.fetch_add(1, Ordering::SeqCst);
                write_control_frame(&mut stream, &envelope(&peer, ControlMessage::Accepted))
                    .await
                    .expect("accepted should send");
                if attempt == 0 {
                    // Land state events while the lease frame is half written.
                    sleep(Duration::from_millis(200)).await;
                    for sequence in [11, 12] {
                        let events = ControlMessage::ClientEvents(vec![ClientEvent {
                            sequence,
                            event: ClientEventKind::StateSnapshot(Box::new(snapshot_at(sequence))),
                        }]);
                        write_control_frame(&mut stream, &envelope(&peer, events))
                            .await
                            .expect("state events should send");
                        sleep(Duration::from_millis(50)).await;
                    }
                }
                sleep(Duration::from_secs(5)).await;
            }
            other => panic!("unexpected stream request: {other:?}"),
        }
    }
}

#[tokio::test]
async fn provider_subscription_survives_cancelled_mid_frame_reads() {
    let unary_socket_path = unique_socket_path("cancel-safety-unary");
    let stream_socket_path = unique_socket_path("cancel-safety-stream");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
    let unary_listener =
        UnixListener::bind(&unary_socket_path).expect("unary listener should bind");
    let stream_listener =
        UnixListener::bind(&stream_socket_path).expect("stream listener should bind");

    let provider = ProviderRecord::builder("provider.cancel", "node-a")
        .resource_type("camera.device")
        .build();
    let lease = LeaseRecord::builder(ResourceId::new("resource.split"))
        .holder_node(NodeId::new("node-a"))
        .build();
    let server = Arc::new(SplitFrameServer {
        provider: provider.clone(),
        lease: lease.clone(),
        lease_connections: AtomicUsize::new(0),
        state_connections: AtomicUsize::new(0),
    });
    let unary_task = tokio::spawn(serve_provider_bootstrap_queries(
        unary_listener,
        provider.clone(),
        Vec::new(),
        snapshot_at(0),
    ));
    let stream_task = tokio::spawn(Arc::clone(&server).serve(stream_listener));

    let runtime = LocalNodeRuntime::new(&unary_socket_path, &stream_socket_path);
    let service = LocalProviderService::new(runtime, "cancel", provider);
    let mut subscription = service
        .subscribe(Revision::ZERO)
        .await
        .expect("provider subscription should connect");
    for _ in 0..2 {
        subscription.next_event().await.expect("bootstrap event");
    }

    let mut saw_lease = false;
    let mut state_sequences = Vec::new();
    while !(saw_lease && state_sequences.len() == 2) {
        let event = timeout(Duration::from_secs(2), subscription.next_event())
            .await
            .expect("every event should arrive without the subscription stalling")
            .expect("event should decode");
        match event {
            LocalProviderEvent::LeasesChanged { sequence, leases } => {
                assert_eq!(sequence, 7);
                assert_eq!(leases, vec![lease.clone()]);
                saw_lease = true;
            }
            LocalProviderEvent::StateSnapshot { sequence, .. } => state_sequences.push(sequence),
            other => panic!("unexpected event {other:?}"),
        }
    }
    assert_eq!(state_sequences, vec![11, 12]);
    assert_eq!(
        server.lease_connections.load(Ordering::SeqCst),
        1,
        "lease stream must not reconnect after a cancelled mid-frame read"
    );
    assert_eq!(server.state_connections.load(Ordering::SeqCst), 1);

    drop(subscription);
    stream_task.abort();
    unary_task.await.expect("unary task should complete");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
}
