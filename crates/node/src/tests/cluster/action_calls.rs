//! Request/response actions (`RunAction` with `wait_ms`, `docs/actions.md`, "Waiting for the
//! result"): many concurrent [`orion_client::ActionCaller`] calls on one resource over one stream,
//! byte payloads the size of an SPI transaction, unary `call_action`, timeouts, and a forwarded
//! action that finishes in one peer round trip.

use super::remote_operator::{OperatorNode, operator_node};
use super::*;
use orion::control_plane::{
    ActionReport, ActionRequest, ActionState, ActionTarget, ClientEventKind, ClientEventPoll,
    ClientHello, ProviderRecord, ProviderStateUpdate, ResourceRecord, TypedConfigValue,
};
use orion_client::{
    ActionCaller, LocalControlPlaneClient, LocalNodeRuntime, LocalProviderService,
    LocalServiceRetryPolicy,
};
use orion_core::ClientName;
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Instant;

const PROVIDER: &str = "provider.lemnos";
const RESOURCE: &str = "lemnos.raw";
/// Largest SPI transaction lemnosd accepts, the size the action limits must carry.
const SPI_MAX: usize = 64 * 1024;

fn raw_resource(node: &NodeApp) -> (ProviderRecord, ResourceRecord) {
    (
        ProviderRecord::builder(ProviderId::new(PROVIDER), node.config.node_id.clone())
            .resource_type(ResourceType::new("lemnos.raw"))
            .build(),
        ResourceRecord::builder(
            orion::ResourceId::new(RESOURCE),
            "lemnos.raw",
            ProviderId::new(PROVIDER),
        )
        .health(HealthState::Healthy)
        .availability(AvailabilityState::Available)
        .build(),
    )
}

/// Publishes the provider and its resource from an in-process provider session at `source`.
fn publish_raw_resource(node: &NodeApp, source: &str) -> LocalAddress {
    let source = LocalAddress::new(source);
    node.apply_local_control_message(
        &source,
        ControlMessage::ClientHello(ClientHello {
            client_name: ClientName::new(source.as_str()),
            role: ClientRole::Provider,
        }),
    )
    .expect("hello");
    let (provider, resource) = raw_resource(node);
    node.apply_local_control_message(
        &source,
        ControlMessage::ProviderState(ProviderStateUpdate {
            provider,
            resources: vec![resource],
        }),
    )
    .expect("provider state");
    source
}

fn transfer(id: &str, delay_ms: u64, tx: Vec<u8>) -> ActionRequest {
    ActionRequest::new(
        id,
        ActionTarget::Resource(orion::ResourceId::new(RESOURCE)),
        "spi.transfer",
    )
    .with_arg("delay_ms", TypedConfigValue::UInt(delay_ms))
    .with_arg("tx", TypedConfigValue::Bytes(tx))
}

/// A provider that answers every action after `delay_ms`, echoing `tx` as `rx`, and handles
/// requests concurrently (one task each); `in_flight_max` records how many overlapped.
struct RawProvider {
    _servers: [crate::app::GracefulTaskHandle<orion::transport::ipc::IpcTransportError>; 2],
    sockets: [PathBuf; 2],
    in_flight_max: Arc<AtomicUsize>,
    _handler: tokio::task::JoinHandle<()>,
}

impl Drop for RawProvider {
    fn drop(&mut self) {
        self._handler.abort();
        for socket in &self.sockets {
            let _ = std::fs::remove_file(socket);
        }
    }
}

async fn raw_provider(node: &OperatorNode) -> RawProvider {
    let socket = temp_socket_path("action-calls");
    let stream_socket = temp_socket_path("action-calls-stream");
    let (_, unary) = node
        .app
        .start_ipc_server_graceful(&socket)
        .await
        .expect("ipc server should start");
    let (_, stream) = node
        .app
        .start_ipc_stream_server_graceful(&stream_socket)
        .await
        .expect("ipc stream server should start");
    // The provider and its resource are published in-process; the provider's own handler
    // session then registers over real sockets.
    publish_raw_resource(&node.app, "lemnos-publisher");
    let (provider, _) = raw_resource(&node.app);
    let service = LocalProviderService::new(
        LocalNodeRuntime::new(&socket, &stream_socket),
        "lemnos-provider",
        provider,
    )
    .with_retry_policy(
        LocalServiceRetryPolicy::fixed_delay(Duration::from_millis(20)).with_max_attempts(10),
    );
    let mut watch = service
        .watch_action_requests()
        .await
        .expect("watch requests");
    let in_flight = Arc::new(AtomicUsize::new(0));
    let in_flight_max = Arc::new(AtomicUsize::new(0));
    let (current, max) = (in_flight.clone(), in_flight_max.clone());
    let handler = tokio::spawn(async move {
        while let Ok(request) = watch.next().await {
            let reporter = watch.reporter();
            let (current, max) = (current.clone(), max.clone());
            tokio::spawn(async move {
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                max.fetch_max(now, Ordering::SeqCst);
                let delay = match request.args.get("delay_ms") {
                    Some(TypedConfigValue::UInt(delay)) => *delay,
                    _ => 0,
                };
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let mut output = BTreeMap::new();
                if let Some(tx) = request.args.get("tx") {
                    output.insert("rx".to_owned(), tx.clone());
                }
                output.insert("bus".to_owned(), TypedConfigValue::F64(1.5));
                current.fetch_sub(1, Ordering::SeqCst);
                let _ = reporter.succeed(request.action_id, output).await;
            });
        }
    });
    RawProvider {
        _servers: [unary, stream],
        sockets: [socket, stream_socket],
        in_flight_max,
        _handler: handler,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn action_caller_runs_concurrent_calls_on_one_resource_over_one_stream() {
    let node = operator_node("node-calls", None).await;
    let provider = raw_provider(&node).await;
    let caller = ActionCaller::connect_at(&provider.sockets[1], "helios-api")
        .await
        .expect("caller connects");

    // 16 calls of 300 ms each on the same resource: one after another they would take 4.8 s.
    let started = Instant::now();
    let calls: Vec<_> = (0..16)
        .map(|index| {
            let caller = caller.clone();
            tokio::spawn(async move {
                caller
                    .call(
                        transfer(&format!("xfer-{index}"), 300, vec![index as u8; 4]),
                        Duration::from_secs(10),
                    )
                    .await
            })
        })
        .collect();
    for (index, call) in calls.into_iter().enumerate() {
        let result = call.await.expect("task").expect("call");
        assert_eq!(result.state, ActionState::Succeeded, "{result:?}");
        assert_eq!(result.action_id, format!("xfer-{index}"));
        assert_eq!(
            result.output.get("rx"),
            Some(&TypedConfigValue::Bytes(vec![index as u8; 4])),
            "each call gets its own result"
        );
        assert_eq!(result.output.get("bus"), Some(&TypedConfigValue::F64(1.5)));
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(16 * 300 / 2),
        "the calls ran concurrently: {elapsed:?}"
    );
    assert!(provider.in_flight_max.load(Ordering::SeqCst) > 1);

    // A full SPI transaction in and out.
    let tx: Vec<u8> = (0..SPI_MAX).map(|index| (index % 251) as u8).collect();
    let result = caller
        .call(transfer("xfer-spi", 0, tx.clone()), Duration::from_secs(10))
        .await
        .expect("large call");
    assert_eq!(result.state, ActionState::Succeeded);
    assert_eq!(result.output.get("rx"), Some(&TypedConfigValue::Bytes(tx)));

    // Past the caller's timeout the latest, still running result comes back.
    let started = Instant::now();
    let result = caller
        .call(
            transfer("xfer-slow", 3_000, Vec::new()),
            Duration::from_millis(200),
        )
        .await
        .expect("slow call");
    assert!(!result.state.is_terminal(), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
    // The same request waits for the same action again; it is not run twice.
    let result = caller
        .call(
            transfer("xfer-slow", 3_000, Vec::new()),
            Duration::from_secs(10),
        )
        .await
        .expect("resumed call");
    assert_eq!(result.state, ActionState::Succeeded);

    // Rejections come back at once.
    let unknown = ActionRequest::new(
        "nobody",
        ActionTarget::Resource(orion::ResourceId::new("no.such.resource")),
        "gpio.get",
    );
    let result = caller
        .call(unknown, Duration::from_secs(5))
        .await
        .expect("rejected call");
    assert!(
        matches!(result.state, ActionState::Rejected { .. }),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unary_call_action_waits_for_the_result_without_polling() {
    let node = operator_node("node-unary-calls", None).await;
    let provider = raw_provider(&node).await;
    let client = LocalControlPlaneClient::connect_at(&provider.sockets[0], "helios-api")
        .expect("unary client");
    let started = Instant::now();
    let before = unary_exchanges(&node, "helios-api");
    let result = client
        .call_action(
            transfer("unary-1", 150, vec![7; 16]),
            Duration::from_secs(10),
        )
        .await
        .expect("call");
    assert_eq!(result.state, ActionState::Succeeded);
    assert_eq!(
        result.output.get("rx"),
        Some(&TypedConfigValue::Bytes(vec![7; 16]))
    );
    assert!(started.elapsed() >= Duration::from_millis(150));
    assert!(
        unary_exchanges(&node, "helios-api") - before <= 2,
        "the node held its answer; the client did not spin"
    );

    // Longer than the node's per-exchange cap (2 s): the client resends and keeps waiting.
    let result = client
        .call_action(
            transfer("unary-2", 2_500, Vec::new()),
            Duration::from_secs(10),
        )
        .await
        .expect("long call");
    assert_eq!(result.state, ActionState::Succeeded);

    // Too large: the node refuses the request instead of carrying it.
    let oversized = transfer("unary-3", 0, vec![0; SPI_MAX + 1]);
    assert!(
        client
            .call_action(oversized, Duration::from_secs(5))
            .await
            .is_err()
    );
}

/// Unary exchanges the node received from local clients named `client`.
fn unary_exchanges(node: &OperatorNode, client: &str) -> u64 {
    node.app
        .observability_snapshot()
        .communication
        .iter()
        .filter(|endpoint| {
            endpoint.id.starts_with("ipc/local-unary/")
                && endpoint.labels.get("client_name").map(String::as_str) == Some(client)
        })
        .map(|endpoint| endpoint.metrics.messages_received_total)
        .sum()
}

fn received_peer_exchanges(node: &OperatorNode) -> u64 {
    node.app
        .observability_snapshot()
        .communication
        .iter()
        .filter(|endpoint| endpoint.id == "tcp/peer-control")
        .map(|endpoint| endpoint.metrics.messages_received_total)
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_actions_finish_in_one_round_trip() {
    let (a, b) = (
        operator_node("node-a", None).await,
        operator_node("node-b", None).await,
    );
    for (node, peer) in [(&a, &b), (&b, &a)] {
        node.app
            .register_peer(
                PeerConfig::new(
                    peer.app.config.node_id.clone(),
                    orion_core::PeerBaseUrl::new(peer.url()),
                )
                .with_trusted_public_key_hex(peer.app.security.public_key_hex()),
            )
            .expect("peer registration");
    }
    // The resource's provider runs on node-b and answers from a polled handler session.
    let source = publish_raw_resource(&b.app, "b-lemnos");
    b.app
        .apply_local_control_message(
            &source,
            ControlMessage::WatchActionRequests(vec![ActionTarget::Provider(ProviderId::new(
                PROVIDER,
            ))]),
        )
        .expect("handler registration");
    a.app
        .sync_peer(&NodeId::new("node-b"))
        .await
        .expect("sync a -> b");
    b.app
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("sync b -> a");

    let handler_app = b.app.clone();
    let handler = tokio::spawn(async move {
        let mut after = 0;
        loop {
            let events = match handler_app.apply_local_control_message(
                &source,
                ControlMessage::PollClientEvents(ClientEventPoll {
                    after_sequence: after,
                    max_events: 64,
                }),
            ) {
                Ok(ControlMessage::ClientEvents(events)) => events,
                _ => Vec::new(),
            };
            for event in events {
                after = after.max(event.sequence);
                if let ClientEventKind::ActionRequest(request) = event.event {
                    let app = handler_app.clone();
                    let source = source.clone();
                    tokio::spawn(async move {
                        if let Some(TypedConfigValue::UInt(delay)) = request.args.get("delay_ms") {
                            tokio::time::sleep(Duration::from_millis(*delay)).await;
                        }
                        let mut report =
                            ActionReport::new(&request.action_id, ActionState::Succeeded);
                        report.output = request
                            .args
                            .get("tx")
                            .map(|tx| BTreeMap::from([("rx".to_owned(), tx.clone())]))
                            .unwrap_or_default();
                        let _ = app.apply_local_control_message(
                            &source,
                            ControlMessage::ReportActionResult(Box::new(report)),
                        );
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });

    // A short action: node-a forwards it and the owner answers once it is final.
    let before = received_peer_exchanges(&b);
    a.app
        .run_action(transfer("fwd-short", 20, vec![1, 2, 3]), "test")
        .expect("submitted on node-a");
    let result = a
        .app
        .await_action("fwd-short", Duration::from_secs(5))
        .await
        .expect("tracked");
    assert_eq!(result.state, ActionState::Succeeded, "{result:?}");
    assert_eq!(result.handled_by, NodeId::new("node-b"));
    assert_eq!(
        result.output.get("rx"),
        Some(&TypedConfigValue::Bytes(vec![1, 2, 3]))
    );
    assert_eq!(
        received_peer_exchanges(&b) - before,
        1,
        "one RunAction exchange, no QueryActions polling"
    );

    // Longer than one forwarding wait (750 ms): node-a resends the same request once.
    let before = received_peer_exchanges(&b);
    a.app
        .run_action(transfer("fwd-long", 1_000, Vec::new()), "test")
        .expect("submitted on node-a");
    let result = a
        .app
        .await_action("fwd-long", Duration::from_secs(5))
        .await
        .expect("tracked");
    assert_eq!(result.state, ActionState::Succeeded, "{result:?}");
    let exchanges = received_peer_exchanges(&b) - before;
    assert!(
        (2..=3).contains(&exchanges),
        "the owner held each answer instead of being polled: {exchanges} exchanges"
    );
    handler.abort();
}
