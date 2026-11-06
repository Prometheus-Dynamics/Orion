//! The client role on Unix links (interface revision 1): a raw client speaks link frames over the
//! socket and queries and watches status and actions of the node.

use super::serial::{WAIT, start};
use super::unix::{SocketDevice, socket_path, unix_link};
use super::*;
use crate::actions::{ActionContext, ActionOutcome};
use crate::auth::{LocalAdmissionPolicy, LocalAuthenticationMode};
use orion::control_plane::{
    ActionQuery, ActionRequest, ActionState, ActionTarget, StatusQuery, StatusSubject,
    TypedConfigValue,
};
use orion::{NodeId, ProviderId};
use orion_link::message::{
    ActionFilterRequest, ActionRun, ErrorCode, Hello, Message, Roles, StatusEntry as LinkStatus,
    StatusRequest,
};
use orion_link::{StreamDecoder, StreamEncoder};
use std::collections::BTreeMap;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A client session on one connection: pings to stay alive, and hands over every message the
/// node sends except the session's own (`Welcome`, `Pong`, `Leases`, `Ack`).
struct RawClient {
    send: mpsc::UnboundedSender<Message>,
    received: mpsc::UnboundedReceiver<Message>,
    task: JoinHandle<()>,
}

struct Writer {
    seq: u16,
}

impl Writer {
    async fn write(
        &mut self,
        writer: &mut tokio::net::unix::OwnedWriteHalf,
        message: &Message,
    ) -> bool {
        self.seq = self.seq.wrapping_add(1);
        let mut frame = vec![0u8; 4096];
        let len = message.encode(self.seq, &mut frame).expect("fits");
        let bytes: Vec<u8> = StreamEncoder::for_frame(&frame[..len]).collect();
        writer.write_all(&bytes).await.is_ok()
    }
}

impl RawClient {
    async fn connect(name: &str, path: &Path) -> Self {
        let mut stream = None;
        for _ in 0..200 {
            if let Ok(connected) = UnixStream::connect(path).await {
                stream = Some(connected);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let stream = stream.expect("the link's socket should accept");
        let (send, mut outgoing) = mpsc::unbounded_channel::<Message>();
        let (deliver, received) = mpsc::unbounded_channel();
        let hello = Message::Hello(Hello {
            device_name: name.into(),
            roles: Roles::CLIENT,
            max_frame: 4096,
            revision: 1,
        });
        let task = tokio::spawn(async move {
            let (mut reader, mut writer) = stream.into_split();
            let mut out = Writer { seq: 0 };
            if !out.write(&mut writer, &hello).await {
                return;
            }
            let mut decoder = StreamDecoder::<4096>::new();
            let mut buf = [0u8; 4096];
            let mut ping =
                tokio::time::interval(Duration::from_millis(u64::from(HEARTBEAT_MS) / 2));
            loop {
                tokio::select! {
                    message = outgoing.recv() => {
                        let Some(message) = message else { return };
                        if !out.write(&mut writer, &message).await {
                            return;
                        }
                    }
                    _ = ping.tick() => {
                        if !out.write(&mut writer, &Message::Ping { now_ms: 0 }).await {
                            return;
                        }
                    }
                    read = reader.read(&mut buf) => {
                        let Ok(n) = read else { return };
                        if n == 0 {
                            return;
                        }
                        for &byte in &buf[..n] {
                            if let Ok(Some(frame)) = decoder.push(byte) {
                                match Message::decode(&frame) {
                                    Ok(Message::Welcome(_) | Message::Pong { .. } | Message::Leases(_) | Message::Ack { .. }) => {}
                                    Ok(message) => {
                                        let _ = deliver.send(message);
                                    }
                                    Err(error) => panic!("undecodable frame from the node: {error}"),
                                }
                            }
                        }
                    }
                }
            }
        });
        Self {
            send,
            received,
            task,
        }
    }

    fn send(&self, message: Message) {
        self.send.send(message).expect("client task runs");
    }

    async fn next(&mut self) -> Message {
        tokio::time::timeout(WAIT, self.received.recv())
            .await
            .expect("an answer within the wait")
            .expect("connection open")
    }

    /// Nothing arrives within `quiet`.
    async fn silent_for(&mut self, quiet: Duration) -> bool {
        tokio::time::timeout(quiet, self.received.recv())
            .await
            .is_err()
    }
}

impl Drop for RawClient {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn provider_subject() -> StatusSubject {
    StatusSubject::Provider(ProviderId::new("provider.imu-board"))
}

fn publish_temperature(device: &SocketDevice, value: i64) {
    device
        .device
        .lock()
        .expect("device lock")
        .publish_status(&[LinkStatus::new(
            "temperature_mc",
            TypedConfigValue::Int(value),
        )])
        .expect("status fits");
}

fn temperature_of(message: &Message) -> Option<i64> {
    let Message::StatusChanges(changes) = message else {
        return None;
    };
    changes.updated.iter().find_map(|entry| match entry.value {
        TypedConfigValue::Int(value) if entry.key == "temperature_mc" => Some(value),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_queries_and_watches_status_published_by_a_device_on_another_connection() {
    let app = test_app("unix-client-status");
    let path = socket_path("client-status");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let device = SocketDevice::connect("imu-board", &path).await;
    device.publish("provider.imu-board", "imu-board.imu-0");
    assert!(wait_until(WAIT, || has_provider(&app, "provider.imu-board")).await);
    publish_temperature(&device, 41);
    let filter = StatusQuery::subject(provider_subject());
    assert!(wait_until(WAIT, || !app.query_status(&filter).is_empty()).await);

    let mut client = RawClient::connect("console", &path).await;
    client.send(Message::StatusQuery(StatusRequest {
        req: 1,
        filter: filter.clone(),
    }));
    let answer = client.next().await;
    assert!(
        matches!(&answer, Message::StatusChanges(c) if c.req == 1 && c.bootstrap && !c.more),
        "{answer:?}"
    );
    assert_eq!(temperature_of(&answer), Some(41));

    client.send(Message::StatusWatch(StatusRequest {
        req: 2,
        filter: filter.clone(),
    }));
    let bootstrap = client.next().await;
    assert!(matches!(&bootstrap, Message::StatusChanges(c) if c.req == 2 && c.bootstrap));
    assert_eq!(temperature_of(&bootstrap), Some(41));

    // A second watch is over the session's limit.
    client.send(Message::StatusWatch(StatusRequest { req: 3, filter }));
    assert!(matches!(
        client.next().await,
        Message::RequestError(e) if e.req == 3 && e.code == ErrorCode::LIMIT
    ));

    // Changes follow the watch, newest value per key.
    publish_temperature(&device, 42);
    let change = client.next().await;
    assert!(matches!(&change, Message::StatusChanges(c) if c.req == 2 && !c.bootstrap));
    assert_eq!(temperature_of(&change), Some(42));

    // Ended, the watch sends nothing more.
    client.send(Message::RequestEnd { req: 2 });
    tokio::time::sleep(Duration::from_millis(50)).await;
    publish_temperature(&device, 43);
    assert!(client.silent_for(Duration::from_millis(600)).await);

    // The client is a local client of the node only while its session lasts.
    let source = orion::transport::ipc::LocalAddress::new(format!(
        "link:{}/console",
        app.link_status()[0].name
    ));
    assert!(app.has_local_client(&source));
    drop(client);
    assert!(wait_until(WAIT, || !app.has_local_client(&source)).await);
    gateway.shutdown().await;
}

fn node_app_with_blink(name: &str) -> NodeApp {
    let socket = std::env::temp_dir().join(format!("orion-lg-{name}-{}.sock", std::process::id()));
    NodeApp::builder()
        .config(crate::NodeConfig::for_local_node(NODE).with_ipc_socket_path(socket))
        .with_action_handler(
            "blink",
            |_request: ActionRequest, context: ActionContext| async move {
                context.progress(Some(500), BTreeMap::new());
                tokio::time::sleep(Duration::from_millis(150)).await;
                ActionOutcome::Succeeded(BTreeMap::from([(
                    "blinks".to_owned(),
                    TypedConfigValue::UInt(3),
                )]))
            },
        )
        .try_build()
        .expect("node app should build")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_runs_an_action_and_follows_it_until_it_is_final() {
    let app = node_app_with_blink("unix-client-actions");
    let path = socket_path("client-actions");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let mut client = RawClient::connect("console", &path).await;
    let node = ActionTarget::Node(NodeId::new(NODE));

    client.send(Message::ActionWatch(ActionFilterRequest {
        req: 1,
        filter: ActionQuery::target(node.clone()),
    }));
    // The watch's first answer: the matching results so far (none).
    assert!(matches!(
        client.next().await,
        Message::ActionResults(r) if r.req == 1 && r.results.is_empty()
    ));

    client.send(Message::ActionRun(Box::new(ActionRun {
        req: 2,
        request: ActionRequest::new("blink-1", node.clone(), "blink"),
    })));
    // Both the run and the watch see every state up to the final one.
    let mut final_for = BTreeMap::new();
    while final_for.len() < 2 {
        let Message::ActionResults(results) = client.next().await else {
            panic!("only action results are expected");
        };
        for result in results.results {
            assert_eq!(result.action_id, "blink-1");
            assert!(
                result.requested_by.contains("link:"),
                "{}",
                result.requested_by
            );
            if result.state.is_terminal() {
                assert_eq!(result.state, ActionState::Succeeded);
                assert_eq!(
                    result.output.get("blinks"),
                    Some(&TypedConfigValue::UInt(3))
                );
                final_for.insert(results.req, result);
            }
        }
    }
    assert_eq!(final_for.keys().copied().collect::<Vec<_>>(), vec![1, 2]);

    // A query answers once; an invalid run is refused.
    client.send(Message::ActionQuery(ActionFilterRequest {
        req: 3,
        filter: ActionQuery::action("blink-1"),
    }));
    assert!(matches!(
        client.next().await,
        Message::ActionResults(r) if r.req == 3 && r.results.len() == 1 && !r.more
    ));
    client.send(Message::ActionRun(Box::new(ActionRun {
        req: 4,
        request: ActionRequest::new("", node, "blink"),
    })));
    assert!(matches!(
        client.next().await,
        Message::RequestError(e) if e.req == 4 && e.code == ErrorCode::INVALID
    ));
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_caller_admitted_only_to_the_provider_lane_is_not_authorized_for_client_requests() {
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    let grants = crate::auth::LocalAccessGrants {
        provider: crate::auth::LocalAccessAllowList::new().allow_uid(uid),
        ..Default::default()
    };
    let mut app = test_app("unix-client-lane");
    app.replace_local_admission(LocalAdmissionPolicy {
        mode: LocalAuthenticationMode::SameUser,
        grants,
        local_uid: uid.wrapping_add(1),
        local_gid: gid.wrapping_add(1),
    });
    let path = socket_path("client-lane");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let mut client = RawClient::connect("console", &path).await;
    client.send(Message::StatusQuery(StatusRequest {
        req: 9,
        filter: StatusQuery::all(),
    }));
    assert!(matches!(
        client.next().await,
        Message::RequestError(e) if e.req == 9 && e.code == ErrorCode::NOT_AUTHORIZED
    ));
    gateway.shutdown().await;
}
