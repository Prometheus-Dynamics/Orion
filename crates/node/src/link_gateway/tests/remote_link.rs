//! `tcp:` links (remote operators, docs/link-protocol.md "Remote links"): the signed handshake,
//! sealed link frames, the operator's policy per request, and the client role only.

use super::serial::{WAIT, start};
use super::*;
use crate::link_gateway::LinkConfig;
use orion::control_plane::{OperatorEnrollmentMethod, OperatorId, OperatorPolicy};
use orion_auth::crypto::SigningKey;
use orion_auth::link_channel::{
    self, ClientHandshake, KIND_ACCEPT, KIND_DATA, KIND_REFUSE, Opener, Sealer,
};
use orion_link::message::{
    ErrorCode, Hello, Message, ProviderState, RecordFilter, RecordKind, RecordRequest,
    RejectReason, Roles,
};
use orion_link::{StreamDecoder, StreamEncoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn free_port() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("address")
}

fn tcp_link(addr: std::net::SocketAddr) -> LinkConfig {
    let entry = format!("tcp:{addr}?heartbeat_ms={HEARTBEAT_MS}&missed_heartbeats=3");
    LinkConfig::parse(&entry).expect("link should parse")
}

fn enroll(app: &NodeApp, name: &str, key: &SigningKey, read: bool) {
    app.security
        .enroll_operator(
            &OperatorId::try_new(name).expect("operator id"),
            key.verifying_key().to_bytes(),
            OperatorEnrollmentMethod::Approval,
            OperatorPolicy {
                read,
                actions: Some(Vec::new()),
            },
        )
        .expect("enroll");
}

/// A remote operator's link session, sealed.
struct Remote {
    stream: TcpStream,
    sealer: Sealer,
    opener: Opener,
    sealed: Vec<u8>,
    decoder: StreamDecoder<4096>,
    seq: u16,
    node_key: [u8; 32],
}

/// The node's answer to the handshake.
enum Outcome {
    Accepted(Box<Remote>),
    Refused(String),
}

async fn connect(addr: std::net::SocketAddr, operator: &str, key: &SigningKey) -> Outcome {
    let mut stream = None;
    for _ in 0..200 {
        if let Ok(connected) = TcpStream::connect(addr).await {
            stream = Some(connected);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut stream = stream.expect("the link should accept");
    let handshake = ClientHandshake::new([0x11; 32], [0x22; 32]);
    stream
        .write_all(&link_channel::frame(handshake.hello()))
        .await
        .expect("write");
    let mut buf = Vec::new();
    let server_hello = loop {
        if let Some(message) = link_channel::take_message(&mut buf).expect("bounded") {
            break message;
        }
        let mut chunk = [0u8; 1024];
        let n = stream.read(&mut chunk).await.expect("read");
        assert!(n > 0, "the node closed during the handshake");
        buf.extend_from_slice(&chunk[..n]);
    };
    let done = handshake
        .finish(&server_hello, None, operator, key)
        .expect("the node's hello verifies");
    assert_eq!(done.node_id, NODE);
    stream
        .write_all(&link_channel::frame(&done.auth))
        .await
        .expect("write");
    let mut remote = Remote {
        stream,
        sealer: done.sealer,
        opener: done.opener,
        sealed: buf,
        decoder: StreamDecoder::new(),
        seq: 0,
        node_key: done.node_key,
    };
    let mut payload = Vec::new();
    match remote.record(&mut payload).await {
        KIND_ACCEPT => Outcome::Accepted(Box::new(remote)),
        KIND_REFUSE => Outcome::Refused(String::from_utf8(payload).expect("utf-8")),
        other => panic!("unexpected first record {other}"),
    }
}

impl Remote {
    async fn record(&mut self, payload: &mut Vec<u8>) -> u8 {
        loop {
            if let Some(kind) = self
                .opener
                .open(&mut self.sealed, payload)
                .expect("records verify")
            {
                return kind;
            }
            let mut chunk = [0u8; 4096];
            let n = tokio::time::timeout(WAIT, self.stream.read(&mut chunk))
                .await
                .expect("a record within the wait")
                .expect("read");
            assert!(n > 0, "connection closed");
            self.sealed.extend_from_slice(&chunk[..n]);
        }
    }

    async fn send(&mut self, message: &Message) {
        self.seq = self.seq.wrapping_add(1);
        let mut frame = vec![0u8; 4096];
        let len = message.encode(self.seq, &mut frame).expect("fits");
        let bytes: Vec<u8> = StreamEncoder::for_frame(&frame[..len]).collect();
        let mut out = Vec::new();
        self.sealer.seal_data(&bytes, &mut out);
        self.stream.write_all(&out).await.expect("write");
    }

    async fn hello(&mut self, roles: Roles) {
        self.send(&Message::Hello(Hello {
            device_name: "console".into(),
            roles,
            max_frame: 4096,
            revision: orion_link::LINK_INTERFACE_REVISION,
        }))
        .await;
    }

    /// Whether the node closes the connection within `wait` (output before that is skipped).
    async fn closed_within(&mut self, wait: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + wait;
        let mut chunk = [0u8; 4096];
        loop {
            match tokio::time::timeout_at(deadline, self.stream.read(&mut chunk)).await {
                Ok(Ok(0) | Err(_)) => return true,
                Ok(Ok(_)) => {}
                Err(_) => return false,
            }
        }
    }

    /// The next message other than pongs and acks.
    async fn next(&mut self) -> Message {
        let mut payload = Vec::new();
        loop {
            payload.clear();
            assert_eq!(self.record(&mut payload).await, KIND_DATA);
            for &byte in &payload {
                if let Ok(Some(frame)) = self.decoder.push(byte) {
                    match Message::decode(&frame).expect("node frames decode") {
                        Message::Pong { .. } | Message::Ack { .. } => {}
                        message => return message,
                    }
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_enrolled_operator_reads_records_over_a_sealed_session() {
    let app = test_app("tcp-records");
    let operator = SigningKey::from_bytes(&[7; 32]);
    enroll(&app, "alice", &operator, true);
    let mut desired = app.state_snapshot().state.desired;
    desired.put_provider(device_provider("provider.imu-board"));
    app.replace_desired(desired);
    let addr = free_port();
    let gateway = start(&app, vec![tcp_link(addr)]);
    let Outcome::Accepted(mut remote) = connect(addr, "operator:alice", &operator).await else {
        panic!("an enrolled operator is accepted");
    };
    assert_eq!(remote.node_key, app.security.public_key_bytes());
    remote.hello(Roles::CLIENT).await;
    assert!(matches!(remote.next().await, Message::Welcome(_)));
    remote
        .send(&Message::RecordQuery(RecordRequest {
            req: 1,
            filter: RecordFilter::all(RecordKind::Provider),
        }))
        .await;
    match remote.next().await {
        Message::Records(records) => {
            assert_eq!(records.req, 1);
            assert_eq!(records.updated.len(), 1, "{records:?}");
        }
        other => panic!("unexpected {other:?}"),
    }
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_session_is_a_client_only_and_its_policy_applies() {
    let app = test_app("tcp-policy");
    let operator = SigningKey::from_bytes(&[8; 32]);
    enroll(&app, "bob", &operator, false);
    let addr = free_port();
    let gateway = start(&app, vec![tcp_link(addr)]);

    // No provider or executor role over TCP.
    let Outcome::Accepted(mut remote) = connect(addr, "operator:bob", &operator).await else {
        panic!("an enrolled operator is accepted");
    };
    remote.hello(Roles::PROVIDER).await;
    assert!(matches!(
        remote.next().await,
        Message::Reject(RejectReason::UnsupportedRoles)
    ));
    remote
        .send(&Message::ProviderState(ProviderState {
            provider: device_provider("provider.remote"),
            resources: Vec::new(),
        }))
        .await;
    assert!(!has_provider(&app, "provider.remote"));

    // Without read access, record reads are refused. An operator with neither read access nor
    // actions may not open a session at all, so its requests are refused the same way.
    let Outcome::Accepted(mut remote) = connect(addr, "operator:bob", &operator).await else {
        panic!("an enrolled operator is accepted");
    };
    remote.hello(Roles::CLIENT).await;
    assert!(matches!(remote.next().await, Message::Welcome(_)));
    remote
        .send(&Message::RecordQuery(RecordRequest {
            req: 4,
            filter: RecordFilter::all(RecordKind::Workload),
        }))
        .await;
    match remote.next().await {
        Message::RequestError(error) => {
            assert_eq!(error.req, 4);
            assert_eq!(error.code, ErrorCode::NOT_AUTHORIZED, "{error:?}");
        }
        other => panic!("unexpected {other:?}"),
    }
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_or_removed_operator_is_refused_with_the_reason() {
    let app = test_app("tcp-refused");
    let addr = free_port();
    let gateway = start(&app, vec![tcp_link(addr)]);
    let stranger = SigningKey::from_bytes(&[9; 32]);
    let Outcome::Refused(reason) = connect(addr, "operator:carol", &stranger).await else {
        panic!("an unknown operator is refused");
    };
    assert!(reason.contains("not enrolled"), "{reason}");
    // Listed as pending, for an administrator to approve by fingerprint.
    assert_eq!(
        app.security
            .pending_operator_key(&OperatorId::try_new("carol").expect("id")),
        Some(stranger.verifying_key().to_bytes())
    );

    // Enrolled, then removed while its session is open: its next request is refused.
    enroll(&app, "carol", &stranger, true);
    let Outcome::Accepted(mut remote) = connect(addr, "operator:carol", &stranger).await else {
        panic!("an enrolled operator is accepted");
    };
    remote.hello(Roles::CLIENT).await;
    assert!(matches!(remote.next().await, Message::Welcome(_)));
    app.security
        .remove_operator(&OperatorId::try_new("carol").expect("id"))
        .expect("remove");
    remote
        .send(&Message::RecordQuery(RecordRequest {
            req: 2,
            filter: RecordFilter::all(RecordKind::Provider),
        }))
        .await;
    match remote.next().await {
        Message::RequestError(error) => assert_eq!(error.code, ErrorCode::NOT_AUTHORIZED),
        other => panic!("unexpected {other:?}"),
    }
    // And the session ends, so its watches stop pushing.
    assert!(remote.closed_within(Duration::from_secs(3)).await);
    gateway.shutdown().await;
}

#[test]
fn tcp_links_need_the_remote_auth_switch() {
    let links = vec![tcp_link("127.0.0.1:7461".parse().expect("addr"))];
    for off in [None, Some(""), Some("off")] {
        let error = crate::link_gateway::config::check_remote_auth(&links, off)
            .expect_err("off by default");
        assert!(
            error.to_string().contains("ORION_NODE_REMOTE_AUTH=keys"),
            "{error}"
        );
    }
    crate::link_gateway::config::check_remote_auth(&links, Some("keys")).expect("on");
    assert!(crate::link_gateway::config::check_remote_auth(&links, Some("tls")).is_err());
    assert!(crate::link_gateway::config::check_remote_auth(&[], None).is_ok());
}
