//! Peer watches (`docs/actions.md`, "Peer watches"): a configured peer's push session carries
//! only the results of the actions that peer forwarded, pushed as they change.

use super::*;
use crate::actions::ActionOutcome;
use crate::app::ActionOrigin;
use crate::peer_tcp::{ResponseFrame, STATUS_OK};
use ed25519_dalek::SigningKey;
use orion::control_plane::{
    ActionQuery, ActionRequest, ActionResult, ActionState, ActionTarget, ClientEvent,
    ClientEventKind, ControlMessage, SessionOpen, TypedConfigValue,
};
use orion::encode_to_vec;
use orion::transport::http::{HttpCodec, HttpRequestPayload, HttpResponsePayload};
use orion::transport::ipc::{ControlEnvelope, LocalAddress};
use orion_auth::PeerRequestPayload;
use orion_auth::crypto::sign_peer_response;
use orion_auth::peer_tcp::{PEER_TCP_STATUS_PUSH, encode_push_body};
use orion_core::PeerBaseUrl;
use orion_transport_ipc::{ControlFrameReadState, write_control_payload_frame};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream, tcp::OwnedWriteHalf};

/// How long the owner's `slow` action runs: longer than a forwarder's first `RunAction` wait, so
/// the forwarder must complete it by push (or by resends, when push is off).
const SLOW_ACTION: Duration = Duration::from_millis(1500);

/// An owner with a `slow` action, served on a real `orion+tcp` listener.
async fn owner_node(
    node_id: &'static str,
) -> (
    NodeApp,
    SocketAddr,
    crate::app::GracefulTaskHandle<NodeError>,
) {
    owner_node_with(node_id, |tuning| tuning).await
}

/// [`owner_node`] with its runtime tuning changed by `configure`.
async fn owner_node_with(
    node_id: &'static str,
    configure: impl FnOnce(NodeRuntimeTuning) -> NodeRuntimeTuning,
) -> (
    NodeApp,
    SocketAddr,
    crate::app::GracefulTaskHandle<NodeError>,
) {
    let app = NodeApp::builder()
        .config(
            test_node_config_with_auth(
                node_id,
                "node-push",
                crate::PeerAuthenticationMode::Required,
            )
            .with_runtime_tuning_mut(configure),
        )
        .with_action_handler("slow", |_request, _context| async {
            tokio::time::sleep(SLOW_ACTION).await;
            ActionOutcome::succeeded()
        })
        .try_build()
        .expect("node app should build");
    let (addr, server) = app
        .start_peer_tcp_server("127.0.0.1:0".parse().expect("address should parse"))
        .await
        .expect("peer TCP listener should start");
    (app, addr, server)
}

/// A forwarder that knows `owner` at `addr` (with its pinned key), and `owner` that knows it back.
fn forwarder_node(node_id: &'static str, owner: &NodeApp, addr: SocketAddr) -> NodeApp {
    let app = forwarder_knowing(node_id, "node-owner", addr, owner.security.public_key_hex());
    owner
        .register_peer(
            PeerConfig::new(
                app.config.node_id.clone(),
                PeerBaseUrl::new("orion+tcp://127.0.0.1:9"),
            )
            .with_trusted_public_key_hex(app.security.public_key_hex()),
        )
        .expect("peer registration should succeed");
    app
}

/// A forwarder that knows the owner `owner_id` at `addr`, pinned to `owner_key`.
fn forwarder_knowing(
    node_id: &'static str,
    owner_id: &str,
    addr: SocketAddr,
    owner_key: impl Into<orion_core::PublicKeyHex>,
) -> NodeApp {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            node_id,
            "node-push",
            crate::PeerAuthenticationMode::Required,
        ))
        .try_build()
        .expect("node app should build");
    app.register_peer(
        PeerConfig::new(
            NodeId::new(owner_id),
            PeerBaseUrl::new(format!("orion+tcp://{addr}")),
        )
        .with_trusted_public_key_hex(owner_key),
    )
    .expect("peer registration should succeed");
    app
}

/// Forwards a `slow` action from `forwarder` to `owner` and returns its final result.
async fn forward_slow(forwarder: &NodeApp, action_id: &str) -> ActionResult {
    forwarder
        .submit_action(
            ActionRequest::new(
                action_id,
                ActionTarget::Node(NodeId::new("node-owner")),
                "slow",
            ),
            ActionOrigin::Local("test".to_owned()),
        )
        .expect("the forwarded action is accepted");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(result) = forwarder
                .query_actions(&ActionQuery::action(action_id))
                .into_iter()
                .find(|result| result.state.is_terminal())
            {
                return result;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the forwarded action finishes in time")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarded_action_completes_by_push_without_resends() {
    let (owner, addr, _server) = owner_node("node-owner").await;
    let forwarder = forwarder_node("node-forwarder", &owner, addr);

    let result = forward_slow(&forwarder, "slow-push").await;
    assert_eq!(result.state, ActionState::Succeeded);
    assert_eq!(
        forwarder.action_forward_resends_for_test(),
        0,
        "the owner's push session delivered the result: no RunAction was resent"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_a_push_session_the_forwarder_resends_and_still_completes() {
    let (owner, addr, _server) = owner_node("node-owner").await;
    let forwarder = forwarder_node("node-forwarder", &owner, addr);
    forwarder.disable_push_for_test();

    let result = forward_slow(&forwarder, "slow-resend").await;
    assert_eq!(result.state, ActionState::Succeeded);
    assert!(
        forwarder.action_forward_resends_for_test() > 0,
        "without push, the forwarder resends RunAction until the action is final"
    );
}

/// The action ids in a pushed `ClientEvents` envelope.
fn pushed_action_ids(envelope: ControlEnvelope) -> Vec<String> {
    let ControlMessage::ClientEvents(events) = envelope.message else {
        panic!("a session pushes client events");
    };
    events
        .into_iter()
        .flat_map(|event| match event.event {
            ClientEventKind::ActionResults(results) => results
                .into_iter()
                .map(|result| result.action_id)
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect()
}

#[tokio::test]
async fn a_peer_session_watches_only_the_actions_that_peer_forwarded() {
    let owner = cluster_node("node-b", |config| config);
    let forwarder = NodeId::new("node-a");
    let other_peer = NodeId::new("node-c");
    let target = ActionTarget::Node(NodeId::new("node-b"));

    owner
        .submit_action(
            ActionRequest::new("fwd-1", target.clone(), "locate"),
            ActionOrigin::Peer(forwarder.clone()),
        )
        .expect("a forwarded action is accepted");
    owner
        .submit_action(
            ActionRequest::new("local-1", target.clone(), "locate"),
            ActionOrigin::Local("cli".to_owned()),
        )
        .expect("a local action is accepted");

    let HttpResponsePayload::Session(welcome) = owner
        .open_peer_session(
            &forwarder,
            SessionOpen {
                client_name: String::new(),
            },
        )
        .expect("a configured peer may open a session")
    else {
        panic!("OpenSession answers with the session");
    };
    let session = LocalAddress::new(welcome.session_id);
    let (sender, mut pushed) = tokio::sync::mpsc::channel(16);
    owner
        .attach_remote_session(&session, sender)
        .expect("the session attaches");

    // A peer may not watch another peer's forwarded actions, use another peer's session, or send
    // anything but a watch or a ping on its session.
    assert!(
        owner
            .apply_peer_session_message(
                &forwarder,
                Some(&session),
                ControlMessage::WatchActions(ActionQuery::all().forwarded_by(other_peer.clone())),
            )
            .is_err()
    );
    assert!(
        owner
            .apply_peer_session_message(
                &other_peer,
                Some(&session),
                ControlMessage::WatchActions(ActionQuery::all()),
            )
            .is_err()
    );
    assert!(
        owner
            .apply_peer_session_message(&forwarder, Some(&session), ControlMessage::QueryPeerTrust,)
            .is_err()
    );

    // The watch is narrowed to the forwarder even though its query asked for everything. Its
    // bootstrap carries the forwarded action only.
    owner
        .apply_peer_session_message(
            &forwarder,
            Some(&session),
            ControlMessage::WatchActions(ActionQuery::all()),
        )
        .expect("the peer's watch is accepted");
    let bootstrap = tokio::time::timeout(Duration::from_secs(5), pushed.recv())
        .await
        .expect("the bootstrap is pushed in time")
        .expect("the session stays open");
    assert_eq!(pushed_action_ids(bootstrap), vec!["fwd-1".to_owned()]);

    // Later changes follow the same rule: a local action is not pushed, a forwarded one is.
    owner
        .submit_action(
            ActionRequest::new("local-2", target.clone(), "locate"),
            ActionOrigin::Local("cli".to_owned()),
        )
        .expect("a local action is accepted");
    owner
        .submit_action(
            ActionRequest::new("fwd-2", target, "locate"),
            ActionOrigin::Peer(forwarder),
        )
        .expect("a forwarded action is accepted");
    let update = tokio::time::timeout(Duration::from_secs(5), pushed.recv())
        .await
        .expect("the forwarded update is pushed in time")
        .expect("the session stays open");
    assert_eq!(pushed_action_ids(update), vec!["fwd-2".to_owned()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_owner_that_refuses_push_sessions_still_serves_run_action() {
    // The owner's own switch (`ORION_NODE_PEER_PUSH_SESSIONS=off`), not the forwarder's test
    // switch: the forwarder opens its session, the owner refuses the `OpenSession`, and the
    // forwarder resends `RunAction` until the action is final.
    let (owner, addr, _server) = owner_node_with("node-owner", |mut tuning| {
        tuning.peer_push_sessions = false;
        tuning
    })
    .await;
    let forwarder = forwarder_node("node-forwarder", &owner, addr);

    let result = forward_slow(&forwarder, "slow-refused").await;
    assert_eq!(result.state, ActionState::Succeeded);
    assert!(
        forwarder.action_forward_resends_for_test() > 0,
        "the refused session falls back to resending RunAction"
    );
}

/// A forged push a fake owner sends on the session once the forwarder's watch is accepted.
#[derive(Clone, Copy, Debug)]
enum Forgery {
    /// The signature does not verify.
    BadSignature,
    /// Signed over some other request, not the session's `OpenSession`.
    OtherOpenRequest,
    /// A valid push with sequence 1 again.
    ReplayedSequence,
    /// A valid push with sequence 2, then one with sequence 1.
    OutOfOrderSequence,
    /// A correctly signed result for an action this session does not watch.
    UnwatchedAction,
}

/// The fake owner's node id and signing key (the forwarder pins the key).
const FAKE_OWNER: &str = "node-owner";

fn fake_owner_key() -> SigningKey {
    SigningKey::from_bytes(&[7; 32])
}

/// A result of `action_id` in `state`, its output naming the source (`genuine` or `forged`).
fn result_from(action_id: &str, state: ActionState, source: &str) -> ActionResult {
    let mut result = ActionResult::new(
        action_id,
        ActionTarget::Node(NodeId::new(FAKE_OWNER)),
        "slow",
        NodeId::new(FAKE_OWNER),
        state,
    );
    result.output.insert(
        "source".to_owned(),
        TypedConfigValue::String(source.to_owned()),
    );
    result
}

/// What the fake owner pushes once a watch is accepted: `(sequence, result, bound to the open)`.
fn forged_pushes(forgery: Forgery, action_id: &str) -> Vec<(u64, ActionResult, bool)> {
    let forged = || result_from(action_id, ActionState::Succeeded, "forged");
    let progress = || {
        result_from(
            action_id,
            ActionState::Running {
                progress: Some(500),
            },
            "progress",
        )
    };
    match forgery {
        Forgery::BadSignature => vec![(1, forged(), true)],
        Forgery::OtherOpenRequest => vec![(1, forged(), false)],
        Forgery::ReplayedSequence => vec![(1, progress(), true), (1, forged(), true)],
        Forgery::OutOfOrderSequence => vec![(2, progress(), true), (1, forged(), true)],
        Forgery::UnwatchedAction => vec![(
            1,
            result_from("not-watched", ActionState::Succeeded, "forged"),
            true,
        )],
    }
}

/// A stand-in owner on a real `orion+tcp` listener. It answers `RunAction` with the action
/// running the first time and with the genuine result afterwards, and it accepts a push session
/// whose pushes are forged as `forgery` says.
struct ForgingOwner {
    addr: SocketAddr,
    closed_sessions: Arc<AtomicU64>,
    _accept: tokio::task::JoinHandle<()>,
}

impl ForgingOwner {
    async fn start(forgery: Forgery) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake owner listener should bind");
        let addr = listener.local_addr().expect("fake owner address");
        let run_action_calls = Arc::new(AtomicU64::new(0));
        let closed_sessions = Arc::new(AtomicU64::new(0));
        let accept = {
            let closed_sessions = closed_sessions.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    tokio::spawn(serve_fake_connection(
                        stream,
                        forgery,
                        run_action_calls.clone(),
                        closed_sessions.clone(),
                    ));
                }
            })
        };
        Self {
            addr,
            closed_sessions,
            _accept: accept,
        }
    }

    /// Waits until the forwarder has closed a push session (the owner reads end of stream).
    async fn wait_for_closed_session(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.closed_sessions.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the forwarder closes the push session after a forged push");
    }
}

/// Writes one response frame (`status`, `body`) signed by the fake owner over `bound_to`.
async fn write_signed(
    writer: &mut OwnedWriteHalf,
    status: u8,
    body: Vec<u8>,
    bound_to: &[u8],
    corrupt: bool,
) {
    let mut signature = sign_peer_response(
        &fake_owner_key(),
        &NodeId::new(FAKE_OWNER),
        bound_to,
        status,
        &body,
    )
    .expect("the fake owner signs");
    if corrupt {
        signature.signature[0] ^= 1;
    }
    let frame = ResponseFrame {
        status,
        signature: Some(signature),
        body,
    }
    .encode()
    .expect("the frame encodes");
    write_control_payload_frame(writer, &frame, usize::MAX)
        .await
        .expect("the frame is written");
}

async fn serve_fake_connection(
    stream: TcpStream,
    forgery: Forgery,
    run_action_calls: Arc<AtomicU64>,
    closed_sessions: Arc<AtomicU64>,
) {
    let (mut reader, mut writer) = stream.into_split();
    let mut read_state = ControlFrameReadState::new();
    // The bytes of this connection's `OpenSession` request, once it opened a push session.
    let mut open_request: Option<Vec<u8>> = None;
    loop {
        let Ok(Some(request)) = read_state.read_payload(&mut reader, usize::MAX).await else {
            if open_request.is_some() {
                closed_sessions.fetch_add(1, Ordering::Release);
            }
            return;
        };
        let Ok(HttpRequestPayload::AuthenticatedPeer(auth)) =
            HttpCodec.decode_request_body(&request)
        else {
            return;
        };
        let PeerRequestPayload::Control(message) = auth.payload else {
            return;
        };
        let watching = matches!(&*message, ControlMessage::WatchActions(_));
        let answer = match *message {
            ControlMessage::OpenSession(_) => {
                open_request = Some(request.clone());
                HttpResponsePayload::Session(orion::control_plane::SessionWelcome {
                    session_id: "fake-owner-session".to_owned(),
                    idle_timeout_ms: 60_000,
                })
            }
            ControlMessage::RunAction(request_action) => {
                let calls = run_action_calls.fetch_add(1, Ordering::AcqRel) + 1;
                let state = if calls == 1 {
                    ActionState::Running { progress: None }
                } else {
                    ActionState::Succeeded
                };
                HttpResponsePayload::Actions(vec![result_from(
                    &request_action.action_id,
                    state,
                    "genuine",
                )])
            }
            ControlMessage::WatchActions(_) | ControlMessage::Ping => HttpResponsePayload::Accepted,
            _ => return,
        };
        let body = encode_to_vec(&answer).expect("the answer encodes");
        write_signed(&mut writer, STATUS_OK, body, &request, false).await;

        // After the watch is accepted, the forged pushes go out on the session they belong to.
        if let (true, Some(open)) = (watching, open_request.as_deref()) {
            for (sequence, result, bound) in forged_pushes(forgery, FORGED_ACTION) {
                let event = ControlMessage::ClientEvents(vec![ClientEvent {
                    sequence,
                    event: ClientEventKind::ActionResults(vec![result]),
                }]);
                let body =
                    encode_push_body(sequence, &encode_to_vec(&event).expect("the push encodes"));
                let bound_to: &[u8] = if bound { open } else { b"some other request" };
                let corrupt = matches!(forgery, Forgery::BadSignature);
                write_signed(&mut writer, PEER_TCP_STATUS_PUSH, body, bound_to, corrupt).await;
            }
        }
    }
}

/// The action the forwarder watches (its forged pushes name it, except for
/// [`Forgery::UnwatchedAction`]).
const FORGED_ACTION: &str = "forged-watched";

/// Forwards an action to a fake owner that sends `forgery`, and checks that the forged push is
/// refused: the session ends, nothing forged is applied, and the action completes by resend.
async fn forged_push_is_refused_and_the_action_still_completes(forgery: Forgery) {
    let owner = ForgingOwner::start(forgery).await;
    let forwarder = forwarder_knowing(
        "node-forwarder",
        FAKE_OWNER,
        owner.addr,
        orion_core::PublicKeyHex::new(orion_auth::hex::encode_hex(
            &fake_owner_key().verifying_key().to_bytes(),
        )),
    );

    let result = forward_slow(&forwarder, FORGED_ACTION).await;
    assert_eq!(
        result.output.get("source"),
        Some(&TypedConfigValue::String("genuine".to_owned())),
        "only the owner's genuine result is applied, never the forged push ({forgery:?})"
    );
    assert_eq!(result.state, ActionState::Succeeded);
    assert!(
        forwarder.action_forward_resends_for_test() > 0,
        "the refused push falls back to resending RunAction ({forgery:?})"
    );
    assert!(
        forwarder
            .query_actions(&ActionQuery::action("not-watched"))
            .is_empty(),
        "a result for an action the session does not watch is not applied ({forgery:?})"
    );
    owner.wait_for_closed_session().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_with_a_bad_signature_is_refused() {
    forged_push_is_refused_and_the_action_still_completes(Forgery::BadSignature).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_bound_to_another_open_session_request_is_refused() {
    forged_push_is_refused_and_the_action_still_completes(Forgery::OtherOpenRequest).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replayed_push_sequence_number_is_refused() {
    forged_push_is_refused_and_the_action_still_completes(Forgery::ReplayedSequence).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_out_of_order_push_sequence_number_is_refused() {
    forged_push_is_refused_and_the_action_still_completes(Forgery::OutOfOrderSequence).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_for_an_unwatched_action_is_refused() {
    forged_push_is_refused_and_the_action_still_completes(Forgery::UnwatchedAction).await;
}
