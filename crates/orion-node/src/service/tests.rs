use super::*;
use crate::{NodeAppBuilder, NodeConfig};
use orion::{
    NodeId, Revision,
    control_plane::{ClientHello, ClientRole, MutationBatch},
};
use orion_service::{MiddlewareNext, RequestMiddleware};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::ThreadId;
use std::time::Duration;

struct ShortCircuitMiddleware;

struct TestAuthenticator;

impl Authenticator for TestAuthenticator {
    fn authenticate(&self, request: &mut ControlRequest) -> Result<(), NodeError> {
        if matches!(request.context.surface, ControlSurface::PeerHttp) {
            request.context.principal = ControlPrincipal::Local {
                source: LocalAddress::new("authn"),
                destination: LocalAddress::new("orion"),
            };
        }
        Ok(())
    }
}

struct DenyMutationsAuthorizer;
struct ThreadCaptureMiddleware {
    expected_thread: ThreadId,
    mismatch: Arc<AtomicBool>,
}

impl Authorizer for DenyMutationsAuthorizer {
    fn authorize(&self, request: &ControlRequest) -> Result<(), NodeError> {
        match request.operation() {
            ControlOperation::Mutations => Err(NodeError::Authorization(
                "mutations blocked by authorizer".into(),
            )),
            _ => Ok(()),
        }
    }
}

impl RequestMiddleware<ControlRequest> for ShortCircuitMiddleware {
    type Response = ControlResponse;
    type Error = NodeError;

    fn handle(
        &self,
        request: ControlRequest,
        next: MiddlewareNext<'_, ControlRequest, Self::Response, Self::Error>,
    ) -> Result<Self::Response, Self::Error> {
        match (request.context.surface, request.operation()) {
            (ControlSurface::PeerHttp, ControlOperation::Mutations) => Ok(ControlResponse::Http(
                Box::new(HttpResponsePayload::Accepted),
            )),
            (ControlSurface::LocalIpc, ControlOperation::Ping) => {
                Ok(ControlResponse::Local(Box::new(ControlMessage::Rejected(
                    "middleware intercepted ping".into(),
                ))))
            }
            _ => next.run(request),
        }
    }
}

impl RequestMiddleware<ControlRequest> for ThreadCaptureMiddleware {
    type Response = ControlResponse;
    type Error = NodeError;

    fn handle(
        &self,
        request: ControlRequest,
        next: MiddlewareNext<'_, ControlRequest, Self::Response, Self::Error>,
    ) -> Result<Self::Response, Self::Error> {
        if std::thread::current().id() != self.expected_thread {
            self.mismatch.store(true, Ordering::SeqCst);
        }
        next.run(request)
    }
}

fn test_app() -> NodeApp {
    let peer_sync_execution = NodeConfig::try_peer_sync_execution_from_env()
        .expect("peer sync execution defaults should parse");
    let runtime_tuning =
        NodeConfig::try_runtime_tuning_from_env().expect("runtime tuning defaults should parse");

    NodeAppBuilder::default()
        .config(NodeConfig {
            node_id: NodeId::new("node-middleware"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-middleware"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: crate::PeerAuthenticationMode::Disabled,
            peer_sync_execution,
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning,
        })
        .with_control_middleware(ShortCircuitMiddleware)
        .try_build()
        .expect("node app should build")
}

fn thread_capture_app(expected_thread: ThreadId, mismatch: Arc<AtomicBool>) -> NodeApp {
    let peer_sync_execution = NodeConfig::try_peer_sync_execution_from_env()
        .expect("peer sync execution defaults should parse");
    let runtime_tuning =
        NodeConfig::try_runtime_tuning_from_env().expect("runtime tuning defaults should parse");

    NodeAppBuilder::default()
        .config(NodeConfig {
            node_id: NodeId::new("node-thread-capture"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-thread-capture"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: crate::PeerAuthenticationMode::Disabled,
            peer_sync_execution,
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning,
        })
        .with_control_middleware(ThreadCaptureMiddleware {
            expected_thread,
            mismatch,
        })
        .with_control_middleware(ShortCircuitMiddleware)
        .try_build()
        .expect("node app should build")
}

#[test]
fn http_requests_flow_through_control_middleware() {
    let app = test_app();
    let handler = app.http_control_handler();

    let response = handler
        .handle_payload(HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(MutationBatch {
                base_revision: Revision::new(42),
                mutations: Vec::new(),
            }),
        )))
        .expect("middleware should short-circuit stale mutation batch");

    assert_eq!(response, HttpResponsePayload::Accepted);
}

#[test]
fn local_ipc_requests_flow_through_control_middleware() {
    let app = test_app();
    let handler = app.unix_control_handler();

    let response = handler
        .handle_control(ControlEnvelope {
            source: LocalAddress::new("provider"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::Ping,
        })
        .expect("middleware should short-circuit local ping");

    assert_eq!(
        response.message,
        ControlMessage::Rejected("middleware intercepted ping".into())
    );
}

#[test]
fn local_stream_requests_share_the_same_control_pipeline() {
    let app = test_app();
    let response = app
        .serve_local_control_message(
            ControlSurface::LocalIpcStream,
            LocalAddress::new("client"),
            LocalAddress::new("orion"),
            ControlMessage::ClientHello(ClientHello {
                client_name: "stream-client".into(),
                role: ClientRole::ControlPlane,
            }),
        )
        .expect("client hello should still flow through the shared pipeline");

    assert!(matches!(response, ControlMessage::ClientWelcome(_)));
}

#[test]
fn authorization_middleware_can_block_requests_before_app_logic() {
    let peer_sync_execution = NodeConfig::try_peer_sync_execution_from_env()
        .expect("peer sync execution defaults should parse");
    let runtime_tuning =
        NodeConfig::try_runtime_tuning_from_env().expect("runtime tuning defaults should parse");
    let app = NodeAppBuilder::default()
        .config(NodeConfig {
            node_id: NodeId::new("node-authz"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-authz"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: crate::PeerAuthenticationMode::Disabled,
            peer_sync_execution,
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning,
        })
        .with_control_middleware(AuthorizationMiddleware::new(
            Arc::new(TestAuthenticator),
            Arc::new(DenyMutationsAuthorizer),
        ))
        .try_build()
        .expect("node app should build");

    let err = app
        .http_control_handler()
        .handle_payload(HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(MutationBatch {
                base_revision: Revision::ZERO,
                mutations: Vec::new(),
            }),
        )))
        .expect_err("authorizer should reject mutations");

    assert!(err.to_string().contains("mutations blocked by authorizer"));
}

#[tokio::test(flavor = "current_thread")]
async fn http_control_requests_stay_on_the_runtime_thread() {
    let mismatch = Arc::new(AtomicBool::new(false));
    let app = thread_capture_app(std::thread::current().id(), Arc::clone(&mismatch));

    for _ in 0..8 {
        let response = app
            .http_control_handler()
            .handle_payload(HttpRequestPayload::Control(Box::new(
                ControlMessage::Mutations(MutationBatch {
                    base_revision: Revision::new(42),
                    mutations: Vec::new(),
                }),
            )))
            .expect("http request should complete on the runtime thread");
        assert_eq!(response, HttpResponsePayload::Accepted);
    }

    assert!(
        !mismatch.load(Ordering::SeqCst),
        "http control requests should not hop to helper threads"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn local_ipc_control_requests_stay_on_the_runtime_thread() {
    let mismatch = Arc::new(AtomicBool::new(false));
    let app = thread_capture_app(std::thread::current().id(), Arc::clone(&mismatch));

    for _ in 0..8 {
        let response = app
            .unix_control_handler()
            .handle_control(ControlEnvelope {
                source: LocalAddress::new("provider"),
                destination: LocalAddress::new("orion"),
                message: ControlMessage::Ping,
            })
            .expect("local ipc request should complete on the runtime thread");
        assert_eq!(
            response.message,
            ControlMessage::Rejected("middleware intercepted ping".into())
        );
    }

    assert!(
        !mismatch.load(Ordering::SeqCst),
        "local ipc control requests should not hop to helper threads"
    );
}
