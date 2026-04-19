use super::*;
use orion_transport_ipc::{read_control_frame, write_control_frame};
use std::sync::Arc;
use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::OnceLock,
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::UnixListener;
use tokio::sync::Mutex;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn unique_socket_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should advance")
        .as_nanos();
    std::env::temp_dir().join(format!("orion-client-{label}-{nanos}.sock"))
}

async fn with_env_var_async<F, Fut>(key: &str, value: &Path, f: F)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    let _guard = env_lock().lock().await;
    let prior = std::env::var_os(key);
    unsafe { std::env::set_var(key, value) };
    f().await;
    match prior {
        Some(value) => unsafe { std::env::set_var(key, value) },
        None => unsafe { std::env::remove_var(key) },
    }
}

#[tokio::test]
async fn local_provider_app_connect_default_uses_default_stream_socket() {
    let socket_path = unique_socket_path("provider-default-stream-app");
    let hello_count = Arc::new(AtomicUsize::new(0));
    let _ = tokio::fs::remove_file(&socket_path).await;
    let listener = UnixListener::bind(&socket_path).expect("listener should bind");
    let server_task = tokio::spawn(serve_stream_accepting_requests(
        listener,
        ClientRole::Provider,
        1,
        Arc::clone(&hello_count),
    ));

    with_env_var_async("ORION_NODE_IPC_STREAM_SOCKET", &socket_path, || async {
        let provider = ProviderRecord {
            provider_id: ProviderId::new("provider.local"),
            node_id: NodeId::new("node-a"),
            resource_types: vec![ResourceType::new("camera.device")],
        };
        let app = LocalProviderApp::connect_default("provider-a", provider)
            .expect("default local provider app should connect");
        app.register()
            .await
            .expect("default provider app should register");
    })
    .await;

    server_task.await.expect("server task should complete");
    assert_eq!(hello_count.load(Ordering::SeqCst), 1);
    let _ = tokio::fs::remove_file(&socket_path).await;
}

#[tokio::test]
async fn local_provider_client_connect_default_uses_default_stream_socket() {
    let socket_path = unique_socket_path("provider-client-default-stream");
    let hello_count = Arc::new(AtomicUsize::new(0));
    let _ = tokio::fs::remove_file(&socket_path).await;
    let listener = UnixListener::bind(&socket_path).expect("listener should bind");
    let server_task = tokio::spawn(serve_stream_accepting_requests(
        listener,
        ClientRole::Provider,
        1,
        Arc::clone(&hello_count),
    ));

    with_env_var_async("ORION_NODE_IPC_STREAM_SOCKET", &socket_path, || async {
        let provider = ProviderRecord {
            provider_id: ProviderId::new("provider.local"),
            node_id: NodeId::new("node-a"),
            resource_types: vec![ResourceType::new("camera.device")],
        };
        let client = LocalProviderClient::connect_default("provider-a")
            .expect("default local provider client should connect");
        client
            .register_provider(provider)
            .await
            .expect("default provider client should register");
    })
    .await;

    server_task.await.expect("server task should complete");
    assert_eq!(hello_count.load(Ordering::SeqCst), 1);
    let _ = tokio::fs::remove_file(&socket_path).await;
}

#[tokio::test]
async fn local_provider_client_reuses_stream_session_across_requests() {
    let socket_path = unique_socket_path("provider-client-stream-reuse");
    let hello_count = Arc::new(AtomicUsize::new(0));
    let _ = tokio::fs::remove_file(&socket_path).await;
    let listener = UnixListener::bind(&socket_path).expect("listener should bind");
    let server_task = tokio::spawn(serve_stream_accepting_requests(
        listener,
        ClientRole::Provider,
        2,
        Arc::clone(&hello_count),
    ));

    with_env_var_async("ORION_NODE_IPC_STREAM_SOCKET", &socket_path, || async {
        let provider = ProviderRecord {
            provider_id: ProviderId::new("provider.local"),
            node_id: NodeId::new("node-a"),
            resource_types: vec![ResourceType::new("camera.device")],
        };
        let client = LocalProviderClient::connect_default("provider-a")
            .expect("default local provider client should connect");
        client
            .register_provider(provider.clone())
            .await
            .expect("first provider registration should succeed");
        client
            .register_provider(provider)
            .await
            .expect("second provider registration should reuse the stream session");
    })
    .await;

    server_task.await.expect("server task should complete");
    assert_eq!(hello_count.load(Ordering::SeqCst), 1);
    let _ = tokio::fs::remove_file(&socket_path).await;
}

#[tokio::test]
async fn local_executor_client_connect_default_uses_default_stream_socket() {
    let socket_path = unique_socket_path("executor-client-default-stream");
    let hello_count = Arc::new(AtomicUsize::new(0));
    let _ = tokio::fs::remove_file(&socket_path).await;
    let listener = UnixListener::bind(&socket_path).expect("listener should bind");
    let server_task = tokio::spawn(serve_stream_accepting_requests(
        listener,
        ClientRole::Executor,
        1,
        Arc::clone(&hello_count),
    ));

    with_env_var_async("ORION_NODE_IPC_STREAM_SOCKET", &socket_path, || async {
        let executor = ExecutorRecord {
            executor_id: ExecutorId::new("executor.local"),
            node_id: NodeId::new("node-a"),
            runtime_types: vec![RuntimeType::new("graph.exec.v1")],
        };
        let client = LocalExecutorClient::connect_default("executor-a")
            .expect("default local executor client should connect");
        client
            .register_executor(executor)
            .await
            .expect("default executor client should register");
    })
    .await;

    server_task.await.expect("server task should complete");
    assert_eq!(hello_count.load(Ordering::SeqCst), 1);
    let _ = tokio::fs::remove_file(&socket_path).await;
}

#[tokio::test]
async fn provider_event_stream_connect_default_uses_default_stream_socket() {
    let socket_path = unique_socket_path("provider-default-stream");
    let _ = tokio::fs::remove_file(&socket_path).await;
    let listener = UnixListener::bind(&socket_path).expect("listener should bind");
    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("stream accept should work");
        let request = read_control_frame(&mut stream)
            .await
            .expect("hello frame should decode")
            .expect("hello frame should exist");
        assert!(matches!(request.message, ControlMessage::ClientHello(_)));
        write_control_frame(
            &mut stream,
            &ControlEnvelope {
                source: LocalAddress::new("orion"),
                destination: request.source,
                message: ControlMessage::ClientWelcome(orion_control_plane::ClientSession {
                    session_id: "node-a:provider-a".into(),
                    role: ClientRole::Provider,
                    node_id: NodeId::new("node-a"),
                    source: "provider-a".into(),
                    client_name: "provider-a".into(),
                }),
            },
        )
        .await
        .expect("welcome frame should send");
    });

    with_env_var_async("ORION_NODE_IPC_STREAM_SOCKET", &socket_path, || async {
        let _stream = ProviderEventStream::connect_default("provider-a")
            .await
            .expect("default provider stream should connect");
    })
    .await;

    server_task.await.expect("server task should complete");
    let _ = tokio::fs::remove_file(&socket_path).await;
}

async fn serve_stream_accepting_requests(
    listener: UnixListener,
    role: ClientRole,
    accepted_requests: usize,
    hello_count: Arc<AtomicUsize>,
) {
    let (mut stream, _) = listener.accept().await.expect("stream accept should work");

    let hello = read_control_frame(&mut stream)
        .await
        .expect("hello frame should decode")
        .expect("hello frame should exist");
    let client_name = match hello.message {
        ControlMessage::ClientHello(hello) => {
            hello_count.fetch_add(1, Ordering::SeqCst);
            assert_eq!(hello.role, role);
            hello.client_name
        }
        message => panic!("expected client hello, got {message:?}"),
    };
    write_control_frame(
        &mut stream,
        &ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: hello.source,
            message: ControlMessage::ClientWelcome(orion_control_plane::ClientSession {
                session_id: format!("node-a:{client_name}").into(),
                role,
                node_id: NodeId::new("node-a"),
                source: client_name.to_string(),
                client_name,
            }),
        },
    )
    .await
    .expect("welcome frame should send");

    for _ in 0..accepted_requests {
        let request = read_control_frame(&mut stream)
            .await
            .expect("request frame should decode")
            .expect("request frame should exist");
        assert!(!matches!(request.message, ControlMessage::ClientHello(_)));
        write_control_frame(
            &mut stream,
            &ControlEnvelope {
                source: LocalAddress::new("orion"),
                destination: request.source,
                message: ControlMessage::Accepted,
            },
        )
        .await
        .expect("accepted frame should send");
    }
}
