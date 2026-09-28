use std::{
    fs::{self, File},
    os::{fd::OwnedFd, unix::fs::FileExt},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{sync::oneshot, task::JoinHandle, time::timeout};

use super::*;
use crate::IpcTransportError;

fn unique_path(name: &str, extension: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "orion-ipc-fd-latest-{name}-{}-{nonce}.{extension}",
        std::process::id()
    ))
}

/// An fd for an unlinked temp file holding `content`.
fn content_fd(name: &str, content: &[u8]) -> OwnedFd {
    let path = unique_path(name, "bin");
    fs::write(&path, content).expect("fd test file should be written");
    let file = File::open(&path).expect("fd test file should open");
    let _ = fs::remove_file(&path);
    OwnedFd::from(file)
}

/// Reads via `pread` so dup'd descriptors sharing one file offset do not interfere.
fn read_fd(fd: &OwnedFd) -> Vec<u8> {
    let file = File::from(fd.try_clone().expect("fd should duplicate"));
    let mut buffer = vec![0_u8; 256];
    let read = file.read_at(&mut buffer, 0).expect("fd should read");
    buffer.truncate(read);
    buffer
}

struct RunningServer {
    path: PathBuf,
    publisher: UnixFdLatestPublisher,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), IpcTransportError>>,
}

impl RunningServer {
    async fn start(name: &str, config: UnixFdLatestConfig) -> Self {
        let path = unique_path(name, "sock");
        let server = UnixFdLatestServer::bind_with_config(&path, config)
            .await
            .expect("fd latest server should bind");
        let publisher = server.publisher();
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(server.serve_with_shutdown(async move {
            let _ = shutdown_rx.await;
        }));
        Self {
            path,
            publisher,
            shutdown: Some(shutdown),
            task,
        }
    }

    async fn client(&self) -> UnixFdLatestClient {
        UnixFdLatestClient::connect(&self.path)
            .await
            .expect("fd latest client should connect")
    }

    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        timeout(Duration::from_secs(1), &mut self.task)
            .await
            .expect("fd latest server should shut down promptly")
            .expect("fd latest server task should join")
            .expect("fd latest server should shut down cleanly");
        assert!(
            !self.path.exists(),
            "socket file should be removed on shutdown"
        );
    }
}

fn expect_frame(reply: UnixFdLatestReply) -> UnixFdLatestFrame {
    match reply {
        UnixFdLatestReply::Frame(frame) => frame,
        other => panic!("expected a frame reply, got {other:?}"),
    }
}

#[tokio::test]
async fn latest_hands_out_dup_fds_and_sequence_over_persistent_connection() {
    let server = RunningServer::start("latest", UnixFdLatestConfig::default()).await;
    let mut client = server.client().await;

    assert!(matches!(
        client.latest().await.expect("empty latest should reply"),
        UnixFdLatestReply::Empty
    ));

    let before_publish = SystemTime::now();
    let sequence = server
        .publisher
        .publish(
            b"{\"planes\":2}".to_vec(),
            vec![
                content_fd("plane-y", b"luma-plane"),
                content_fd("plane-uv", b"chroma-plane"),
            ],
        )
        .expect("frame should publish");
    assert_eq!(sequence, 1);
    assert_eq!(server.publisher.latest_sequence(), Some(1));

    let first = expect_frame(client.latest().await.expect("latest should reply"));
    assert_eq!(first.sequence, 1);
    assert!(first.published_at >= before_publish - Duration::from_secs(1));
    assert_eq!(first.frame.payload, b"{\"planes\":2}");
    assert_eq!(first.frame.fds.len(), 2);
    assert_eq!(read_fd(&first.frame.fds[0]), b"luma-plane");
    assert_eq!(read_fd(&first.frame.fds[1]), b"chroma-plane");

    // Same connection, repeated requests: the same frame again, then the replacement.
    let again = expect_frame(client.latest().await.expect("repeat latest should reply"));
    assert_eq!(again.sequence, 1);
    assert_eq!(read_fd(&again.frame.fds[0]), b"luma-plane");

    server
        .publisher
        .publish(b"next".to_vec(), vec![content_fd("next", b"next-plane")])
        .expect("second frame should publish");
    let second = expect_frame(client.latest().await.expect("latest should reply"));
    assert_eq!(second.sequence, 2);
    assert_eq!(second.frame.payload, b"next");
    assert_eq!(read_fd(&second.frame.fds[0]), b"next-plane");
    // Previously received descriptors stay valid after the server replaced its frame.
    assert_eq!(read_fd(&first.frame.fds[1]), b"chroma-plane");

    server.publisher.clear();
    assert!(matches!(
        client.latest().await.expect("cleared latest should reply"),
        UnixFdLatestReply::Empty
    ));
    assert_eq!(
        server
            .publisher
            .publish(Vec::new(), Vec::new())
            .expect("publish after clear should work"),
        3,
        "sequence numbering continues after clear"
    );

    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn next_after_waits_for_newer_sequence_or_times_out() {
    let server = RunningServer::start("next-after", UnixFdLatestConfig::default()).await;
    let mut client = server.client().await;

    let publisher = server.publisher.clone();
    let publish = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        publisher
            .publish(b"first".to_vec(), vec![content_fd("wait", b"waited-for")])
            .expect("frame should publish")
    });
    let waited = expect_frame(
        client
            .next_after(0, Duration::from_secs(2))
            .await
            .expect("next_after should reply"),
    );
    assert_eq!(publish.await.expect("publisher should join"), 1);
    assert_eq!(waited.sequence, 1);
    assert_eq!(read_fd(&waited.frame.fds[0]), b"waited-for");

    // Already have 1: nothing newer arrives, so the wait times out and reports the latest.
    let reply = client
        .next_after(1, Duration::from_millis(50))
        .await
        .expect("timed out wait should still reply");
    assert!(matches!(
        reply,
        UnixFdLatestReply::Timeout {
            latest_sequence: Some(1)
        }
    ));

    // A frame newer than the requested sequence is returned without waiting.
    let started = std::time::Instant::now();
    let immediate = expect_frame(
        client
            .next_after(0, Duration::from_secs(5))
            .await
            .expect("next_after should reply"),
    );
    assert_eq!(immediate.sequence, 1);
    assert!(started.elapsed() < Duration::from_secs(1));

    // Intermediate publishes collapse: a waiter sees only the latest value.
    server.publisher.publish(b"2".to_vec(), Vec::new()).unwrap();
    server.publisher.publish(b"3".to_vec(), Vec::new()).unwrap();
    let latest = expect_frame(
        client
            .next_after(1, Duration::from_secs(1))
            .await
            .expect("next_after should reply"),
    );
    assert_eq!(latest.sequence, 3);
    assert_eq!(latest.frame.payload, b"3");

    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn frames_older_than_max_age_are_reported_stale() {
    let config = UnixFdLatestConfig::default().with_max_age(Duration::from_millis(50));
    let server = RunningServer::start("stale", config).await;
    let mut client = server.client().await;

    server
        .publisher
        .publish(b"fresh".to_vec(), vec![content_fd("stale", b"old-plane")])
        .expect("frame should publish");
    assert_eq!(
        expect_frame(client.latest().await.expect("latest should reply")).sequence,
        1
    );

    tokio::time::sleep(Duration::from_millis(120)).await;
    match client.latest().await.expect("stale latest should reply") {
        UnixFdLatestReply::Stale { sequence, .. } => assert_eq!(sequence, 1),
        other => panic!("expected stale reply, got {other:?}"),
    }
    assert!(matches!(
        client
            .next_after(0, Duration::from_millis(50))
            .await
            .expect("stale next_after should reply"),
        UnixFdLatestReply::Stale { sequence: 1, .. }
    ));

    server
        .publisher
        .publish(b"fresh-again".to_vec(), Vec::new())
        .expect("frame should publish");
    let fresh = expect_frame(client.latest().await.expect("latest should reply"));
    assert_eq!(fresh.sequence, 2);
    assert_eq!(fresh.frame.payload, b"fresh-again");

    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn extra_clients_over_max_clients_are_rejected_until_a_slot_frees() {
    let config = UnixFdLatestConfig::default().with_max_clients(1);
    let server = RunningServer::start("max-clients", config).await;
    server.publisher.publish(b"x".to_vec(), Vec::new()).unwrap();

    let mut first = server.client().await;
    expect_frame(first.latest().await.expect("first client should be served"));

    let mut rejected = server.client().await;
    assert!(matches!(
        rejected.latest().await,
        Err(IpcTransportError::ConnectionRefused(_))
    ));

    // A client parked in a long wait frees its slot as soon as it disconnects.
    let waiting = tokio::spawn(async move {
        let _ = first.next_after(1, Duration::from_secs(20)).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    waiting.abort();
    let _ = waiting.await;

    let served = timeout(Duration::from_secs(2), async {
        loop {
            let mut client = server.client().await;
            match client.latest().await {
                Ok(reply) => break reply,
                Err(IpcTransportError::ConnectionRefused(_)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(err) => panic!("unexpected client error: {err}"),
            }
        }
    })
    .await
    .expect("slot should free promptly after the waiting client disconnects");
    assert_eq!(expect_frame(served).sequence, 1);

    server.stop().await;
}

#[tokio::test]
async fn multiple_clients_receive_the_same_latest_frame() {
    let server = RunningServer::start("multi", UnixFdLatestConfig::default()).await;

    let mut waiters = Vec::new();
    for _ in 0..8 {
        let mut client = server.client().await;
        waiters.push(tokio::spawn(async move {
            client
                .next_after(0, Duration::from_secs(2))
                .await
                .expect("waiting client should reply")
        }));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    server
        .publisher
        .publish(
            b"shared".to_vec(),
            vec![content_fd("multi", b"shared-plane")],
        )
        .expect("frame should publish");

    for waiter in waiters {
        let frame = expect_frame(waiter.await.expect("waiter should join"));
        assert_eq!(frame.sequence, 1);
        assert_eq!(frame.frame.payload, b"shared");
        assert_eq!(read_fd(&frame.frame.fds[0]), b"shared-plane");
    }

    server.stop().await;
}

#[tokio::test]
async fn publish_enforces_payload_and_descriptor_limits() {
    let path = unique_path("limits", "sock");
    let server = UnixFdLatestServer::bind_with_config(
        &path,
        UnixFdLatestConfig::default()
            .with_max_payload_bytes(4)
            .with_max_fds(1),
    )
    .await
    .expect("fd latest server should bind");

    assert!(matches!(
        server.publish(vec![0; 5], Vec::new()),
        Err(IpcTransportError::EncodeFailed(_))
    ));
    assert!(matches!(
        server.publish(
            Vec::new(),
            vec![content_fd("limit-a", b"a"), content_fd("limit-b", b"b")]
        ),
        Err(IpcTransportError::EncodeFailed(_))
    ));
    assert_eq!(server.publisher().latest_sequence(), None);
    assert_eq!(
        server
            .publish(vec![0; 4], vec![content_fd("limit-ok", b"ok")])
            .expect("in-limit frame should publish"),
        1
    );

    drop(server);
    assert!(!path.exists(), "dropping the server removes its socket");
}

#[tokio::test]
async fn bind_replaces_stale_socket_but_not_live_listeners_or_regular_files() {
    let path = unique_path("stale-socket", "sock");
    drop(std::os::unix::net::UnixListener::bind(&path).expect("stale listener should bind"));
    assert!(path.exists(), "dropped std listener leaves a stale socket");

    let server = UnixFdLatestServer::bind(&path)
        .await
        .expect("bind should replace a stale socket");
    assert!(matches!(
        UnixFdLatestServer::bind(&path).await,
        Err(IpcTransportError::BindFailed(_))
    ));
    assert!(path.exists(), "failed bind must not remove a live socket");
    drop(server);
    assert!(!path.exists());

    let regular = unique_path("regular-file", "sock");
    fs::write(&regular, b"not a socket").expect("regular file should be written");
    assert!(matches!(
        UnixFdLatestServer::bind(&regular).await,
        Err(IpcTransportError::BindFailed(_))
    ));
    assert!(Path::new(&regular).exists());
    let _ = fs::remove_file(regular);
}
