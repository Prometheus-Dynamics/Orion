//! A peer that goes silent (its link dropped, the connection still looks open) is noticed within
//! the liveness timeout on both sides. Real server and client; the silent peer is a WebSocket
//! that completes the handshake and is then never polled, so it answers nothing.

#![cfg(all(feature = "client", feature = "server"))]

use std::time::{Duration, Instant};

use orion_nt4::{Client, ClientConfig, ClientEvent, SUBPROTOCOL_V4_1, Server, ServerConfig};
use orion_ws::WsOptions;
use tokio::net::TcpListener;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(10);

async fn next_event(client: &mut Client) -> ClientEvent {
    timeout(WAIT, client.next_event())
        .await
        .expect("no client event")
        .expect("client ended")
}

#[tokio::test]
async fn the_client_drops_a_server_that_goes_silent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (accepted, mut parked) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let (stream, from) = listener.accept().await.unwrap();
            let socket = orion_ws::accept(stream, from, &WsOptions::default(), &[SUBPROTOCOL_V4_1])
                .await
                .unwrap()
                .expect("an NT4 client");
            // Kept open and never polled: no pong, no reply, no close.
            let _ = accepted.send(socket);
        }
    });

    let mut config = ClientConfig::new("127.0.0.1", "liveness");
    config.port = port;
    config.liveness_timeout = Duration::from_millis(300);
    config.reconnect_min = Duration::from_millis(50);
    let mut client = Client::start(config);
    assert!(matches!(
        next_event(&mut client).await,
        ClientEvent::Connected
    ));
    let connected = Instant::now();
    let reason = loop {
        if let ClientEvent::Disconnected { reason } = next_event(&mut client).await {
            break reason;
        }
    };
    assert!(reason.contains("nothing from the server"), "{reason}");
    // The timer starts just before `Connected` is delivered, so allow for that.
    assert!(
        connected.elapsed() >= Duration::from_millis(250),
        "{:?}",
        connected.elapsed()
    );
    // It reconnects on its own.
    assert!(matches!(
        next_event(&mut client).await,
        ClientEvent::Connected
    ));
    assert!(parked.recv().await.is_some());
}

async fn server(client_timeout: Duration) -> Server {
    Server::start(ServerConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        client_timeout,
        ping_interval: Duration::from_millis(100),
        ..ServerConfig::default()
    })
    .await
    .unwrap()
}

async fn wait_for_clients(server: &Server, count: usize) {
    timeout(WAIT, async {
        while server.handle().clients().len() != count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the server never had {count} clients"));
}

#[tokio::test]
async fn the_server_closes_a_client_that_goes_silent() {
    let server = server(Duration::from_millis(300)).await;
    let mut socket = orion_ws::connect(server.local_addr(), "/nt/silent", SUBPROTOCOL_V4_1, None)
        .await
        .unwrap();
    wait_for_clients(&server, 1).await;
    // Not polled, so the server's pings go unanswered, and it drops the client.
    wait_for_clients(&server, 0).await;
    // The connection is closed: reading it ends after whatever the server sent before.
    timeout(WAIT, async {
        while let Ok(Some(_)) = socket.read.next(1 << 20).await {}
    })
    .await
    .expect("the server kept the connection open");
}

#[tokio::test]
async fn a_client_that_answers_stays_connected() {
    let server = server(Duration::from_secs(1)).await;
    let mut config = ClientConfig::new("127.0.0.1", "steady");
    config.port = server.local_addr().port();
    config.liveness_timeout = Duration::from_secs(1);
    let mut client = Client::start(config);
    assert!(matches!(
        next_event(&mut client).await,
        ClientEvent::Connected
    ));
    wait_for_clients(&server, 1).await;
    // Several timeouts' worth of an idle connection: both sides keep hearing each other.
    let quiet = timeout(Duration::from_secs(3), async {
        loop {
            if let ClientEvent::Disconnected { reason } = next_event(&mut client).await {
                return reason;
            }
        }
    })
    .await;
    assert!(quiet.is_err(), "disconnected: {quiet:?}");
    assert_eq!(server.handle().clients().len(), 1);
}
