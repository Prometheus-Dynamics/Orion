//! An NT4 server behind a WebSocket listener it shares with another protocol: the listener
//! upgrades each connection and hands the NT4 ones over.

#![cfg(all(feature = "client", feature = "server"))]

use std::time::Duration;

use orion_nt4::{
    Client, ClientConfig, ClientEvent, Properties, SUBPROTOCOL_V4_0, SUBPROTOCOL_V4_1, Server,
    ServerConfig, SubscribeOptions, TopicEvent, Value,
};
use orion_ws::{Heard, WsListener, WsOptions};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(5);

#[test]
fn clients_reach_the_server_through_a_listener_it_shares() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&runtime, async {
        let server = Server::start(ServerConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            ..ServerConfig::default()
        })
        .await
        .unwrap();
        server
            .handle()
            .publish("/shared/speed", "double", Properties::new())
            .unwrap();
        server
            .handle()
            .set_value("/shared/speed", Value::Double(4.5))
            .unwrap();

        let mut listener = WsListener::bind("127.0.0.1:0".parse().unwrap(), WsOptions::default())
            .await
            .unwrap();
        let shared = listener.local_addr().unwrap();
        let mut new = listener.protocol(SUBPROTOCOL_V4_1);
        let mut old = listener.protocol(SUBPROTOCOL_V4_0);
        let mut other = listener.protocol("echo");
        tokio::task::spawn_local(listener.run());
        tokio::task::spawn_local(async move {
            loop {
                tokio::select! {
                    Some(socket) = new.next() => server.take(socket),
                    Some(socket) = old.next() => server.take(socket),
                    else => break,
                }
            }
        });
        // The other protocol on the port says back what it is told.
        tokio::task::spawn_local(async move {
            while let Some(mut socket) = other.next().await {
                while let Ok(Some(Heard::Text)) = socket.read.next(1024).await {
                    let text = String::from_utf8_lossy(socket.read.message()).into_owned();
                    let _ = socket.write.send_text(&text).await;
                }
            }
        });

        // An NT4 client on the shared port: connected, named by its path, and sent the value.
        let mut config = ClientConfig::new("127.0.0.1", "panel");
        config.port = shared.port();
        let mut client = Client::start(config);
        loop {
            match timeout(WAIT, client.next_event()).await.unwrap().unwrap() {
                ClientEvent::Connected => break,
                ClientEvent::Disconnected { reason } => panic!("connect failed: {reason}"),
                _ => {}
            }
        }
        let options = SubscribeOptions {
            prefix: true,
            ..Default::default()
        };
        let mut topics = client.handle().subscribe(&["/shared/"], options).unwrap();
        loop {
            let event = timeout(WAIT, topics.next()).await.unwrap().unwrap();
            if let TopicEvent::Value { name, value, .. } = event {
                assert_eq!(
                    (name.as_str(), value),
                    ("/shared/speed", Value::Double(4.5))
                );
                break;
            }
        }

        // And the other protocol still has the port too.
        let mut echo = orion_ws::connect(shared, "/echo", "echo", None)
            .await
            .unwrap();
        echo.write.send_text("still here").await.unwrap();
        assert_eq!(echo.read.next(1024).await.unwrap(), Some(Heard::Text));
        assert_eq!(echo.read.message(), b"still here");
    });
}
