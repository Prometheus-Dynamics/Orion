//! Behaviour under slow readers, json topics, and persistence timing. Real server, real clients.

#![cfg(all(feature = "client", feature = "server"))]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use futures_util::SinkExt;
use orion_nt4::{
    Client, ClientConfig, ClientEvent, Properties, Server, ServerConfig, SubscribeOptions,
    Subscription, TopicEvent, Value,
};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;

const WAIT: Duration = Duration::from_secs(5);

async fn start_server() -> Server {
    let config = ServerConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        persist_path: None,
    };
    Server::start(config).await.unwrap()
}

async fn connect(server: &Server, name: &str) -> Client {
    let mut config = ClientConfig::new("127.0.0.1", name);
    config.port = server.local_addr().port();
    let mut client = Client::start(config);
    loop {
        match timeout(WAIT, client.next_event()).await.unwrap().unwrap() {
            ClientEvent::Connected => return client,
            ClientEvent::Disconnected { reason } => panic!("connect failed: {reason}"),
            _ => {}
        }
    }
}

async fn wait_for_value(server: &Server, name: &str, expected: Value) {
    timeout(WAIT, async {
        while server.handle().value(name).as_ref() != Some(&expected) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("server never saw the value");
}

async fn next_match(sub: &mut Subscription, pred: impl Fn(&TopicEvent) -> bool) -> TopicEvent {
    loop {
        let event = timeout(WAIT, sub.next())
            .await
            .expect("timed out")
            .expect("stream ended");
        if pred(&event) {
            return event;
        }
    }
}

fn prefix() -> SubscribeOptions {
    SubscribeOptions {
        prefix: true,
        ..Default::default()
    }
}

fn value_of(event: &TopicEvent) -> Option<(&str, Value)> {
    match event {
        TopicEvent::Value { name, value, .. } => Some((name.as_str(), value.clone())),
        _ => None,
    }
}

fn persistent() -> Properties {
    let mut props = Properties::new();
    props.insert("persistent".into(), serde_json::Value::Bool(true));
    props
}

fn temp_file(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("orion-nt4-{tag}-{}.json", std::process::id()))
}

#[tokio::test]
async fn json_topics_announce_as_strings_and_carry_text_values() {
    let server = start_server().await;
    let publisher = connect(&server, "pub").await;
    let result = publisher.handle().publish("/app/result", "json").unwrap();
    let text = r#"{"ok":true,"detections":3}"#;
    result.set(Value::String(text.into())).unwrap();
    wait_for_value(&server, "/app/result", Value::String(text.into())).await;
    assert_eq!(
        server.handle().topic("/app/result").unwrap().type_name,
        "json"
    );

    let subscriber = connect(&server, "sub").await;
    let mut sub = subscriber.handle().subscribe(&["/app/"], prefix()).unwrap();
    match next_match(&mut sub, |e| matches!(e, TopicEvent::Announced(_))).await {
        TopicEvent::Announced(info) => assert_eq!(info.type_name, "json"),
        other => panic!("expected an announce, got {other:?}"),
    }
    let event = next_match(&mut sub, |e| matches!(e, TopicEvent::Value { .. })).await;
    assert_eq!(
        value_of(&event),
        Some(("/app/result", Value::String(text.into())))
    );

    // The host can edit a json topic with a string too.
    server
        .handle()
        .set_value("/app/result", Value::String("{}".into()))
        .unwrap();
    let event = next_match(&mut sub, |e| matches!(e, TopicEvent::Value { .. })).await;
    assert_eq!(
        value_of(&event),
        Some(("/app/result", Value::String("{}".into())))
    );
}

#[tokio::test]
async fn persisted_json_topics_reload_with_their_type() {
    let path = temp_file("json");
    let _ = std::fs::remove_file(&path);
    let config = ServerConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        persist_path: Some(path.clone()),
    };
    {
        let server = Server::start(config.clone()).await.unwrap();
        server
            .handle()
            .publish("/cfg/state", "json", persistent())
            .unwrap();
        server
            .handle()
            .set_value("/cfg/state", Value::String("{\"mode\":\"run\"}".into()))
            .unwrap();
    }
    let server = Server::start(config).await.unwrap();
    let topic = server.handle().topic("/cfg/state").expect("restored");
    assert_eq!(topic.type_name, "json");
    assert_eq!(
        topic.value,
        Some(Value::String("{\"mode\":\"run\"}".into()))
    );
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn persistence_is_compact_and_written_after_a_debounce() {
    let path = temp_file("debounce");
    let _ = std::fs::remove_file(&path);
    let config = ServerConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        persist_path: Some(path.clone()),
    };
    let server = Server::start(config).await.unwrap();
    server
        .handle()
        .publish("/cfg/gain", "double", persistent())
        .unwrap();
    server
        .handle()
        .set_value("/cfg/gain", Value::Double(1.25))
        .unwrap();
    // Not written inline: the file appears on the writer's next tick.
    let deadline = Instant::now() + WAIT;
    let text = loop {
        if let Ok(text) = std::fs::read_to_string(&path)
            && !text.is_empty()
        {
            break text;
        }
        assert!(Instant::now() < deadline, "file never written");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(!text.contains('\n'), "file is not compact: {text}");
    assert!(text.contains("1.25"), "{text}");
    drop(server);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn slow_subscriber_gets_the_latest_value_not_every_value() {
    let server = start_server().await;
    let publisher = connect(&server, "pub").await;
    let speed = publisher.handle().publish("/slow/speed", "int").unwrap();
    let subscriber = connect(&server, "sub").await;
    let mut sub = subscriber
        .handle()
        .subscribe(&["/slow/"], prefix())
        .unwrap();
    next_match(&mut sub, |e| matches!(e, TopicEvent::Announced(_))).await;

    // Flood while the subscriber does not read.
    for i in 0..2000 {
        speed.set(Value::Int(i)).unwrap();
    }
    wait_for_value(&server, "/slow/speed", Value::Int(1999)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut values = Vec::new();
    while let Ok(Some(event)) = timeout(Duration::from_millis(200), sub.next()).await {
        if let Some((_, value)) = value_of(&event) {
            values.push(value);
        }
    }
    assert_eq!(values.last(), Some(&Value::Int(1999)));
    assert!(
        values.len() < 100,
        "{} values queued for a slow reader",
        values.len()
    );
}

#[tokio::test]
async fn client_that_never_reads_its_control_messages_is_disconnected() {
    let server = start_server().await;
    let url = format!("ws://127.0.0.1:{}/nt/stuck", server.local_addr().port());
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(orion_nt4::SUBPROTOCOL_V4_1),
    );
    let (mut raw, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    raw.send(Message::Text(
        r#"[{"method":"subscribe","params":{"topics":["/churn"],"subuid":1,"options":{}}}]"#.into(),
    ))
    .await
    .unwrap();
    timeout(WAIT, async {
        while !server
            .handle()
            .clients()
            .iter()
            .any(|c| c.subscriptions == 1)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("subscription never arrived");

    // This loop never yields, so the connection task cannot drain its outbox: announces and
    // unannounces pile up until the control backlog overflows.
    let host = server.handle();
    for _ in 0..100_000 {
        host.publish("/churn", "int", Properties::new()).unwrap();
        host.delete("/churn").unwrap();
    }
    assert!(
        server.handle().clients().is_empty(),
        "a client that cannot keep up must be disconnected, not buffered forever"
    );
}
