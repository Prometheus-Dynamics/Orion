//! In-process round trips: a real server on a loopback port, real clients over WebSocket.

use std::time::Duration;

use orion_nt4::{
    Client, ClientConfig, ClientEvent, Properties, Server, ServerConfig, ServerEvent,
    SubscribeOptions, Subscription, TopicEvent, TopicOwner, Value,
};
use tokio::time::timeout;

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

/// Waits until the server holds `expected` for `name` (the client's sets travel asynchronously).
async fn wait_for_value(server: &Server, name: &str, expected: Value) {
    timeout(WAIT, async {
        while server.handle().value(name).as_ref() != Some(&expected) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("server never saw the value");
}

/// The next topic event matching `pred`, skipping others.
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

#[tokio::test]
async fn publish_reaches_prefix_subscriber_and_late_joiner_gets_last_value() {
    let server = start_server().await;
    let publisher = connect(&server, "pub").await;
    let publisher_handle = publisher.handle();
    let speed = publisher_handle.publish("/robot/speed", "double").unwrap();
    speed.set(Value::Double(1.0)).unwrap();
    speed.set(Value::Double(2.5)).unwrap();
    wait_for_value(&server, "/robot/speed", Value::Double(2.5)).await;

    // A late joiner subscribes after the last value and still gets it.
    let subscriber = connect(&server, "late").await;
    let mut sub = subscriber
        .handle()
        .subscribe(&["/robot/"], prefix())
        .unwrap();
    let announced = next_match(&mut sub, |e| matches!(e, TopicEvent::Announced(_))).await;
    match announced {
        TopicEvent::Announced(info) => {
            assert_eq!(info.name, "/robot/speed");
            assert_eq!(info.type_name, "double");
            assert_eq!(info.pubuid, None);
        }
        other => panic!("unexpected {other:?}"),
    }
    let first = next_match(&mut sub, |e| value_of(e).is_some()).await;
    let (name, value) = value_of(&first).unwrap();
    assert_eq!(name, "/robot/speed");
    assert_eq!(value, Value::Double(2.5));

    // A later value is pushed live.
    speed.set(Value::Double(3.0)).unwrap();
    let live = next_match(&mut sub, |e| value_of(e).is_some()).await;
    let (_, value) = value_of(&live).unwrap();
    assert_eq!(value, Value::Double(3.0));

    // Names outside the prefix are not delivered.
    let other = publisher_handle.publish("/else/x", "int").unwrap();
    other.set(Value::Int(1)).unwrap();
    speed.set(Value::Double(4.0)).unwrap();
    let next = next_match(&mut sub, |e| value_of(e).is_some()).await;
    assert_eq!(value_of(&next).unwrap().0, "/robot/speed");
}

#[tokio::test]
async fn publisher_gets_its_own_announce_with_pubuid_and_no_echo_unless_all() {
    let server = start_server().await;
    let client = connect(&server, "self").await;
    let handle = client.handle();
    let mut own = handle.subscribe(&["/self/"], prefix()).unwrap();
    let mut echo = handle
        .subscribe(
            &["/self/"],
            SubscribeOptions {
                prefix: true,
                all: true,
                ..Default::default()
            },
        )
        .unwrap();

    let publisher = handle.publish("/self/x", "string").unwrap();
    match next_match(&mut own, |e| matches!(e, TopicEvent::Announced(_))).await {
        TopicEvent::Announced(info) => assert!(info.pubuid.is_some()),
        other => panic!("unexpected {other:?}"),
    }
    publisher.set(Value::String("hi".into())).unwrap();
    // `all` gets the echo; the plain subscription does not.
    let event = next_match(&mut echo, |e| value_of(e).is_some()).await;
    let (_, v) = value_of(&event).unwrap();
    assert_eq!(v, Value::String("hi".into()));
    let quiet = timeout(
        Duration::from_millis(300),
        next_match(&mut own, |e| value_of(e).is_some()),
    )
    .await;
    assert!(quiet.is_err(), "own publication echoed without `all`");
}

#[tokio::test]
async fn unpublish_unannounces_to_subscribers() {
    let server = start_server().await;
    let publisher = connect(&server, "pub").await;
    let subscriber = connect(&server, "sub").await;
    let mut sub = subscriber
        .handle()
        .subscribe(&["/gone"], SubscribeOptions::default())
        .unwrap();

    let topic = publisher.handle().publish("/gone", "boolean").unwrap();
    topic.set(Value::Boolean(true)).unwrap();
    next_match(&mut sub, |e| matches!(e, TopicEvent::Announced(_))).await;
    topic.unpublish().unwrap();
    match next_match(&mut sub, |e| matches!(e, TopicEvent::Unannounced { .. })).await {
        TopicEvent::Unannounced { name, .. } => assert_eq!(name, "/gone"),
        other => panic!("unexpected {other:?}"),
    }
    assert!(server.handle().topic("/gone").is_none());
}

#[tokio::test]
async fn multiple_clients_share_values_and_publisher_drop_unannounces() {
    let server = start_server().await;
    let a = connect(&server, "a").await;
    let b = connect(&server, "b").await;
    let c = connect(&server, "c").await;
    let mut sub_b = b
        .handle()
        .subscribe(&["/shared"], SubscribeOptions::default())
        .unwrap();
    let mut sub_c = c
        .handle()
        .subscribe(&["/shared"], SubscribeOptions::default())
        .unwrap();

    let publisher = a.handle().publish("/shared", "int").unwrap();
    publisher.set(Value::Int(7)).unwrap();
    for sub in [&mut sub_b, &mut sub_c] {
        let event = next_match(sub, |e| value_of(e).is_some()).await;
        let (_, v) = value_of(&event).unwrap();
        assert_eq!(v, Value::Int(7));
    }

    // Publisher disconnects: the topic is removed and subscribers are told.
    drop(publisher);
    drop(a);
    next_match(&mut sub_b, |e| matches!(e, TopicEvent::Unannounced { .. })).await;
    assert_eq!(server.handle().clients().len(), 2);
}

#[tokio::test]
async fn local_api_edits_are_relayed_and_client_values_are_reported() {
    let server = start_server().await;
    let host = server.handle();
    host.publish("/Camera/exposure", "double", Properties::new())
        .unwrap();
    host.set_value("/Camera/exposure", Value::Double(20.0))
        .unwrap();
    let mut events = host.events();

    let client = connect(&server, "viewer").await;
    let mut sub = client.handle().subscribe(&["/Camera/"], prefix()).unwrap();
    next_match(&mut sub, |e| value_of(e).is_some()).await;

    host.set_value("/Camera/exposure", Value::Double(33.0))
        .unwrap();
    let event = next_match(&mut sub, |e| value_of(e).is_some()).await;
    let (_, v) = value_of(&event).unwrap();
    assert_eq!(v, Value::Double(33.0));

    // Type mismatches are rejected by the local API.
    assert!(host.set_value("/Camera/exposure", Value::Int(1)).is_err());

    // A client write shows up as a ValueChanged event attributed to that client.
    let writer = client.handle().publish("/Camera/mode", "string").unwrap();
    writer.set(Value::String("auto".into())).unwrap();
    let client_id = server
        .handle()
        .clients()
        .iter()
        .find(|c| c.name == "viewer")
        .unwrap()
        .id;
    let found = timeout(WAIT, async {
        loop {
            if let Ok(ServerEvent::ValueChanged { name, client, .. }) = events.recv().await
                && name == "/Camera/mode"
            {
                return client;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(found, Some(client_id));
    assert_eq!(
        host.topic("/Camera/mode").unwrap().owner,
        TopicOwner::Client(client_id)
    );

    host.delete("/Camera/exposure").unwrap();
    assert!(host.value("/Camera/exposure").is_none());
}

#[tokio::test]
async fn every_value_type_round_trips_to_a_subscriber() {
    let server = start_server().await;
    let host = server.handle();
    let cases: Vec<(&str, Value)> = vec![
        ("boolean", Value::Boolean(true)),
        ("double", Value::Double(-0.5)),
        ("int", Value::Int(-(1 << 40))),
        ("float", Value::Float(1.25)),
        ("string", Value::String("ünï".into())),
        ("raw", Value::Raw(vec![0, 9, 255])),
        ("struct:Pose2d", Value::Raw(vec![1, 2, 3])),
        ("boolean[]", Value::BooleanArray(vec![false, true])),
        ("double[]", Value::DoubleArray(vec![1.0, 2.0])),
        ("int[]", Value::IntArray(vec![3])),
        ("float[]", Value::FloatArray(vec![0.5])),
        ("string[]", Value::StringArray(vec!["a".into(), "".into()])),
    ];
    let client = connect(&server, "types").await;
    let mut sub = client.handle().subscribe(&["/types/"], prefix()).unwrap();
    for (type_name, value) in cases {
        let name = format!("/types/{type_name}");
        host.publish(&name, type_name, Properties::new()).unwrap();
        host.set_value(&name, value.clone()).unwrap();
        let got = loop {
            let event = next_match(&mut sub, |e| value_of(e).is_some()).await;
            if value_of(&event).unwrap().0 == name {
                break value_of(&event).unwrap().1;
            }
        };
        assert_eq!(got, value, "type {type_name}");
    }
}

#[tokio::test]
async fn rtt_sync_estimates_the_server_clock() {
    let server = start_server().await;
    let mut client = connect(&server, "rtt").await;
    let handle = client.handle();
    let synced = timeout(WAIT, async {
        loop {
            if let Some(ClientEvent::TimeSync { offset_us, rtt_us }) = client.next_event().await {
                return (offset_us, rtt_us);
            }
        }
    })
    .await
    .unwrap();
    // Both ends share this process clock, so the offset is about zero.
    assert!(synced.1 >= 0);
    assert!(synced.0.abs() < 50_000, "offset {} us", synced.0);
    assert!(handle.time_offset_us().is_some());
}

#[tokio::test]
async fn persistent_topics_survive_a_server_restart() {
    let path = std::env::temp_dir().join(format!("orion-nt4-test-{}.json", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let config = ServerConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        persist_path: Some(path.clone()),
    };
    {
        let server = Server::start(config.clone()).await.unwrap();
        let mut props = Properties::new();
        props.insert("persistent".into(), serde_json::Value::Bool(true));
        server
            .handle()
            .publish("/cfg/gain", "double", props)
            .unwrap();
        server
            .handle()
            .set_value("/cfg/gain", Value::Double(0.75))
            .unwrap();
        server
            .handle()
            .publish("/cfg/transient", "int", Properties::new())
            .unwrap();
    }
    let server = Server::start(config).await.unwrap();
    let topic = server.handle().topic("/cfg/gain").expect("restored");
    assert_eq!(topic.value, Some(Value::Double(0.75)));
    assert_eq!(topic.owner, TopicOwner::Unpublished);
    assert!(server.handle().topic("/cfg/transient").is_none());
    let _ = std::fs::remove_file(&path);
}
