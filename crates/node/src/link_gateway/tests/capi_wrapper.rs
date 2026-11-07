//! The Rust wrapper over liborion's C surface (`liborion-rs`, static build) against a node's
//! `unix:` link: a status query and a record query, decoded through its `Event`.

use super::capi::{PROVIDER, publisher};
use super::serial::start;
use super::unix::{socket_path, unix_link};
use super::*;
use liborion_rs::{EventKind, Link, Subject, Value};
use orion::control_plane::TypedConfigValue;
use orion_link::message::{Message, StatusEntry as LinkStatus};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_rust_wrapper_queries_status_and_records() {
    let app = test_app("capi-wrapper");
    let path = socket_path("capi-wrapper");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let publisher = publisher(&app, &path).await;
    publisher.send(Message::Status(vec![LinkStatus::new(
        "mode",
        TypedConfigValue::String("tracking".into()),
    )]));
    let filter = orion::control_plane::StatusQuery::all();
    assert!(
        wait_until(Duration::from_secs(5), || !app
            .query_status(&filter)
            .is_empty())
        .await
    );

    let socket = path.clone();
    tokio::task::spawn_blocking(move || {
        let mut link =
            Link::connect(&socket, "rust-console", Duration::from_secs(5)).expect("a session");
        let req = link
            .status_query(Some((Subject::Provider, PROVIDER)), None)
            .expect("asks");
        let mut seen = None;
        loop {
            let event = link
                .next(Some(Duration::from_secs(5)))
                .expect("session")
                .expect("an answer");
            if event.req() != req {
                continue;
            }
            match event.kind() {
                EventKind::Status => {
                    assert_eq!(event.subject_str(), PROVIDER);
                    assert_eq!(event.key_str(), "mode");
                    seen = event
                        .value()
                        .map(|value| value == Value::String("tracking"));
                }
                EventKind::Answered => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(seen, Some(true));

        let req = link
            .record_query(Subject::Provider, Some(PROVIDER), None)
            .expect("asks");
        let record = loop {
            let event = link
                .next(Some(Duration::from_secs(5)))
                .expect("session")
                .expect("an answer");
            if event.req() == req && event.kind() == EventKind::Record {
                break event;
            }
        };
        assert_eq!(record.subject_str(), PROVIDER);
        let json = String::from_utf8_lossy(record.data()).into_owned();
        assert!(json.contains(PROVIDER), "{json}");
    })
    .await
    .expect("the client runs");
    drop(publisher);
    gateway.shutdown().await;
}
