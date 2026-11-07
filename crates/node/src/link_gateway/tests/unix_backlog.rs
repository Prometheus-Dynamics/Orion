//! A client that asks faster than it reads: the answers a session holds are bounded in bytes.
//! Queries past the cap are refused with `limit`, and the connection is not read until the
//! answers drain. The session stays up meanwhile.

use super::serial::{WAIT, start};
use super::unix::{socket_path, unix_link};
use super::unix_client::RawClient;
use super::*;
use crate::link_gateway::unix_client::ANSWER_BYTES;
use orion::ProviderId;
use orion::control_plane::{StatusQuery, StatusSubject, TypedConfigValue};
use orion_link::message::{
    ErrorCode, Message, ProviderState, Roles, StatusEntry as LinkStatus, StatusRequest,
};

const PROVIDER: &str = "provider.imu-board";
const ENTRIES: usize = 60;
const VALUE_BYTES: usize = 1_000;
const QUERIES: u32 = 40;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queries_past_the_answer_cap_are_refused_and_the_session_stays_up() {
    let name = "unix-backlog";
    let app = test_app(name);
    let path = socket_path(name);
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let publisher = RawClient::connect_as("imu-board", &path, Roles::PROVIDER).await;
    publisher.send(Message::ProviderState(ProviderState {
        provider: device_provider(PROVIDER),
        resources: vec![device_resource("imu-board.imu-0", PROVIDER, "unix")],
    }));
    assert!(wait_until(WAIT, || has_provider(&app, PROVIDER)).await);
    // About 60 KiB of status: within one answer's 32 frames; four answers reach the cap.
    for i in 0..ENTRIES {
        let value = TypedConfigValue::String("v".repeat(VALUE_BYTES));
        publisher.send(Message::Status(vec![LinkStatus::new(
            format!("k{i}"),
            value,
        )]));
    }
    let filter = StatusQuery::subject(StatusSubject::Provider(ProviderId::new(PROVIDER)));
    assert!(wait_until(WAIT, || app.query_status(&filter).len() == ENTRIES).await);
    const { assert!(ENTRIES * VALUE_BYTES < ANSWER_BYTES) };

    let mut client = RawClient::connect("console", &path).await;
    client.set_reading(false);
    for req in 1..=QUERIES {
        client.send(Message::StatusQuery(StatusRequest {
            req,
            filter: filter.clone(),
        }));
    }
    // Several heartbeat timeouts without reading a byte.
    tokio::time::sleep(Duration::from_millis(u64::from(HEARTBEAT_MS) * 10)).await;
    client.set_reading(true);

    let (mut answered, mut limited) = (0u32, 0u32);
    let mut first_error = None;
    while answered + limited < QUERIES {
        match client.next().await {
            Message::StatusChanges(changes) if !changes.more => {
                assert_eq!(changes.req, answered + limited + 1, "in order");
                answered += 1;
            }
            Message::StatusChanges(_) => {}
            Message::RequestError(error) => {
                assert_eq!(error.code, ErrorCode::LIMIT, "{error:?}");
                assert_eq!(error.req, answered + limited + 1, "in order");
                first_error.get_or_insert(error.reason.clone());
                limited += 1;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(
        answered >= 1,
        "the first query is always served: {first_error:?}"
    );
    assert!(
        answered <= 8,
        "at most the cap's worth (plus what the socket holds) is answered: {answered}"
    );
    let status = &app.link_status()[0];
    assert_eq!(status.device_timeouts, 0, "a session timed out");

    // Once read, the session serves queries again.
    client.send(Message::StatusQuery(StatusRequest {
        req: QUERIES + 1,
        filter,
    }));
    loop {
        match client.next().await {
            Message::StatusChanges(changes) if !changes.more => {
                assert_eq!(changes.req, QUERIES + 1);
                break;
            }
            Message::StatusChanges(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    gateway.shutdown().await;
}
