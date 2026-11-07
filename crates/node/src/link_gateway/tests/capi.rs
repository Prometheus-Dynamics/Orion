//! liborion's link client (`orion-capi`, docs/capi.md) against a node's `unix:` link: a status
//! watch through the C surface, with its bootstrap, changes, a refused request and a buffer too
//! small; and the blocking client keeping its session alive by pinging while it waits.

use super::serial::{WAIT, start};
use super::unix::{socket_path, unix_link};
use super::unix_client::RawClient;
use super::*;
use orion::control_plane::TypedConfigValue;
use orion_capi::link::{Event, LinkClient};
use orion_capi::{
    ORION_EVENT_ANSWERED, ORION_EVENT_BOOTSTRAP, ORION_EVENT_REQUEST_ERROR, ORION_EVENT_STATUS,
    ORION_SUBJECT_PROVIDER, ORION_VALUE_INT, ORION_VALUE_STRING, OrionEvent,
};
use orion_link::message::{Message, ProviderState, Roles, StatusEntry as LinkStatus};
use std::ffi::CString;

const PROVIDER: &str = "provider.imu-board";

fn empty_event() -> OrionEvent {
    OrionEvent {
        struct_size: size_of::<OrionEvent>() as u32,
        kind: 0,
        req: 0,
        flags: 0,
        subject_kind: 0,
        value_type: 0,
        int_value: 0,
        uint_value: 0,
        float_value: 0.0,
        ttl_ms: 0,
        published_at_ms: 0,
        subject_len: 0,
        key_len: 0,
        data_len: 0,
        reserved: 0,
    }
}

/// A provider session publishing status on the link at `path`.
async fn publisher(app: &NodeApp, path: &std::path::Path) -> RawClient {
    let publisher = RawClient::connect_as("imu-board", path, Roles::PROVIDER).await;
    publisher.send(Message::ProviderState(ProviderState {
        provider: device_provider(PROVIDER),
        resources: vec![device_resource("imu-board.imu-0", PROVIDER, "unix")],
    }));
    assert!(wait_until(WAIT, || has_provider(app, PROVIDER)).await);
    publisher
}

/// The next event through the C surface, with its bytes.
fn next(handle: abi::Handle, buf: &mut Vec<u8>) -> (OrionEvent, Vec<u8>) {
    let mut event = empty_event();
    loop {
        // SAFETY: a valid event and a buffer of `buf.len()` bytes.
        let status = unsafe {
            orion_capi::orion_link_next(handle, 5_000, &mut event, buf.as_mut_ptr(), buf.len())
        };
        if status == abi::Status::BUFFER_TOO_SMALL {
            let needed = (event.subject_len + event.key_len + event.data_len) as usize;
            assert!(needed > buf.len());
            buf.resize(needed, 0);
            continue;
        }
        assert_eq!(status, abi::Status::OK);
        let len = (event.subject_len + event.key_len + event.data_len) as usize;
        return (event, buf[..len].to_vec());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_c_client_watches_status_through_liborion() {
    let app = test_app("capi-watch");
    let path = socket_path("capi-watch");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let publisher = publisher(&app, &path).await;
    let value = |key: &str, v: TypedConfigValue| Message::Status(vec![LinkStatus::new(key, v)]);
    publisher.send(value("count", TypedConfigValue::Int(-1)));
    let filter_all = orion::control_plane::StatusQuery::all();
    assert!(wait_until(WAIT, || !app.query_status(&filter_all).is_empty()).await);

    let socket = CString::new(path.to_str().expect("utf-8 path")).expect("no NUL");
    let watcher = tokio::task::spawn_blocking(move || {
        let name = CString::new("c-console").expect("no NUL");
        let mut handle = abi::Handle::NULL;
        // SAFETY: valid strings and handle.
        let status = unsafe {
            orion_capi::orion_link_connect(socket.as_ptr(), name.as_ptr(), 5_000, &mut handle)
        };
        assert_eq!(status, abi::Status::OK);
        let provider = CString::new(PROVIDER).expect("no NUL");
        let watch = |req: &mut u32| {
            // SAFETY: a valid subject id, no prefix, a valid request id.
            unsafe {
                orion_capi::orion_link_status_watch(
                    handle,
                    ORION_SUBJECT_PROVIDER,
                    provider.as_ptr(),
                    std::ptr::null(),
                    req,
                )
            }
        };
        let mut req = 0;
        assert_eq!(watch(&mut req), abi::Status::OK);
        // A second watch on the session is refused.
        let mut second = 0;
        assert_eq!(watch(&mut second), abi::Status::OK);

        // Bootstrap: the current value, then the end of the answer. The buffer starts empty, so
        // the first entry asks for more room.
        let mut buf = Vec::new();
        let mut events = Vec::new();
        loop {
            let (event, bytes) = next(handle, &mut buf);
            let done = event.kind == ORION_EVENT_ANSWERED && event.req == req;
            events.push((event, bytes));
            if done {
                break;
            }
        }
        let refused = events
            .iter()
            .any(|(e, _)| e.kind == ORION_EVENT_REQUEST_ERROR && e.req == second);
        let (entry, bytes) = events
            .iter()
            .find(|(e, _)| e.kind == ORION_EVENT_STATUS)
            .expect("the bootstrap has the entry");
        assert_eq!(entry.flags & ORION_EVENT_BOOTSTRAP, ORION_EVENT_BOOTSTRAP);
        assert_eq!(entry.subject_kind, ORION_SUBJECT_PROVIDER);
        assert_eq!(entry.value_type, ORION_VALUE_INT);
        assert_eq!(entry.int_value, -1);
        assert_eq!(&bytes[..], format!("{PROVIDER}count").as_bytes());
        (handle, buf, req, refused)
    })
    .await
    .expect("the watcher runs");
    let (handle, mut buf, req, mut refused) = watcher;

    // Changes: a string value.
    publisher.send(value("mode", TypedConfigValue::String("tracking".into())));
    let changed = tokio::task::spawn_blocking(move || {
        loop {
            let (event, bytes) = next(handle, &mut buf);
            if event.kind == ORION_EVENT_REQUEST_ERROR {
                refused = true;
                continue;
            }
            if event.kind == ORION_EVENT_STATUS && event.req == req {
                assert_eq!(event.flags & ORION_EVENT_BOOTSTRAP, 0);
                assert_eq!(event.value_type, ORION_VALUE_STRING);
                let data = &bytes[(event.subject_len + event.key_len) as usize..];
                assert_eq!(data, b"tracking");
                break;
            }
        }
        assert_eq!(orion_capi::orion_close(handle), abi::Status::OK);
        assert_eq!(orion_capi::orion_close(handle), abi::Status::STALE_HANDLE);
        refused
    })
    .await
    .expect("the watcher runs");
    assert!(changed, "the second watch is refused");
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocking_client_waiting_past_the_heartbeat_keeps_its_session() {
    let app = test_app("capi-alive");
    let path = socket_path("capi-alive");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let publisher = publisher(&app, &path).await;
    let socket = path.clone();
    let client = tokio::task::spawn_blocking(move || {
        let mut client =
            LinkClient::connect(&socket, "waiter", Duration::from_secs(5)).expect("welcome");
        let req = client
            .status_query(orion::control_plane::StatusQuery::all())
            .expect("query");
        loop {
            match client.next_event(Some(WAIT)).expect("session").cloned() {
                Some(Event::Answered { req: r, .. }) if r == req => break,
                Some(_) => {
                    client.pop_event();
                }
                None => panic!("no answer"),
            }
        }
        client.pop_event();
        // Several heartbeat timeouts with nothing to read: the client pings while it waits.
        let quiet = Duration::from_millis(u64::from(HEARTBEAT_MS) * 3 * 3);
        assert!(client.next_event(Some(quiet)).expect("still up").is_none());
        client
    })
    .await
    .expect("the client runs");
    assert_eq!(
        app.link_status()[0].device_timeouts,
        0,
        "a session timed out"
    );
    drop(client);
    drop(publisher);
    gateway.shutdown().await;
}
