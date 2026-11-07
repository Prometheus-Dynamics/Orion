//! liborion's actions, records and stream subscriptions through the C surface against a node's
//! `unix:` link: an action run to its final state, a record query, a refused argument, and a
//! stream's catalog, backfill and live batches.

use super::capi::{PROVIDER, next, publisher};
use super::serial::start;
use super::unix::{socket_path, unix_link};
use super::unix_client::{RawClient, node_app_with_blink};
use super::unix_streams::{chunk, now_us};
use super::*;
use orion::control_plane::{HistoryPolicy, SeriesDecl, SeriesKind};
use orion_capi::{
    ORION_ACTION_SUCCEEDED, ORION_EVENT_ACTION, ORION_EVENT_ANSWERED, ORION_EVENT_BOOTSTRAP,
    ORION_EVENT_LIVE, ORION_EVENT_RECORD, ORION_EVENT_STREAM_SAMPLES, ORION_EVENT_STREAM_SERIES,
    ORION_SUBJECT_NODE, ORION_SUBJECT_PROVIDER, ORION_VALUE_F64,
};
use orion_link::message::{Message, StreamDeclare, StreamPublish};
use std::ffi::CString;

fn connect(path: &std::path::Path, name: &str) -> abi::Handle {
    let socket = CString::new(path.to_str().expect("utf-8 path")).expect("no NUL");
    let name = CString::new(name).expect("no NUL");
    let mut handle = abi::Handle::NULL;
    // SAFETY: valid strings and handle.
    let status = unsafe {
        orion_capi::orion_link_connect(socket.as_ptr(), name.as_ptr(), 5_000, &mut handle)
    };
    assert_eq!(status, abi::Status::OK);
    handle
}

fn c(text: &str) -> CString {
    CString::new(text).expect("no NUL")
}

fn data(event: &orion_capi::OrionEvent, bytes: &[u8]) -> serde_json::Value {
    let at = (event.subject_len + event.key_len) as usize;
    serde_json::from_slice(&bytes[at..]).expect("the data is JSON")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_c_client_runs_an_action_and_queries_records_through_liborion() {
    let app = node_app_with_blink("capi-actions");
    let path = socket_path("capi-actions");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let publisher = publisher(&app, &path).await;
    let socket = path.clone();
    tokio::task::spawn_blocking(move || {
        let handle = connect(&socket, "c-console");
        let (node, id, name) = (c(NODE), c("blink-c"), c("blink"));
        let run = |args: &CString, req: &mut u32| {
            // SAFETY: valid strings and request id.
            unsafe {
                orion_capi::orion_link_action_run(
                    handle,
                    ORION_SUBJECT_NODE,
                    node.as_ptr(),
                    id.as_ptr(),
                    name.as_ptr(),
                    args.as_ptr(),
                    0,
                    req,
                )
            }
        };
        let mut req = 0;
        // Arguments are a JSON object of scalars.
        assert_eq!(run(&c("[1]"), &mut req), abi::Status::INVALID_ARGUMENT);
        assert_eq!(
            run(&c(r#"{"times": 3, "fast": true}"#), &mut req),
            abi::Status::OK
        );
        let mut buf = Vec::new();
        let result = loop {
            let (event, bytes) = next(handle, &mut buf);
            if event.kind == ORION_EVENT_ACTION
                && event.req == req
                && event.uint_value == u64::from(ORION_ACTION_SUCCEEDED)
            {
                break (event, bytes);
            }
        };
        let (event, bytes) = result;
        assert_eq!(event.subject_kind, ORION_SUBJECT_NODE);
        let at = event.subject_len as usize;
        assert_eq!(&bytes[..at], NODE.as_bytes());
        assert_eq!(&bytes[at..at + event.key_len as usize], b"blink-c");
        let json = data(&event, &bytes);
        assert_eq!(json["action_id"], "blink-c");
        assert!(json["output"]["blinks"].to_string().contains('3'), "{json}");

        // The provider's record, then the end of the answer.
        let provider = c(PROVIDER);
        let mut records = 0;
        // SAFETY: a valid record id, no node filter, a valid request id.
        let status = unsafe {
            orion_capi::orion_link_record_query(
                handle,
                ORION_SUBJECT_PROVIDER,
                provider.as_ptr(),
                std::ptr::null(),
                &mut records,
            )
        };
        assert_eq!(status, abi::Status::OK);
        let mut found = false;
        loop {
            let (event, bytes) = next(handle, &mut buf);
            if event.req != records {
                continue;
            }
            if event.kind == ORION_EVENT_RECORD {
                assert_eq!(event.subject_kind, ORION_SUBJECT_PROVIDER);
                assert_eq!(&bytes[..event.subject_len as usize], PROVIDER.as_bytes());
                assert_eq!(data(&event, &bytes)["provider_id"], PROVIDER);
                found = true;
            }
            if event.kind == ORION_EVENT_ANSWERED {
                break;
            }
        }
        assert!(found, "the query answers with the provider");
        assert_eq!(orion_capi::orion_close(handle), abi::Status::OK);
    })
    .await
    .expect("the C client runs");
    drop(publisher);
    gateway.shutdown().await;
}

/// The samples of a `ORION_EVENT_STREAM_SAMPLES` event.
fn samples(event: &orion_capi::OrionEvent, bytes: &[u8]) -> Vec<(u64, f64)> {
    let at = (event.subject_len + event.key_len) as usize;
    let data = &bytes[at..at + event.data_len as usize];
    assert_eq!(data.len() % 16, 0);
    data.as_chunks::<16>()
        .0
        .iter()
        .map(|pair| {
            let (t, v) = pair.split_at(8);
            (
                u64::from_ne_bytes(t.try_into().expect("8 bytes")),
                f64::from_ne_bytes(v.try_into().expect("8 bytes")),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_c_client_follows_a_stream_through_liborion() {
    let app = test_app("capi-stream");
    let path = socket_path("capi-stream");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let mut publisher: RawClient = publisher(&app, &path).await;
    publisher.send(Message::StreamDeclare(StreamDeclare {
        req: 1,
        id: "imu".into(),
        series: vec![
            SeriesDecl::new("imu.accel_x", SeriesKind::F32, 1_000)
                .with_history(HistoryPolicy::full(60_000)),
        ],
    }));
    assert!(matches!(publisher.next().await, Message::StreamDemand(_)));
    let t0 = now_us() - 2_000_000;
    publisher.send(Message::StreamPublish(StreamPublish {
        id: "imu".into(),
        chunks: vec![chunk(t0, 100)],
    }));

    let socket = path.clone();
    let (handle, req, mut buf) = tokio::task::spawn_blocking(move || {
        let handle = connect(&socket, "c-plot");
        let (owner, id, series) = (c(PROVIDER), c("imu"), c("imu.*"));
        let mut req = 0;
        // The history is published before the subscription; wait until the node has it.
        let mut buf = Vec::new();
        let mut backfill = Vec::new();
        for _attempt in 0..50 {
            // SAFETY: valid strings and request id.
            let status = unsafe {
                orion_capi::orion_link_stream_subscribe(
                    handle,
                    ORION_SUBJECT_PROVIDER,
                    owner.as_ptr(),
                    id.as_ptr(),
                    series.as_ptr(),
                    0,
                    60_000,
                    &mut req,
                )
            };
            assert_eq!(status, abi::Status::OK);
            let mut catalog = 0;
            loop {
                let (event, bytes) = next(handle, &mut buf);
                if event.req != req {
                    continue;
                }
                match event.kind {
                    ORION_EVENT_STREAM_SERIES => {
                        assert_eq!(event.int_value, 0);
                        assert_eq!(event.value_type, ORION_VALUE_F64);
                        assert_eq!(event.flags & ORION_EVENT_LIVE, ORION_EVENT_LIVE);
                        assert_eq!(event.ttl_ms, 1_000, "the period, in microseconds");
                        let at = event.subject_len as usize;
                        assert_eq!(&bytes[at..at + event.key_len as usize], b"imu.accel_x");
                        catalog += 1;
                    }
                    ORION_EVENT_STREAM_SAMPLES => {
                        assert_eq!(event.flags & ORION_EVENT_BOOTSTRAP, ORION_EVENT_BOOTSTRAP);
                        backfill.extend(samples(&event, &bytes));
                    }
                    ORION_EVENT_ANSWERED => break,
                    _ => {}
                }
            }
            assert_eq!(catalog, 1);
            if backfill.len() == 100 {
                break;
            }
            assert_eq!(orion_capi::orion_link_end(handle, req), abi::Status::OK);
            backfill.clear();
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(backfill.len(), 100, "the whole history");
        assert_eq!(backfill[0], (t0, 0.0));
        assert_eq!(backfill[99], (t0 + 99_000, 99.0));
        (handle, req, buf)
    })
    .await
    .expect("the C client runs");

    // A live batch.
    publisher.send(Message::StreamPublish(StreamPublish {
        id: "imu".into(),
        chunks: vec![chunk(t0 + 1_000_000, 3)],
    }));
    tokio::task::spawn_blocking(move || {
        let live = loop {
            let (event, bytes) = next(handle, &mut buf);
            if event.req == req && event.kind == ORION_EVENT_STREAM_SAMPLES {
                assert_eq!(event.flags & ORION_EVENT_BOOTSTRAP, 0);
                break samples(&event, &bytes);
            }
        };
        assert_eq!(
            live,
            vec![
                (t0 + 1_000_000, 0.0),
                (t0 + 1_001_000, 1.0),
                (t0 + 1_002_000, 2.0)
            ]
        );
        assert_eq!(orion_capi::orion_close(handle), abi::Status::OK);
    })
    .await
    .expect("the C client runs");
    gateway.shutdown().await;
}
