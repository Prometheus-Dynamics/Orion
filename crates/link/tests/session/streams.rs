//! Streams (revision 2) on the host session: provider declarations and batches, subscriptions,
//! data split to the frame size, and refusals.

use orion_control_plane as cp;
use orion_core::ProviderId;
use orion_link::host::{ClientRequest, HostConfig, HostEvent};
use orion_link::message::{
    ErrorCode, Message, Roles, StreamData, StreamDeclare, StreamPublish, StreamSubscribe,
    max_chunk_data,
};

use crate::client::{Client, serving};

fn streaming() -> HostConfig {
    let mut config = serving();
    config.serve_streams = true;
    config
}

fn series() -> Vec<cp::SeriesDecl> {
    vec![cp::SeriesDecl::new(
        "imu.accel_x",
        cp::SeriesKind::F32,
        10_000,
    )]
}

fn chunk(samples: u64) -> cp::SampleChunk {
    let mut builder = cp::SampleChunkBuilder::new(0, cp::SeriesKind::F32);
    for i in 0..samples {
        builder.push(1_000_000 + i * 10_000, i as f64);
    }
    builder.take()
}

#[test]
fn a_provider_session_declares_and_publishes_and_gets_its_demand() {
    let mut provider = Client::new(streaming());
    provider.hello(Roles::PROVIDER, 2, 4096);
    provider.received();
    provider.events();
    provider.send(&Message::StreamDeclare(StreamDeclare {
        req: 1,
        id: "imu".into(),
        series: series(),
    }));
    provider.send(&Message::StreamPublish(StreamPublish {
        id: "imu".into(),
        chunks: vec![chunk(3)],
    }));
    assert_eq!(
        provider.events(),
        vec![
            HostEvent::StreamDeclare {
                device_name: "console".into(),
                req: 1,
                id: "imu".into(),
                series: series(),
            },
            HostEvent::StreamPublish {
                device_name: "console".into(),
                id: "imu".into(),
                chunks: vec![chunk(3)],
            },
        ]
    );
    provider.host.send_stream_demand("imu", 1, &[0]).unwrap();
    assert!(matches!(
        &provider.received()[..],
        [Message::StreamDemand(demand)] if demand.id == "imu" && demand.series == [0]
    ));
    // A provider session sends no client answers.
    assert!(
        provider
            .host
            .send_stream_reset(1, cp::StreamResetReason::Removed)
            .is_err()
    );
}

#[test]
fn a_subscription_is_reported_and_large_data_goes_in_parts() {
    let mut client = Client::new(streaming());
    client.hello(Roles::CLIENT, 2, 512);
    client.received();
    client.events();
    let subscription = cp::StreamSubscription::all(cp::StreamRef {
        owner: cp::StatusSubject::Provider(ProviderId::new("provider.imu-board")),
        id: "imu".into(),
    });
    client.send(&Message::StreamSubscribe(Box::new(StreamSubscribe {
        req: 4,
        subscription: subscription.clone(),
    })));
    assert_eq!(
        client.events(),
        vec![HostEvent::ClientRequest {
            device_name: "console".into(),
            request: ClientRequest::StreamSubscribe {
                req: 4,
                subscription: Box::new(subscription),
            },
        }]
    );

    // Split to the frame first, as the gateway does, then into parts by chunk.
    let whole = chunk(400);
    let pieces = whole
        .split(cp::SeriesKind::F32, max_chunk_data(512))
        .expect("splits");
    assert!(pieces.len() > 1);
    let data = StreamData {
        req: 4,
        cursor: cp::StreamCursor { epoch: 7, seq: 9 },
        prev_seq: 8,
        backfill: true,
        more: false,
        chunks: pieces,
    };
    client.host.send_stream_data(&data).expect("queued");
    let parts: Vec<StreamData> = client
        .received()
        .into_iter()
        .map(|message| match message {
            Message::StreamData(part) => part,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert!(parts.len() > 1);
    let (last, rest) = parts.split_last().unwrap();
    assert!(rest.iter().all(|part| part.more) && !last.more);
    assert!(
        parts
            .iter()
            .all(|part| part.req == 4 && part.cursor.seq == 9 && part.backfill)
    );
    let samples: Vec<(u64, f64)> = parts
        .iter()
        .flat_map(|part| &part.chunks)
        .flat_map(|chunk| chunk.samples(cp::SeriesKind::F32).unwrap())
        .collect();
    assert_eq!(samples, whole.samples(cp::SeriesKind::F32).unwrap());

    // An unsplit chunk larger than a frame is refused.
    let too_big = StreamData {
        chunks: vec![whole],
        ..data
    };
    assert!(client.host.send_stream_data(&too_big).is_err());
}

#[test]
fn streams_are_refused_where_they_are_not_served() {
    // A link without streams refuses declarations and subscriptions...
    let mut provider = Client::new(serving());
    provider.hello(Roles::PROVIDER.union(Roles::CLIENT), 2, 4096);
    provider.received();
    provider.events();
    provider.send(&Message::StreamDeclare(StreamDeclare {
        req: 1,
        id: "imu".into(),
        series: series(),
    }));
    provider.send(&Message::StreamPublish(StreamPublish {
        id: "imu".into(),
        chunks: vec![chunk(1)],
    }));
    provider.send(&Message::StreamSubscribe(Box::new(StreamSubscribe {
        req: 2,
        subscription: cp::StreamSubscription::all(cp::StreamRef {
            owner: cp::StatusSubject::Node("node-a".into()),
            id: "board".into(),
        }),
    })));
    let refused: Vec<(u32, ErrorCode)> = provider
        .received()
        .into_iter()
        .filter_map(|message| match message {
            Message::RequestError(error) => Some((error.req, error.code)),
            _ => None,
        })
        .collect();
    assert_eq!(
        refused,
        vec![(1, ErrorCode::UNSUPPORTED), (2, ErrorCode::UNSUPPORTED)]
    );
    assert!(provider.events().is_empty());

    // ...and a revision-1 session's stream kinds are ignored as unknown.
    let mut old = Client::new(streaming());
    old.hello(Roles::PROVIDER, 1, 4096);
    old.received();
    old.events();
    old.send(&Message::StreamDeclare(StreamDeclare {
        req: 3,
        id: "imu".into(),
        series: series(),
    }));
    assert!(old.received().is_empty() && old.events().is_empty());

    // A client-only session cannot declare.
    let mut client = Client::new(streaming());
    client.hello(Roles::CLIENT, 2, 4096);
    client.received();
    client.events();
    client.send(&Message::StreamDeclare(StreamDeclare {
        req: 5,
        id: "imu".into(),
        series: series(),
    }));
    assert!(matches!(
        &client.received()[..],
        [Message::RequestError(error)] if error.req == 5 && error.code == ErrorCode::UNSUPPORTED
    ));
}
