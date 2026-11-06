//! The client role (revision 1) on the host session: admission, request events, answers split to
//! the frame size, and refusals.

use orion_control_plane as cp;
use orion_core::ResourceId;
use orion_link::host::{ActionSendError, ClientRequest, HostConfig, HostEvent, HostSession};
use orion_link::message::{
    ErrorCode, Hello, Message, RejectReason, Roles, StatusChanges, StatusRequest, TypedConfigValue,
    kind,
};
use orion_link::{LINK_INTERFACE_REVISION, Stream, StreamDecoder, StreamEncoder};

use crate::sim::host_config;

struct Client {
    host: HostSession<Stream>,
    rx: StreamDecoder<4096>,
    seq: u16,
    now: u64,
}

impl Client {
    fn new(config: HostConfig) -> Self {
        Self {
            host: HostSession::stream(config),
            rx: StreamDecoder::new(),
            seq: 0,
            now: 0,
        }
    }

    fn send(&mut self, message: &Message) {
        self.seq = self.seq.wrapping_add(1);
        let mut buf = vec![0u8; 4096];
        let len = message.encode(self.seq, &mut buf).expect("fits");
        let bytes: Vec<u8> = StreamEncoder::for_frame(&buf[..len]).collect();
        self.host.receive(&bytes);
        self.now += 1;
        self.host.poll(self.now);
    }

    fn hello(&mut self, roles: Roles, revision: u16, max_frame: u32) {
        self.send(&Message::Hello(Hello {
            device_name: "console".into(),
            roles,
            max_frame,
            revision,
        }));
    }

    /// Messages the host sent since the last call.
    fn received(&mut self) -> Vec<Message> {
        let mut out = Vec::new();
        while let Some(bytes) = self.host.transmit() {
            for byte in bytes {
                if let Ok(Some(frame)) = self.rx.push(byte) {
                    out.push(Message::decode(&frame).expect("host frames decode"));
                }
            }
        }
        out
    }

    fn events(&mut self) -> Vec<HostEvent> {
        std::iter::from_fn(|| self.host.next_event()).collect()
    }
}

fn serving() -> HostConfig {
    let mut config = host_config();
    config.serve_clients = true;
    config
}

fn entry(index: usize) -> cp::StatusEntry {
    cp::StatusEntry {
        subject: cp::StatusSubject::Resource(ResourceId::new("imu-board.imu-0")),
        key: format!("sample_{index:03}"),
        value: TypedConfigValue::String("x".repeat(40)),
        ttl_ms: 1_000,
        published_at_ms: 1,
    }
}

#[test]
fn a_client_session_is_welcomed_gets_no_leases_and_reports_requests() {
    let mut client = Client::new(serving());
    client.hello(Roles::CLIENT, 1, 4096);
    let welcome = client.received();
    assert!(
        matches!(&welcome[..], [Message::Welcome(w)] if w.revision == LINK_INTERFACE_REVISION),
        "a client-only session gets a Welcome and no Leases: {welcome:?}"
    );
    assert_eq!(client.host.roles(), Some(Roles::CLIENT));
    assert!(client.events().iter().any(|event| matches!(
        event,
        HostEvent::DeviceConnected { roles, .. } if *roles == Roles::CLIENT
    )));

    let filter = cp::StatusQuery::all().with_key_prefix("sample_");
    client.send(&Message::StatusWatch(StatusRequest {
        req: 9,
        filter: filter.clone(),
    }));
    assert_eq!(
        client.events(),
        vec![HostEvent::ClientRequest {
            device_name: "console".into(),
            request: ClientRequest::StatusWatch { req: 9, filter },
        }]
    );
    client.send(&Message::RequestEnd { req: 9 });
    assert!(matches!(
        &client.events()[..],
        [HostEvent::ClientRequest {
            request: ClientRequest::End { req: 9 },
            ..
        }]
    ));
}

#[test]
fn answers_larger_than_a_frame_go_in_parts() {
    let mut client = Client::new(serving());
    client.hello(Roles::CLIENT, 1, 512);
    client.received();
    let entries: Vec<cp::StatusEntry> = (0..40).map(entry).collect();
    client
        .host
        .send_status_changes(3, true, &entries, &[])
        .expect("queued");
    let parts: Vec<StatusChanges> = client
        .received()
        .into_iter()
        .map(|message| match message {
            Message::StatusChanges(part) => part,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert!(parts.len() > 1, "40 entries do not fit one 512-byte frame");
    assert!(parts.iter().all(|part| part.req == 3 && part.bootstrap));
    let (last, rest) = parts.split_last().expect("parts");
    assert!(rest.iter().all(|part| part.more) && !last.more);
    let joined: Vec<cp::StatusEntry> = parts.into_iter().flat_map(|part| part.updated).collect();
    assert_eq!(joined, entries);

    // An empty answer is still one part.
    client.host.send_status_changes(4, true, &[], &[]).unwrap();
    assert!(matches!(
        &client.received()[..],
        [Message::StatusChanges(StatusChanges { req: 4, more: false, updated, .. })] if updated.is_empty()
    ));

    // A record larger than the frame cannot be sent.
    let mut huge = entry(0);
    huge.value = TypedConfigValue::String("x".repeat(600));
    assert!(matches!(
        client.host.send_status_changes(5, false, &[huge], &[]),
        Err(ActionSendError::TooLarge { max_frame: 512, .. })
    ));
}

#[test]
fn client_requests_are_refused_where_the_role_is_not_served() {
    // A link that does not serve clients rejects a client-only Hello...
    let mut client = Client::new(host_config());
    client.hello(Roles::CLIENT, 1, 4096);
    assert_eq!(
        client.received(),
        vec![Message::Reject(RejectReason::UnsupportedRoles)]
    );

    // ...and refuses the requests of a provider session that also asked for the role.
    client.hello(Roles::PROVIDER.union(Roles::CLIENT), 1, 4096);
    client.received();
    assert_eq!(client.host.roles(), Some(Roles::PROVIDER));
    client.send(&Message::StatusQuery(StatusRequest {
        req: 11,
        filter: cp::StatusQuery::all(),
    }));
    assert!(matches!(
        &client.received()[..],
        [Message::RequestError(error)] if error.req == 11 && error.code == ErrorCode::UNSUPPORTED
    ));

    // A revision-0 session cannot take the role, and its client kinds are ignored as unknown.
    let mut client = Client::new(serving());
    client.hello(Roles::PROVIDER.union(Roles::CLIENT), 0, 4096);
    client.received();
    client.events();
    assert_eq!(client.host.roles(), Some(Roles::PROVIDER));
    client.send(&Message::StatusQuery(StatusRequest {
        req: 12,
        filter: cp::StatusQuery::all(),
    }));
    assert!(client.received().is_empty());
    assert!(client.events().is_empty());
}

#[test]
fn a_malformed_request_body_is_answered_invalid() {
    let mut client = Client::new(serving());
    client.hello(Roles::CLIENT, 1, 4096);
    client.received();
    client.events();
    // The request id (13), then a filter that is not one.
    let mut buf = [0u8; 64];
    let len = orion_link::frame::encode(
        orion_link::frame::FrameHeader::new(kind::STATUS_QUERY, 2),
        &[13, 7, 7, 7],
        &mut buf,
    )
    .expect("fits");
    let bytes: Vec<u8> = StreamEncoder::for_frame(&buf[..len]).collect();
    client.host.receive(&bytes);
    assert!(matches!(
        &client.received()[..],
        [Message::RequestError(error)] if error.req == 13 && error.code == ErrorCode::INVALID
    ));
    assert!(client.events().is_empty());

    // A long reason is cut at a character boundary.
    client
        .host
        .send_request_error(14, ErrorCode::INTERNAL, &"é".repeat(300))
        .unwrap();
    assert!(matches!(
        &client.received()[..],
        [Message::RequestError(error)] if error.reason.len() <= orion_link::message::MAX_REASON_BYTES
            && error.reason.chars().all(|c| c == 'é')
    ));
}
