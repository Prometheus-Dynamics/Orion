use super::*;
use crate::records::ResourceRecord;

#[derive(Debug, PartialEq, Eq)]
struct FrameLeaseEndpoint {
    socket_path: String,
}

impl CustomEndpointScheme for FrameLeaseEndpoint {
    const SCHEME: &'static str = "styx-frame-lease+unix";

    fn from_payload(payload: &str) -> Option<Self> {
        Some(Self {
            socket_path: payload.to_owned(),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SerialEndpoint {
    device: String,
}

impl CustomEndpointScheme for SerialEndpoint {
    const SCHEME: &'static str = "serial";
    const TYPE_NAME: &'static str = "serial port";

    fn from_payload(payload: &str) -> Option<Self> {
        payload.starts_with("tty").then(|| Self {
            device: payload.to_owned(),
        })
    }
}

#[test]
fn parse_accepts_custom_scheme() {
    let endpoint = ResourceEndpoint::parse("styx-frame-lease+unix:///run/helios/cam0.sock")
        .expect("custom scheme should parse");

    let custom = endpoint.as_custom().expect("should be custom");
    assert!(endpoint.is_custom());
    assert_eq!(endpoint.scheme(), "styx-frame-lease+unix");
    assert_eq!(endpoint.payload(), "/run/helios/cam0.sock");
    assert_eq!(custom.base_scheme(), "styx-frame-lease");
    assert_eq!(custom.transport_suffix(), Some("unix"));
}

#[test]
fn custom_scheme_without_suffix_has_no_transport() {
    let endpoint = ResourceEndpoint::parse("v4l2.dev://video0").expect("should parse");
    let custom = endpoint.as_custom().expect("should be custom");
    assert_eq!(custom.base_scheme(), "v4l2.dev");
    assert_eq!(custom.transport_suffix(), None);
}

#[test]
fn parse_lowercases_schemes() {
    let endpoint = ResourceEndpoint::parse("Styx-Frame-Lease+UNIX://cam0").expect("should parse");
    assert_eq!(endpoint.scheme(), "styx-frame-lease+unix");
    assert_eq!(endpoint.to_string(), "styx-frame-lease+unix://cam0");

    let shm = ResourceEndpoint::parse("SHM://graph").expect("builtin should parse");
    assert!(matches!(shm, ResourceEndpoint::SharedMemory(ref e) if e.name == "graph"));
}

#[test]
fn parse_rejects_malformed_endpoints() {
    for (input, expected) in [
        ("no-scheme", "missing"),
        ("://payload", "missing"),
        ("custom://", "empty"),
        ("9custom://x", "invalid"),
        ("-custom://x", "invalid"),
        ("cus tom://x", "invalid"),
        ("cus_tom://x", "invalid"),
        ("cüstom://x", "invalid"),
    ] {
        let error = ResourceEndpoint::parse(input).expect_err(input);
        let kind = match error {
            ResourceEndpointError::MissingScheme { .. } => "missing",
            ResourceEndpointError::EmptyPayload { .. } => "empty",
            ResourceEndpointError::InvalidScheme { .. } => "invalid",
            other => panic!("unexpected error for `{input}`: {other}"),
        };
        assert_eq!(kind, expected, "input `{input}`");
    }
}

#[test]
fn scheme_validation_follows_rfc3986() {
    assert!(is_valid_endpoint_scheme("a"));
    assert!(is_valid_endpoint_scheme("svn+ssh"));
    assert!(is_valid_endpoint_scheme("x-1.2+unix"));
    assert!(!is_valid_endpoint_scheme(""));
    assert!(!is_valid_endpoint_scheme("1a"));
    assert!(!is_valid_endpoint_scheme("+a"));
    assert!(!is_valid_endpoint_scheme("a/b"));
}

#[test]
fn endpoints_round_trip_through_display() {
    for input in [
        "shm://vision-graph",
        "ipc://graph-control",
        "unix:///tmp/orion.sock",
        "tcp://127.0.0.1:1234",
        "http://localhost:8080/graph",
        "https://example.test/graph?x=1",
        "styx-frame-lease+unix:///run/helios/cam0.sock",
        "serial://ttyUSB0",
    ] {
        let endpoint = ResourceEndpoint::parse(input).expect(input);
        assert_eq!(endpoint.to_string(), input);
        assert_eq!(String::from(&endpoint), input);
        let reparsed: ResourceEndpoint = endpoint.to_string().parse().expect(input);
        assert_eq!(reparsed, endpoint);
    }
}

#[test]
fn builtin_scheme_accessors() {
    let cases = [
        ("shm://a", "shm", "a"),
        ("ipc://b", "ipc", "b"),
        ("unix:///c", "unix", "/c"),
        ("tcp://d:1", "tcp", "d:1"),
        ("http://e/f", "http", "e/f"),
        ("https://g/h", "https", "g/h"),
    ];
    for (input, scheme, payload) in cases {
        let endpoint = ResourceEndpoint::parse(input).expect(input);
        assert_eq!(endpoint.scheme(), scheme);
        assert_eq!(endpoint.payload(), payload);
        assert!(!endpoint.is_custom());
    }
}

#[test]
fn custom_endpoint_new_validates() {
    let endpoint = CustomEndpoint::new("Serial", "ttyUSB0").expect("valid");
    assert_eq!(endpoint.scheme(), "serial");
    assert_eq!(endpoint.to_string(), "serial://ttyUSB0");
    assert_eq!(
        ResourceEndpoint::parse(endpoint.to_string()).expect("reparse"),
        ResourceEndpoint::from(endpoint)
    );

    assert!(matches!(
        CustomEndpoint::new("bad scheme", "x"),
        Err(ResourceEndpointError::InvalidScheme { .. })
    ));
    assert!(matches!(
        CustomEndpoint::new("serial", ""),
        Err(ResourceEndpointError::EmptyPayload { .. })
    ));
    assert!(matches!(
        CustomEndpoint::new("UNIX", "/tmp/x"),
        Err(ResourceEndpointError::ReservedScheme { scheme, .. }) if scheme == "unix"
    ));
}

#[test]
fn typed_custom_endpoint_is_resolved_from_record() {
    let resource = ResourceRecord::builder("resource.cam0", "camera.frames", "provider.helios")
        .endpoint("shm://cam0-frames")
        .endpoint("serial://not-a-tty")
        .endpoint(FrameLeaseEndpoint::endpoint_string("/run/helios/cam0.sock"))
        .build();

    let lease = resource
        .endpoint::<FrameLeaseEndpoint>()
        .expect("frame lease endpoint should resolve");
    assert_eq!(lease.socket_path, "/run/helios/cam0.sock");

    let any_custom = resource
        .endpoint::<CustomEndpoint>()
        .expect("custom endpoint should resolve");
    assert_eq!(any_custom.scheme, "serial");

    let error = resource
        .endpoint::<SerialEndpoint>()
        .expect_err("payload rejected by from_payload");
    assert!(matches!(
        error,
        ResourceEndpointError::EndpointTypeNotFound {
            endpoint_type: "serial port",
            ..
        }
    ));
}

#[test]
fn typed_custom_endpoint_does_not_match_other_schemes() {
    let endpoint = ResourceEndpoint::parse("styx-frame-lease+tcp://10.0.0.1:9000").expect("parse");
    assert_eq!(FrameLeaseEndpoint::from_endpoint(&endpoint), None);
    assert_eq!(
        <FrameLeaseEndpoint as TypedResourceEndpoint>::TYPE_NAME,
        "styx-frame-lease+unix"
    );
    let serial = ResourceEndpoint::parse("serial://ttyS0").expect("parse");
    assert_eq!(
        SerialEndpoint::from_endpoint(&serial),
        Some(SerialEndpoint {
            device: "ttyS0".to_owned()
        })
    );
}

#[test]
fn record_with_custom_endpoints_round_trips_through_serde_and_rkyv() {
    let resource = ResourceRecord::builder("resource.cam0", "camera.frames", "provider.helios")
        .endpoint("styx-frame-lease+unix:///run/helios/cam0.sock")
        .endpoint("shm://cam0-frames")
        .build();

    let json = serde_json::to_string(&resource).expect("serde encode");
    let from_json: ResourceRecord = serde_json::from_str(&json).expect("serde decode");
    assert_eq!(from_json, resource);

    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&resource).expect("rkyv encode");
    let from_rkyv =
        rkyv::from_bytes::<ResourceRecord, rkyv::rancor::Error>(&bytes).expect("rkyv decode");
    assert_eq!(from_rkyv, resource);
    assert_eq!(
        from_rkyv
            .endpoint::<FrameLeaseEndpoint>()
            .expect("typed endpoint after decode")
            .socket_path,
        "/run/helios/cam0.sock"
    );
}

#[cfg(feature = "std")]
#[test]
fn shared_memory_endpoint_reads_from_custom_root() {
    let root = std::env::temp_dir().join(format!("orion-shm-test-{}", std::process::id()));
    std::fs::create_dir_all(&root).expect("temp root should be creatable");
    let endpoint = SharedMemoryEndpoint {
        name: "graph-payload".to_owned(),
    };
    let path = endpoint.path_in(&root);
    std::fs::write(&path, b"{\"graph\":3}").expect("payload should be writable");

    let content = endpoint
        .read_string_from(&root)
        .expect("payload should be readable");
    assert_eq!(content, "{\"graph\":3}");

    std::fs::remove_file(&path).expect("payload should be removable");
    std::fs::remove_dir(&root).expect("temp root should be removable");
}

#[cfg(feature = "std")]
#[test]
fn unix_endpoint_reads_file_payload() {
    let path = std::env::temp_dir().join(format!(
        "orion-unix-endpoint-test-{}.txt",
        std::process::id()
    ));
    std::fs::write(&path, b"unix-endpoint-payload").expect("payload should be writable");

    let endpoint = UnixEndpoint {
        path: path.display().to_string(),
    };
    let content = endpoint
        .read_string()
        .expect("payload should be readable through unix endpoint");
    assert_eq!(content, "unix-endpoint-payload");

    std::fs::remove_file(&path).expect("payload should be removable");
}
