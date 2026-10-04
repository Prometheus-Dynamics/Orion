use crate::NodeError;
use crate::link_gateway::{CanLinkConfig, LinkConfig, LinkTransport, SerialLinkConfig};
use std::collections::BTreeSet;
use std::path::PathBuf;

fn parse(raw: &str) -> Vec<LinkConfig> {
    LinkConfig::parse_list(raw).expect("link list should parse")
}

fn error(raw: &str) -> String {
    match LinkConfig::parse_list(raw) {
        Err(NodeError::Config(message)) => message,
        other => panic!("expected a config error for `{raw}`, got {other:?}"),
    }
}

#[test]
fn serial_defaults() {
    let links = parse("serial:/dev/ttyAMA0");
    assert_eq!(
        links,
        vec![LinkConfig {
            name: "serial:/dev/ttyAMA0".into(),
            transport: LinkTransport::Serial(SerialLinkConfig {
                path: PathBuf::from("/dev/ttyAMA0"),
                baud: 115_200,
            }),
            allowed_devices: None,
            heartbeat_ms: 1_000,
            missed_heartbeats: 3,
            max_frame: 4_096,
        }]
    );
}

#[test]
fn serial_with_every_key() {
    let links = parse(
        " serial:/dev/ttyUSB0?baud=921600&allow=imu-board, motor-a&heartbeat_ms=250&missed_heartbeats=5&max_frame=512 ",
    );
    let link = &links[0];
    assert_eq!(
        link.transport,
        LinkTransport::Serial(SerialLinkConfig {
            path: PathBuf::from("/dev/ttyUSB0"),
            baud: 921_600,
        })
    );
    assert_eq!(
        link.allowed_devices,
        Some(BTreeSet::from([
            "imu-board".to_owned(),
            "motor-a".to_owned()
        ]))
    );
    assert_eq!(
        (link.heartbeat_ms, link.missed_heartbeats, link.max_frame),
        (250, 5, 512)
    );
    let host = link.host_config(orion::NodeId::new("node-a"), 7);
    assert_eq!(host.heartbeat_ms, 250);
    assert_eq!(host.missed_heartbeats, 5);
    assert_eq!(host.max_frame, 512);
    assert_eq!(host.session_seed, 7);
    assert_eq!(host.allowed_devices, link.allowed_devices);
}

#[test]
fn can_defaults_and_every_key() {
    let links = parse(
        "can:can0;; can:can1?device_base=0x100&host_base=0x180&addresses=5&fd=true&extended=false&allow=a",
    );
    assert_eq!(links.len(), 2);
    assert_eq!(
        links[0].transport,
        LinkTransport::Can(CanLinkConfig {
            interface: "can0".into(),
            device_base: 0x600,
            host_base: 0x680,
            addresses: 1..=16,
            fd: false,
            extended: false,
        })
    );
    assert_eq!(links[0].name, "can:can0");
    let LinkTransport::Can(can) = &links[1].transport else {
        panic!("expected a CAN link");
    };
    assert_eq!(
        (
            can.device_base,
            can.host_base,
            can.addresses.clone(),
            can.fd,
            can.extended
        ),
        (0x100, 0x180, 5..=5, true, false)
    );
    assert_eq!(can.device_ids().collect::<Vec<_>>(), vec![0x105]);

    let links =
        parse("can:can0?device_base=0x18000000&host_base=0x18100000&addresses=0-511&extended=yes");
    let LinkTransport::Can(can) = &links[0].transport else {
        panic!("expected a CAN link");
    };
    assert!(can.extended);
    assert_eq!(can.device_ids().count(), 512);
}

#[test]
fn rejects_malformed_entries() {
    let cases = [
        ("ttyAMA0", "expected `serial:<path>`"),
        ("serial:", "target is empty"),
        ("serial:?baud=9600", "target is empty"),
        ("usb:/dev/x", "unknown link kind"),
        ("serial:/dev/x?baud", "is not key=value"),
        ("serial:/dev/x?baud=9600&baud=9600", "given twice"),
        (
            "serial:/dev/x?parity=even",
            "unknown or inapplicable key `parity`",
        ),
        (
            "serial:/dev/x?device_base=1",
            "unknown or inapplicable key `device_base`",
        ),
        ("can:can0?baud=9600", "unknown or inapplicable key `baud`"),
        ("serial:/dev/x?baud=12345", "unsupported baud"),
        (
            "serial:/dev/x?baud=fast",
            "baud: `fast` is not a valid unsigned integer",
        ),
        (
            "serial:/dev/x?allow=",
            "allow must list at least one device name",
        ),
        (
            "serial:/dev/x?allow=,,",
            "allow must list at least one device name",
        ),
        (
            "serial:/dev/x?heartbeat_ms=5",
            "heartbeat_ms must be between",
        ),
        (
            "serial:/dev/x?heartbeat_ms=700000",
            "heartbeat_ms must be between",
        ),
        (
            "serial:/dev/x?missed_heartbeats=0",
            "missed_heartbeats must be at least 1",
        ),
        ("serial:/dev/x?max_frame=16", "max_frame must be between"),
        ("serial:/dev/x?max_frame=8192", "max_frame must be between"),
        ("can:can0?fd=maybe", "fd must be true or false"),
        ("can:can0?extended=2", "extended must be true or false"),
        ("can:can0?addresses=9-3", "empty range"),
        ("can:can0?addresses=1-x", "not a valid unsigned integer"),
        ("can:can0?addresses=0-512", "at most 512 devices"),
        (
            "can:can0?device_base=0x7F0&addresses=1-16",
            "leaves the 11-bit identifier range",
        ),
        (
            "can:can0?device_base=0x100&host_base=0x100&addresses=0",
            "leaves the 11-bit",
        ),
        (
            "can:can0?device_base=0x100&host_base=0x108&addresses=1-16",
            "overlap host identifiers",
        ),
        ("can:this-name-is-too-long0", "invalid CAN interface name"),
        ("serial:/dev/x;serial:/dev/x?baud=9600", "configured twice"),
    ];
    for (raw, expected) in cases {
        let message = error(raw);
        assert!(
            message.contains(expected),
            "`{raw}`: expected `{expected}` in `{message}`"
        );
        assert!(message.contains("ORION_NODE_LINKS"), "{message}");
    }
}

#[test]
fn empty_list_means_no_links() {
    assert!(parse("").is_empty());
    assert!(parse(" ; ;").is_empty());
}
