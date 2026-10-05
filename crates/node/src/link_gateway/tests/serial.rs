//! Serial end-to-end: the gateway opens the slave side of a pseudo-terminal; a simulated device
//! (`orion_link::device::StreamDevice`) runs on the master side.

use super::*;
use crate::link_gateway::{LinkConfig, LinkGatewayHandle};
use orion::control_plane::{ClientHello, ClientRole, ControlMessage, ProviderStateUpdate};
use orion::transport::ipc::LocalAddress;
use orion_link::Stream;
use orion_link::device::{DeviceEvent, StreamDevice};
use orion_link::message::RejectReason;
use std::ffi::CStr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

/// A pseudo-terminal pair: the master stays here, the slave path goes to the gateway.
pub(crate) struct Pty {
    pub(crate) master: OwnedFd,
    pub(crate) slave_path: PathBuf,
}

/// Opens a pty with `posix_openpt` (no libutil needed). The master is non-blocking.
pub(crate) fn open_pty() -> Pty {
    // SAFETY: plain libc calls on a fd we own; buffers are sized as passed.
    unsafe {
        let master = libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC);
        assert!(
            master >= 0,
            "posix_openpt: {}",
            std::io::Error::last_os_error()
        );
        let master_fd = OwnedFd::from_raw_fd(master);
        assert_eq!(libc::grantpt(master), 0, "grantpt");
        assert_eq!(libc::unlockpt(master), 0, "unlockpt");
        let mut name = [0 as libc::c_char; 128];
        assert_eq!(
            libc::ptsname_r(master, name.as_mut_ptr(), name.len()),
            0,
            "ptsname_r"
        );
        let flags = libc::fcntl(master, libc::F_GETFL);
        assert_eq!(
            libc::fcntl(master, libc::F_SETFL, flags | libc::O_NONBLOCK),
            0
        );
        let slave_path = PathBuf::from(
            CStr::from_ptr(name.as_ptr())
                .to_str()
                .expect("pty name is UTF-8"),
        );
        Pty {
            master: master_fd,
            slave_path,
        }
    }
}

fn read_master(fd: &OwnedFd, buf: &mut [u8]) -> usize {
    // SAFETY: `buf` is valid for `buf.len()` bytes.
    let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
    if n > 0 { n.unsigned_abs() } else { 0 }
}

fn write_master(fd: &OwnedFd, mut bytes: &[u8]) {
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while !bytes.is_empty() && std::time::Instant::now() < deadline {
        // SAFETY: `bytes` is valid for `bytes.len()` bytes.
        let n = unsafe { libc::write(fd.as_raw_fd(), bytes.as_ptr().cast(), bytes.len()) };
        if n > 0 {
            bytes = bytes.get(n.unsigned_abs()..).unwrap_or_default();
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// A simulated MCU on the master side of a pty.
pub(crate) struct SimDevice {
    pub(crate) device: Arc<Mutex<StreamDevice<512, 512, String>>>,
    pub(crate) events: Arc<Mutex<Vec<DeviceEvent>>>,
    /// While set, the "cable" is pulled: nothing is sent or received.
    pub(crate) unplugged: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl SimDevice {
    pub(crate) fn start(name: &str, pty: Pty) -> Self {
        let device = Arc::new(Mutex::new(StreamDevice::<512, 512, String>::new(
            fast_device(name),
            Stream,
        )));
        let events = Arc::new(Mutex::new(Vec::new()));
        let unplugged = Arc::new(AtomicBool::new(false));
        let task = {
            let (device, events, unplugged) = (device.clone(), events.clone(), unplugged.clone());
            tokio::spawn(async move {
                let started = std::time::Instant::now();
                let mut buf = [0u8; 1024];
                let mut tx = [0u8; 256];
                loop {
                    let pulled = unplugged.load(Ordering::SeqCst);
                    let mut out = Vec::new();
                    {
                        let mut device = device.lock().expect("device lock");
                        loop {
                            let n = read_master(&pty.master, &mut buf);
                            if n == 0 {
                                break;
                            }
                            if !pulled {
                                device.receive(&buf[..n]);
                            }
                        }
                        device.poll(started.elapsed().as_millis() as u64);
                        while let Some(event) = device.next_event() {
                            events.lock().expect("events lock").push(event);
                        }
                        loop {
                            let n = device.transmit(&mut tx);
                            if n == 0 {
                                break;
                            }
                            out.extend_from_slice(&tx[..n]);
                        }
                    }
                    if !pulled && !out.is_empty() {
                        write_master(&pty.master, &out);
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        };
        Self {
            device,
            events,
            unplugged,
            task,
        }
    }

    pub(crate) fn publish(&self, provider: &ProviderRecord, resources: &[ResourceRecord]) {
        self.device
            .lock()
            .expect("device lock")
            .publish_provider_state(provider, resources)
            .expect("snapshot fits");
    }

    pub(crate) fn last_leases(&self) -> Option<Vec<LeaseRecord>> {
        self.events
            .lock()
            .expect("events lock")
            .contains(&DeviceEvent::LeasesChanged)
            .then(|| self.device.lock().expect("device lock").lease_records())
    }

    pub(crate) fn saw(&self, predicate: impl Fn(&DeviceEvent) -> bool) -> bool {
        self.events
            .lock()
            .expect("events lock")
            .iter()
            .any(predicate)
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.device.lock().expect("device lock").is_connected()
    }
}

impl Drop for SimDevice {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) fn serial_link(pty: &Pty, query: &str) -> LinkConfig {
    let entry = format!(
        "serial:{}?heartbeat_ms={HEARTBEAT_MS}&missed_heartbeats=3{query}",
        pty.slave_path.display()
    );
    LinkConfig::parse(&entry).expect("link should parse")
}

pub(super) fn start(app: &NodeApp, links: Vec<LinkConfig>) -> LinkGatewayHandle {
    app.start_link_gateway(links).expect("gateway should start")
}

pub(super) const WAIT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serial_device_becomes_provider_gets_leases_and_survives_reconnect() {
    let app = test_app("serial-e2e");
    let pty = open_pty();
    let link = serial_link(&pty, "&allow=imu-board");
    let gateway = start(&app, vec![link.clone()]);
    let device = SimDevice::start("imu-board", pty);
    device.publish(
        &device_provider("provider.imu-board"),
        &[device_resource(
            "imu-board.imu-0",
            "provider.imu-board",
            "rate=100hz",
        )],
    );

    // The device appears as a provider on this node with its resources.
    assert!(
        wait_until(WAIT, || has_provider(&app, "provider.imu-board")
            && observed_resource(&app, "imu-board.imu-0").is_some())
        .await,
        "device provider did not appear: {:?} {:?}",
        app.link_status(),
        device.events.lock().unwrap()
    );
    assert!(
        wait_until(WAIT, || device
            .saw(|e| matches!(e, DeviceEvent::StateAcked)))
        .await
    );
    let status = app.link_status();
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].name, link.name);
    assert!(status[0].open);
    assert_eq!(status[0].devices, vec!["imu-board".to_owned()]);
    assert!(status[0].frames_rx > 0 && status[0].frames_tx > 0);

    // A lease assigned in desired state reaches the device.
    assign_lease(&app, lease("imu-board.imu-0", "workload.pose"));
    assert!(
        wait_until(WAIT, || device.last_leases()
            == Some(vec![lease("imu-board.imu-0", "workload.pose")]))
        .await,
        "lease did not reach the device"
    );

    // A new snapshot replaces the old one.
    device.publish(
        &device_provider("provider.imu-board"),
        &[device_resource(
            "imu-board.imu-0",
            "provider.imu-board",
            "rate=200hz",
        )],
    );
    assert!(
        wait_until(WAIT, || observed_resource(&app, "imu-board.imu-0")
            .is_some_and(|r| r.labels == vec!["rate=200hz".to_owned()]))
        .await
    );

    // Pull the cable: the resources become unavailable but the records and the lease stay.
    device.unplugged.store(true, Ordering::SeqCst);
    assert!(
        wait_until(WAIT, || observed_resource(&app, "imu-board.imu-0")
            .is_some_and(|r| r.availability == AvailabilityState::Unavailable
                && r.health == HealthState::Unknown))
        .await,
        "lost device's resources were not marked unavailable"
    );
    assert!(has_provider(&app, "provider.imu-board"));
    assert!(
        app.state_snapshot()
            .state
            .desired
            .leases
            .contains_key(&orion::ResourceId::new("imu-board.imu-0")),
        "the lease must survive the disconnect"
    );
    assert!(wait_until(WAIT, || app.link_status()[0].devices.is_empty()).await);

    // Plug it back in: the device reconnects, republishes, and gets its lease again.
    device.events.lock().expect("events").clear();
    device.unplugged.store(false, Ordering::SeqCst);
    assert!(
        wait_until(WAIT, || observed_resource(&app, "imu-board.imu-0")
            .is_some_and(|r| r.availability == AvailabilityState::Available))
        .await,
        "reconnected device's resources were not restored"
    );
    assert!(
        wait_until(WAIT, || device.last_leases()
            == Some(vec![lease("imu-board.imu-0", "workload.pose")]))
        .await
    );
    assert!(app.link_status()[0].sessions >= 2);

    // Shutdown closes the link and marks the device lost.
    gateway.shutdown().await;
    assert_eq!(
        observed_resource(&app, "imu-board.imu-0").map(|r| r.availability),
        Some(AvailabilityState::Unavailable)
    );
    assert!(!app.link_status()[0].open);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serial_allowlist_rejects_unknown_device() {
    let app = test_app("serial-allow");
    let pty = open_pty();
    let gateway = start(&app, vec![serial_link(&pty, "&allow=imu-board")]);
    let device = SimDevice::start("rogue", pty);
    device.publish(
        &device_provider("provider.rogue"),
        &[device_resource("rogue.0", "provider.rogue", "x")],
    );
    assert!(
        wait_until(WAIT, || device.saw(|e| matches!(
            e,
            DeviceEvent::Rejected(RejectReason::UnknownDevice)
        )))
        .await,
        "device was not rejected"
    );
    assert!(!device.is_connected());
    assert!(!has_provider(&app, "provider.rogue"));
    assert!(observed_resource(&app, "rogue.0").is_none());
    assert!(app.link_status()[0].hello_rejects >= 1);
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serial_device_cannot_take_over_ipc_provider_and_vice_versa() {
    let app = test_app("serial-ipc-collision");
    let ipc = LocalAddress::new("ipc-provider");
    let hello = ControlMessage::ClientHello(ClientHello {
        client_name: "ipc-provider".into(),
        role: ClientRole::Provider,
    });
    app.apply_local_control_message(&ipc, hello).expect("hello");
    let mut ipc_provider = device_provider("provider.shared");
    ipc_provider.node_id = orion::NodeId::new(NODE);
    let update = ControlMessage::ProviderState(ProviderStateUpdate {
        provider: ipc_provider.clone(),
        resources: vec![device_resource("shared.0", "provider.shared", "from-ipc")],
    });
    app.apply_local_control_message(&ipc, update)
        .expect("ipc provider state");

    let pty = open_pty();
    let gateway = start(&app, vec![serial_link(&pty, "")]);
    let device = SimDevice::start("intruder", pty);
    device.publish(
        &device_provider("provider.shared"),
        &[device_resource(
            "shared.0",
            "provider.shared",
            "from-device",
        )],
    );
    assert!(
        wait_until(WAIT, || app
            .link_status()
            .first()
            .is_some_and(|s| s.snapshot_rejects >= 1))
        .await,
        "collision was not rejected"
    );
    // The IPC client's records are untouched and the device gets no leases.
    assert_eq!(
        observed_resource(&app, "shared.0").map(|r| r.labels),
        Some(vec!["from-ipc".to_owned()])
    );
    assert!(app.link_status()[0].devices.is_empty());
    assert!(
        app.link_status()[0]
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("local IPC client"))
    );

    // The other direction: a device's provider cannot be overwritten over IPC.
    let pty = open_pty();
    let gateway_b = start(&app, vec![serial_link(&pty, "")]);
    let owner = SimDevice::start("owner", pty);
    owner.publish(
        &device_provider("provider.owner"),
        &[device_resource("owner.0", "provider.owner", "from-device")],
    );
    assert!(wait_until(WAIT, || has_provider(&app, "provider.owner")).await);
    let mut hijack = device_provider("provider.owner");
    hijack.node_id = orion::NodeId::new(NODE);
    let result = app.apply_local_control_message(
        &ipc,
        ControlMessage::ProviderState(ProviderStateUpdate {
            provider: hijack,
            resources: vec![device_resource("owner.0", "provider.owner", "from-ipc")],
        }),
    );
    assert!(
        result.is_err(),
        "IPC overwrite of a device provider must fail"
    );
    assert_eq!(
        observed_resource(&app, "owner.0").map(|r| r.labels),
        Some(vec!["from-device".to_owned()])
    );
    drop(device);
    gateway.shutdown().await;
    gateway_b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serial_device_cannot_claim_another_devices_resources() {
    let app = test_app("serial-device-collision");
    let (pty_a, pty_b) = (open_pty(), open_pty());
    let gateway = start(&app, vec![serial_link(&pty_a, ""), serial_link(&pty_b, "")]);
    let a = SimDevice::start("dev-a", pty_a);
    a.publish(
        &device_provider("provider.a"),
        &[device_resource("bus.sensor-0", "provider.a", "a")],
    );
    assert!(wait_until(WAIT, || has_provider(&app, "provider.a")).await);

    // Same resource id under another provider.
    let b = SimDevice::start("dev-b", pty_b);
    b.publish(
        &device_provider("provider.b"),
        &[device_resource("bus.sensor-0", "provider.b", "b")],
    );
    assert!(
        wait_until(WAIT, || app.link_status().iter().any(|s| s
            .snapshot_rejects
            >= 1
            && s.last_error
                .as_deref()
                .is_some_and(|e| e.contains("belongs to provider provider.a"))))
        .await,
        "resource collision was not rejected"
    );
    // Same provider id from another device.
    b.publish(
        &device_provider("provider.a"),
        &[device_resource("bus.sensor-1", "provider.a", "b")],
    );
    assert!(
        wait_until(WAIT, || app.link_status().iter().any(|s| s
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("belongs to device `dev-a`"))))
        .await,
        "provider collision was not rejected"
    );
    assert_eq!(
        observed_resource(&app, "bus.sensor-0").map(|r| r.labels),
        Some(vec!["a".to_owned()])
    );
    assert!(observed_resource(&app, "bus.sensor-1").is_none());
    assert!(!has_provider(&app, "provider.b"));

    // With a distinct provider and resources, dev-b is accepted.
    b.publish(
        &device_provider("provider.b"),
        &[device_resource("bus.sensor-1", "provider.b", "b")],
    );
    assert!(
        wait_until(WAIT, || has_provider(&app, "provider.b")
            && observed_resource(&app, "bus.sensor-1").is_some())
        .await
    );
    gateway.shutdown().await;
}
