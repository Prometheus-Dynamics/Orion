//! Unix links end to end: simulated devices (`orion_link::device::StreamDevice`) connect to the
//! link's socket, one per connection.

use super::serial::{WAIT, start};
use super::*;
use crate::auth::{LocalAdmissionPolicy, LocalAuthenticationMode};
use crate::link_gateway::LinkConfig;
use orion_link::Stream;
use orion_link::device::{DeviceEvent, StreamDevice};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;

pub(super) fn socket_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("orion-ul-{name}-{}.sock", std::process::id()))
}

pub(super) fn unix_link(path: &Path, query: &str) -> LinkConfig {
    let entry = format!(
        "unix:{}?heartbeat_ms={HEARTBEAT_MS}&missed_heartbeats=3{query}",
        path.display()
    );
    LinkConfig::parse(&entry).expect("link should parse")
}

/// A simulated device on one connection to the link's socket.
pub(super) struct SocketDevice {
    pub(super) device: Arc<Mutex<StreamDevice<512, 512, String>>>,
    events: Arc<Mutex<Vec<DeviceEvent>>>,
    /// Set when the node closed the connection.
    hung_up: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl SocketDevice {
    pub(super) async fn connect(name: &str, path: &Path) -> Self {
        let mut stream = None;
        for _ in 0..200 {
            if let Ok(connected) = UnixStream::connect(path).await {
                stream = Some(connected);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let stream = stream.expect("the link's socket should accept");
        let device = Arc::new(Mutex::new(StreamDevice::<512, 512, String>::new(
            fast_device(name),
            Stream,
        )));
        let events = Arc::new(Mutex::new(Vec::new()));
        let hung_up = Arc::new(AtomicBool::new(false));
        let task = {
            let (device, events, hung_up) = (device.clone(), events.clone(), hung_up.clone());
            tokio::spawn(async move {
                let (mut reader, mut writer) = stream.into_split();
                let started = std::time::Instant::now();
                let mut buf = [0u8; 1024];
                let mut tx = [0u8; 256];
                loop {
                    let mut out = Vec::new();
                    {
                        let mut device = device.lock().expect("device lock");
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
                    if !out.is_empty() && writer.write_all(&out).await.is_err() {
                        hung_up.store(true, Ordering::SeqCst);
                        return;
                    }
                    match tokio::time::timeout(Duration::from_millis(5), reader.read(&mut buf))
                        .await
                    {
                        Ok(Ok(0) | Err(_)) => {
                            hung_up.store(true, Ordering::SeqCst);
                            return;
                        }
                        Ok(Ok(n)) => device.lock().expect("device lock").receive(&buf[..n]),
                        Err(_) => {}
                    }
                }
            })
        };
        Self {
            device,
            events,
            hung_up,
            task,
        }
    }

    pub(super) fn publish(&self, provider: &str, resource: &str) {
        self.device
            .lock()
            .expect("device lock")
            .publish_provider_state(
                &device_provider(provider),
                &[device_resource(resource, provider, "unix")],
            )
            .expect("snapshot fits");
    }

    fn saw(&self, predicate: impl Fn(&DeviceEvent) -> bool) -> bool {
        self.events
            .lock()
            .expect("events lock")
            .iter()
            .any(predicate)
    }

    fn leases(&self) -> Vec<LeaseRecord> {
        self.device.lock().expect("device lock").lease_records()
    }
}

impl Drop for SocketDevice {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn available(app: &NodeApp, resource: &str) -> bool {
    observed_resource(app, resource).is_some_and(|r| r.availability == AvailabilityState::Available)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn devices_on_separate_connections_are_providers_and_a_closed_connection_loses_only_its_own()
{
    let app = test_app("unix-e2e");
    let path = socket_path("e2e");
    let link = unix_link(&path, "");
    let gateway = start(&app, vec![link.clone()]);
    let imu = SocketDevice::connect("imu-board", &path).await;
    let arm = SocketDevice::connect("arm-board", &path).await;
    imu.publish("provider.imu-board", "imu-board.imu-0");
    arm.publish("provider.arm-board", "arm-board.joint-0");

    assert!(
        wait_until(WAIT, || available(&app, "imu-board.imu-0")
            && available(&app, "arm-board.joint-0"))
        .await,
        "devices did not appear: {:?}",
        app.link_status()
    );
    assert!(has_provider(&app, "provider.imu-board") && has_provider(&app, "provider.arm-board"));
    let status = app.link_status();
    assert_eq!(status[0].name, link.name);
    assert!(status[0].open);
    assert_eq!(
        status[0].devices,
        vec!["arm-board".to_owned(), "imu-board".to_owned()]
    );

    // Leases go to the connection of the device that owns the resource.
    assign_lease(&app, lease("arm-board.joint-0", "workload.arm"));
    assert!(
        wait_until(WAIT, || arm.leases()
            == vec![lease("arm-board.joint-0", "workload.arm")])
        .await,
        "lease did not reach the device"
    );
    assert!(imu.leases().is_empty());

    // Closing one connection loses that device only.
    drop(imu);
    assert!(
        wait_until(WAIT, || observed_resource(&app, "imu-board.imu-0")
            .is_some_and(|r| r.availability == AvailabilityState::Unavailable))
        .await,
        "closed connection's device was not lost"
    );
    assert!(available(&app, "arm-board.joint-0"));
    assert_eq!(app.link_status()[0].devices, vec!["arm-board".to_owned()]);

    // The device connects again on a new connection; the counters keep the closed session.
    let imu = SocketDevice::connect("imu-board", &path).await;
    imu.publish("provider.imu-board", "imu-board.imu-0");
    assert!(wait_until(WAIT, || available(&app, "imu-board.imu-0")).await);
    assert!(app.link_status()[0].sessions >= 3);

    gateway.shutdown().await;
    assert!(!app.link_status()[0].open);
    assert!(!available(&app, "arm-board.joint-0"));
    assert!(!path.exists(), "shutdown removes the socket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_connection_naming_a_connected_device_is_closed() {
    let app = test_app("unix-dup");
    let path = socket_path("dup");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let first = SocketDevice::connect("imu-board", &path).await;
    first.publish("provider.imu-board", "imu-board.imu-0");
    assert!(wait_until(WAIT, || available(&app, "imu-board.imu-0")).await);

    let second = SocketDevice::connect("imu-board", &path).await;
    assert!(
        wait_until(WAIT, || second.hung_up.load(Ordering::SeqCst)).await,
        "the second connection was not closed"
    );
    // The first keeps its device and session.
    tokio::time::sleep(Duration::from_millis(u64::from(HEARTBEAT_MS) * 4)).await;
    assert!(available(&app, "imu-board.imu-0"));
    assert!(!first.hung_up.load(Ordering::SeqCst));
    assert!(first.saw(|event| matches!(event, DeviceEvent::StateAcked)));
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callers_the_local_policy_refuses_are_closed_and_the_provider_lane_is_admitted() {
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    let refusing = LocalAdmissionPolicy {
        mode: LocalAuthenticationMode::SameUser,
        grants: crate::auth::LocalAccessGrants::default(),
        local_uid: uid.wrapping_add(1),
        local_gid: gid.wrapping_add(1),
    };

    let mut app = test_app("unix-refused");
    app.local_admission = Arc::new(refusing.clone());
    let path = socket_path("refused");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let device = SocketDevice::connect("imu-board", &path).await;
    device.publish("provider.imu-board", "imu-board.imu-0");
    assert!(
        wait_until(WAIT, || device.hung_up.load(Ordering::SeqCst)).await,
        "a refused caller was not closed"
    );
    assert!(!has_provider(&app, "provider.imu-board"));
    gateway.shutdown().await;

    // The same caller in the provider allow-list may publish its provider.
    let mut app = test_app("unix-provider-lane");
    let mut lane = refusing;
    lane.grants.provider = crate::auth::LocalAccessAllowList::new().allow_uid(uid);
    app.local_admission = Arc::new(lane);
    let path = socket_path("provider-lane");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let device = SocketDevice::connect("imu-board", &path).await;
    device.publish("provider.imu-board", "imu-board.imu-0");
    assert!(wait_until(WAIT, || available(&app, "imu-board.imu-0")).await);
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connections_past_max_connections_are_closed() {
    let app = test_app("unix-full");
    let path = socket_path("full");
    let gateway = start(&app, vec![unix_link(&path, "&max_connections=1")]);
    let first = SocketDevice::connect("imu-board", &path).await;
    first.publish("provider.imu-board", "imu-board.imu-0");
    assert!(wait_until(WAIT, || available(&app, "imu-board.imu-0")).await);
    let second = SocketDevice::connect("arm-board", &path).await;
    assert!(wait_until(WAIT, || second.hung_up.load(Ordering::SeqCst)).await);
    assert!(!first.hung_up.load(Ordering::SeqCst));
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_that_is_not_a_socket_is_left_alone() {
    let app = test_app("unix-not-socket");
    let path = socket_path("not-socket");
    std::fs::write(&path, b"keep me").expect("write");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    assert!(
        wait_until(WAIT, || app
            .link_status()
            .first()
            .is_some_and(|s| s.io_errors >= 1 && !s.open))
        .await
    );
    gateway.shutdown().await;
    assert_eq!(std::fs::read(&path).expect("read"), b"keep me");
    let _ = std::fs::remove_file(&path);
}
