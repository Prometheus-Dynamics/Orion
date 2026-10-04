//! CAN bridging: `HostBus` over an in-memory bus with several devices, and an ignored test on a
//! real `vcan0` interface.

use super::*;
use crate::link_gateway::can_link::{self, CanFrame, CanIo};
use crate::link_gateway::events::LinkContext;
use crate::link_gateway::socketcan::SocketCan;
use crate::link_gateway::{CanLinkConfig, LinkConfig, LinkTransport};
use orion_link::device::{CanDevice, DeviceEvent};
use orion_link::{CanLinkIds, Packet};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A broadcast bus: every frame sent on one port is received by all other ports.
#[derive(Default)]
struct MemoryBus {
    ports: Mutex<Vec<mpsc::UnboundedSender<CanFrame>>>,
}

struct MemoryPort {
    index: usize,
    bus: Arc<MemoryBus>,
    rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<CanFrame>>,
}

impl MemoryBus {
    fn port(self: &Arc<Self>) -> MemoryPort {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut ports = self.ports.lock().expect("bus lock");
        ports.push(tx);
        MemoryPort {
            index: ports.len() - 1,
            bus: self.clone(),
            rx: tokio::sync::Mutex::new(rx),
        }
    }
}

impl CanIo for MemoryPort {
    async fn recv(&self) -> io::Result<CanFrame> {
        self.rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::other("bus closed"))
    }

    fn try_send(&self, frame: &CanFrame) -> io::Result<bool> {
        let ports = self.bus.ports.lock().expect("bus lock");
        for (index, port) in ports.iter().enumerate() {
            if index != self.index {
                let _ = port.send(frame.clone());
            }
        }
        Ok(true)
    }
}

/// A simulated CAN device at `address`.
struct BusDevice {
    device: Arc<Mutex<CanDevice<256, 256>>>,
    events: Arc<Mutex<Vec<DeviceEvent>>>,
    unplugged: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl BusDevice {
    fn start<P: CanIo>(name: &str, ids: CanLinkIds, transport: Packet, port: P) -> Self {
        let device = Arc::new(Mutex::new(CanDevice::<256, 256>::new(
            fast_device(name),
            transport,
        )));
        let events = Arc::new(Mutex::new(Vec::new()));
        let unplugged = Arc::new(AtomicBool::new(false));
        let task = {
            let (device, events, unplugged) = (device.clone(), events.clone(), unplugged.clone());
            tokio::spawn(async move {
                let started = std::time::Instant::now();
                loop {
                    let received =
                        tokio::time::timeout(Duration::from_millis(5), port.recv()).await;
                    let pulled = unplugged.load(Ordering::SeqCst);
                    let mut device = device.lock().expect("device lock");
                    if let Ok(Ok(frame)) = received
                        && !pulled
                        && frame.id == ids.host_to_device
                        && frame.extended == ids.extended
                    {
                        device.receive_segment(&frame.data);
                    }
                    device.poll(started.elapsed().as_millis() as u64);
                    while let Some(event) = device.next_event() {
                        events.lock().expect("events lock").push(event);
                    }
                    let mut out = Vec::new();
                    while let Some(segment) = device.next_segment() {
                        out.push(CanFrame {
                            id: ids.device_to_host,
                            extended: ids.extended,
                            fd: transport != Packet::CLASSIC,
                            data: segment.as_bytes().to_vec(),
                        });
                    }
                    drop(device);
                    if !pulled {
                        for frame in out {
                            let _ = port.try_send(&frame);
                        }
                    }
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

    fn publish(&self, provider: &ProviderRecord, resources: &[ResourceRecord]) {
        self.device
            .lock()
            .expect("device lock")
            .publish_provider_state(provider, resources)
            .expect("snapshot fits");
    }

    fn last_leases(&self) -> Option<Vec<LeaseRecord>> {
        self.events
            .lock()
            .expect("events lock")
            .iter()
            .rev()
            .find_map(|event| match event {
                DeviceEvent::Leases(leases) => Some(leases.clone()),
                _ => None,
            })
    }
}

impl Drop for BusDevice {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn can_config(link: &LinkConfig) -> CanLinkConfig {
    match &link.transport {
        LinkTransport::Can(can) => can.clone(),
        LinkTransport::Serial(_) => panic!("expected a CAN link"),
    }
}

fn provider_for(address: u32) -> (ProviderRecord, Vec<ResourceRecord>) {
    let provider_id = format!("provider.dev-{address}");
    (
        device_provider(&provider_id),
        vec![device_resource(
            &format!("dev-{address}.sensor"),
            &provider_id,
            "ok",
        )],
    )
}

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn can_bus_bridges_several_devices() {
    let app = test_app("can-memory");
    let link = LinkConfig::parse(&format!(
        "can:mem0?device_base=0x100&host_base=0x180&addresses=1-8&heartbeat_ms={HEARTBEAT_MS}"
    ))
    .expect("link should parse");
    let config = can_config(&link);
    let bus = Arc::new(MemoryBus::default());
    let gateway_port = Mutex::new(Some(bus.port()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let host = link.host_config(orion::NodeId::new(NODE), 1);
    let ctx = LinkContext::new(app.clone(), link.name.clone());
    let opener = move || {
        gateway_port
            .lock()
            .expect("port lock")
            .take()
            .ok_or_else(|| io::Error::other("memory port already taken"))
    };
    let gateway = tokio::spawn(can_link::run(
        ctx,
        config.clone(),
        host,
        opener,
        shutdown_rx,
    ));

    let devices: Vec<(u32, BusDevice)> = [1u32, 2, 5]
        .into_iter()
        .map(|address| {
            let ids = CanLinkIds::for_address(config.base_ids(), address).expect("ids");
            let device =
                BusDevice::start(&format!("dev-{address}"), ids, Packet::CLASSIC, bus.port());
            let (provider, resources) = provider_for(address);
            device.publish(&provider, &resources);
            (address, device)
        })
        .collect();

    for (address, _) in &devices {
        assert!(
            wait_until(WAIT, || has_provider(
                &app,
                &format!("provider.dev-{address}")
            ) && observed_resource(
                &app,
                &format!("dev-{address}.sensor")
            )
            .is_some())
            .await,
            "device {address} did not appear"
        );
    }
    assert!(
        wait_until(WAIT, || app
            .link_status()
            .first()
            .is_some_and(|s| s.devices.len() == 3))
        .await
    );

    // A lease reaches only its device.
    assign_lease(&app, lease("dev-2.sensor", "workload.motor"));
    assert!(
        wait_until(WAIT, || devices[1].1.last_leases()
            == Some(vec![lease("dev-2.sensor", "workload.motor")]))
        .await,
        "lease did not reach device 2"
    );
    assert_eq!(devices[0].1.last_leases(), Some(Vec::new()));

    // Device 5 goes silent: only its resources become unavailable.
    devices[2].1.unplugged.store(true, Ordering::SeqCst);
    assert!(
        wait_until(WAIT, || observed_resource(&app, "dev-5.sensor")
            .is_some_and(|r| r.availability == AvailabilityState::Unavailable))
        .await
    );
    assert_eq!(
        observed_resource(&app, "dev-1.sensor").map(|r| r.availability),
        Some(AvailabilityState::Available)
    );
    devices[2].1.unplugged.store(false, Ordering::SeqCst);
    assert!(
        wait_until(WAIT, || observed_resource(&app, "dev-5.sensor")
            .is_some_and(|r| r.availability == AvailabilityState::Available))
        .await
    );
    let status = app.link_status();
    assert!(status[0].frames_rx > 0 && status[0].frames_tx > 0);
    assert_eq!(status[0].crc_errors, 0);

    let _ = shutdown_tx.send(true);
    gateway.await.expect("gateway task");
    for (address, _) in &devices {
        assert_eq!(
            observed_resource(&app, &format!("dev-{address}.sensor")).map(|r| r.availability),
            Some(AvailabilityState::Unavailable)
        );
    }
}

/// Needs a `vcan0` interface:
/// `sudo ip link add dev vcan0 type vcan && sudo ip link set up vcan0`. Skips if it is missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a vcan0 interface (see docs/testing.md)"]
async fn can_gateway_on_vcan0() {
    if !std::path::Path::new("/sys/class/net/vcan0").exists() {
        eprintln!("skipping: vcan0 is missing");
        return;
    }
    let app = test_app("can-vcan0");
    let link = LinkConfig::parse(&format!(
        "can:vcan0?device_base=0x610&host_base=0x690&addresses=1-4&heartbeat_ms={HEARTBEAT_MS}"
    ))
    .expect("link should parse");
    let config = can_config(&link);
    let gateway = app.start_link_gateway(vec![link]).expect("gateway");
    let ids = CanLinkIds::for_address(config.base_ids(), 3).expect("ids");
    let port =
        SocketCan::open("vcan0", false, false, &[ids.host_to_device]).expect("device socket");
    let device = BusDevice::start("dev-3", ids, Packet::CLASSIC, port);
    let (provider, resources) = provider_for(3);
    device.publish(&provider, &resources);
    assert!(
        wait_until(WAIT, || has_provider(&app, "provider.dev-3")
            && observed_resource(&app, "dev-3.sensor").is_some())
        .await,
        "vcan device did not appear"
    );
    assign_lease(&app, lease("dev-3.sensor", "workload.vcan"));
    assert!(
        wait_until(WAIT, || device.last_leases()
            == Some(vec![lease("dev-3.sensor", "workload.vcan")]))
        .await
    );
    gateway.shutdown().await;
}
