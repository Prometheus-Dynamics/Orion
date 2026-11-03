//! Firmware over a CAN link, and the per-device release, end to end: simulated devices on one
//! in-memory bus (`DeviceSession` with a `FirmwareSink`) run the node's `link.firmware.update`,
//! `link.release` and `link.attach` actions. A neighbour device shares the bus and must keep
//! working. The ignored test runs the same on a real `vcan0` (`docs/testing.md`).

use super::can::MemoryBus;
use super::firmware::{Flash, committed, firmware_app, image, output_text, settle};
use super::*;
use crate::link_gateway::can_link::{self, CanFrame, CanIo};
use crate::link_gateway::events::LinkContext;
use crate::link_gateway::socketcan::SocketCan;
use crate::link_gateway::{CanLinkConfig, LinkConfig, LinkTransport};
use orion::NodeId;
use orion::ProviderId;
use orion::control_plane::{ActionRequest, ActionState, ActionTarget, TypedConfigValue};
use orion_link::device::DeviceSession;
use orion_link::wire::max_chunk_for_frame;
use orion_link::{CanLinkIds, Packet};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// The device that takes the firmware (address 1) and its neighbour on the bus (address 2).
const WAIT: std::time::Duration = std::time::Duration::from_secs(5);
const UPDATED: &str = "motor-a";
const NEIGHBOUR: &str = "motor-b";
const BASE_DEVICE: u32 = 0x100;
const BASE_HOST: u32 = 0x180;
/// The largest chunk the classic budget allows: 32 segments of 7 bytes, less the chunk frame's
/// overhead (`can_link::firmware_chunk_len`).
const CLASSIC_CHUNK: usize = max_chunk_for_frame(32 * 7);

fn provider_of(device: &str) -> String {
    format!("provider.{device}")
}

fn resource_of(device: &str) -> String {
    format!("{device}.speed")
}

/// A device that advertises chunks as large as the CAN frame allows, so the budget is what bounds
/// them.
fn can_flash(version: &str) -> Flash {
    let mut flash = Flash::factory(version);
    flash.max_chunk = 512;
    flash
}

type CanSession = DeviceSession<Packet, 512, 512, String, 512, Flash>;

/// A fresh session of `name` with its provider published, and its flash.
fn session(name: &str, flash: Flash) -> CanSession {
    let mut device = DeviceSession::with_firmware(fast_device(name), Packet::CLASSIC, flash);
    device
        .publish_provider_state(
            &device_provider(&provider_of(name)),
            &[device_resource(
                &resource_of(name),
                &provider_of(name),
                "rpm",
            )],
        )
        .expect("snapshot fits");
    device
}

/// A device on the bus at `address`, running its own loop. It reboots when its firmware asks, and
/// goes silent after a handoff (the bootloader owns the bus).
struct CanDevice {
    device: Arc<Mutex<CanSession>>,
    task: JoinHandle<()>,
}

impl CanDevice {
    fn start<P: CanIo>(port: P, ids: CanLinkIds, name: &str, flash: Flash) -> Self {
        let device = Arc::new(Mutex::new(session(name, flash)));
        let silent = Arc::new(AtomicBool::new(false));
        let task = {
            let (device, name) = (device.clone(), name.to_owned());
            tokio::spawn(async move {
                let started = Instant::now();
                loop {
                    let received =
                        tokio::time::timeout(std::time::Duration::from_millis(5), port.recv())
                            .await;
                    let mut out = Vec::new();
                    {
                        let mut device = device.lock().expect("device lock");
                        let quiet = silent.load(Ordering::SeqCst);
                        if let Ok(Ok(frame)) = received
                            && !quiet
                            && frame.id == ids.host_to_device
                            && frame.extended == ids.extended
                        {
                            device.receive_segment(&frame.data);
                        }
                        device.poll(started.elapsed().as_millis() as u64);
                        while device.next_event().is_some() {}
                        if device.firmware().handoff {
                            silent.store(true, Ordering::SeqCst);
                        }
                        if device.firmware().reboot_pending {
                            let mut flash = device.firmware().clone();
                            flash.power_cycle();
                            *device = session(&name, flash);
                        }
                        if !silent.load(Ordering::SeqCst) {
                            while let Some(segment) = device.next_segment() {
                                out.push(CanFrame {
                                    id: ids.device_to_host,
                                    extended: ids.extended,
                                    fd: false,
                                    data: segment.as_bytes().to_vec(),
                                });
                            }
                        }
                    }
                    for frame in out {
                        let _ = port.try_send(&frame);
                    }
                }
            })
        };
        Self { device, task }
    }

    fn is_connected(&self) -> bool {
        self.device.lock().expect("device lock").is_connected()
    }

    fn flash(&self) -> Flash {
        self.device.lock().expect("device lock").firmware().clone()
    }
}

impl Drop for CanDevice {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A node with the CAN link `can:mem0` on an in-memory bus and two devices on it.
struct Bench {
    app: crate::NodeApp,
    link: String,
    updated: CanDevice,
    neighbour: CanDevice,
    shutdown: watch::Sender<bool>,
    gateway: JoinHandle<()>,
    dir: PathBuf,
}

impl Bench {
    async fn new(name: &str, updated_flash: Flash) -> Self {
        let (app, dir) = firmware_app(name);
        let link = LinkConfig::parse(&format!(
            "can:mem0?device_base={BASE_DEVICE:#x}&host_base={BASE_HOST:#x}&addresses=1-8&heartbeat_ms={HEARTBEAT_MS}"
        ))
        .expect("link should parse");
        let config = can_config(&link);
        let ids = |address| CanLinkIds::for_address(config.base_ids(), address).expect("ids");
        let bus = Arc::new(MemoryBus::default());
        let gateway_port = Mutex::new(Some(bus.port()));
        let (shutdown, shutdown_rx) = watch::channel(false);
        let host = link.host_config(NodeId::new(NODE), 1);
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
        let updated = CanDevice::start(bus.port(), ids(1), UPDATED, updated_flash);
        let neighbour = CanDevice::start(bus.port(), ids(2), NEIGHBOUR, can_flash("1.2"));
        let link_name = link.name.clone();
        let bench = Self {
            app,
            link: link_name,
            updated,
            neighbour,
            shutdown,
            gateway,
            dir,
        };
        assert!(
            wait_until(WAIT, || bench.both_serving()).await,
            "both devices did not connect"
        );
        bench
    }

    fn both_serving(&self) -> bool {
        has_provider(&self.app, &provider_of(UPDATED))
            && has_provider(&self.app, &provider_of(NEIGHBOUR))
            && self.updated.is_connected()
            && self.neighbour.is_connected()
    }

    fn availability(&self, device: &str) -> Option<AvailabilityState> {
        observed_resource(&self.app, &resource_of(device)).map(|r| r.availability)
    }

    async fn close(self) {
        let _ = self.shutdown.send(true);
        self.gateway.await.expect("gateway task");
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn can_config(link: &LinkConfig) -> CanLinkConfig {
    match &link.transport {
        LinkTransport::Can(can) => can.clone(),
        LinkTransport::Serial(_) => panic!("expected a CAN link"),
    }
}

fn update_request(
    action_id: &str,
    link: &str,
    device: &str,
    blob: &str,
    version: &str,
    activate: Option<&str>,
) -> ActionRequest {
    let mut request = ActionRequest::new(
        action_id,
        ActionTarget::Node(NodeId::new(NODE)),
        "link.firmware.update",
    )
    .with_arg("link", TypedConfigValue::String(link.to_owned()))
    .with_arg("device", TypedConfigValue::String(device.to_owned()))
    .with_arg("blob", TypedConfigValue::String(blob.to_owned()))
    .with_arg(
        "component",
        TypedConfigValue::String("motor-app".to_owned()),
    )
    .with_arg("version", TypedConfigValue::String(version.to_owned()))
    .with_arg("target_slot", TypedConfigValue::UInt(1));
    if let Some(mode) = activate {
        request = request.with_arg("activate", TypedConfigValue::String(mode.to_owned()));
    }
    request
}

fn link_action(action_id: &str, name: &str, link: &str, device: &str) -> ActionRequest {
    ActionRequest::new(
        action_id,
        ActionTarget::Node(NodeId::new(NODE)),
        name.to_owned(),
    )
    .with_arg("link", TypedConfigValue::String(link.to_owned()))
    .with_arg("device", TypedConfigValue::String(device.to_owned()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn can_update_reaches_only_the_addressed_device() {
    let bench = Bench::new("can-fw-update", can_flash("1.2")).await;
    let data = image(3_000);
    committed(&bench.app, "app-1-3", &data).await;

    bench
        .app
        .run_action(
            update_request("can-fw-1", &bench.link, UPDATED, "app-1-3", "1.3", None),
            "test",
        )
        .expect("accepted");
    let (done, progress) = settle(&bench.app, "can-fw-1").await;

    assert_eq!(done.state, ActionState::Succeeded, "{done:?}");
    assert_eq!(output_text(&done, "phase").as_deref(), Some("installed"));
    // Sampling can miss intermediate values on a fast in-memory bus, so only check the order.
    assert!(
        progress.windows(2).all(|pair| pair[0] <= pair[1]) && progress.iter().all(|v| *v <= 1000),
        "progress never goes backwards: {progress:?}"
    );
    let flash = bench.updated.flash();
    assert_eq!(flash.active, 1, "the new slot runs");
    assert!(!flash.trial, "confirmed by the node after the health gate");
    assert_eq!(flash.slots[1].image, data, "the device holds the image");
    assert_eq!(
        flash.largest_write, CLASSIC_CHUNK,
        "chunks are bounded by the classic CAN segment budget"
    );

    // The neighbour was never addressed and keeps serving.
    let neighbour = bench.neighbour.flash();
    assert_eq!(neighbour.active, 0);
    assert_eq!(neighbour.largest_write, 0, "no image bytes reached it");
    assert!(neighbour.staging.is_none());
    assert_eq!(
        bench.availability(NEIGHBOUR),
        Some(AvailabilityState::Available)
    );
    assert!(bench.app.link_status()[0].open, "the bus stays open");
    bench.close().await;
}

/// The trial fails its health check on the bus: the device's bootloader reverts it and reports the
/// reason, and the action says so (the same report the serial path reads).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn can_trial_rollback_is_reported_with_its_reason() {
    let mut flash = can_flash("1.2");
    flash.unhealthy_trial = true;
    let bench = Bench::new("can-fw-rollback", flash).await;
    committed(&bench.app, "app-1-3", &image(1_500)).await;

    bench
        .app
        .run_action(
            update_request("can-fw-rb-1", &bench.link, UPDATED, "app-1-3", "1.3", None),
            "test",
        )
        .expect("accepted");
    let (done, _) = settle(&bench.app, "can-fw-rb-1").await;

    match &done.state {
        ActionState::Failed { reason } => {
            assert!(reason.contains("rolled back"), "reason: {reason}");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    assert_eq!(
        output_text(&done, "rollback_reason").as_deref(),
        Some("not_confirmed")
    );
    assert_eq!(bench.updated.flash().active, 0, "the old slot runs");
    bench.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn can_release_takes_one_device_off_the_bus_and_attach_brings_it_back() {
    let bench = Bench::new("can-release", can_flash("1.2")).await;

    bench
        .app
        .run_action(
            link_action("can-rel-1", "link.release", &bench.link, UPDATED),
            "test",
        )
        .expect("accepted");
    let (done, _) = settle(&bench.app, "can-rel-1").await;
    assert_eq!(done.state, ActionState::Succeeded, "{done:?}");
    assert_eq!(output_text(&done, "state").as_deref(), Some("released"));
    assert!(
        wait_until(WAIT, || bench.availability(UPDATED)
            == Some(AvailabilityState::Unavailable))
        .await,
        "the released device's providers become unavailable"
    );
    assert_eq!(
        bench.availability(NEIGHBOUR),
        Some(AvailabilityState::Available)
    );
    assert!(bench.app.link_status()[0].open, "the bus is not closed");

    // Its actions are refused, and the device gets no session while it is released.
    let refused = bench
        .app
        .run_action(
            ActionRequest::new(
                "can-rel-locate",
                ActionTarget::Provider(ProviderId::new(provider_of(UPDATED))),
                "locate",
            ),
            "test",
        )
        .expect("accepted");
    assert!(
        matches!(
            refused.state,
            ActionState::Rejected { .. } | ActionState::Failed { .. }
        ),
        "{refused:?}"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    assert!(
        !bench.updated.is_connected(),
        "a released device is not routed to"
    );
    assert!(
        bench.neighbour.is_connected(),
        "the neighbour is unaffected"
    );

    bench
        .app
        .run_action(
            link_action("can-att-1", "link.attach", &bench.link, UPDATED),
            "test",
        )
        .expect("accepted");
    let (attached, _) = settle(&bench.app, "can-att-1").await;
    assert_eq!(attached.state, ActionState::Succeeded, "{attached:?}");
    assert_eq!(output_text(&attached, "state").as_deref(), Some("attached"));
    assert!(
        wait_until(WAIT, || bench.updated.is_connected()
            && bench.availability(UPDATED)
                == Some(AvailabilityState::Available))
        .await,
        "attach brings the device back"
    );
    bench.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn can_firmware_to_a_released_device_is_refused() {
    let bench = Bench::new("can-fw-released", can_flash("1.2")).await;
    committed(&bench.app, "app-1-3", &image(1_000)).await;
    bench
        .app
        .run_action(
            link_action("can-rel-2", "link.release", &bench.link, UPDATED),
            "test",
        )
        .expect("accepted");
    assert_eq!(
        settle(&bench.app, "can-rel-2").await.0.state,
        ActionState::Succeeded
    );

    bench
        .app
        .run_action(
            update_request(
                "can-fw-released",
                &bench.link,
                UPDATED,
                "app-1-3",
                "1.3",
                None,
            ),
            "test",
        )
        .expect("accepted");
    let (done, _) = settle(&bench.app, "can-fw-released").await;
    match &done.state {
        ActionState::Rejected { reason } => assert!(reason.contains("released"), "{reason}"),
        other => panic!("expected Rejected, got {other:?}"),
    }
    assert_eq!(bench.updated.flash().largest_write, 0, "nothing was sent");
    bench.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn can_handoff_releases_only_the_handed_off_device() {
    let bench = Bench::new("can-handoff", can_flash("1.2")).await;
    committed(&bench.app, "app-1-3", &image(1_000)).await;

    bench
        .app
        .run_action(
            update_request(
                "can-handoff-1",
                &bench.link,
                UPDATED,
                "app-1-3",
                "1.3",
                Some("handoff"),
            ),
            "test",
        )
        .expect("accepted");
    let (done, _) = settle(&bench.app, "can-handoff-1").await;
    assert_eq!(done.state, ActionState::Succeeded, "{done:?}");
    assert_eq!(output_text(&done, "phase").as_deref(), Some("handoff"));
    assert!(
        wait_until(WAIT, || bench.availability(UPDATED)
            == Some(AvailabilityState::Unavailable))
        .await,
        "the handed-off device is released"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    assert_eq!(
        bench.availability(UPDATED),
        Some(AvailabilityState::Unavailable),
        "a handed-off device is not routed to again"
    );
    assert_eq!(
        bench.availability(NEIGHBOUR),
        Some(AvailabilityState::Available)
    );
    assert!(bench.app.link_status()[0].open, "the bus stays open");
    bench.close().await;
}

/// Needs a `vcan0` interface:
/// `sudo ip link add dev vcan0 type vcan && sudo ip link set up vcan0`. Skips if it is missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a vcan0 interface (see docs/testing.md)"]
async fn can_firmware_on_vcan0() {
    if !std::path::Path::new("/sys/class/net/vcan0").exists() {
        eprintln!("skipping: vcan0 is missing");
        return;
    }
    let (app, dir) = firmware_app("can-vcan0-fw");
    let link = LinkConfig::parse(&format!(
        "can:vcan0?device_base=0x630&host_base=0x6b0&addresses=5-8&heartbeat_ms={HEARTBEAT_MS}"
    ))
    .expect("link should parse");
    let config = can_config(&link);
    let name = link.name.clone();
    let ids = CanLinkIds::for_address(config.base_ids(), 5).expect("ids");
    let gateway = app.start_link_gateway(vec![link]).expect("gateway");
    let port =
        SocketCan::open("vcan0", false, false, &[ids.host_to_device]).expect("device socket");
    let device = CanDevice::start(port, ids, UPDATED, can_flash("1.2"));
    assert!(
        wait_until(WAIT, || has_provider(&app, &provider_of(UPDATED))
            && device.is_connected())
        .await,
        "vcan device did not connect"
    );
    committed(&app, "app-1-3", &image(2_000)).await;
    app.run_action(
        update_request("vcan-fw-1", &name, UPDATED, "app-1-3", "1.3", None),
        "test",
    )
    .expect("accepted");
    let (done, _) = settle(&app, "vcan-fw-1").await;
    assert_eq!(done.state, ActionState::Succeeded, "{done:?}");
    assert_eq!(device.flash().active, 1, "the new slot runs on the bus");
    assert!(device.flash().largest_write <= CLASSIC_CHUNK);
    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}
