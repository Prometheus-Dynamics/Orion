//! Actions routed to a link device: the node sends an `ActionRequest` over the serial link, the
//! simulated device (`StreamDevice` on the pty master) runs it, and its `ActionResult`s become the
//! node's action results.

use super::serial::{SimDevice, WAIT, open_pty, serial_link, start};
use super::*;
use crate::link_gateway::LinkGatewayHandle;
use orion::ProviderId;
use orion::control_plane::LinkDeviceActions;
use orion::control_plane::{
    ActionQuery, ActionRequest, ActionResult, ActionState, ActionTarget, TypedConfigValue,
};
use orion_link::device::DeviceEvent;
use orion_link::message::{decode_action_map, encode_action_map};
use orion_link::wire::ActionState as DeviceState;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

const PROVIDER: &str = "provider.imu-board";

fn result(app: &NodeApp, action_id: &str) -> Option<ActionResult> {
    app.query_actions(&ActionQuery {
        action_id: Some(action_id.to_owned()),
        ..ActionQuery::default()
    })
    .into_iter()
    .next()
}

fn provider_request(action_id: &str, name: &str) -> ActionRequest {
    ActionRequest::new(
        action_id,
        ActionTarget::Provider(ProviderId::new(PROVIDER)),
        name,
    )
}

/// The device's side of actions: for each request, `script(name, payload)` gives the reports to
/// send. Each report is sent on its own (the device replaces an unsent report with a newer one).
type Script = fn(&str, &[u8]) -> Vec<(DeviceState, Vec<u8>)>;

fn spawn_handler(device: &SimDevice, script: Script) -> tokio::task::JoinHandle<()> {
    let shared = device.device.clone();
    tokio::spawn(async move {
        loop {
            let request = shared
                .lock()
                .expect("device lock")
                .action()
                .map(|request| (request.name.to_owned(), request.payload.to_vec()));
            let Some((name, payload)) = request else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            };
            let reports = script(&name, &payload);
            if reports.is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            for (state, body) in reports {
                shared
                    .lock()
                    .expect("device lock")
                    .report_action(state, &body)
                    .expect("report accepted");
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
        }
    })
}

/// `locate` (with `enabled = true`): progress, then success with an output map. `hang` never
/// reports. Anything else is refused as unknown.
fn script(name: &str, payload: &[u8]) -> Vec<(DeviceState, Vec<u8>)> {
    match name {
        "locate" => {
            let args = decode_action_map(payload).unwrap_or_default();
            if args.get("enabled") != Some(&TypedConfigValue::Bool(true)) {
                return vec![(DeviceState::Rejected, b"bad arguments".to_vec())];
            }
            let output = BTreeMap::from([("ok".to_owned(), TypedConfigValue::Bool(true))]);
            vec![
                // Progress is a per-mille varint: 500 is `0xF4 0x03`.
                (DeviceState::Running, vec![0xF4, 0x03]),
                (
                    DeviceState::Succeeded,
                    encode_action_map(&output).expect("output encodes"),
                ),
            ]
        }
        "hang" => Vec::new(),
        _ => vec![(DeviceState::Rejected, b"unknown action".to_vec())],
    }
}

/// A node with `imu-board` connected on a serial link and its provider published.
async fn setup(id: &str) -> (NodeApp, SimDevice, LinkGatewayHandle) {
    let app = test_app(id);
    let pty = open_pty();
    let gateway = start(&app, vec![serial_link(&pty, "&allow=imu-board")]);
    let device = SimDevice::start("imu-board", pty);
    device.publish(
        &device_provider(PROVIDER),
        &[device_resource("imu-board.imu-0", PROVIDER, "rate=100hz")],
    );
    assert!(
        wait_until(WAIT, || has_provider(&app, PROVIDER)
            && device.is_connected())
        .await,
        "device did not connect"
    );
    (app, device, gateway)
}

async fn settled(app: &NodeApp, action_id: &str) -> ActionResult {
    assert!(
        wait_until(WAIT, || result(app, action_id)
            .is_some_and(|r| r.state.is_terminal()))
        .await,
        "action {action_id} did not finish: {:?}",
        result(app, action_id)
    );
    result(app, action_id).expect("tracked")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_for_a_device_provider_runs_on_the_device_and_reports_back() {
    let (app, device, gateway) = setup("link-action-ok").await;
    let _handler = spawn_handler(&device, script);

    let request =
        provider_request("locate-1", "locate").with_arg("enabled", TypedConfigValue::Bool(true));
    let accepted = app.run_action(request, "test").expect("accepted");
    assert_eq!(accepted.handled_by, orion::NodeId::new(NODE));

    let done = settled(&app, "locate-1").await;
    assert_eq!(done.state, ActionState::Succeeded);
    assert_eq!(
        done.output.get("ok"),
        Some(&TypedConfigValue::Bool(true)),
        "the device's output comes back as the action's output"
    );
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_name_the_device_does_not_handle_is_refused_by_it() {
    let (app, device, gateway) = setup("link-action-unknown").await;
    let _handler = spawn_handler(&device, script);

    app.run_action(provider_request("frob-1", "frobnicate"), "test")
        .expect("accepted");
    let done = settled(&app, "frob-1").await;
    assert_eq!(
        done.state,
        ActionState::Rejected {
            reason: "unknown action".into()
        }
    );
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_disconnect_mid_action_fails_it_with_a_reason() {
    let (app, device, gateway) = setup("link-action-disconnect").await;
    let _handler = spawn_handler(&device, script);

    app.run_action(provider_request("hang-1", "hang"), "test")
        .expect("accepted");
    // The device has the request before it goes away.
    assert!(
        wait_until(WAIT, || device
            .device
            .lock()
            .expect("device lock")
            .action()
            .is_some())
        .await
    );
    device.unplugged.store(true, Ordering::SeqCst);

    let done = settled(&app, "hang-1").await;
    match done.state {
        ActionState::Failed { reason } => {
            assert!(reason.contains("disconnected"), "reason: {reason}");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_deadline_is_honored_for_a_device_that_never_reports() {
    let (app, device, gateway) = setup("link-action-deadline").await;
    let _handler = spawn_handler(&device, script);

    app.run_action(
        provider_request("hang-2", "hang").with_deadline_ms(300),
        "test",
    )
    .expect("accepted");
    let done = settled(&app, "hang-2").await;
    assert_eq!(done.state, ActionState::TimedOut);
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_for_an_unknown_provider_is_refused_at_once() {
    let app = test_app("link-action-no-provider");
    let accepted = app
        .run_action(provider_request("nope-1", "locate"), "test")
        .expect("accepted");
    assert!(matches!(accepted.state, ActionState::Rejected { .. }));
}

/// A job named `flash` that runs until the host cancels it: the device reports `Cancelled` once it
/// has received the cancel (the device session queues it while the job runs).
fn spawn_cancellable_job(device: &SimDevice) -> tokio::task::JoinHandle<()> {
    let shared = device.device.clone();
    let events = device.events.clone();
    tokio::spawn(async move {
        loop {
            let id = shared
                .lock()
                .expect("device lock")
                .action()
                .map(|request| request.id);
            let Some(id) = id else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            };
            loop {
                let cancelled = events
                    .lock()
                    .expect("events lock")
                    .contains(&DeviceEvent::ActionCancelRequested { id });
                if cancelled {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            shared
                .lock()
                .expect("device lock")
                .report_action(DeviceState::Cancelled, &[])
                .expect("cancel report accepted");
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_companion_reaches_the_busy_device_and_the_job_ends_cancelled() {
    let (app, device, gateway) = setup("link-action-cancel").await;
    let _job = spawn_cancellable_job(&device);

    app.run_action(provider_request("job-1", "flash"), "test")
        .expect("accepted");
    assert!(
        wait_until(WAIT, || device
            .device
            .lock()
            .expect("device lock")
            .action()
            .is_some())
        .await,
        "the device has the job"
    );
    // The companion is an ordinary action on the same target; the node sends it as an
    // `ActionCancel` for the running job, which the busy device accepts.
    app.run_action(provider_request("job-1.cancel", "flash.cancel"), "test")
        .expect("accepted");

    let job = settled(&app, "job-1").await;
    assert_eq!(job.state, ActionState::Cancelled);
    let companion = settled(&app, "job-1.cancel").await;
    assert_eq!(companion.state, ActionState::Succeeded);
    assert!(device.saw(|e| matches!(e, DeviceEvent::ActionCancelRequested { .. })));
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_without_a_running_job_is_an_ordinary_request_to_the_device() {
    let (app, device, gateway) = setup("link-action-cancel-idle").await;
    let _handler = spawn_handler(&device, script);

    app.run_action(
        provider_request("flash-idle.cancel", "flash.cancel"),
        "test",
    )
    .expect("accepted");
    let done = settled(&app, "flash-idle.cancel").await;
    assert_eq!(
        done.state,
        ActionState::Rejected {
            reason: "unknown action".into()
        },
        "nothing runs, so the name goes to the device as it is"
    );
    assert!(!device.saw(|e| matches!(e, DeviceEvent::ActionCancelRequested { .. })));
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_is_rejected_when_the_job_finishes_first() {
    let (app, device, gateway) = setup("link-action-cancel-late").await;
    let _handler = spawn_handler(&device, script);

    // `locate` reports Running, then Succeeded 80 ms later; the cancel arrives in between, and the
    // handler does not stop for it.
    app.run_action(
        provider_request("locate-late", "locate").with_arg("enabled", TypedConfigValue::Bool(true)),
        "test",
    )
    .expect("accepted");
    // The device has reported `Running` (its request is no longer readable): the job is in flight.
    assert!(
        wait_until(WAIT, || result(&app, "locate-late")
            .is_some_and(|r| matches!(r.state, ActionState::Running { .. })))
        .await
    );
    app.run_action(
        provider_request("locate-late.cancel", "locate.cancel"),
        "test",
    )
    .expect("accepted");

    let job = settled(&app, "locate-late").await;
    assert_eq!(job.state, ActionState::Succeeded);
    let companion = settled(&app, "locate-late.cancel").await;
    assert_eq!(
        companion.state,
        ActionState::Rejected {
            reason: "the action finished as succeeded before the cancel took effect".into()
        }
    );
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_name_the_device_did_not_declare_is_refused_before_it_is_sent() {
    let (app, device, gateway) = setup("link-action-declared").await;
    let _handler = spawn_handler(&device, script);
    device
        .device
        .lock()
        .expect("device lock")
        .declare_actions(&["locate", "hang"])
        .expect("names fit");
    assert!(
        wait_until(WAIT, || app.link_status().iter().any(|link| link.actions
            == vec![LinkDeviceActions {
                device: "imu-board".into(),
                names: vec!["locate".into(), "hang".into()],
            }]))
        .await,
        "the declared names show in the link status: {:?}",
        app.link_status()
    );

    app.run_action(provider_request("frob-2", "frobnicate"), "test")
        .expect("accepted");
    let done = settled(&app, "frob-2").await;
    match done.state {
        ActionState::Rejected { reason } => {
            assert!(
                reason.contains("imu-board")
                    && reason.ends_with("does not declare action `frobnicate`"),
                "reason: {reason}"
            );
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
    assert!(
        !device.saw(|e| matches!(e, DeviceEvent::ActionRequested)),
        "the device never had the request"
    );

    // A declared name still runs.
    app.run_action(
        provider_request("locate-2", "locate").with_arg("enabled", TypedConfigValue::Bool(true)),
        "test",
    )
    .expect("accepted");
    assert_eq!(
        settled(&app, "locate-2").await.state,
        ActionState::Succeeded
    );
    gateway.shutdown().await;
}
