//! Actions over a simulated byte stream: the host sends `ActionRequest`s, the device session
//! queues them for its application, and the application's reports come back as `ActionResult`s.

use orion_link::device::{ActionReportError, DeviceEvent};
use orion_link::host::{ActionSendError, HostEvent};
use orion_link::message::LinkActionState;
use orion_link::wire::ActionState;

use crate::sim::{StreamPair, host_config};

/// Arguments as the node encodes them: a postcard `Vec<(String, TypedConfigValue)>`.
fn args() -> Vec<u8> {
    vec![1, 3, b'f', b'o', b'o', 2, 0x0a]
}

fn connected_pair(seed: u64) -> StreamPair {
    let mut pair = StreamPair::new("imu-board", host_config(), seed);
    assert!(pair.run_until(2_000, StreamPair::connected), "handshake");
    pair
}

/// `(id, state, payload)` of every `ActionResult` the host has seen, in order.
fn results(pair: &StreamPair) -> Vec<(u32, LinkActionState, Vec<u8>)> {
    pair.host_events
        .iter()
        .filter_map(|(_, event)| match event {
            HostEvent::ActionResult {
                id, state, payload, ..
            } => Some((*id, *state, payload.clone())),
            _ => None,
        })
        .collect()
}

#[test]
fn request_runs_on_the_device_and_reports_come_back() {
    let mut pair = connected_pair(1);
    pair.host.send_action(7, "locate", &args()).unwrap();
    assert!(pair.run_until(500, |p| p.device.action().is_some()));

    let request = pair.device.action().unwrap();
    assert_eq!(request.id, 7);
    assert_eq!(request.name, "locate");
    assert_eq!(request.payload, args().as_slice());
    assert_eq!(pair.count_device(|e| *e == DeviceEvent::ActionRequested), 1);
    // The request bytes are gone once the device reports; the report is what the host waits for.
    assert!(pair.device.action().is_some());

    pair.device.report_progress(500).unwrap();
    assert!(pair.device.action().is_none());
    assert!(pair.run_until(500, |p| results(p).len() == 1));
    pair.device
        .report_action(ActionState::Succeeded, b"done")
        .unwrap();
    assert!(pair.run_until(500, |p| results(p).len() == 2));
    assert_eq!(
        results(&pair),
        vec![
            // Progress is a per-mille varint: 500 is `0xF4 0x03`.
            (7, LinkActionState::Running, vec![0xF4, 0x03]),
            (7, LinkActionState::Succeeded, b"done".to_vec()),
        ]
    );
}

#[test]
fn one_action_at_a_time_and_the_slot_frees_after_the_final_result() {
    let mut pair = connected_pair(2);
    pair.host.send_action(1, "reboot", &[]).unwrap();
    assert!(pair.run_until(500, |p| p.device.action().is_some()));
    // A second request while the first runs is refused as busy; the first is unaffected.
    pair.host.send_action(2, "locate", &[]).unwrap();
    assert!(pair.run_until(500, |p| results(p).iter().any(|(id, ..)| *id == 2)));
    assert_eq!(
        results(&pair),
        vec![(2, LinkActionState::Rejected, b"busy".to_vec())]
    );
    assert_eq!(pair.device.action().map(|a| a.id), Some(1));

    pair.device
        .report_action(ActionState::Failed, b"jammed")
        .unwrap();
    assert!(pair.run_until(500, |p| results(p).len() == 2));
    assert_eq!(
        results(&pair)[1],
        (1, LinkActionState::Failed, b"jammed".to_vec())
    );

    // The device is free again once its final result was sent.
    pair.host.send_action(3, "locate", &[]).unwrap();
    assert!(pair.run_until(500, |p| p.device.action().is_some()));
    assert_eq!(pair.device.action().map(|a| a.id), Some(3));
}

#[test]
fn reports_without_a_running_action_or_after_a_final_one_are_refused() {
    let mut pair = connected_pair(3);
    assert_eq!(
        pair.device.report_action(ActionState::Succeeded, &[]),
        Err(ActionReportError::NoAction)
    );

    pair.host.send_action(4, "locate", &[]).unwrap();
    assert!(pair.run_until(500, |p| p.device.action().is_some()));
    pair.device
        .report_action(ActionState::Succeeded, &[])
        .unwrap();
    assert_eq!(
        pair.device.report_progress(10),
        Err(ActionReportError::Finished)
    );
}

#[test]
fn a_request_for_a_host_that_is_not_connected_is_not_queued() {
    let mut pair = StreamPair::new("imu-board", host_config(), 4);
    assert_eq!(
        pair.host.send_action(1, "locate", &[]),
        Err(ActionSendError::NotConnected)
    );
}
