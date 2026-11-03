//! Actions over a simulated byte stream: the host sends `ActionRequest`s, the device session
//! queues them for its application, and the application's reports come back as `ActionResult`s.

use orion_link::device::{ActionReportError, DeviceEvent};
use orion_link::host::{ActionSendError, HostEvent};
use orion_link::message::LinkActionState;
use orion_link::wire::{ActionState, kind};

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

/// Action names the host has seen the device declare, one entry per `ActionCatalog` event.
fn catalogs(pair: &StreamPair) -> Vec<Vec<String>> {
    pair.host_events
        .iter()
        .filter_map(|(_, event)| match event {
            HostEvent::ActionCatalog { names, .. } => Some(names.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn cancel_reaches_a_busy_device_and_the_action_reports_cancelled() {
    let mut pair = connected_pair(5);
    pair.host.send_action(9, "reboot", &[]).unwrap();
    assert!(pair.run_until(500, |p| p.device.action().is_some()));

    // The device is busy, which refuses every other request, but a cancel for its action is
    // accepted: it reaches the application as an event carrying the id.
    pair.host.send_action_cancel(9).unwrap();
    assert!(pair.run_until(500, |p| {
        p.count_device(|e| *e == DeviceEvent::ActionCancelRequested { id: 9 }) == 1
    }));
    assert_eq!(pair.device.action().map(|a| a.id), Some(9));

    pair.device
        .report_action(ActionState::Cancelled, &[])
        .unwrap();
    assert!(pair.run_until(500, |p| results(p).len() == 1));
    assert_eq!(
        results(&pair),
        vec![(9, LinkActionState::Cancelled, vec![])]
    );

    // The device is free again: the slot frees after the final result, as for any action.
    pair.host.send_action(10, "locate", &[]).unwrap();
    assert!(pair.run_until(500, |p| p.device.action().is_some()));
    assert_eq!(pair.device.action().map(|a| a.id), Some(10));
}

#[test]
fn cancel_for_another_or_a_finished_action_is_ignored() {
    let mut pair = connected_pair(6);
    pair.host.send_action(1, "reboot", &[]).unwrap();
    assert!(pair.run_until(500, |p| p.device.action().is_some()));

    pair.host.send_action_cancel(2).unwrap();
    pair.run(200);
    assert_eq!(
        pair.count_device(|e| matches!(e, DeviceEvent::ActionCancelRequested { .. })),
        0,
        "a cancel for another id is not the running action"
    );
    assert_eq!(pair.device.action().map(|a| a.id), Some(1));

    pair.device
        .report_action(ActionState::Succeeded, &[])
        .unwrap();
    assert!(pair.run_until(500, |p| results(p).len() == 1));
    pair.host.send_action_cancel(1).unwrap();
    pair.run(200);
    assert_eq!(
        pair.count_device(|e| matches!(e, DeviceEvent::ActionCancelRequested { .. })),
        0,
        "a cancel after the final result is not queued"
    );
}

#[test]
fn declared_action_names_reach_the_host_and_stop_once_acknowledged() {
    let mut pair = connected_pair(7);
    pair.device.declare_actions(&["locate", "flash"]).unwrap();
    assert!(pair.run_until(500, |p| catalogs(p).len() == 1));
    assert_eq!(
        catalogs(&pair),
        vec![vec!["locate".to_owned(), "flash".to_owned()]]
    );

    // Acknowledged: no further transmissions of the catalog, however long the link stays up.
    let sent = pair
        .up
        .sent_kinds
        .iter()
        .filter(|k| **k == kind::ACTION_CATALOG)
        .count();
    pair.run(2_000);
    assert_eq!(
        pair.up
            .sent_kinds
            .iter()
            .filter(|k| **k == kind::ACTION_CATALOG)
            .count(),
        sent
    );
    assert_eq!(catalogs(&pair).len(), 1, "a repeat is not reported again");
}

#[test]
fn a_lost_catalog_is_retransmitted_until_the_host_acknowledges_it() {
    let mut pair = connected_pair(8);
    pair.up.drop_kinds = vec![kind::ACTION_CATALOG];
    pair.device.declare_actions(&["locate"]).unwrap();
    assert!(pair.run_until(2_000, |p| catalogs(p).len() == 1));
    assert_eq!(catalogs(&pair), vec![vec!["locate".to_owned()]]);
    let attempts = pair
        .up
        .sent_kinds
        .iter()
        .filter(|k| **k == kind::ACTION_CATALOG)
        .count();
    assert_eq!(attempts, 2, "the dropped frame was sent again once");
}

#[test]
fn withdrawing_the_declaration_sends_nothing_and_an_oversized_list_is_refused() {
    let mut pair = connected_pair(9);
    pair.device.declare_actions(&[]).unwrap();
    pair.run(500);
    assert!(catalogs(&pair).is_empty(), "no names declared, no catalog");

    let long = "x".repeat(600);
    assert!(matches!(
        pair.device.declare_actions(&[long.as_str()]),
        Err(orion_link::device::PublishError::TooLarge { .. })
    ));
}
