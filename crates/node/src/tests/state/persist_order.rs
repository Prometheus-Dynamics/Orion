//! Snapshot writes commit in capture order: a persist that captured older state must not be
//! queued behind a newer write, or the manifest it writes names revisions the section files do not
//! hold (`load_snapshot_sections` then fails with "snapshot manifest revisions do not match").

use super::observed_coalescing::{coalescing_app, provider_update, register_provider_client};
use super::*;
use crate::app::set_after_capture_hook;
use crate::storage::NodeStorage;
use orion::control_plane::NodeRecord;
use std::{sync::mpsc, thread};

#[test]
fn older_persist_cannot_commit_over_a_newer_snapshot() {
    let state_dir = temp_state_dir("persist-capture-order");
    // A zero interval writes every observed change at once, without a coalescer.
    let app = coalescing_app("node.persist.order", state_dir.clone(), Duration::ZERO);
    let source = LocalAddress::new("coalesce-provider");
    register_provider_client(&app, &source);
    app.apply_local_control_message(&source, provider_update(&app, 0))
        .expect("the first provider state should apply");
    app.persist_state().expect("baseline persist");

    // Writer A captures the baseline, then stalls before it queues its bundle.
    let (captured_tx, captured_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let writer_a = {
        let app = app.clone();
        thread::spawn(move || {
            set_after_capture_hook(move || {
                captured_tx.send(()).expect("test should be waiting");
                let _ = release_rx.recv();
            });
            app.persist_state()
        })
    };
    captured_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("writer A should capture its bundle");

    // While A is stalled, a newer change moves the observed revision and writes.
    let (newer_done_tx, newer_done_rx) = mpsc::channel();
    let writer_b = {
        let app = app.clone();
        let source = source.clone();
        thread::spawn(move || {
            let mut desired = app.state_snapshot().state.desired;
            desired.put_node(NodeRecord::builder(NodeId::new("node.persist.order.extra")).build());
            app.replace_desired(desired);
            let result = app.apply_local_control_message(&source, provider_update(&app, 1));
            let _ = newer_done_tx.send(());
            result
        })
    };
    // The newer change may finish now or wait for A; either way A must not be overwritten.
    let _ = newer_done_rx.recv_timeout(Duration::from_secs(1));
    release_tx.send(()).expect("writer A should be waiting");

    writer_a
        .join()
        .expect("writer A thread should not panic")
        .expect("writer A should persist");
    writer_b
        .join()
        .expect("writer B thread should not panic")
        .expect("writer B should apply and persist");

    let storage = NodeStorage::new(&state_dir);
    let sections = storage
        .load_snapshot_sections()
        .expect("snapshot should load after both writes")
        .expect("snapshot should exist");
    let label = sections
        .observed
        .resources
        .get(&ResourceId::new("resource.coalesce.camera"))
        .and_then(|resource| resource.labels.first().cloned());
    // Observed state is not persisted, so no observed label survives the restart.
    assert_eq!(label, None);
    let _ = std::fs::remove_dir_all(state_dir);
}
