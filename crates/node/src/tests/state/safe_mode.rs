use super::*;
use crate::SafeModeCause;
use orion::control_plane::{MaintenanceAction, MaintenanceCommand, MaintenanceMode};

fn safe_mode_config(state_dir: PathBuf) -> NodeConfig {
    test_node_config_with_state_dir_and_auth(
        "node-a",
        "node-test",
        state_dir,
        crate::PeerAuthenticationMode::Optional,
    )
}

fn build(state_dir: &std::path::Path, safe_mode: Option<bool>) -> NodeApp {
    let mut builder = NodeApp::builder().config(safe_mode_config(state_dir.to_path_buf()));
    if let Some(enabled) = safe_mode {
        builder = builder.with_safe_mode(enabled);
    }
    builder.try_build().expect("node app should build")
}

#[test]
fn corrupt_state_is_quarantined_and_the_node_starts_in_safe_mode() {
    let state_dir = temp_state_dir("safe-corrupt");
    fs::create_dir_all(&state_dir).expect("state dir should be created");
    fs::write(state_dir.join("snapshot-manifest.rkyv"), b"not-rkyv")
        .expect("corrupt manifest should be written");

    let app = build(&state_dir, None);

    let Some(SafeModeCause::StateQuarantined { quarantine, .. }) = app.safe_mode().cloned() else {
        panic!("expected quarantined state, got {:?}", app.safe_mode());
    };
    assert!(quarantine.starts_with(&state_dir));
    assert!(
        quarantine
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("quarantine-"))
    );
    assert_eq!(
        fs::read(quarantine.join("snapshot-manifest.rkyv")).expect("quarantined file should exist"),
        b"not-rkyv"
    );
    assert!(!state_dir.join("snapshot-manifest.rkyv").exists());

    let health = app.health_snapshot();
    assert!(
        health
            .reasons
            .iter()
            .any(|reason| reason.starts_with("safe_mode: state quarantined to")),
        "health reasons were {:?}",
        health.reasons
    );
    assert_eq!(
        app.maintenance_status().state.mode,
        MaintenanceMode::Maintenance
    );
    assert!(app.state_snapshot().state.desired.providers.is_empty());

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn newer_snapshot_format_is_quarantined_not_read() {
    let state_dir = temp_state_dir("safe-newer");
    let app = build(&state_dir, None);
    app.persist_state().expect("state should persist");
    drop(app);

    let storage = NodeStorage::new(state_dir.clone());
    let mut manifest = storage
        .load_snapshot_manifest()
        .expect("manifest should load")
        .expect("manifest should exist");
    manifest.format_version = 99;
    fs::write(
        state_dir.join("snapshot-manifest.rkyv"),
        orion::encode_to_vec(&manifest).expect("manifest should encode"),
    )
    .expect("newer manifest should be written");

    let app = build(&state_dir, None);
    match app.safe_mode() {
        Some(SafeModeCause::StateQuarantined { reason, .. }) => {
            assert!(reason.contains("newer"), "reason was {reason}");
        }
        other => panic!("expected quarantine for a newer format, got {other:?}"),
    }

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn safe_mode_env_request_blocks_maintenance_and_makes_sync_receive_only() {
    let state_dir = temp_state_dir("safe-requested");
    let app = build(&state_dir, Some(true));

    assert_eq!(app.safe_mode(), Some(&SafeModeCause::Requested));
    assert!(app.peer_sync_receive_only());
    assert!(!app.peer_sync_disabled());
    assert!(
        app.health_snapshot()
            .reasons
            .iter()
            .any(|reason| reason == "safe_mode: requested by ORION_NODE_SAFE_MODE")
    );

    let err = app
        .update_maintenance(MaintenanceCommand {
            action: MaintenanceAction::Enter,
            allow_runtime_types: Vec::new(),
            allow_workload_ids: Vec::new(),
        })
        .expect_err("maintenance changes should be refused in safe mode");
    assert!(matches!(err, NodeError::Config(_)));

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn safe_mode_persists_until_maintenance_exit_and_restart() {
    let state_dir = temp_state_dir("safe-persist");
    drop(build(&state_dir, Some(true)));

    // Restart without the request: the marker keeps the node in safe mode.
    let app = build(&state_dir, Some(false));
    assert!(matches!(app.safe_mode(), Some(SafeModeCause::Persisted(_))));
    app.update_maintenance(MaintenanceCommand {
        action: MaintenanceAction::Exit,
        allow_runtime_types: Vec::new(),
        allow_workload_ids: Vec::new(),
    })
    .expect("exit should clear the marker");
    drop(app);

    let app = build(&state_dir, Some(false));
    assert!(app.safe_mode().is_none());
    assert_eq!(app.maintenance_status().state.mode, MaintenanceMode::Normal);
    assert!(!app.peer_sync_receive_only());

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn unreadable_identity_gives_safe_mode_without_peer_sync_and_keeps_the_key_file() {
    let state_dir = temp_state_dir("safe-identity");
    drop(build(&state_dir, None));

    let identity_path = NodeStorage::new(state_dir.clone()).identity_path();
    fs::write(&identity_path, b"short").expect("corrupt identity should be written");

    let app = build(&state_dir, None);
    assert!(matches!(
        app.safe_mode(),
        Some(SafeModeCause::IdentityUnreadable { .. })
    ));
    assert!(app.peer_sync_disabled());
    assert_eq!(
        fs::read(&identity_path).expect("identity file should still exist"),
        b"short"
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn safe_mode_is_reported_by_readiness_health_and_metrics() {
    let state_dir = temp_state_dir("safe-report");
    let app = NodeApp::builder()
        .config(safe_mode_config(state_dir.clone()))
        .with_safe_mode(true)
        .try_build()
        .expect("node app should build");

    let reason = "safe_mode: requested by ORION_NODE_SAFE_MODE";
    let readiness = app.readiness_snapshot();
    assert!(!readiness.ready, "a node in safe mode is not ready");
    assert!(
        readiness.reasons.iter().any(|entry| entry == reason),
        "readiness reasons were {:?}",
        readiness.reasons
    );
    assert!(
        app.health_snapshot()
            .reasons
            .iter()
            .any(|entry| entry == reason)
    );
    assert!(
        app.safe_mode_metrics()
            .contains("orion_safe_mode{node_id=\"node-a\",cause=\"requested\"} 1\n"),
        "{}",
        app.safe_mode_metrics()
    );

    let _ = fs::remove_dir_all(state_dir);
}
