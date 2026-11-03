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
    // The manifest decodes (so the snapshot format is the current one), but the applied section
    // does not: that is damaged state, which is quarantined and starts safe mode.
    let state_dir = temp_state_dir("safe-corrupt");
    let app = build(&state_dir, None);
    app.persist_state().expect("state should persist");
    drop(app);
    fs::write(state_dir.join("snapshot-applied.rkyv"), b"not-rkyv")
        .expect("corrupt applied section should be written");

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
        fs::read(quarantine.join("snapshot-applied.rkyv")).expect("quarantined file should exist"),
        b"not-rkyv"
    );
    assert!(!state_dir.join("snapshot-applied.rkyv").exists());

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

/// Rewrites the snapshot manifest of a persisted state directory with `rewrite`, then starts a node
/// over it and returns the node and the state directory.
fn start_over_rewritten_manifest(
    label: &str,
    rewrite: impl FnOnce(&std::path::Path),
) -> (NodeApp, PathBuf) {
    let state_dir = temp_state_dir(label);
    let app = build(&state_dir, None);
    app.persist_state().expect("state should persist");
    drop(app);
    rewrite(&state_dir);
    (build(&state_dir, None), state_dir)
}

/// The directories `unsupported-format-<ms>` under a state directory.
fn unsupported_format_dirs(state_dir: &std::path::Path) -> Vec<PathBuf> {
    fs::read_dir(state_dir)
        .expect("state dir should list")
        .map(|entry| entry.expect("entry should read").path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("unsupported-format-"))
        })
        .collect()
}

#[test]
fn snapshot_in_another_format_is_moved_aside_and_the_node_starts_fresh() {
    // A manifest that decodes but names another format (here a newer one) is moved aside.
    let (app, state_dir) = start_over_rewritten_manifest("safe-other-format", |state_dir| {
        let storage = NodeStorage::new(state_dir.to_path_buf());
        let mut manifest = storage
            .load_snapshot_manifest()
            .expect("manifest should load")
            .expect("manifest should exist");
        manifest.format_version = 99;
        fs::write(
            state_dir.join("snapshot-manifest.rkyv"),
            orion::encode_to_vec(&manifest).expect("manifest should encode"),
        )
        .expect("manifest should be written");
    });

    // Not safe mode: the node is up with an empty desired state and nothing was deleted.
    assert_eq!(app.safe_mode(), None);
    assert!(app.state_snapshot().state.desired.providers.is_empty());
    let moved = unsupported_format_dirs(&state_dir);
    assert_eq!(
        moved.len(),
        1,
        "one unsupported-format directory is expected"
    );
    assert!(moved[0].join("snapshot-manifest.rkyv").exists());
    assert!(!state_dir.join("snapshot-manifest.rkyv").exists());

    // The fresh state persists again and the moved files stay where they were.
    app.persist_state().expect("fresh state should persist");
    assert!(state_dir.join("snapshot-manifest.rkyv").exists());
    assert!(moved[0].join("snapshot-manifest.rkyv").exists());

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn undecodable_snapshot_manifest_is_moved_aside_and_the_node_starts_fresh() {
    // A manifest that does not decode with the current layout (an older layout, or damage).
    let (app, state_dir) = start_over_rewritten_manifest("safe-undecodable", |state_dir| {
        fs::write(
            state_dir.join("snapshot-manifest.rkyv"),
            b"not an archived manifest",
        )
        .expect("manifest should be written");
    });

    assert_eq!(app.safe_mode(), None);
    assert_eq!(unsupported_format_dirs(&state_dir).len(), 1);
    assert!(!state_dir.join("snapshot-manifest.rkyv").exists());

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
