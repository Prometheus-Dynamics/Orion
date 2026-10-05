use super::*;
use crate::storage::DesiredStateSectionCounts;
use crate::{NodeApp, NodeConfig};
use orion::control_plane::{
    AppliedClusterState, ClockSourceKind, DesiredState, DesiredStateSectionFingerprints,
};
use std::path::PathBuf;

fn temp_dir(label: &str) -> PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "orion-migrate-v4-{label}-{}-{unique}",
        std::process::id()
    ))
}

fn v4_node(clock: bool) -> V4NodeRecord {
    V4NodeRecord {
        node_id: NodeId::new("node-a"),
        health: HealthState::Healthy,
        schedulable: true,
        labels: vec!["zone=north".into()],
        clock: clock.then(|| NodeClockFacts::unknown(42).with_source(ClockSourceKind::Ptp)),
    }
}

/// A format-4 directory: checkpoint at revision 1 (a stamped node), one history batch to 2.
fn write_format_4_state(storage: &NodeStorage) -> HlcTimestamp {
    storage.ensure_layout().expect("layout should be created");
    let stamp = HlcTimestamp::new(1_791_000_000_000, 0, orion::hlc_node_tag("node-a"));
    let mut checkpoint = V4DesiredClusterState {
        revision: Revision::new(1),
        ..V4DesiredClusterState::default()
    };
    checkpoint
        .nodes
        .insert(NodeId::new("node-a"), v4_node(false));
    checkpoint.stamps.nodes.insert(NodeId::new("node-a"), stamp);
    let history = vec![V4MutationBatch {
        base_revision: Revision::new(1),
        mutations: vec![V4DesiredStateMutation::PutWorkload(
            WorkloadRecord::builder("workload.pose", "graph.exec.v1", "artifact.pose")
                .desired_state(DesiredState::Stopped)
                .assigned_to("node-a")
                .build(),
        )],
        stamps: vec![stamp],
    }];
    let mut observed = V4ObservedClusterState {
        revision: Revision::new(3),
        ..V4ObservedClusterState::default()
    };
    observed.nodes.insert(NodeId::new("node-a"), v4_node(true));
    let manifest = SnapshotManifest {
        format_version: FORMAT_4,
        desired_snapshot_revision: Revision::new(1),
        latest_desired_revision: Revision::new(2),
        observed_revision: Revision::new(3),
        applied_revision: Revision::ZERO,
        desired_section_fingerprints: DesiredStateSectionFingerprints::default(),
        desired_section_counts: DesiredStateSectionCounts {
            nodes: 1,
            artifacts: 0,
            workloads: 0,
            tombstones: 0,
            resources: 0,
            providers: 0,
            executors: 0,
            leases: 0,
        },
    };
    let write = |path: PathBuf, bytes: Vec<u8>| std::fs::write(path, bytes).expect("written");
    write(
        storage.snapshot_manifest_path(),
        encode_to_vec(&manifest).expect("manifest encodes"),
    );
    write(
        storage.snapshot_desired_path(),
        encode_to_vec(&checkpoint).expect("desired encodes"),
    );
    write(
        storage.mutation_history_path(),
        encode_to_vec(&history).expect("history encodes"),
    );
    write(
        storage.snapshot_observed_path(),
        encode_to_vec(&observed).expect("observed encodes"),
    );
    write(
        storage.snapshot_applied_path(),
        encode_to_vec(&AppliedClusterState::default()).expect("applied encodes"),
    );
    stamp
}

#[test]
fn format_4_state_keeps_every_record_and_stamp() {
    let dir = temp_dir("ok");
    let storage = NodeStorage::new(&dir);
    let stamp = write_format_4_state(&storage);
    let tag = orion::hlc_node_tag("node-a");

    let report = storage
        .migrate_legacy_state(tag)
        .expect("migration should succeed")
        .expect("a format-4 directory is migrated");
    assert_eq!(report.from_format, FORMAT_4);
    assert_eq!(report.to_format, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(report.desired_revision, Revision::new(2));
    assert!(report.backup_dir.ends_with(FORMAT_4_BACKUP_DIR));
    assert!(report.backup_dir.join("snapshot-desired.rkyv").exists());
    assert_eq!(
        storage.migrate_legacy_state(tag).expect("second call"),
        None,
        "an upgraded directory is left alone"
    );

    let app = NodeApp::try_new(
        NodeConfig::for_local_node("node-a")
            .with_state_dir(&dir)
            .with_ipc_socket_path(dir.join("control.sock")),
    )
    .expect("node should start on the migrated state");
    let state = app.state_snapshot().state;
    assert_eq!(state.desired.revision, Revision::new(2));
    let node = &state.desired.nodes[&NodeId::new("node-a")];
    assert_eq!(node.labels, vec!["zone=north"]);
    assert_eq!(node.host, None);
    assert_eq!(state.desired.stamps.nodes[&NodeId::new("node-a")], stamp);
    assert_eq!(
        state.desired.stamps.workloads[&WorkloadId::new("workload.pose")],
        stamp,
        "the replayed history keeps its stamps"
    );
    assert_eq!(state.observed.revision, Revision::new(3));
    assert_eq!(
        state.observed.nodes[&NodeId::new("node-a")]
            .clock
            .as_ref()
            .map(|clock| clock.source.clone()),
        Some(ClockSourceKind::Ptp),
        "observed records are converted, not reset"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn unreadable_format_4_state_fails_startup_without_overwriting_it() {
    let dir = temp_dir("bad");
    let storage = NodeStorage::new(&dir);
    write_format_4_state(&storage);
    std::fs::write(storage.mutation_history_path(), b"not an archive").expect("written");
    let manifest_before = std::fs::read(storage.snapshot_manifest_path()).expect("readable");

    let err = NodeApp::try_new(
        NodeConfig::for_local_node("node-a")
            .with_state_dir(&dir)
            .with_ipc_socket_path(dir.join("control.sock")),
    )
    .err()
    .expect("startup must fail on state it cannot migrate");
    let message = err.to_string();
    assert!(message.contains("could not be migrated"), "{message}");
    assert_eq!(
        std::fs::read(storage.snapshot_manifest_path()).expect("readable"),
        manifest_before,
        "the old manifest is not overwritten"
    );
    let _ = std::fs::remove_dir_all(dir);
}
