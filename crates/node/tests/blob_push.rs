//! Blob push over real local IPC (`docs/blobs.md`): a client pushes a multi-chunk file to the node
//! with `LocalControlPlaneClient::push_blob`, the node verifies it and places it in its blob
//! directory, a second push finds it committed, and a wrong digest is rejected.
#![cfg(unix)]

use orion::client::LocalControlPlaneClient;
use orion::control_plane::BlobState;
use orion_node::{NodeApp, NodeConfig, NodeId};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

fn scratch(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-blob-e2e-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn hex_digest(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_is_pushed_in_chunks_verified_and_placed() {
    let dir = scratch("push");
    let socket = dir.join("control.sock");
    let state = dir.join("state");
    let app = NodeApp::try_new(
        NodeConfig::for_local_node(NodeId::new("node-blobs"))
            .with_ipc_socket_path(socket.clone())
            .with_state_dir(state.clone()),
    )
    .expect("node app should build");
    let (_, server) = app
        .start_ipc_server_graceful(&socket)
        .await
        .expect("ipc server should start");

    // 2.5 MiB in a file the client reads in 1 MiB chunks.
    let data: Vec<u8> = (0..(5 * 512 * 1024u32))
        .map(|i| (i.wrapping_mul(31) % 251) as u8)
        .collect();
    let path = dir.join("image.img");
    std::fs::write(&path, &data).expect("write the image");
    let sha = hex_digest(&data);

    let client = LocalControlPlaneClient::connect_at(&socket, "operator").expect("client");
    let committed = client
        .push_blob("image-1", &path, &sha, 1024 * 1024)
        .await
        .expect("push succeeds");
    assert_eq!(committed.state, BlobState::Committed, "{committed:?}");
    assert_eq!(committed.received, data.len() as u64);
    let placed = committed.path.expect("committed blobs have a path");
    assert_eq!(std::fs::read(&placed).expect("placed file"), data);
    assert!(Path::new(&placed).starts_with(state.join("blobs")));

    // Pushing it again finds the committed blob: nothing is sent twice.
    let again = client
        .push_blob("image-1", &path, &sha, 1024 * 1024)
        .await
        .expect("second push answers");
    assert_eq!(again.state, BlobState::Committed);

    // A wrong digest is rejected after the bytes arrive, and nothing is placed.
    let wrong = client
        .push_blob("image-2", &path, &"0".repeat(64), 512 * 1024)
        .await
        .expect("the push completes");
    assert!(
        matches!(&wrong.state, BlobState::Rejected { reason } if reason.contains("mismatch")),
        "{wrong:?}"
    );
    assert!(!state.join("blobs").join("image-2").exists());

    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}
