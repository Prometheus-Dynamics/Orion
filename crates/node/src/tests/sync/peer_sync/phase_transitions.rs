//! Message sequence of one sync round (hello, summary, push, pull) against a real peer node whose
//! HTTP surface records every request it serves.

use super::*;

#[derive(Clone)]
struct SequencePeer {
    events: Arc<Mutex<Vec<&'static str>>>,
    remote: NodeApp,
}

impl HttpControlHandler for SequencePeer {
    fn handle_payload(
        &self,
        payload: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        let message = peer_control_message(payload)?;
        self.events
            .lock()
            .expect("sequence log should not be poisoned")
            .push(match &message {
                ControlMessage::Hello(_) => "hello",
                ControlMessage::SyncSummaryRequest(_) => "summary",
                ControlMessage::Mutations(_) => "push",
                ControlMessage::SyncDiffRequest(_) => "pull",
                other => panic!("unexpected peer-sync control message: {other:?}"),
            });
        self.remote
            .apply_control_message(None, message)
            .map_err(|err| HttpTransportError::request_failed(err.to_string()))
    }
}

fn test_node(node_id: &'static str) -> NodeApp {
    NodeApp::builder()
        .config(test_node_config_with_auth(
            node_id,
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build")
}

fn put_artifact(node: &NodeApp, id: &'static str) {
    let mut desired = node.state_snapshot().state.desired;
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder(id).build());
    node.replace_desired(desired);
}

/// Runs one round from `local` against `remote` and returns the requests `remote` served.
async fn sync_round(local: &NodeApp, remote: &NodeApp) -> Vec<&'static str> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("socket address should parse"),
        Arc::new(SequencePeer {
            events: events.clone(),
            remote: remote.clone(),
        }),
    )
    .await
    .expect("sequence HTTP server should bind");
    let server_task = tokio::spawn(server.serve(listener));
    let peer_id = remote.config.node_id.clone();
    let _ = local.enroll_peer(PeerConfig::new(
        peer_id.clone(),
        orion_core::PeerBaseUrl::new(format!("http://{addr}")),
    ));
    local
        .sync_peer(&peer_id)
        .await
        .expect("peer sync should succeed");
    server_task.abort();
    let events = events.lock().expect("sequence log should not be poisoned");
    events.clone()
}

fn assert_converged(left: &NodeApp, right: &NodeApp) {
    assert_eq!(
        desired_content(&left.state_snapshot().state.desired),
        desired_content(&right.state_snapshot().state.desired)
    );
}

#[tokio::test]
async fn node_sync_peer_phase_equal_state_stops_after_hello() {
    let (local, remote) = (test_node("node-b"), test_node("node-a"));
    assert_eq!(sync_round(&local, &remote).await, vec!["hello"]);
    let peer = local
        .peer_state_for_test(&NodeId::new("node-a"))
        .expect("peer state should exist");
    assert_eq!(peer.sync_status, PeerSyncStatus::Synced);
}

#[tokio::test]
async fn node_sync_peer_phase_remote_only_changes_are_pulled() {
    let (local, remote) = (test_node("node-b"), test_node("node-a"));
    put_artifact(&remote, "artifact.remote");
    assert_eq!(
        sync_round(&local, &remote).await,
        vec!["hello", "summary", "pull"]
    );
    assert_converged(&local, &remote);
}

#[tokio::test]
async fn node_sync_peer_phase_local_only_changes_are_pushed() {
    let (local, remote) = (test_node("node-b"), test_node("node-a"));
    put_artifact(&local, "artifact.local");
    assert_eq!(
        sync_round(&local, &remote).await,
        vec!["hello", "summary", "push"]
    );
    assert_converged(&local, &remote);
}

#[tokio::test]
async fn node_sync_peer_phase_divergent_changes_are_pushed_and_pulled_in_one_round() {
    let (local, remote) = (test_node("node-b"), test_node("node-a"));
    put_artifact(&local, "artifact.local");
    put_artifact(&remote, "artifact.remote");
    assert_eq!(
        sync_round(&local, &remote).await,
        vec!["hello", "summary", "push", "pull"]
    );
    assert_converged(&local, &remote);
    // A second round has nothing left to do.
    assert_eq!(sync_round(&local, &remote).await, vec!["hello"]);
}
