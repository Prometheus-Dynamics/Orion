//! Multi-node tests of peer sync and per-object conflict resolution (`docs/peer-sync.md`).
//!
//! `conflicts` drives the transport-independent engine over an in-memory transport;
//! `peer_tcp` runs real `orion+tcp` listeners (no HTTP feature needed).

use super::*;
use crate::app::PeerSyncTransport;
use crate::{ControlRequest, ControlResponse, NodeError};
use orion::{
    HlcTimestamp,
    control_plane::{DesiredObjectKey, DesiredStateMutation},
};

#[cfg(feature = "peer-tcp")]
mod action_calls;
#[cfg(feature = "peer-tcp")]
mod binding;
mod conflicts;
#[cfg(feature = "peer-tcp")]
mod fixtures;
#[cfg(all(feature = "transport-http", feature = "peer-tcp"))]
mod mixed;
#[cfg(feature = "peer-tcp")]
mod peer_tcp;
#[cfg(feature = "peer-tcp")]
mod placement;
#[cfg(feature = "peer-tcp")]
mod remote_operator;
#[cfg(feature = "peer-tcp")]
mod remote_operator_actions;

/// Delivers peer requests by calling the remote node's control pipeline directly (including
/// peer authentication), so the sync engine runs without sockets.
struct MemoryTransport {
    remote: NodeApp,
}

impl PeerSyncTransport for MemoryTransport {
    fn label(&self) -> &'static str {
        "memory"
    }

    async fn exchange(
        &self,
        app: &NodeApp,
        _node_id: &NodeId,
        request: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, NodeError> {
        let request = app.security.wrap_http_payload_async(request).await?;
        match self
            .remote
            .serve_control_request(ControlRequest::from_http_payload(request))?
        {
            ControlResponse::Http(response) => Ok(*response),
            ControlResponse::Local(_) => Err(NodeError::Storage(
                "local response on the peer surface".into(),
            )),
        }
    }
}

/// Small deterministic PRNG (SplitMix64) so property tests are reproducible from their seed.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound.max(1) as u64) as usize
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for index in (1..items.len()).rev() {
            items.swap(index, self.below(index + 1));
        }
    }
}

fn cluster_node(node_id: &'static str, tuning: impl FnOnce(NodeConfig) -> NodeConfig) -> NodeApp {
    NodeApp::builder()
        .config(tuning(test_node_config_with_auth(
            node_id,
            "node-cluster",
            crate::PeerAuthenticationMode::Optional,
        )))
        .try_build()
        .expect("node app should build")
}

/// Builds `ids.len()` nodes that know each other as peers (for the in-memory transport).
fn memory_cluster(ids: &[&'static str]) -> Vec<NodeApp> {
    let nodes: Vec<_> = ids.iter().map(|id| cluster_node(id, |c| c)).collect();
    for node in &nodes {
        for peer in &nodes {
            if peer.config.node_id != node.config.node_id {
                node.register_peer(PeerConfig::new(
                    peer.config.node_id.clone(),
                    "http://memory.invalid:1",
                ))
                .expect("peer registration should succeed");
            }
        }
    }
    nodes
}

/// One sync round from `local` to `remote` over the in-memory transport.
async fn memory_round(local: &NodeApp, remote: &NodeApp) {
    let remote_id = remote.config.node_id.clone();
    let peer = local
        .peer_state_for_test(&remote_id)
        .expect("peer should be registered");
    local
        .sync_peer_over(
            &remote_id,
            &peer,
            &MemoryTransport {
                remote: remote.clone(),
            },
        )
        .await
        .expect("in-memory sync round should succeed");
}

/// Syncs every ordered pair until all nodes report the same desired fingerprint.
async fn converge_memory(nodes: &[NodeApp]) {
    for _ in 0..4 {
        for local in nodes {
            for remote in nodes {
                if local.config.node_id != remote.config.node_id {
                    memory_round(local, remote).await;
                }
            }
        }
        if all_converged(nodes) {
            return;
        }
    }
    panic!("cluster did not converge");
}

fn fingerprint(node: &NodeApp) -> u64 {
    node.desired_metadata_for_test()
        .expect("desired metadata should compute")
        .1
}

fn all_converged(nodes: &[NodeApp]) -> bool {
    nodes
        .windows(2)
        .all(|pair| fingerprint(&pair[0]) == fingerprint(&pair[1]))
}

fn assert_all_equal(nodes: &[NodeApp]) -> orion::control_plane::DesiredClusterState {
    let first = desired_content(&nodes[0].state_snapshot().state.desired);
    for node in &nodes[1..] {
        assert_eq!(
            desired_content(&node.state_snapshot().state.desired),
            first,
            "{} and {} disagree",
            nodes[0].config.node_id,
            node.config.node_id
        );
    }
    first
}

fn artifact(id: &str, size: u64) -> orion::control_plane::ArtifactRecord {
    orion::control_plane::ArtifactRecord::builder(ArtifactId::new(id))
        .size_bytes(size)
        .build()
}

/// Local write through the public API; returns the version it produced, if any.
fn write_local(
    node: &NodeApp,
    mutation: DesiredStateMutation,
) -> Option<(DesiredStateMutation, HlcTimestamp)> {
    let key = mutation.key();
    let mut desired = node.state_snapshot().state.desired;
    let before = desired.version_of(&key);
    orion::control_plane::MutationBatch::new(desired.revision, vec![mutation.clone()])
        .apply_to(&mut desired);
    node.replace_desired(desired);
    let after = node.state_snapshot().state.desired.version_of(&key)?;
    (Some(after) != before).then_some((mutation, after.stamp))
}

fn delete(key: &DesiredObjectKey) -> DesiredStateMutation {
    key.remove_mutation()
}
