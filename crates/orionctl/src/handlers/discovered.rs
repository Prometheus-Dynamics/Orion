//! `orionctl get discovered-peers`, `orionctl peers enroll <node-id>` and `orionctl peers remove`
//! (`docs/discovery.md`).

use std::io::{BufRead, IsTerminal, Write};

use clap::Args;
use orion_client::LocalControlPlaneClient;
use orion_control_plane::{DiscoveredPeerEnrollment, DiscoveredPeerRecord, DiscoverySnapshot};
use orion_core::NodeId;

use crate::cli::{LocalControlArgs, OutputFormat};
use crate::render::{join_display, print_structured};

#[derive(Args, Clone, Debug)]
pub(crate) struct PeerRemoveArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    /// The peer to remove.
    pub(crate) node_id: String,
}

pub(super) async fn get_discovered_peers(args: LocalControlArgs) -> Result<(), String> {
    let snapshot = args
        .client()?
        .query_discovery()
        .await
        .map_err(|error| error.to_string())?;
    match args.output {
        OutputFormat::Summary => {
            print!("{}", render_discovery_summary(&snapshot, now_ms()));
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
            print_structured(&snapshot, args.output)
        }
        OutputFormat::Metrics => {
            Err("metrics output is supported only for observability views".to_owned())
        }
    }
}

/// Shows the advertised key of a discovered peer, asks for confirmation, then pins exactly that
/// key (the node refuses if the advertisement changed in between).
pub(super) async fn enroll_discovered_peer(
    client: &LocalControlPlaneClient,
    node_id: &str,
    fingerprint: Option<String>,
    yes: bool,
) -> Result<(), String> {
    let snapshot = client
        .query_discovery()
        .await
        .map_err(|error| error.to_string())?;
    if snapshot.backend.is_empty() {
        return Err(
            "peer discovery is not running on this node (ORION_NODE_DISCOVERY=mdns)".into(),
        );
    }
    let peer = snapshot
        .peers
        .iter()
        .find(|peer| peer.node_id.as_str() == node_id)
        .ok_or_else(|| {
            format!("peer {node_id} has not been discovered; see `orionctl get discovered-peers`")
        })?;
    println!(
        "peer {} cluster={} urls={}\n  key fingerprint {}\n  public key      {}",
        peer.node_id,
        peer.cluster,
        join_display(&peer.peer_urls),
        peer.key_fingerprint,
        peer.public_key_hex
    );
    let expected = match fingerprint {
        Some(expected) if expected == peer.key_fingerprint => expected,
        Some(expected) => {
            return Err(format!(
                "the advertised key fingerprint {} does not match {expected}; not enrolling",
                peer.key_fingerprint
            ));
        }
        None if yes => peer.key_fingerprint.clone(),
        None => {
            confirm(&format!(
                "Compare the fingerprint with `orionctl get discovered-peers` on {node_id} \
                 (local fingerprint). Trust this key? [y/N] "
            ))?;
            peer.key_fingerprint.clone()
        }
    };
    client
        .enroll_discovered_peer(DiscoveredPeerEnrollment {
            node_id: NodeId::new(node_id),
            expected_key_fingerprint: Some(expected),
        })
        .await
        .map_err(|error| error.to_string())?;
    println!("peers enroll accepted: {node_id} is trusted and registered for sync");
    Ok(())
}

pub(super) fn confirm(prompt: &str) -> Result<(), String> {
    if !std::io::stdin().is_terminal() {
        return Err(
            "refusing to trust a key without confirmation; pass --fingerprint <sha256:...> or --yes"
                .into(),
        );
    }
    print!("{prompt}");
    std::io::stdout()
        .flush()
        .map_err(|error| error.to_string())?;
    let mut answer = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut answer)
        .map_err(|error| error.to_string())?;
    if matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        Ok(())
    } else {
        Err("not enrolled".into())
    }
}

pub(super) async fn remove_peer(args: PeerRemoveArgs) -> Result<(), String> {
    args.local
        .client()?
        .remove_peer(NodeId::new(args.node_id.clone()))
        .await
        .map_err(|error| error.to_string())?;
    println!(
        "peers remove accepted: {} is revoked and no longer synced",
        args.node_id
    );
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn render_discovery_summary(snapshot: &DiscoverySnapshot, now_ms: u64) -> String {
    if !snapshot.metrics.enabled {
        return "discovery enabled=false (set ORION_NODE_DISCOVERY=mdns)\n".to_owned();
    }
    let metrics = &snapshot.metrics;
    let mut out = format!(
        "discovery backend={} cluster={} local_fingerprint={} enrollment_key={} discovered={} \
         enrolled={} announcements={} ignored={} expired={} enrollment_attempts={} \
         enrollment_successes={} enrollment_failures={}\n",
        snapshot.backend,
        snapshot.cluster,
        snapshot.local_key_fingerprint,
        snapshot.enrollment_key_configured,
        metrics.discovered_peers,
        metrics.enrolled_peers,
        metrics.announcements_received,
        metrics.announcements_ignored,
        metrics.peers_expired,
        metrics.enrollment_attempts,
        metrics.enrollment_successes,
        metrics.enrollment_failures,
    );
    for peer in &snapshot.peers {
        out.push_str(&render_peer(peer, now_ms));
    }
    out
}

fn render_peer(peer: &DiscoveredPeerRecord, now_ms: u64) -> String {
    format!(
        "peer node={} state={} fingerprint={} protocol=v{} urls={} last_seen_ms_ago={} \
         expires_in_ms={} last_enrollment_error={}\n",
        peer.node_id,
        peer.state,
        peer.key_fingerprint,
        peer.control_protocol_version,
        join_display(&peer.peer_urls),
        now_ms.saturating_sub(peer.last_seen_at_ms),
        peer.expires_at_ms.saturating_sub(now_ms),
        peer.last_enrollment_error.as_deref().unwrap_or("-"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_control_plane::{DiscoveredPeerState, DiscoveryMetricsSnapshot};
    use orion_core::{PeerBaseUrl, PublicKeyHex};

    #[test]
    fn summary_lists_counters_and_one_line_per_peer() {
        let snapshot = DiscoverySnapshot {
            metrics: DiscoveryMetricsSnapshot {
                enabled: true,
                discovered_peers: 1,
                enrollment_attempts: 2,
                enrollment_failures: 1,
                enrollment_successes: 1,
                ..DiscoveryMetricsSnapshot::default()
            },
            backend: "mdns".into(),
            cluster: "lab".into(),
            local_key_fingerprint: "sha256:aa".into(),
            enrollment_key_configured: true,
            peers: vec![DiscoveredPeerRecord {
                node_id: NodeId::new("node-b"),
                cluster: "lab".into(),
                public_key_hex: PublicKeyHex::new("00".repeat(32)),
                key_fingerprint: "sha256:bb".into(),
                peer_urls: vec![PeerBaseUrl::new("orion+tcp://10.0.0.2:9200")],
                control_protocol_version: 3,
                state: DiscoveredPeerState::Discovered,
                first_seen_at_ms: 1_000,
                last_seen_at_ms: 1_500,
                expires_at_ms: 3_000,
                last_enrollment_error: None,
            }],
        };
        let text = render_discovery_summary(&snapshot, 2_000);
        assert!(text.starts_with("discovery backend=mdns cluster=lab local_fingerprint=sha256:aa"));
        assert!(text.contains("enrollment_attempts=2"));
        assert!(text.contains(
            "peer node=node-b state=discovered fingerprint=sha256:bb protocol=v3 \
             urls=orion+tcp://10.0.0.2:9200 last_seen_ms_ago=500 expires_in_ms=1000"
        ));
        let disabled = render_discovery_summary(&DiscoverySnapshot::default(), 0);
        assert!(disabled.starts_with("discovery enabled=false"));
    }
}
