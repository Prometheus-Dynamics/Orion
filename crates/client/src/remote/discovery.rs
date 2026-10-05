//! Browsing for nodes with mDNS/DNS-SD (feature `discovery`).
//!
//! Nodes that run discovery (`ORION_NODE_DISCOVERY=mdns`) advertise `_orion._tcp` with their node
//! id, public key, cluster and `orion+tcp` port; the TXT layout and its parser are shared with
//! `orion-node` (`orion_auth::discovery`). mDNS is unauthenticated: an advertisement is a hint.
//! Connect with [`super::NodeTrust::Key`] only after checking the advertised fingerprint out of
//! band, or authenticate the node with [`super::RemoteOperator::enroll_with_key`].

use super::RemoteError;
use mdns_sd::{ServiceDaemon, ServiceEvent};
pub use orion_auth::discovery::SERVICE_TYPE;
use orion_auth::{crypto::key_fingerprint, discovery::Advertisement};
use orion_core::{CONTROL_PROTOCOL_VERSION, PeerBaseUrl};
use std::{
    collections::BTreeMap,
    net::IpAddr,
    time::{Duration, Instant},
};

/// A node found on the local network.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveredNode {
    pub advertisement: Advertisement,
    /// `sha256:` fingerprint of the advertised key.
    pub key_fingerprint: String,
    /// Peer URLs built from the advertised addresses: `orion+tcp://` first.
    pub urls: Vec<PeerBaseUrl>,
    /// Whether the node speaks this client's control protocol version.
    pub compatible: bool,
}

impl DiscoveredNode {
    /// The first `orion+tcp://` URL, which [`super::RemoteOperator::connect`] accepts.
    pub fn orion_tcp_url(&self) -> Option<&PeerBaseUrl> {
        self.urls.iter().find(|url| {
            url.as_str()
                .starts_with(orion_auth::peer_tcp::PEER_TCP_SCHEME)
        })
    }
}

/// Browses `_orion._tcp` for `duration` and returns the nodes that answered, by node id. When
/// `cluster` is set, nodes of other clusters are skipped. Runs the blocking mDNS daemon on a
/// blocking thread.
pub async fn browse_nodes(
    duration: Duration,
    cluster: Option<&str>,
) -> Result<Vec<DiscoveredNode>, RemoteError> {
    let cluster = cluster.map(str::to_owned);
    tokio::task::spawn_blocking(move || browse_blocking(duration, cluster.as_deref()))
        .await
        .map_err(|err| RemoteError::Discovery(err.to_string()))?
}

fn browse_blocking(
    duration: Duration,
    cluster: Option<&str>,
) -> Result<Vec<DiscoveredNode>, RemoteError> {
    let daemon = ServiceDaemon::new().map_err(|err| RemoteError::Discovery(err.to_string()))?;
    let receiver = daemon
        .browse(SERVICE_TYPE)
        .map_err(|err| RemoteError::Discovery(err.to_string()))?;
    let deadline = Instant::now() + duration;
    let mut nodes = BTreeMap::new();
    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        match receiver.recv_timeout(remaining.min(Duration::from_millis(250))) {
            Ok(ServiceEvent::ServiceResolved(service)) => {
                let txt: Vec<(String, String)> = service
                    .txt_properties
                    .iter()
                    .map(|prop| (prop.key().to_owned(), prop.val_str().to_owned()))
                    .collect();
                let Ok(advertisement) = Advertisement::from_txt(&txt) else {
                    continue;
                };
                if cluster.is_some_and(|cluster| cluster != advertisement.cluster) {
                    continue;
                }
                let addresses: Vec<IpAddr> =
                    service.addresses.iter().map(|ip| ip.to_ip_addr()).collect();
                let node = DiscoveredNode {
                    key_fingerprint: key_fingerprint(&advertisement.public_key),
                    urls: advertisement.peer_urls(&addresses),
                    compatible: advertisement.control_protocol_version == CONTROL_PROTOCOL_VERSION,
                    advertisement,
                };
                nodes.insert(node.advertisement.node_id.clone(), node);
            }
            Ok(_) | Err(mdns_sd::RecvTimeoutError::Timeout) => {}
            Err(mdns_sd::RecvTimeoutError::Disconnected) => break,
        }
    }
    let _ = daemon.stop_browse(SERVICE_TYPE);
    if let Ok(status) = daemon.shutdown() {
        let _ = status.recv_timeout(Duration::from_secs(1));
    }
    Ok(nodes.into_values().collect())
}
