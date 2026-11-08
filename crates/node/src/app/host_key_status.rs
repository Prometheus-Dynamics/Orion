//! Publishes the SSH host key fingerprints as status keys, the channel a fleet tool can trust
//! (`docs/node-env.md`, `ORION_NODE_SSH_HOST_KEYS`).

use super::NodeApp;
use crate::identity::{SshHostKey, read_ssh_host_keys};
use orion::control_plane::{StatusEntry, StatusSubject, TypedConfigValue};
use std::path::PathBuf;
use std::time::Duration;
use tracing::warn;

const HOST_KEY_STATUS_PUBLISHER: &str = "node:identity";
/// How often the directory is read while it holds no key (the board may write it after the node
/// starts), and once it does (a key can be replaced).
const LOOK_AGAIN: Duration = Duration::from_secs(5);
const REFRESH: Duration = Duration::from_secs(60);

impl NodeApp {
    /// Keeps `identity.ssh_host_key.<kind>` on this node current with the keys in `dir`.
    pub fn spawn_host_key_status(&self, dir: PathBuf) -> tokio::task::JoinHandle<()> {
        let app = self.clone();
        tokio::spawn(async move {
            let mut published: Vec<SshHostKey> = Vec::new();
            loop {
                let keys = read_ssh_host_keys(&dir);
                if keys != published {
                    let subject = StatusSubject::Node(app.config.node_id.clone());
                    let entries = keys
                        .iter()
                        .map(|key| {
                            StatusEntry::new(
                                subject.clone(),
                                format!("identity.ssh_host_key.{}", key.kind),
                                TypedConfigValue::String(key.sha256.clone()),
                            )
                        })
                        .collect();
                    match app.publish_status_as(HOST_KEY_STATUS_PUBLISHER, entries) {
                        Ok(_) => published = keys,
                        Err(error) => warn!(%error, "failed to publish the SSH host keys"),
                    }
                }
                tokio::time::sleep(if published.is_empty() {
                    LOOK_AGAIN
                } else {
                    REFRESH
                })
                .await;
            }
        })
    }
}
