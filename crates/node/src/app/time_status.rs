//! Starting the time sync (`crate::time_sync`) with the node's state directory and status lane.

use super::NodeApp;
use crate::NodeError;
use crate::time_sync::{TimeSyncConfig, TimeSyncHandle};
use orion::control_plane::{StatusEntry, StatusSubject};
use tracing::warn;

const TIME_STATUS_PUBLISHER: &str = "node:time";

impl NodeApp {
    /// Starts the time service and sync loop of `config`; `None` when it turns neither on. The
    /// sync publishes `time.*` status keys on this node.
    pub fn start_time_sync(
        &self,
        config: TimeSyncConfig,
    ) -> Result<Option<TimeSyncHandle>, NodeError> {
        if !config.enabled() {
            return Ok(None);
        }
        let app = self.clone();
        let handle =
            crate::time_sync::spawn(config, self.config.state_dir.clone(), move |status, ttl| {
                let subject = StatusSubject::Node(app.config.node_id.clone());
                let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
                let entries = status
                    .entries()
                    .into_iter()
                    .map(|(key, value)| {
                        StatusEntry::new(subject.clone(), key, value).with_ttl_ms(ttl_ms)
                    })
                    .collect();
                if let Err(error) = app.publish_status_as(TIME_STATUS_PUBLISHER, entries) {
                    warn!(%error, "failed to publish the time status");
                }
            })?;
        Ok(Some(handle))
    }
}
