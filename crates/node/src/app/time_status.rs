//! Starting the time sync (`crate::time_sync`) with the node's state directory and status lane.

use super::NodeApp;
use crate::NodeError;
#[cfg(feature = "time-sync")]
use crate::time_sync::{TimeSyncConfig, TimeSyncHandle};
use orion::control_plane::TimeOffer;
#[cfg(feature = "time-sync")]
use orion::control_plane::{StatusEntry, StatusSubject};
use orion::transport::http::HttpResponsePayload;
#[cfg(feature = "time-sync")]
use tracing::warn;

#[cfg(feature = "time-sync")]
const TIME_STATUS_PUBLISHER: &str = "node:time";

impl NodeApp {
    /// One step of an operator offering its clock (the `operator` time source).
    pub(crate) fn accept_time_offer(
        &self,
        offer: &TimeOffer,
    ) -> Result<HttpResponsePayload, NodeError> {
        #[cfg(feature = "time-sync")]
        return self
            .operator_time
            .offer(offer)
            .map(HttpResponsePayload::TimeTurn)
            .map_err(NodeError::Authorization);
        #[cfg(not(feature = "time-sync"))]
        {
            let _ = offer;
            Err(NodeError::Authorization(
                "this node is built without time sync".to_owned(),
            ))
        }
    }

    /// Starts the time service and sync loop of `config`; `None` when it turns neither on. The
    /// sync publishes `time.*` status keys on this node.
    #[cfg(feature = "time-sync")]
    pub fn start_time_sync(
        &self,
        config: TimeSyncConfig,
    ) -> Result<Option<TimeSyncHandle>, NodeError> {
        if !config.enabled() {
            return Ok(None);
        }
        let app = self.clone();
        let operator = self.operator_time.clone();
        let handle = crate::time_sync::spawn(
            config,
            self.config.state_dir.clone(),
            operator,
            move |status, ttl| {
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
            },
        )?;
        Ok(Some(handle))
    }
}
