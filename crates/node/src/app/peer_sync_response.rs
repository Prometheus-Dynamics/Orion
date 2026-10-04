//! Responder side of peer sync: answers `SyncRequest` and `SyncDiffRequest` with the local
//! object versions that win the merge rule against the requester's summary.

use super::{NodeApp, NodeError, desired_sync::versions_newer_than_summary};
use orion::{
    control_plane::{SyncDiffRequest, SyncRequest},
    transport::http::HttpResponsePayload,
};

impl NodeApp {
    pub(super) fn build_sync_response(
        &self,
        request: SyncRequest,
    ) -> Result<HttpResponsePayload, NodeError> {
        let desired_metadata = self.desired_metadata()?;
        if request.desired_fingerprint == desired_metadata.fingerprint {
            return Ok(HttpResponsePayload::Accepted);
        }
        let Some(summary) = request.desired_summary else {
            return Ok(HttpResponsePayload::Snapshot(self.state_snapshot()));
        };
        let cutoff = self.tombstone_cutoff_ms(Self::wall_clock_ms());
        let batch = self.with_desired_state_read(|desired| {
            versions_newer_than_summary(
                desired,
                &summary,
                &request.sections,
                &request.object_selectors,
                cutoff,
            )
        });
        if batch.mutations.is_empty() {
            Ok(HttpResponsePayload::Accepted)
        } else {
            Ok(HttpResponsePayload::Mutations(batch))
        }
    }

    pub(super) fn build_sync_diff_response(
        &self,
        request: &SyncDiffRequest,
    ) -> Result<HttpResponsePayload, NodeError> {
        let cutoff = self.tombstone_cutoff_ms(Self::wall_clock_ms());
        let batch = self.with_desired_state_read(|desired| {
            versions_newer_than_summary(
                desired,
                &request.desired_summary,
                &request.sections,
                &request.object_selectors,
                cutoff,
            )
        });
        if batch.mutations.is_empty() {
            Ok(HttpResponsePayload::Accepted)
        } else {
            Ok(HttpResponsePayload::Mutations(batch))
        }
    }
}
