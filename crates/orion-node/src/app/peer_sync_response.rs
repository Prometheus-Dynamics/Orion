use super::{NodeApp, NodeError};
use orion::{
    control_plane::{SyncDiffRequest, SyncRequest},
    transport::http::HttpResponsePayload,
};

impl NodeApp {
    pub(super) fn build_sync_response(
        &self,
        request: SyncRequest,
    ) -> Result<HttpResponsePayload, NodeError> {
        let revisions = self.current_revisions();
        let desired_metadata = self.desired_metadata()?;

        if request.desired_revision == revisions.desired
            && request.desired_fingerprint == desired_metadata.fingerprint
        {
            return Ok(HttpResponsePayload::Accepted);
        }

        if request.desired_revision < revisions.desired
            && let Some(summary) = request.desired_summary
        {
            let local_summary = self.desired_state_summary_for_sections(&request.sections)?;
            let batch = self.with_desired_state_read(|desired| {
                super::diff_desired_against_summary_sections(
                    desired,
                    &local_summary,
                    &summary,
                    &request.sections,
                    &request.object_selectors,
                    request.desired_revision,
                )
            })?;
            if batch.mutations.is_empty() {
                return Ok(HttpResponsePayload::Accepted);
            }
            return Ok(HttpResponsePayload::Mutations(batch));
        }

        if let Some(batch) = self.mutation_batch_since(request.desired_revision) {
            return Ok(HttpResponsePayload::Mutations(batch));
        }

        Ok(HttpResponsePayload::Snapshot(self.state_snapshot()))
    }

    pub(super) fn build_sync_diff_response(
        &self,
        request: &SyncDiffRequest,
    ) -> Result<HttpResponsePayload, NodeError> {
        let local_summary = self.desired_state_summary_for_sections(&request.sections)?;
        let batch = self.with_desired_state_read(|desired| {
            super::diff_desired_against_summary_sections(
                desired,
                &local_summary,
                &request.desired_summary,
                &request.sections,
                &request.object_selectors,
                request.desired_revision,
            )
        })?;
        if batch.mutations.is_empty() {
            Ok(HttpResponsePayload::Accepted)
        } else {
            Ok(HttpResponsePayload::Mutations(batch))
        }
    }
}
