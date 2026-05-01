use super::tls_bootstrap::build_bootstrap_peer_http_client;
use super::{HttpMutualTlsMode, NodeApp, NodeError};
use crate::peer::{PeerState, PeerSyncStatus};
use orion::{
    CompatibilityState, NodeId,
    control_plane::{
        ControlMessage, DesiredStateSection, DesiredStateSectionFingerprints, DesiredStateSummary,
        PeerHello, SyncRequest,
    },
    transport::http::{HttpClient, HttpRequestPayload, HttpResponsePayload, HttpTransportError},
};

#[derive(Clone)]
struct PeerDesiredSyncTarget {
    desired_revision: orion::Revision,
    desired_fingerprint: u64,
    desired_section_fingerprints: DesiredStateSectionFingerprints,
}

#[derive(Clone)]
struct PeerSyncStartState {
    peer: PeerState,
    revisions: crate::app::ClusterRevisionState,
    desired_metadata: crate::app::DesiredStateMetadataCache,
    client: Option<HttpClient>,
    needs_https_bootstrap: bool,
}

impl NodeApp {
    fn peer_sync_request_payload(
        &self,
        desired_revision: orion::Revision,
        desired_fingerprint: u64,
        desired_summary: Option<DesiredStateSummary>,
        sections: Vec<DesiredStateSection>,
    ) -> HttpRequestPayload {
        HttpRequestPayload::Control(Box::new(ControlMessage::SyncRequest(SyncRequest {
            node_id: self.config.node_id.clone(),
            desired_revision,
            desired_fingerprint,
            desired_summary,
            sections,
            object_selectors: Vec::new(),
        })))
    }

    async fn try_complete_peer_sync_response(
        &self,
        node_id: &NodeId,
        client: &HttpClient,
        response: HttpResponsePayload,
        desired_target: Option<PeerDesiredSyncTarget>,
    ) -> Result<bool, NodeError> {
        match response {
            HttpResponsePayload::Accepted => {
                if let Some(target) = desired_target {
                    self.infer_peer_matches_local_desired(
                        node_id,
                        target.desired_revision,
                        target.desired_fingerprint,
                        target.desired_section_fingerprints,
                    )?;
                } else {
                    self.infer_peer_matches_current_desired(node_id)?;
                }
                Ok(true)
            }
            HttpResponsePayload::Mutations(batch) => {
                self.apply_remote_mutations_async(&batch).await?;
                let updated = self.desired_metadata()?;
                let updated_revisions = self.current_revisions();
                self.infer_peer_matches_local_desired(
                    node_id,
                    updated_revisions.desired,
                    updated.fingerprint,
                    updated.section_fingerprints,
                )?;
                Ok(true)
            }
            HttpResponsePayload::Snapshot(snapshot) => {
                self.reconcile_conflicting_remote_snapshot(node_id, snapshot, client)
                    .await?;
                Ok(true)
            }
            HttpResponsePayload::Hello(_)
            | HttpResponsePayload::Summary(_)
            | HttpResponsePayload::Observability(_)
            | HttpResponsePayload::Health(_)
            | HttpResponsePayload::Readiness(_) => Ok(false),
        }
    }

    fn local_desired_target(
        &self,
        revisions: crate::app::ClusterRevisionState,
        desired_metadata: &crate::app::DesiredStateMetadataCache,
    ) -> PeerDesiredSyncTarget {
        PeerDesiredSyncTarget {
            desired_revision: revisions.desired,
            desired_fingerprint: desired_metadata.fingerprint,
            desired_section_fingerprints: desired_metadata.section_fingerprints.clone(),
        }
    }

    fn prepare_peer_sync_start(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<PeerSyncStartState>, NodeError> {
        let now_ms = Self::current_time_ms();
        let peer = {
            let peers = self.peers_read();
            let peer = peers
                .get(node_id)
                .cloned()
                .ok_or_else(|| NodeError::UnknownPeer(node_id.clone()))?;
            if !peer.can_attempt_sync_at(now_ms) {
                return Ok(None);
            }
            peer
        };

        let revisions = self.current_revisions();
        let desired_metadata = self.desired_metadata()?;
        let needs_https_bootstrap = Self::is_https_peer(&peer)
            && peer.tls_root_cert_path.is_none()
            && self
                .security
                .trusted_peer_tls_root_cert_pem(node_id)?
                .is_none();
        if needs_https_bootstrap && self.http_mutual_tls_mode == HttpMutualTlsMode::Required {
            return Err(NodeError::Authentication(format!(
                "HTTPS peer {} requires prior TLS enrollment before mTLS-required sync",
                node_id
            )));
        }
        let client = if needs_https_bootstrap {
            None
        } else {
            Some(self.peer_client(node_id, &peer.base_url, peer.tls_root_cert_path.as_deref())?)
        };

        Ok(Some(PeerSyncStartState {
            peer,
            revisions,
            desired_metadata,
            client,
            needs_https_bootstrap,
        }))
    }

    async fn try_cached_peer_sync_request(
        &self,
        node_id: &NodeId,
        client: &HttpClient,
        payload: HttpRequestPayload,
    ) -> Result<bool, NodeError> {
        let Ok(response) = self.send_peer_http_request(node_id, client, payload).await else {
            return Ok(false);
        };
        self.try_complete_peer_sync_response(node_id, client, response, None)
            .await
    }

    async fn try_cached_peer_sync_attempt(
        &self,
        node_id: &NodeId,
        client: &HttpClient,
        should_try: bool,
        build_payload: impl FnOnce() -> Result<HttpRequestPayload, NodeError>,
    ) -> Result<bool, NodeError> {
        if !should_try {
            return Ok(false);
        }
        self.try_cached_peer_sync_request(node_id, client, build_payload()?)
            .await
    }

    async fn try_cached_peer_sync_fast_paths(
        &self,
        node_id: &NodeId,
        start: &PeerSyncStartState,
    ) -> Result<bool, NodeError> {
        let Some(client) = start.client.as_ref() else {
            return Ok(false);
        };
        let peer = &start.peer;
        let revisions = start.revisions;
        let desired_metadata = &start.desired_metadata;

        if peer.compatibility.is_none() {
            return Ok(false);
        }

        if self
            .try_cached_peer_sync_attempt(
                node_id,
                client,
                peer.desired_revision == revisions.desired
                    && peer.desired_fingerprint == desired_metadata.fingerprint,
                || {
                    Ok(self.peer_sync_request_payload(
                        revisions.desired,
                        desired_metadata.fingerprint,
                        None,
                        Vec::new(),
                    ))
                },
            )
            .await?
        {
            return Ok(true);
        }

        if peer.desired_revision > revisions.desired {
            let mut sections = super::changed_sections(
                &peer.desired_section_fingerprints,
                &desired_metadata.section_fingerprints,
            );
            if sections.is_empty() {
                sections = super::all_desired_sections();
            }
            let local_summary = self.desired_state_summary_for_sections(&sections)?;
            if self
                .try_cached_peer_sync_attempt(node_id, client, true, || {
                    Ok(self.peer_sync_request_payload(
                        revisions.desired,
                        desired_metadata.fingerprint,
                        Some(local_summary),
                        sections,
                    ))
                })
                .await?
            {
                return Ok(true);
            }
        }

        if revisions.desired > peer.desired_revision
            && let Some(batch) = self.mutation_batch_since(peer.desired_revision)
            && self
                .try_cached_peer_sync_attempt(node_id, client, true, || {
                    Ok(HttpRequestPayload::Control(Box::new(
                        ControlMessage::Mutations(batch),
                    )))
                })
                .await?
        {
            return Ok(true);
        }

        if self
            .try_cached_peer_sync_attempt(
                node_id,
                client,
                peer.desired_revision == revisions.desired
                    && peer.desired_fingerprint != desired_metadata.fingerprint,
                || {
                    Ok(self.peer_sync_request_payload(
                        revisions.desired,
                        desired_metadata.fingerprint,
                        None,
                        Vec::new(),
                    ))
                },
            )
            .await?
        {
            return Ok(true);
        }

        Ok(false)
    }

    async fn complete_peer_sync_hello_phase(
        &self,
        node_id: &NodeId,
        start: &PeerSyncStartState,
        started: std::time::Instant,
    ) -> Result<(HttpClient, PeerHello), NodeError> {
        let request_hello = self.peer_hello()?;
        let remote_hello = if start.needs_https_bootstrap {
            let bootstrap_client = build_bootstrap_peer_http_client(&start.peer.base_url)?;
            self.fetch_peer_hello(
                node_id,
                &start.peer,
                request_hello,
                &bootstrap_client,
                started,
            )
            .await?
        } else {
            self.fetch_peer_hello(
                node_id,
                &start.peer,
                request_hello,
                start.client.as_ref().ok_or_else(|| {
                    NodeError::HttpTransport(HttpTransportError::request_failed(
                        "peer client should exist before peer hello",
                    ))
                })?,
                started,
            )
            .await?
        };

        self.record_peer_hello(node_id, &remote_hello, CompatibilityState::Preferred)?;
        self.set_peer_sync_status(node_id, PeerSyncStatus::Syncing)?;
        let client = self.peer_client(
            node_id,
            &start.peer.base_url,
            start.peer.tls_root_cert_path.as_deref(),
        )?;

        Ok((client, remote_hello))
    }

    async fn sync_when_remote_ahead(
        &self,
        node_id: &NodeId,
        client: &HttpClient,
        remote_hello: &PeerHello,
        revisions: crate::app::ClusterRevisionState,
        desired_metadata: &crate::app::DesiredStateMetadataCache,
    ) -> Result<(), NodeError> {
        let sections = super::changed_sections(
            &remote_hello.desired_section_fingerprints,
            &desired_metadata.section_fingerprints,
        );
        if sections.is_empty() {
            return Ok(());
        }
        let local_summary = self.desired_state_summary_for_sections(&sections)?;
        match self
            .send_peer_http_request(
                node_id,
                client,
                self.peer_sync_request_payload(
                    revisions.desired,
                    desired_metadata.fingerprint,
                    Some(local_summary),
                    sections,
                ),
            )
            .await?
        {
            response @ (HttpResponsePayload::Accepted
            | HttpResponsePayload::Mutations(_)
            | HttpResponsePayload::Snapshot(_)) => {
                self.try_complete_peer_sync_response(node_id, client, response, None)
                    .await?;
            }
            HttpResponsePayload::Hello(_)
            | HttpResponsePayload::Summary(_)
            | HttpResponsePayload::Observability(_)
            | HttpResponsePayload::Health(_)
            | HttpResponsePayload::Readiness(_) => {
                return Err(NodeError::HttpTransport(
                    HttpTransportError::request_failed("unexpected response to sync diff request"),
                ));
            }
        }
        Ok(())
    }

    async fn push_local_snapshot_to_peer(
        &self,
        node_id: &NodeId,
        client: &HttpClient,
        revisions: crate::app::ClusterRevisionState,
        desired_metadata: &crate::app::DesiredStateMetadataCache,
    ) -> Result<(), NodeError> {
        self.send_peer_http_request(
            node_id,
            client,
            HttpRequestPayload::Control(Box::new(ControlMessage::Snapshot(self.state_snapshot()))),
        )
        .await?;
        self.infer_peer_matches_local_desired(
            node_id,
            revisions.desired,
            desired_metadata.fingerprint,
            desired_metadata.section_fingerprints.clone(),
        )?;
        Ok(())
    }

    async fn sync_when_local_ahead(
        &self,
        node_id: &NodeId,
        client: &HttpClient,
        remote_hello: &PeerHello,
        revisions: crate::app::ClusterRevisionState,
        desired_metadata: &crate::app::DesiredStateMetadataCache,
    ) -> Result<(), NodeError> {
        let sections = super::changed_sections(
            &desired_metadata.section_fingerprints,
            &remote_hello.desired_section_fingerprints,
        );
        if sections.is_empty() {
            self.push_local_snapshot_to_peer(node_id, client, revisions, desired_metadata)
                .await?;
            return Ok(());
        }

        let desired_target = self.local_desired_target(revisions, desired_metadata);
        if let Some(batch) = self.mutation_batch_since(remote_hello.desired_revision) {
            match self
                .send_peer_http_request(
                    node_id,
                    client,
                    HttpRequestPayload::Control(Box::new(ControlMessage::Mutations(batch))),
                )
                .await?
            {
                response @ (HttpResponsePayload::Accepted | HttpResponsePayload::Snapshot(_)) => {
                    self.try_complete_peer_sync_response(
                        node_id,
                        client,
                        response,
                        Some(desired_target),
                    )
                    .await?;
                }
                HttpResponsePayload::Mutations(_)
                | HttpResponsePayload::Hello(_)
                | HttpResponsePayload::Summary(_)
                | HttpResponsePayload::Observability(_)
                | HttpResponsePayload::Health(_)
                | HttpResponsePayload::Readiness(_) => {}
            }
            return Ok(());
        }

        let local_summary = self.desired_state_summary_for_sections(&sections)?;
        let desired = self.current_desired_state();
        let batch = super::diff_desired_against_summary_sections(
            &desired,
            &local_summary,
            &super::empty_summary_for_sections(remote_hello.desired_revision, &sections),
            &sections,
            &[],
            remote_hello.desired_revision,
        )?;
        match self
            .send_peer_http_request(
                node_id,
                client,
                HttpRequestPayload::Control(Box::new(ControlMessage::Mutations(batch))),
            )
            .await?
        {
            response @ (HttpResponsePayload::Accepted | HttpResponsePayload::Snapshot(_)) => {
                self.try_complete_peer_sync_response(
                    node_id,
                    client,
                    response,
                    Some(desired_target),
                )
                .await?;
            }
            HttpResponsePayload::Mutations(_)
            | HttpResponsePayload::Hello(_)
            | HttpResponsePayload::Summary(_)
            | HttpResponsePayload::Observability(_)
            | HttpResponsePayload::Health(_)
            | HttpResponsePayload::Readiness(_) => {
                self.send_peer_http_request(
                    node_id,
                    client,
                    HttpRequestPayload::Control(Box::new(ControlMessage::Snapshot(
                        self.state_snapshot(),
                    ))),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn sync_equal_revision_conflict(
        &self,
        node_id: &NodeId,
        client: &HttpClient,
        revisions: crate::app::ClusterRevisionState,
        desired_metadata: &crate::app::DesiredStateMetadataCache,
    ) -> Result<(), NodeError> {
        match self
            .send_peer_http_request(
                node_id,
                client,
                self.peer_sync_request_payload(
                    revisions.desired,
                    desired_metadata.fingerprint,
                    None,
                    Vec::new(),
                ),
            )
            .await?
        {
            response @ (HttpResponsePayload::Accepted | HttpResponsePayload::Snapshot(_)) => {
                self.try_complete_peer_sync_response(
                    node_id,
                    client,
                    response,
                    Some(self.local_desired_target(revisions, desired_metadata)),
                )
                .await?;
            }
            HttpResponsePayload::Mutations(batch) => {
                self.apply_remote_mutations_async(&batch).await?;
                let updated = self.desired_metadata()?;
                let updated_revisions = self.current_revisions();
                self.infer_peer_matches_local_desired(
                    node_id,
                    updated_revisions.desired,
                    updated.fingerprint,
                    updated.section_fingerprints,
                )?;
                let snapshot = match self
                    .send_peer_http_request(
                        node_id,
                        client,
                        self.peer_sync_request_payload(
                            self.current_revisions().desired,
                            self.desired_metadata()?.fingerprint,
                            None,
                            Vec::new(),
                        ),
                    )
                    .await?
                {
                    HttpResponsePayload::Snapshot(snapshot) => snapshot,
                    HttpResponsePayload::Accepted => return Ok(()),
                    other => {
                        return Err(NodeError::HttpTransport(
                            HttpTransportError::request_failed(format!(
                                "unexpected response while reconciling equal-revision conflict: {other:?}"
                            )),
                        ));
                    }
                };
                self.reconcile_conflicting_remote_snapshot(node_id, snapshot, client)
                    .await?;
            }
            HttpResponsePayload::Hello(_)
            | HttpResponsePayload::Summary(_)
            | HttpResponsePayload::Observability(_)
            | HttpResponsePayload::Health(_)
            | HttpResponsePayload::Readiness(_) => {
                return Err(NodeError::HttpTransport(
                    HttpTransportError::request_failed("unexpected hello response to sync request"),
                ));
            }
        }
        Ok(())
    }

    async fn sync_after_peer_hello(
        &self,
        node_id: &NodeId,
        client: &HttpClient,
        remote_hello: &PeerHello,
        revisions: crate::app::ClusterRevisionState,
        desired_metadata: &crate::app::DesiredStateMetadataCache,
    ) -> Result<(), NodeError> {
        if remote_hello.desired_revision > revisions.desired {
            self.sync_when_remote_ahead(node_id, client, remote_hello, revisions, desired_metadata)
                .await
        } else if revisions.desired > remote_hello.desired_revision {
            self.sync_when_local_ahead(node_id, client, remote_hello, revisions, desired_metadata)
                .await
        } else {
            self.sync_equal_revision_conflict(node_id, client, revisions, desired_metadata)
                .await
        }
    }

    pub async fn sync_peer(&self, node_id: &NodeId) -> Result<(), NodeError> {
        if self.peer_sync_paused() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let Some(start) = self.prepare_peer_sync_start(node_id)? else {
            return Ok(());
        };
        self.set_peer_sync_status(node_id, PeerSyncStatus::Negotiating)?;

        if self
            .try_cached_peer_sync_fast_paths(node_id, &start)
            .await?
        {
            self.record_peer_sync_success(node_id, started.elapsed())?;
            return Ok(());
        }

        let (client, remote_hello) = self
            .complete_peer_sync_hello_phase(node_id, &start, started)
            .await?;

        let revisions = self.current_revisions();
        let desired_metadata = self.desired_metadata()?;
        if remote_hello.desired_revision == revisions.desired
            && remote_hello.desired_fingerprint == desired_metadata.fingerprint
        {
            self.record_peer_sync_success(node_id, started.elapsed())?;
            return Ok(());
        }

        self.sync_after_peer_hello(
            node_id,
            &client,
            &remote_hello,
            revisions,
            &desired_metadata,
        )
        .await?;

        self.record_peer_sync_success(node_id, started.elapsed())?;
        Ok(())
    }
}
