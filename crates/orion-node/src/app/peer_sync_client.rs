use super::tls_bootstrap::{
    build_bootstrap_peer_http_client, build_peer_http_client, stable_fingerprint,
};
use super::{
    CachedPeerClient, HttpMutualTlsMode, NodeApp, NodeError, classify_peer_sync_error,
    is_https_base_url,
};
use crate::peer::PeerState;
use crate::storage_io::blocking_read_file;
use orion::{
    NodeId,
    control_plane::{ControlMessage, PeerHello},
    transport::http::{HttpRequestPayload, HttpResponsePayload, HttpTransportError},
};

fn read_file_bytes(path: &std::path::Path) -> Result<Vec<u8>, NodeError> {
    blocking_read_file(path, "failed to read file")
}

impl NodeApp {
    pub(super) fn peer_client(
        &self,
        node_id: &NodeId,
        base_url: &orion_core::PeerBaseUrl,
        tls_root_cert_path: Option<&std::path::Path>,
    ) -> Result<orion::transport::http::HttpClient, NodeError> {
        let client_tls = self.transport_security.http_client_config(
            node_id,
            base_url.as_str(),
            tls_root_cert_path.and_then(|path| path.to_str()),
        )?;
        let uses_client_identity = client_tls
            .as_ref()
            .map(|tls| tls.client_cert_pem.is_some() && tls.client_key_pem.is_some())
            .unwrap_or(false);
        let tls_root_cert_fingerprint = client_tls
            .as_ref()
            .map(|tls| stable_fingerprint(&tls.root_cert_pem));
        if let Some(client) = self.peer_clients_read().get(node_id)
            && client.base_url == *base_url
            && client.tls_root_cert_path.as_deref() == tls_root_cert_path
            && client.tls_root_cert_fingerprint == tls_root_cert_fingerprint
            && client.uses_client_identity == uses_client_identity
        {
            return Ok(client.client.clone());
        }

        let client = build_peer_http_client(base_url.as_str(), client_tls)?;
        self.with_peer_clients_mut(|peer_clients| {
            peer_clients.insert(
                node_id.clone(),
                CachedPeerClient {
                    base_url: base_url.clone(),
                    tls_root_cert_path: tls_root_cert_path.map(ToOwned::to_owned),
                    tls_root_cert_fingerprint,
                    uses_client_identity,
                    client: client.clone(),
                },
            );
        });
        Ok(client)
    }

    pub(super) fn evict_peer_client(&self, node_id: &NodeId) {
        self.with_peer_clients_mut(|peer_clients| {
            peer_clients.remove(node_id);
        });
    }

    pub(super) async fn send_peer_http_request(
        &self,
        client: &orion::transport::http::HttpClient,
        payload: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        let payload = self
            .security
            .wrap_http_payload_async(payload)
            .await
            .map_err(|err| HttpTransportError::request_failed(err.to_string()))?;
        client.send(&payload).await
    }

    pub(super) fn is_https_peer(peer: &PeerState) -> bool {
        is_https_base_url(&peer.base_url)
    }

    fn transport_binding_from_hello(
        &self,
        hello: &PeerHello,
    ) -> Result<Option<orion::auth::NodeTransportBinding>, NodeError> {
        match (
            hello.transport_binding_version,
            hello.transport_binding_public_key.clone(),
            hello.transport_tls_cert_pem.clone(),
            hello.transport_binding_signature.clone(),
        ) {
            (None, None, None, None) => Ok(None),
            (Some(version), Some(public_key), Some(tls_cert_pem), Some(signature)) => {
                Ok(Some(orion::auth::NodeTransportBinding {
                    version,
                    node_id: hello.node_id.clone(),
                    public_key,
                    tls_cert_pem,
                    signature,
                }))
            }
            _ => Err(NodeError::Authentication(
                "peer hello contained a partial transport binding".into(),
            )),
        }
    }

    fn apply_peer_transport_binding(
        &self,
        node_id: &NodeId,
        peer: &PeerState,
        hello: &PeerHello,
    ) -> Result<(), NodeError> {
        if !Self::is_https_peer(peer) {
            return Ok(());
        }
        let Some(binding) = self.transport_binding_from_hello(hello)? else {
            return Err(NodeError::Authentication(format!(
                "HTTPS peer {} did not advertise a transport binding",
                node_id
            )));
        };
        let changed = self
            .security
            .validate_or_update_transport_binding(node_id, &binding)?;
        if changed {
            self.evict_peer_client(node_id);
        }
        Ok(())
    }

    pub(super) async fn fetch_peer_hello(
        &self,
        node_id: &NodeId,
        peer: &PeerState,
        request_hello: PeerHello,
        client: &orion::transport::http::HttpClient,
        started: std::time::Instant,
    ) -> Result<PeerHello, NodeError> {
        let response = self
            .send_peer_http_request(
                client,
                HttpRequestPayload::Control(Box::new(ControlMessage::Hello(request_hello.clone()))),
            )
            .await;

        let remote_hello = match response {
            Ok(HttpResponsePayload::Hello(hello)) => hello,
            Ok(other) => {
                let error = NodeError::HttpTransport(HttpTransportError::request_failed(format!(
                    "expected hello response from peer, got {other:?}"
                )));
                let _ = self.record_peer_error_with_kind(
                    node_id,
                    error.to_string(),
                    Some(classify_peer_sync_error(&error)),
                );
                self.record_peer_sync_failure(Some(node_id), started.elapsed(), &error);
                return Err(error);
            }
            Err(err) => {
                if self.http_mutual_tls_mode == HttpMutualTlsMode::Optional
                    && Self::is_https_peer(peer)
                    && let Some(hello) = self
                        .try_rebind_peer_hello(node_id, peer, &request_hello)
                        .await?
                {
                    return Ok(hello);
                }
                let error = NodeError::HttpTransport(err);
                let _ = self.record_peer_error_with_kind(
                    node_id,
                    error.to_string(),
                    Some(classify_peer_sync_error(&error)),
                );
                self.record_peer_sync_failure(Some(node_id), started.elapsed(), &error);
                return Err(error);
            }
        };
        self.apply_peer_transport_binding(node_id, peer, &remote_hello)?;
        Ok(remote_hello)
    }

    async fn try_rebind_peer_hello(
        &self,
        node_id: &NodeId,
        peer: &PeerState,
        request_hello: &PeerHello,
    ) -> Result<Option<PeerHello>, NodeError> {
        let tls_root_cert_pem = match peer.tls_root_cert_path.as_deref() {
            Some(path) => Some(read_file_bytes(path)?),
            None => self.security.trusted_peer_tls_root_cert_pem(node_id)?,
        };
        if let Some(root_cert_pem) = tls_root_cert_pem {
            let client = build_peer_http_client(
                &peer.base_url,
                Some(orion::transport::http::HttpClientTlsConfig {
                    root_cert_pem,
                    client_cert_pem: None,
                    client_key_pem: None,
                }),
            )?;
            if let Ok(HttpResponsePayload::Hello(hello)) = self
                .send_peer_http_request(
                    &client,
                    HttpRequestPayload::Control(Box::new(ControlMessage::Hello(
                        request_hello.clone(),
                    ))),
                )
                .await
            {
                self.apply_peer_transport_binding(node_id, peer, &hello)?;
                return Ok(Some(hello));
            }
        }
        if peer.tls_root_cert_path.is_none() {
            let client = build_bootstrap_peer_http_client(&peer.base_url)?;
            if let Ok(HttpResponsePayload::Hello(hello)) = self
                .send_peer_http_request(
                    &client,
                    HttpRequestPayload::Control(Box::new(ControlMessage::Hello(
                        request_hello.clone(),
                    ))),
                )
                .await
            {
                self.apply_peer_transport_binding(node_id, peer, &hello)?;
                return Ok(Some(hello));
            }
        }
        Ok(None)
    }

    pub(super) fn peer_hello(&self) -> Result<PeerHello, NodeError> {
        let revisions = self.current_revisions();
        let desired_metadata = self.desired_metadata()?;
        let (
            transport_binding_version,
            transport_binding_public_key,
            transport_tls_cert_pem,
            transport_binding_signature,
        ) = match self.http_tls_cert_path.as_deref() {
            Some(cert_path) => {
                let cert_pem = read_file_bytes(cert_path)?;
                let binding = self.security.transport_binding(&cert_pem)?;
                (
                    Some(binding.version),
                    Some(binding.public_key),
                    Some(binding.tls_cert_pem),
                    Some(binding.signature),
                )
            }
            None => (None, None, None, None),
        };
        Ok(PeerHello {
            node_id: self.config.node_id.clone(),
            desired_revision: revisions.desired,
            desired_fingerprint: desired_metadata.fingerprint,
            desired_section_fingerprints: desired_metadata.section_fingerprints.clone(),
            observed_revision: revisions.observed,
            applied_revision: revisions.applied,
            transport_binding_version,
            transport_binding_public_key,
            transport_tls_cert_pem,
            transport_binding_signature,
        })
    }
}
