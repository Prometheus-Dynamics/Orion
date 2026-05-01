use super::*;
use crate::{NodeApp, NodeConfig};
use orion::{
    NodeId, Revision,
    control_plane::{
        ArtifactRecord, ClientHello, ClientRole, ControlMessage, DesiredClusterState, HealthState,
        MutationBatch, NodeRecord, ObservedStateUpdate, PeerHello, StateWatch,
    },
    transport::http::HttpControlHandler,
    transport::ipc::{LocalAddress, UnixPeerIdentity},
};
use std::path::PathBuf;
use std::time::Duration;

mod http;
mod local_ipc;
mod persistence;

fn temp_state_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "orion-node-auth-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos()
    ))
}

fn signed_peer_hello(app: &NodeApp) -> orion::transport::http::HttpRequestPayload {
    app.security
        .wrap_http_payload(orion::transport::http::HttpRequestPayload::Control(
            Box::new(ControlMessage::Hello(PeerHello {
                node_id: app.config.node_id.clone(),
                desired_revision: Revision::ZERO,
                desired_fingerprint: 0,
                desired_section_fingerprints:
                    orion::control_plane::DesiredStateSectionFingerprints {
                        nodes: 0,
                        artifacts: 0,
                        workloads: 0,
                        resources: 0,
                        providers: 0,
                        executors: 0,
                        leases: 0,
                    },
                observed_revision: Revision::ZERO,
                applied_revision: Revision::ZERO,
                transport_binding_version: None,
                transport_binding_public_key: None,
                transport_tls_cert_pem: None,
                transport_binding_signature: None,
            })),
        ))
        .expect("peer hello should sign")
}

fn signed_peer_payload(
    app: &NodeApp,
    payload: orion::transport::http::HttpRequestPayload,
) -> orion::transport::http::HttpRequestPayload {
    app.security
        .wrap_http_payload(payload)
        .expect("peer payload should sign")
}
