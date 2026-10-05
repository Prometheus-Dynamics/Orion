//! The responder side of the shared-key handshake, driven message by message.

use super::*;
use crate::discovery::enrollment::{
    PendingChallenge, PendingChallenges, Role, Transcript, proof, random_nonce, signed_message,
    verify_proof,
};
use orion::control_plane::{
    ENROLLMENT_PROTOCOL_VERSION, EnrollmentChallenge, EnrollmentConfirm, EnrollmentHello,
};
use orion_core::PeerBaseUrl;

const INITIATOR_URL: &str = "orion+tcp://127.0.0.1:9301";

/// An initiator that is not running discovery: it only signs and computes proofs.
struct Initiator {
    app: NodeApp,
    key: EnrollmentKey,
    nonce: [u8; 32],
}

impl Initiator {
    fn new(node_id: &str, key: &str) -> Self {
        Self {
            app: build_app(node_id, None),
            key: EnrollmentKey::try_new(key).expect("valid key"),
            nonce: random_nonce(),
        }
    }

    fn hello(&self, responder: &NodeApp) -> EnrollmentHello {
        EnrollmentHello {
            version: ENROLLMENT_PROTOCOL_VERSION,
            cluster: CLUSTER.into(),
            initiator: self.app.config.node_id.clone(),
            initiator_public_key: self.app.security.public_key_bytes().to_vec(),
            initiator_nonce: self.nonce.to_vec(),
            initiator_url: Some(PeerBaseUrl::new(INITIATOR_URL)),
            responder: responder.config.node_id.clone(),
        }
    }

    fn transcript(&self, challenge: &EnrollmentChallenge) -> Vec<u8> {
        Transcript {
            cluster: CLUSTER,
            initiator: &self.app.config.node_id,
            initiator_key: &self.app.security.public_key_bytes(),
            initiator_nonce: &self.nonce,
            initiator_url: INITIATOR_URL,
            responder: &challenge.responder,
            responder_key: &challenge.responder_public_key,
            responder_nonce: &challenge.responder_nonce,
        }
        .bytes()
    }

    fn confirm(&self, challenge: &EnrollmentChallenge) -> EnrollmentConfirm {
        let transcript = self.transcript(challenge);
        EnrollmentConfirm {
            initiator: self.app.config.node_id.clone(),
            responder: challenge.responder.clone(),
            responder_nonce: challenge.responder_nonce.clone(),
            proof: proof(&self.key, Role::Initiator, &transcript),
            signature: self
                .app
                .security
                .sign_bytes(&signed_message(Role::Initiator, &transcript)),
        }
    }
}

fn responder(bus: &MemoryDiscoveryBus, node_id: &str) -> (NodeApp, DiscoveryHandle) {
    let app = build_app(node_id, None);
    let handle = app
        .start_discovery(
            discovery_config(Some(KEY)),
            Box::new(bus.backend()),
            endpoints(9300),
        )
        .expect("discovery should start");
    (app, handle)
}

#[tokio::test]
async fn handshake_enrolls_the_initiator_and_rejects_replays() {
    let bus = MemoryDiscoveryBus::new();
    let (responder, handle) = responder(&bus, "node-r");
    let initiator = Initiator::new("node-i", KEY);
    let initiator_id = initiator.app.config.node_id.clone();

    let challenge = responder
        .serve_enrollment_hello(initiator.hello(&responder))
        .expect("hello should be answered");
    assert_eq!(challenge.responder, responder.config.node_id);
    // The initiator can check the responder's proof with the shared key.
    verify_proof(
        &initiator.key,
        Role::Responder,
        &initiator.transcript(&challenge),
        &challenge.proof,
    )
    .expect("responder proof should verify");
    assert!(!responder.is_registered_peer(&initiator_id));

    let confirm = initiator.confirm(&challenge);
    responder
        .serve_enrollment_confirm(confirm.clone())
        .expect("confirmation should enroll the initiator");
    assert!(responder.is_registered_peer(&initiator_id));
    assert_eq!(
        responder.registered_peer_base_url(&initiator_id),
        Some(PeerBaseUrl::new(INITIATOR_URL))
    );
    assert!(
        responder
            .trusted_key_hex(&initiator_id)
            .expect("key should be pinned")
            .eq_ignore_ascii_case(initiator.app.security.public_key_hex().as_str())
    );

    // The responder nonce is single-use.
    let replay = responder
        .serve_enrollment_confirm(confirm)
        .expect_err("a replayed confirmation must be rejected");
    assert!(replay.to_string().contains("already used"), "{replay}");

    let metrics = responder.query_discovery().metrics;
    assert_eq!(metrics.enrollment_attempts, 2);
    assert_eq!(metrics.enrollment_successes, 1);
    assert_eq!(metrics.enrollment_failures, 1);
    handle.shutdown().await;
}

#[tokio::test]
async fn handshake_rejects_wrong_keys_and_mismatched_ids() {
    let bus = MemoryDiscoveryBus::new();
    let (responder, handle) = responder(&bus, "node-r");

    // A different enrollment key: the responder's proof does not verify for the initiator, and
    // the initiator's proof is rejected by the responder.
    let outsider = Initiator::new("node-x", OTHER_KEY);
    let challenge = responder
        .serve_enrollment_hello(outsider.hello(&responder))
        .expect("hello should be answered");
    assert!(
        verify_proof(
            &outsider.key,
            Role::Responder,
            &outsider.transcript(&challenge),
            &challenge.proof,
        )
        .is_err()
    );
    let err = responder
        .serve_enrollment_confirm(outsider.confirm(&challenge))
        .expect_err("a proof made with another key must be rejected");
    assert!(err.to_string().contains("proof"), "{err}");
    assert!(!responder.is_registered_peer(&outsider.app.config.node_id));

    let initiator = Initiator::new("node-i", KEY);
    // Addressed to another node.
    let mut hello = initiator.hello(&responder);
    hello.responder = NodeId::new("node-elsewhere");
    assert!(responder.serve_enrollment_hello(hello).is_err());
    // Another cluster.
    let mut hello = initiator.hello(&responder);
    hello.cluster = "other-cluster".into();
    assert!(responder.serve_enrollment_hello(hello).is_err());
    // Claiming the responder's own id.
    let mut hello = initiator.hello(&responder);
    hello.initiator = responder.config.node_id.clone();
    assert!(responder.serve_enrollment_hello(hello).is_err());

    // A confirmation for another node id than the hello: rejected even with a valid proof.
    let challenge = responder
        .serve_enrollment_hello(initiator.hello(&responder))
        .expect("hello should be answered");
    let mut confirm = initiator.confirm(&challenge);
    confirm.initiator = NodeId::new("node-impostor");
    assert!(responder.serve_enrollment_confirm(confirm).is_err());
    // That challenge is consumed; a fresh one with a tampered signature fails too.
    let challenge = responder
        .serve_enrollment_hello(initiator.hello(&responder))
        .expect("hello should be answered");
    let mut confirm = initiator.confirm(&challenge);
    confirm.signature[0] ^= 0xff;
    assert!(responder.serve_enrollment_confirm(confirm).is_err());
    // A proof bound to another initiator nonce does not verify.
    let challenge = responder
        .serve_enrollment_hello(initiator.hello(&responder))
        .expect("hello should be answered");
    let other_nonce = Initiator {
        nonce: random_nonce(),
        ..Initiator::new("node-i", KEY)
    };
    let mut confirm = other_nonce.confirm(&challenge);
    confirm.signature = initiator.confirm(&challenge).signature;
    assert!(responder.serve_enrollment_confirm(confirm).is_err());
    assert!(!responder.is_registered_peer(&initiator.app.config.node_id));

    // An operator-removed node is not re-enrolled with the shared key.
    responder
        .remove_peer(&initiator.app.config.node_id)
        .expect("remove should succeed");
    let err = responder
        .serve_enrollment_hello(initiator.hello(&responder))
        .expect_err("removed peers must not re-enroll automatically");
    assert!(err.to_string().contains("removed"), "{err}");
    handle.shutdown().await;
}

#[tokio::test]
async fn nodes_without_an_enrollment_key_refuse_the_handshake() {
    let bus = MemoryDiscoveryBus::new();
    let app = build_app("node-r", None);
    let initiator = Initiator::new("node-i", KEY);
    // Discovery off.
    assert!(app.serve_enrollment_hello(initiator.hello(&app)).is_err());
    // Discovery on, no key.
    let handle = app
        .start_discovery(
            discovery_config(None),
            Box::new(bus.backend()),
            endpoints(1),
        )
        .expect("discovery should start");
    let err = app
        .serve_enrollment_hello(initiator.hello(&app))
        .expect_err("no key, no handshake");
    assert!(err.to_string().contains("not enabled"), "{err}");
    handle.shutdown().await;
}

#[test]
fn pending_challenges_expire_and_are_bounded() {
    let hello = EnrollmentHello {
        version: ENROLLMENT_PROTOCOL_VERSION,
        cluster: CLUSTER.into(),
        initiator: NodeId::new("node-i"),
        initiator_public_key: vec![1; 32],
        initiator_nonce: vec![2; 32],
        initiator_url: None,
        responder: NodeId::new("node-r"),
    };
    let mut pending = PendingChallenges::default();
    let mut nonces = Vec::new();
    for index in 0..100u64 {
        let nonce = random_nonce();
        nonces.push(nonce);
        pending.insert(PendingChallenge {
            hello: hello.clone(),
            responder_nonce: nonce,
            transcript: Vec::new(),
            issued_at_ms: 1_000 + index,
        });
    }
    assert_eq!(pending.len(), 64);
    // The oldest were dropped; a recent one is still there until it expires.
    assert!(pending.take(&nonces[0], 1_200).is_none());
    assert!(pending.take(&nonces[99], 1_200).is_some());
    assert!(
        pending
            .take(
                &nonces[98],
                1_098 + crate::discovery::enrollment::PENDING_TTL_MS
            )
            .is_none()
    );
}
