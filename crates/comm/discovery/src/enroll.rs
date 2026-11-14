//! The asking side of shared-key enrollment: connect to a discovered peer, prove knowledge of
//! the enrollment key, check the peer's proof, and pin its key. `orion_keys::enroll` has the
//! handshake; the keys service is the answering side.

use std::net::SocketAddr;

use orion_keys::enroll::{NONCE_LEN, Side, Transcript, proof, verify_proof};
use orion_keys::{random, verify};
use orion_msg_peers::{EnrollChallenge, EnrollConfirm, EnrollHello, EnrollRole};
use orion_msg_session::{Hello, Refusal, Reject, RejectReason, Roles, Welcome};
use orion_tcp::{TcpConnection, TcpOptions};
use orion_types::NodeName;
use orion_wire::{
    Body, Bytes, Decode, INTERFACE_LEVEL, INTERFACE_REVISION, encode_body, frame, kind,
};

use crate::advert::Advertisement;

const BUF: usize = 2048;

async fn send<B: Body>(connection: &mut TcpConnection, body: &B, seq: u16) -> Result<(), String> {
    let mut out = [0_u8; BUF];
    let len = encode_body(body, seq, &mut out).map_err(|error| error.to_string())?;
    connection
        .send(&out[..len])
        .await
        .map_err(|error| error.to_string())
}

/// Waits for a frame of kind `want` and returns its length in `buf`. A refusal is the error.
async fn expect(connection: &mut TcpConnection, buf: &mut [u8], want: u8) -> Result<usize, String> {
    loop {
        let len = connection
            .recv(buf)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("the peer closed the connection")?;
        let view = frame::decode(&buf[..len]).map_err(|error| error.to_string())?;
        match view.kind() {
            got if got == want => return Ok(len),
            kind::REFUSAL => {
                let refusal = Refusal::decode(view.payload()).map_err(|error| error.to_string())?;
                return Err(match refusal.error() {
                    Some(error) => format!("the peer refused: {error}"),
                    None => "the peer refused".into(),
                });
            }
            kind::REJECT => {
                let reject = Reject::decode(view.payload()).map_err(|error| error.to_string())?;
                return Err(if reject.reason == RejectReason::RevisionMismatch {
                    format!(
                        "the peer speaks wire revision {} and this node {}",
                        reject.node_revision, reject.peer_revision
                    )
                } else {
                    format!("the peer rejected the session: {:?}", reject.reason)
                });
            }
            _ => {}
        }
    }
}

/// Enrolls with the peer that advertised `peer` at `addr`. On success both nodes hold each
/// other's key.
pub(crate) async fn enroll(
    addr: SocketAddr,
    options: &TcpOptions,
    node: &NodeName,
    peer: &Advertisement,
) -> Result<(), String> {
    let (secret, cluster, public) = {
        let keys = options.keys.lock();
        let secret = keys.enrollment_key().ok_or("no enrollment key")?.to_vec();
        (secret, keys.cluster().to_owned(), keys.identity().public())
    };
    // The peer must hold the key it advertised: an announcement proves nothing by itself.
    let mut connection = orion_tcp::connect(addr, options, Some(peer.public_key))
        .await
        .map_err(|error| error.to_string())?;
    let mut buf = vec![0_u8; BUF];
    let hello = Hello {
        name: node.as_str(),
        roles: Roles::CLIENT,
        max_frame: BUF as u32,
        revision: INTERFACE_REVISION,
        level: INTERFACE_LEVEL,
    };
    send(&mut connection, &hello, 1).await?;
    let len = expect(&mut connection, &mut buf, kind::WELCOME).await?;
    let view = frame::decode(&buf[..len]).map_err(|error| error.to_string())?;
    let welcome = Welcome::decode(view.payload()).map_err(|error| error.to_string())?;
    if welcome.revision != INTERFACE_REVISION {
        return Err(format!(
            "the peer speaks wire revision {} and this node {INTERFACE_REVISION}",
            welcome.revision
        ));
    }

    let nonce: [u8; NONCE_LEN] = random();
    let ask = EnrollHello {
        cluster: &cluster,
        initiator: node.clone(),
        public_key: Bytes(&public),
        nonce: Bytes(&nonce),
        responder: peer.node.clone(),
        role: EnrollRole::Node,
    };
    send(&mut connection, &ask, 2).await?;
    let len = expect(&mut connection, &mut buf, kind::ENROLL_CHALLENGE).await?;
    let view = frame::decode(&buf[..len]).map_err(|error| error.to_string())?;
    let challenge = EnrollChallenge::decode(view.payload()).map_err(|error| error.to_string())?;
    let transcript = Transcript {
        role: EnrollRole::Node.code(),
        cluster: &cluster,
        initiator: node.as_str(),
        initiator_key: &public,
        initiator_nonce: &nonce,
        responder: peer.node.as_str(),
        responder_key: &peer.public_key,
        responder_nonce: challenge.nonce.0,
    };
    let theirs = transcript.message(Side::Responder);
    let genuine = challenge.responder == peer.node
        && challenge.public_key.0 == peer.public_key
        && challenge.nonce.0.len() == NONCE_LEN
        && verify_proof(&secret, &theirs, challenge.proof.0)
        && verify(&peer.public_key, &theirs, challenge.signature.0);
    if !genuine {
        return Err("the peer did not prove it holds the enrollment key".into());
    }
    let ours = transcript.message(Side::Initiator);
    let (proved, signature) = (
        proof(&secret, &ours),
        options.keys.lock().identity().sign(&ours),
    );
    let peer_nonce = challenge.nonce.0.to_vec();
    let confirm = EnrollConfirm {
        initiator: node.clone(),
        responder: peer.node.clone(),
        nonce: Bytes(&peer_nonce),
        proof: Bytes(&proved),
        signature: Bytes(&signature),
    };
    send(&mut connection, &confirm, 3).await?;
    // The peer has pinned this node's key when it answers; then this node pins the peer's.
    expect(&mut connection, &mut buf, kind::ENROLL_CONFIRM).await?;
    match options.keys.lock().trust_peer(&peer.node, peer.public_key) {
        Ok(true) => Ok(()),
        Ok(false) => Err("this node holds another key for that peer, or has revoked it".into()),
        Err(error) => Err(format!("the key could not be saved: {error}")),
    }
}
