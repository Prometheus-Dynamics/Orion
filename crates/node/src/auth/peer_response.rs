//! Response signatures for the `orion+tcp` peer transport (see "Threat model" in
//! `docs/peer-sync.md`). Requests are authenticated by [`super::AuthenticatedPeerRequest`]; over
//! plain TCP the responder additionally signs every response, bound to the request it answers.

use super::{NodeSecurity, PeerAuthenticationMode, crypto::parse_public_key_bytes};
use crate::NodeError;
pub(crate) use orion_auth::peer_tcp::PeerResponseSignature;
use orion_core::NodeId;

impl NodeSecurity {
    /// Signs a response to `request` with this node's identity. Returns `None` when peer
    /// authentication is disabled.
    pub(crate) fn sign_peer_response(
        &self,
        request: &[u8],
        status: u8,
        body: &[u8],
    ) -> Result<Option<PeerResponseSignature>, NodeError> {
        if self.mode == PeerAuthenticationMode::Disabled {
            return Ok(None);
        }
        orion_auth::crypto::sign_peer_response(
            &self.identity,
            &self.local_node_id,
            request,
            status,
            body,
        )
        .map(Some)
        .map_err(|err| NodeError::Authentication(err.to_string()))
    }

    /// Verifies the response of peer `responder` to `request`.
    ///
    /// A signed response must verify against the key configured, enrolled or (in `optional`
    /// mode) pinned for `responder`. An unsigned response is accepted unless peer authentication
    /// is `required`.
    pub(crate) fn verify_peer_response(
        &self,
        responder: &NodeId,
        request: &[u8],
        status: u8,
        body: &[u8],
        signature: Option<&PeerResponseSignature>,
    ) -> Result<(), NodeError> {
        if self.mode == PeerAuthenticationMode::Disabled {
            return Ok(());
        }
        let Some(signature) = signature else {
            if self.mode == PeerAuthenticationMode::Required {
                return Err(NodeError::Authentication(format!(
                    "peer {responder} sent an unsigned response; peer authentication is required"
                )));
            }
            return Ok(());
        };
        let public_key = parse_public_key_bytes(&signature.public_key)?;
        self.ensure_peer_is_not_revoked(responder)?;
        self.validate_configured_or_trusted_peer_key(responder, public_key)?;
        orion_auth::crypto::verify_peer_response(
            responder,
            &public_key,
            request,
            status,
            body,
            signature,
        )
        .map_err(|err| {
            NodeError::Authentication(format!(
                "response signature from peer {responder} did not verify: {err}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerConfig;

    fn security(node: &str, mode: PeerAuthenticationMode, peers: &[PeerConfig]) -> NodeSecurity {
        NodeSecurity::load_or_create(NodeId::new(node), mode, peers, None, 8)
            .expect("security should initialize")
    }

    #[test]
    fn response_signatures_bind_responder_request_status_and_body() {
        let responder = security("node-b", PeerAuthenticationMode::Required, &[]);
        let trusting = |mode| {
            security(
                "node-a",
                mode,
                &[PeerConfig::new("node-b", "orion+tcp://b:1")
                    .with_trusted_public_key_hex(responder.public_key_hex())],
            )
        };
        let client = trusting(PeerAuthenticationMode::Required);
        let node_b = NodeId::new("node-b");
        let signature = responder
            .sign_peer_response(b"request-1", 0, b"body")
            .expect("signing should work")
            .expect("required mode signs");

        client
            .verify_peer_response(&node_b, b"request-1", 0, b"body", Some(&signature))
            .expect("the genuine response verifies");
        for (request, status, body) in [
            (&b"request-2"[..], 0, &b"body"[..]),
            (&b"request-1"[..], 1, &b"body"[..]),
            (&b"request-1"[..], 0, &b"bodY"[..]),
        ] {
            assert!(
                client
                    .verify_peer_response(&node_b, request, status, body, Some(&signature))
                    .is_err(),
                "a replayed or altered response must not verify"
            );
        }
        // Signed by a key other than the one configured for node-b.
        let impostor = security("node-b", PeerAuthenticationMode::Required, &[]);
        let forged = impostor
            .sign_peer_response(b"request-1", 0, b"body")
            .expect("signing should work")
            .expect("required mode signs");
        assert!(
            client
                .verify_peer_response(&node_b, b"request-1", 0, b"body", Some(&forged))
                .is_err()
        );
        // Unsigned responses: rejected when required, accepted when optional.
        assert!(
            client
                .verify_peer_response(&node_b, b"request-1", 0, b"body", None)
                .is_err()
        );
        trusting(PeerAuthenticationMode::Optional)
            .verify_peer_response(&node_b, b"request-1", 0, b"body", None)
            .expect("optional mode accepts unsigned responses");
        assert_eq!(
            security("node-c", PeerAuthenticationMode::Disabled, &[])
                .sign_peer_response(b"request", 0, b"body")
                .expect("signing should work"),
            None
        );
    }
}
