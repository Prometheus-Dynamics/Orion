//! Remote operator client (feature `remote`, `docs/remote-operator.md`).
//!
//! Desktop and fleet tools use it to talk to `orion-node` over the signed `orion+tcp` transport
//! without running a node themselves:
//!
//! ```no_run
//! # async fn demo() -> Result<(), orion_client::remote::RemoteError> {
//! use orion_client::remote::{NodeTrust, OperatorIdentity, RemoteOperator};
//! use std::time::Duration;
//!
//! let identity = OperatorIdentity::generate("alice")?; // store identity.secret_key_bytes()
//! let operator =
//!     RemoteOperator::connect("orion+tcp://10.0.0.2:9200", identity, NodeTrust::FirstUse).await?;
//! if !operator.is_enrolled() {
//!     operator.enroll_with_key(b"<the cluster's enrollment key>").await?;
//! }
//! for node in operator.nodes().await? {
//!     println!("{} {:?}", node.node_id, node.host.as_ref().and_then(|h| h.hostname.clone()));
//! }
//! # Ok(()) }
//! ```
//!
//! - **Identity**: [`OperatorIdentity`] is an ed25519 key plus an `operator:<name>` id; the
//!   caller stores the secret key.
//! - **Node identity**: every response is signed by the node and verified against the key chosen
//!   with [`NodeTrust`].
//! - **Enrollment**: an administrator approves the operator on the node (`orionctl operators
//!   enroll operator:alice --fingerprint sha256:...`; the fingerprint is
//!   [`OperatorIdentity::fingerprint`]), or the operator proves knowledge of the cluster's shared
//!   enrollment key ([`RemoteOperator::enroll_with_key`]).
//! - **Calls**: [`RemoteOperator::nodes`] (cluster-wide node records with host and clock facts),
//!   [`RemoteOperator::status`], [`RemoteOperator::run_action`] (also for targets owned by other
//!   nodes: the connected node forwards them), [`RemoteOperator::wait_for_action`],
//!   [`RemoteOperator::observability`]. Watches poll ([`RemoteOperator::watch_status`],
//!   [`RemoteOperator::watch_actions`]).
//!
//! Operators are never cluster members: nodes do not sync with them, do not count them for
//! liveness or placement, and refuse every desired-state write from them.

mod api;
#[cfg(feature = "discovery")]
pub mod discovery;
mod error;
mod identity;
mod operator;
mod watch;

pub use error::RemoteError;
pub use identity::OperatorIdentity;
pub use operator::{NodeTrust, RemoteOperator, RemoteOperatorConfig};
pub use orion_control_plane::{
    ActionQuery, ActionRequest, ActionResult, ActionState, ActionTarget, NodeRecord, OperatorId,
    OperatorTrustState, OperatorWelcome, StatusEntry, StatusQuery, StatusSubject,
};
pub use watch::{RemoteActionWatch, RemoteStatusWatch};
