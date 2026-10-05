//! Remote operator trust (`docs/remote-operator.md`).
//!
//! Operators are a principal kind of their own: they sign requests like peers, with an
//! `operator:<name>` principal id, but they are never cluster members. This module keeps the
//! enrolled operators (pinned key, enrollment method, policy), the revoked ones and a bounded,
//! in-memory list of pending operators (validly signed requests from unknown operators, shown by
//! `orionctl get operators` for approval). Enrolled and revoked operators are persisted in
//! `trusted-operators.json` next to the trust store; the peer trust store format is unchanged.

use super::NodeSecurity;
use crate::lock::{read_rwlock, write_rwlock};
use crate::storage_io::{atomic_write_file, blocking_read_file};
use crate::{NodeError, NodeStorage};
use orion::control_plane::{
    OperatorEnrollmentMethod, OperatorId, OperatorPolicy, OperatorRecord, OperatorTrustState,
};
use orion_auth::{
    PeerRequestAuth, PeerRequestPayload,
    crypto::{key_fingerprint, verify_peer_request},
    hex::{encode_hex, parse_key_hex},
};
use orion_core::PublicKeyHex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

const OPERATORS_FILE: &str = "trusted-operators.json";
const STORE_VERSION: u32 = 1;
/// Most pending (unapproved) operators remembered; the oldest is dropped beyond this.
const MAX_PENDING_OPERATORS: usize = 32;
/// How long a pending operator is listed after its last request.
const PENDING_OPERATOR_TTL_MS: u64 = 600_000;

/// An authenticated, enrolled operator, with the policy in force for this request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedOperator {
    pub operator_id: OperatorId,
    pub public_key_hex: PublicKeyHex,
    pub policy: OperatorPolicy,
    /// The action patterns in force (the policy's, or the node default).
    pub allowed_actions: Vec<String>,
}

impl AuthenticatedOperator {
    /// Whether the operator may run action `name`.
    pub fn allows_action(&self, name: &str) -> bool {
        self.allowed_actions
            .iter()
            .any(|pattern| orion::control_plane::action_pattern_matches(pattern, name))
    }
}

/// Outcome of authenticating a request signed by an operator principal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OperatorAuthentication {
    Enrolled(AuthenticatedOperator),
    /// A validly signed `OperatorHello` from an operator that is not enrolled.
    Unenrolled {
        operator_id: OperatorId,
        public_key: [u8; 32],
    },
}

#[derive(Clone, Debug)]
struct EnrolledOperator {
    key: [u8; 32],
    method: OperatorEnrollmentMethod,
    policy: OperatorPolicy,
    enrolled_at_ms: u64,
    last_seen_ms: u64,
}

#[derive(Clone, Debug)]
struct PendingOperator {
    key: [u8; 32],
    first_seen_ms: u64,
    last_seen_ms: u64,
}

#[derive(Debug, Default)]
pub(super) struct OperatorTrust {
    enrolled: BTreeMap<OperatorId, EnrolledOperator>,
    revoked: BTreeSet<OperatorId>,
    pending: BTreeMap<OperatorId, PendingOperator>,
    default_actions: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredOperator {
    operator_id: OperatorId,
    public_key_hex: String,
    method: OperatorEnrollmentMethod,
    policy: OperatorPolicy,
    enrolled_at_ms: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StoreFile {
    version: u32,
    #[serde(default)]
    operators: Vec<StoredOperator>,
    #[serde(default)]
    revoked: Vec<OperatorId>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn store_path(storage: &NodeStorage) -> PathBuf {
    storage.root().join(OPERATORS_FILE)
}

pub(super) fn load_operator_trust(
    storage: Option<&NodeStorage>,
) -> Result<OperatorTrust, NodeError> {
    let mut trust = OperatorTrust::default();
    let Some(storage) = storage else {
        return Ok(trust);
    };
    let path = store_path(storage);
    if !path.exists() {
        return Ok(trust);
    }
    let bytes = blocking_read_file(&path, "read trusted operator store")?;
    let file: StoreFile = serde_json::from_slice(&bytes)
        .map_err(|err| NodeError::Storage(format!("failed to decode {}: {err}", path.display())))?;
    if file.version != STORE_VERSION {
        return Err(NodeError::Storage(format!(
            "{} has unsupported version {}",
            path.display(),
            file.version
        )));
    }
    for operator in file.operators {
        let key = parse_key_hex(&operator.public_key_hex).map_err(|err| {
            NodeError::Storage(format!(
                "{}: {}: {err}",
                path.display(),
                operator.operator_id
            ))
        })?;
        trust.enrolled.insert(
            operator.operator_id,
            EnrolledOperator {
                key,
                method: operator.method,
                policy: operator.policy,
                enrolled_at_ms: operator.enrolled_at_ms,
                last_seen_ms: 0,
            },
        );
    }
    trust.revoked = file.revoked.into_iter().collect();
    Ok(trust)
}

impl OperatorTrust {
    fn effective_actions(&self, policy: &OperatorPolicy) -> Vec<String> {
        policy.effective_actions(&self.default_actions).to_vec()
    }

    fn record_pending(&mut self, operator_id: &OperatorId, key: [u8; 32], now_ms: u64) {
        self.pending.retain(|_, pending| {
            now_ms.saturating_sub(pending.last_seen_ms) < PENDING_OPERATOR_TTL_MS
        });
        match self.pending.get_mut(operator_id) {
            // A different key replaces the pending one: the newest request is what an
            // administrator compares fingerprints against.
            Some(pending) => {
                if pending.key != key {
                    pending.key = key;
                    pending.first_seen_ms = now_ms;
                }
                pending.last_seen_ms = now_ms;
            }
            None => {
                while self.pending.len() >= MAX_PENDING_OPERATORS {
                    let oldest = self
                        .pending
                        .iter()
                        .min_by_key(|(_, pending)| pending.last_seen_ms)
                        .map(|(id, _)| id.clone());
                    match oldest {
                        Some(oldest) => self.pending.remove(&oldest),
                        None => break,
                    };
                }
                self.pending.insert(
                    operator_id.clone(),
                    PendingOperator {
                        key,
                        first_seen_ms: now_ms,
                        last_seen_ms: now_ms,
                    },
                );
            }
        }
    }

    fn store_file(&self) -> StoreFile {
        StoreFile {
            version: STORE_VERSION,
            operators: self
                .enrolled
                .iter()
                .map(|(operator_id, operator)| StoredOperator {
                    operator_id: operator_id.clone(),
                    public_key_hex: encode_hex(&operator.key),
                    method: operator.method,
                    policy: operator.policy.clone(),
                    enrolled_at_ms: operator.enrolled_at_ms,
                })
                .collect(),
            revoked: self.revoked.iter().cloned().collect(),
        }
    }
}

impl NodeSecurity {
    /// Default action patterns for operators whose policy does not name its own
    /// (`ORION_NODE_OPERATOR_ACTIONS`).
    pub fn set_operator_default_actions(&self, patterns: Vec<String>) {
        write_rwlock(self.operators.write(), "operators").default_actions = patterns;
    }

    pub fn operator_default_actions(&self) -> Vec<String> {
        read_rwlock(self.operators.read(), "operators")
            .default_actions
            .clone()
    }

    fn persist_operators(&self) -> Result<(), NodeError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(());
        };
        let file = read_rwlock(self.operators.read(), "operators").store_file();
        let bytes = serde_json::to_vec_pretty(&file)
            .map_err(|err| NodeError::Storage(format!("failed to encode operators: {err}")))?;
        atomic_write_file(
            &store_path(storage),
            &bytes,
            "create trusted operator store directory",
            "write trusted operator store",
            "install trusted operator store",
        )
    }

    /// Authenticates a request signed by an operator principal (`operator:<name>`).
    ///
    /// The signature must verify against the key the request carries. An enrolled operator must
    /// present its pinned key and a fresh nonce. A validly signed request from an operator that
    /// is not enrolled is recorded as pending; only its `OperatorHello` is answered.
    pub(crate) fn authenticate_operator(
        &self,
        auth: &PeerRequestAuth,
        payload: &PeerRequestPayload,
        is_hello: bool,
    ) -> Result<OperatorAuthentication, NodeError> {
        let operator_id = OperatorId::from_principal(&auth.node_id).ok_or_else(|| {
            NodeError::Authentication(format!("invalid operator principal `{}`", auth.node_id))
        })?;
        let key = verify_peer_request(auth, payload)
            .map_err(|err| NodeError::Authentication(format!("operator {operator_id}: {err}")))?;
        let now = now_ms();
        let enrolled = {
            let mut trust = write_rwlock(self.operators.write(), "operators");
            if trust.revoked.contains(&operator_id) {
                return Err(NodeError::Authorization(format!(
                    "operator {operator_id} was removed from node {}",
                    self.local_node_id
                )));
            }
            match trust.enrolled.get_mut(&operator_id) {
                Some(operator) if operator.key == key => {
                    operator.last_seen_ms = now;
                    let policy = operator.policy.clone();
                    Some(AuthenticatedOperator {
                        operator_id: operator_id.clone(),
                        public_key_hex: PublicKeyHex::new(encode_hex(&key)),
                        allowed_actions: trust.effective_actions(&policy),
                        policy,
                    })
                }
                Some(_) => {
                    return Err(NodeError::Authentication(format!(
                        "operator {operator_id} signed with key {}, not its enrolled key",
                        key_fingerprint(&key)
                    )));
                }
                None => {
                    trust.record_pending(&operator_id, key, now);
                    None
                }
            }
        };
        match enrolled {
            Some(operator) => {
                self.record_and_validate_nonce(&auth.node_id, auth.nonce)?;
                Ok(OperatorAuthentication::Enrolled(operator))
            }
            None if is_hello => Ok(OperatorAuthentication::Unenrolled {
                operator_id,
                public_key: key,
            }),
            None => Err(NodeError::Authorization(format!(
                "operator {operator_id} is not enrolled on node {} (key fingerprint {}); approve \
                 it with `orionctl operators enroll {operator_id} --fingerprint {}` or enroll \
                 with the shared enrollment key",
                self.local_node_id,
                key_fingerprint(&key),
                key_fingerprint(&key)
            ))),
        }
    }

    /// Pins `key` for `operator_id` with `policy`, lifting a revocation.
    pub(crate) fn enroll_operator(
        &self,
        operator_id: &OperatorId,
        key: [u8; 32],
        method: OperatorEnrollmentMethod,
        policy: OperatorPolicy,
    ) -> Result<(), NodeError> {
        policy.validate().map_err(NodeError::Config)?;
        {
            let mut trust = write_rwlock(self.operators.write(), "operators");
            trust.revoked.remove(operator_id);
            trust.pending.remove(operator_id);
            trust.enrolled.insert(
                operator_id.clone(),
                EnrolledOperator {
                    key,
                    method,
                    policy,
                    enrolled_at_ms: now_ms(),
                    last_seen_ms: 0,
                },
            );
        }
        self.clear_seen_nonces(&operator_id.to_principal())?;
        self.persist_operators()
    }

    /// Revokes `operator_id` (persisted). Returns whether anything changed.
    pub(crate) fn remove_operator(&self, operator_id: &OperatorId) -> Result<bool, NodeError> {
        let changed = {
            let mut trust = write_rwlock(self.operators.write(), "operators");
            let enrolled = trust.enrolled.remove(operator_id).is_some();
            let pending = trust.pending.remove(operator_id).is_some();
            let revoked = trust.revoked.insert(operator_id.clone());
            enrolled || pending || revoked
        };
        self.clear_seen_nonces(&operator_id.to_principal())?;
        if changed {
            self.persist_operators()?;
        }
        Ok(changed)
    }

    /// The key of a pending (unapproved) operator.
    pub(crate) fn pending_operator_key(&self, operator_id: &OperatorId) -> Option<[u8; 32]> {
        read_rwlock(self.operators.read(), "operators")
            .pending
            .get(operator_id)
            .map(|pending| pending.key)
    }

    /// The trust state of `operator_id` and the key it is known by.
    #[cfg_attr(not(feature = "discovery-mdns"), allow(dead_code))]
    pub(crate) fn operator_state(
        &self,
        operator_id: &OperatorId,
    ) -> Option<(OperatorTrustState, Option<[u8; 32]>)> {
        let trust = read_rwlock(self.operators.read(), "operators");
        if trust.revoked.contains(operator_id) {
            return Some((OperatorTrustState::Revoked, None));
        }
        if let Some(operator) = trust.enrolled.get(operator_id) {
            return Some((OperatorTrustState::Enrolled, Some(operator.key)));
        }
        trust
            .pending
            .get(operator_id)
            .map(|pending| (OperatorTrustState::Pending, Some(pending.key)))
    }

    /// Shared-key enrollment never overrides an administrator: it is refused for revoked
    /// operators and for operators enrolled with another key.
    #[cfg_attr(not(feature = "discovery-mdns"), allow(dead_code))]
    pub(crate) fn check_operator_auto_enrollable(
        &self,
        operator_id: &OperatorId,
        key: &[u8; 32],
    ) -> Result<(), NodeError> {
        match self.operator_state(operator_id) {
            Some((OperatorTrustState::Revoked, _)) => Err(NodeError::Authorization(format!(
                "operator {operator_id} was removed by an administrator"
            ))),
            Some((OperatorTrustState::Enrolled, Some(existing))) if &existing != key => {
                Err(NodeError::Authentication(format!(
                    "operator {operator_id} is already enrolled with a different key"
                )))
            }
            _ => Ok(()),
        }
    }

    /// Every enrolled, pending and revoked operator.
    pub(crate) fn operator_records(&self) -> Vec<OperatorRecord> {
        let now = now_ms();
        let trust = read_rwlock(self.operators.read(), "operators");
        let mut records: Vec<OperatorRecord> = trust
            .enrolled
            .iter()
            .map(|(operator_id, operator)| OperatorRecord {
                operator_id: operator_id.clone(),
                state: OperatorTrustState::Enrolled,
                public_key_hex: Some(PublicKeyHex::new(encode_hex(&operator.key))),
                key_fingerprint: Some(key_fingerprint(&operator.key)),
                method: Some(operator.method),
                effective_actions: trust.effective_actions(&operator.policy),
                policy: operator.policy.clone(),
                since_ms: operator.enrolled_at_ms,
                last_seen_ms: operator.last_seen_ms,
            })
            .collect();
        records.extend(
            trust
                .pending
                .iter()
                .filter(|(_, pending)| {
                    now.saturating_sub(pending.last_seen_ms) < PENDING_OPERATOR_TTL_MS
                })
                .map(|(operator_id, pending)| OperatorRecord {
                    operator_id: operator_id.clone(),
                    state: OperatorTrustState::Pending,
                    public_key_hex: Some(PublicKeyHex::new(encode_hex(&pending.key))),
                    key_fingerprint: Some(key_fingerprint(&pending.key)),
                    method: None,
                    policy: OperatorPolicy {
                        read: false,
                        actions: Some(Vec::new()),
                    },
                    effective_actions: Vec::new(),
                    since_ms: pending.first_seen_ms,
                    last_seen_ms: pending.last_seen_ms,
                }),
        );
        records.extend(trust.revoked.iter().map(|operator_id| OperatorRecord {
            operator_id: operator_id.clone(),
            state: OperatorTrustState::Revoked,
            public_key_hex: None,
            key_fingerprint: None,
            method: None,
            policy: OperatorPolicy {
                read: false,
                actions: Some(Vec::new()),
            },
            effective_actions: Vec::new(),
            since_ms: 0,
            last_seen_ms: 0,
        }));
        records.sort_by(|a, b| a.operator_id.cmp(&b.operator_id));
        records
    }
}
