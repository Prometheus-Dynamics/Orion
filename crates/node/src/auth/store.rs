use super::{
    IDENTITY_FILE, NONCE_CACHE_LIMIT, NodeSecurity, TRUST_STORE_FILE, TrustedPeerState,
    TrustedPeerStore, crypto::parse_public_key_bytes,
};
use crate::blocking::{
    WorkerThread, request_worker_operation_async, request_worker_operation_blocking,
    run_possibly_blocking,
};
use crate::lock::read_rwlock;
use crate::storage_io::{atomic_write_file, storage_path_error};
use crate::{NodeError, NodeStorage};
use ed25519_dalek::SigningKey;
use getrandom::{SysRng, rand_core::UnwrapErr};
use orion::{NodeId, encode_to_vec};
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot};

fn decode_store_error(context: &str, err: impl std::fmt::Display) -> NodeError {
    NodeError::Storage(format!("failed to decode {context}: {err}"))
}

impl NodeSecurity {
    /// Captures the store and its generation. Both read guards are held while the generation is
    /// taken: writers hold a write guard, so generation order is the order of the captured state.
    fn capture_persisted_security_state(&self) -> Result<(TrustedPeerStore, u64), NodeError> {
        let trusted = self.trusted_peer_state.read();
        let trusted = read_rwlock(trusted, "trusted_peer_state");
        let seen_nonces = self.seen_nonces.read();
        let seen_nonces = read_rwlock(seen_nonces, "seen_nonces");
        let generation = self.persist_order.next.fetch_add(1, Ordering::SeqCst) + 1;

        let store = TrustedPeerStore {
            peers: trusted
                .peers
                .iter()
                .map(|(node_id, key)| (node_id.clone(), key.to_vec()))
                .collect(),
            tls_root_certs: trusted.tls_root_certs.clone(),
            revoked: trusted.revoked.clone(),
            seen_nonces: seen_nonces
                .iter()
                .map(|(node_id, nonces)| (node_id.clone(), nonces.iter().copied().collect()))
                .collect(),
            next_outbound_nonce: self.next_nonce.load(Ordering::SeqCst),
        };
        Ok((store, generation))
    }

    pub(super) async fn persist_security_state_async(&self) -> Result<(), NodeError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(());
        };
        let (store, generation) = self.capture_persisted_security_state()?;
        let order = self.persist_order.clone();
        if let Some(worker) = &self.auth_state_worker {
            return worker.persist_state_async(store, generation, order).await;
        }
        persist_trusted_peer_store_async(storage.clone(), store, generation, order).await
    }

    pub(super) fn persist_security_state(&self) -> Result<(), NodeError> {
        // Keep the blocking variant for startup/admin callers and other sync-only edges. Async
        // request-serving code should use `persist_security_state_async()` instead.
        let Some(storage) = self.storage.as_ref() else {
            return Ok(());
        };
        let (store, generation) = self.capture_persisted_security_state()?;
        let order = self.persist_order.clone();
        if let Some(worker) = &self.auth_state_worker {
            return worker.persist_state_blocking(store, generation, order);
        }
        order.write_if_newer(generation, || persist_trusted_peer_store(storage, &store))
    }
}

/// Trust-store write order. Captures race to the writer (worker queue, blocking pool or the
/// caller), so a capture taken earlier can arrive later; it is skipped once a newer one is on disk,
/// since every capture is a full copy of the state.
#[derive(Debug, Default)]
pub(super) struct PersistOrder {
    pub(super) next: AtomicU64,
    written: Mutex<u64>,
}

impl PersistOrder {
    fn write_if_newer(
        &self,
        generation: u64,
        write: impl FnOnce() -> Result<(), NodeError>,
    ) -> Result<(), NodeError> {
        let mut written = self.written.lock().unwrap_or_else(PoisonError::into_inner);
        if generation <= *written {
            return Ok(());
        }
        write()?;
        *written = generation;
        Ok(())
    }
}

enum AuthStateCommand {
    Persist {
        store: Box<TrustedPeerStore>,
        generation: u64,
        order: Arc<PersistOrder>,
        reply: oneshot::Sender<Result<(), NodeError>>,
    },
}

pub(super) struct AuthStateWorker {
    thread: WorkerThread<AuthStateCommand>,
}

impl AuthStateWorker {
    pub(super) fn new(storage: NodeStorage, queue_capacity: usize) -> Result<Self, NodeError> {
        Ok(Self {
            thread: WorkerThread::spawn(
                "orion-auth-state",
                queue_capacity,
                "auth state worker",
                move |receiver| run_auth_state_worker(storage, receiver),
            )?,
        })
    }

    pub(super) fn queue_depth(&self) -> u64 {
        self.thread.queue_depth()
    }

    async fn persist_state_async(
        &self,
        store: TrustedPeerStore,
        generation: u64,
        order: Arc<PersistOrder>,
    ) -> Result<(), NodeError> {
        request_worker_operation_async(
            self.thread.sender(),
            |reply| AuthStateCommand::Persist {
                store: Box::new(store),
                generation,
                order,
                reply,
            },
            NodeError::AuthStateWorkerUnavailable,
            NodeError::AuthStateWorkerTerminated,
        )
        .await
    }

    fn persist_state_blocking(
        &self,
        store: TrustedPeerStore,
        generation: u64,
        order: Arc<PersistOrder>,
    ) -> Result<(), NodeError> {
        request_worker_operation_blocking(
            self.thread.sender(),
            |reply| AuthStateCommand::Persist {
                store: Box::new(store),
                generation,
                order,
                reply,
            },
            NodeError::AuthStateWorkerUnavailable,
            NodeError::AuthStateWorkerTerminated,
        )
    }
}

fn run_auth_state_worker(storage: NodeStorage, mut receiver: mpsc::Receiver<AuthStateCommand>) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            AuthStateCommand::Persist {
                store,
                generation,
                order,
                reply,
            } => {
                let result = order
                    .write_if_newer(generation, || persist_trusted_peer_store(&storage, &store));
                let _ = reply.send(result);
            }
        }
    }
}

async fn persist_trusted_peer_store_async(
    storage: NodeStorage,
    store: TrustedPeerStore,
    generation: u64,
    order: Arc<PersistOrder>,
) -> Result<(), NodeError> {
    tokio::task::spawn_blocking(move || {
        order.write_if_newer(generation, || persist_trusted_peer_store(&storage, &store))
    })
    .await
    .map_err(|err| NodeError::Storage(format!("auth state task join failed: {err}")))?
}

fn persist_trusted_peer_store(
    storage: &NodeStorage,
    store: &TrustedPeerStore,
) -> Result<(), NodeError> {
    let bytes =
        encode_to_vec(store).map_err(|err| decode_store_error("trusted peer store", err))?;
    let trust_store_path = storage.trust_store_path();
    atomic_write_file(
        &trust_store_path,
        &bytes,
        "failed to create state directory",
        "failed to write trusted peer store",
        "failed to install trusted peer store",
    )
}

pub(super) fn load_or_create_identity(
    storage: Option<&NodeStorage>,
) -> Result<SigningKey, NodeError> {
    let Some(storage) = storage else {
        return Ok(SigningKey::generate(&mut UnwrapErr(SysRng)));
    };

    let path = storage.identity_path();
    if path.exists() {
        let bytes = run_possibly_blocking(|| {
            std::fs::read(&path)
                .map_err(|err| storage_path_error("failed to read node identity", &path, err))
        })?;
        let secret: [u8; 32] = bytes.try_into().map_err(|_| {
            NodeError::Storage(format!(
                "invalid identity key length in `{}`",
                path.display()
            ))
        })?;
        return Ok(SigningKey::from_bytes(&secret));
    }

    let signing_key = SigningKey::generate(&mut UnwrapErr(SysRng));
    atomic_write_file(
        &path,
        &signing_key.to_bytes(),
        "failed to create state directory",
        "failed to write node identity",
        "failed to install node identity",
    )?;
    Ok(signing_key)
}

pub(super) fn load_trusted_peer_state(
    storage: Option<&NodeStorage>,
) -> Result<TrustedPeerState, NodeError> {
    let Some(storage) = storage else {
        return Ok(TrustedPeerState::default());
    };
    let path = storage.trust_store_path();
    if !path.exists() {
        return Ok(TrustedPeerState::default());
    }
    let bytes = run_possibly_blocking(|| {
        std::fs::read(&path)
            .map_err(|err| storage_path_error("failed to read trusted peer store", &path, err))
    })?;
    let store = load_trusted_peer_store("trusted peer store", &bytes)?;
    let peers = store
        .peers
        .into_iter()
        .map(|(node_id, key)| Ok((node_id, parse_public_key_bytes(&key)?)))
        .collect::<Result<BTreeMap<_, _>, NodeError>>()?;
    Ok(TrustedPeerState {
        peers,
        tls_root_certs: store.tls_root_certs,
        revoked: store.revoked,
    })
}

pub(super) fn load_seen_nonces(
    storage: Option<&NodeStorage>,
) -> Result<BTreeMap<NodeId, VecDeque<u64>>, NodeError> {
    let Some(storage) = storage else {
        return Ok(BTreeMap::new());
    };
    let path = storage.trust_store_path();
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let bytes = run_possibly_blocking(|| {
        std::fs::read(&path)
            .map_err(|err| storage_path_error("failed to read trusted peer store", &path, err))
    })?;
    let store = load_trusted_peer_store("trusted peer store", &bytes)?;
    Ok(store
        .seen_nonces
        .into_iter()
        .filter_map(|(node_id, nonces)| {
            let skip = nonces.len().saturating_sub(NONCE_CACHE_LIMIT);
            let retained = nonces.into_iter().skip(skip).collect::<VecDeque<_>>();
            (!retained.is_empty()).then_some((node_id, retained))
        })
        .collect())
}

pub(super) fn load_next_outbound_nonce(storage: Option<&NodeStorage>) -> Result<u64, NodeError> {
    let Some(storage) = storage else {
        return Ok(1);
    };
    let path = storage.trust_store_path();
    if !path.exists() {
        return Ok(1);
    }
    let bytes = run_possibly_blocking(|| {
        std::fs::read(&path)
            .map_err(|err| storage_path_error("failed to read trusted peer store", &path, err))
    })?;
    Ok(load_trusted_peer_store("trusted peer store", &bytes)?
        .next_outbound_nonce
        .max(1))
}

fn load_trusted_peer_store(context: &str, bytes: &[u8]) -> Result<TrustedPeerStore, NodeError> {
    orion::decode_from_slice(bytes).map_err(|err| decode_store_error(context, err))
}

impl NodeStorage {
    pub fn identity_path(&self) -> PathBuf {
        self.root().join(IDENTITY_FILE)
    }

    pub fn trust_store_path(&self) -> PathBuf {
        self.root().join(TRUST_STORE_FILE)
    }
}

#[cfg(test)]
mod tests {
    use super::PersistOrder;
    use std::sync::atomic::Ordering;

    #[test]
    fn an_older_capture_arriving_late_is_not_written() {
        let order = PersistOrder::default();
        let older = order.next.fetch_add(1, Ordering::SeqCst) + 1;
        let newer = order.next.fetch_add(1, Ordering::SeqCst) + 1;
        let mut writes = Vec::new();
        order
            .write_if_newer(newer, || {
                writes.push(newer);
                Ok(())
            })
            .expect("newer capture should be written");
        order
            .write_if_newer(older, || {
                writes.push(older);
                Ok(())
            })
            .expect("a skipped capture is not an error");
        assert_eq!(writes, vec![newer]);
    }

    #[test]
    fn a_failed_write_does_not_advance_the_order() {
        let order = PersistOrder::default();
        let first = order.next.fetch_add(1, Ordering::SeqCst) + 1;
        let failed =
            order.write_if_newer(first, || Err(crate::NodeError::Storage("disk full".into())));
        assert!(failed.is_err());
        let mut written = false;
        order
            .write_if_newer(first, || {
                written = true;
                Ok(())
            })
            .expect("the same capture can be retried");
        assert!(written);
    }
}
