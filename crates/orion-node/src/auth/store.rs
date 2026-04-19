use super::{
    IDENTITY_FILE, NodeSecurity, TRUST_STORE_FILE, TrustedPeerState, TrustedPeerStore,
    crypto::parse_public_key_bytes,
};
use crate::blocking::{
    WorkerThread, request_worker_operation_async, request_worker_operation_blocking,
    run_possibly_blocking,
};
use crate::lock::read_rwlock;
use crate::storage_io::{atomic_write_file, storage_path_error};
use crate::{NodeError, NodeStorage};
use ed25519_dalek::SigningKey;
use orion::{NodeId, encode_to_vec};
use rand_core::OsRng;
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::atomic::Ordering,
};
use tokio::sync::{mpsc, oneshot};

fn decode_store_error(context: &str, err: impl std::fmt::Display) -> NodeError {
    NodeError::Storage(format!("failed to decode {context}: {err}"))
}

impl NodeSecurity {
    fn capture_persisted_security_state(&self) -> Result<TrustedPeerStore, NodeError> {
        let trusted = self.trusted_peer_state.read();
        let trusted = read_rwlock(trusted, "trusted_peer_state");
        let peers = trusted
            .peers
            .iter()
            .map(|(node_id, key)| (node_id.clone(), key.to_vec()))
            .collect();
        let tls_root_certs = trusted.tls_root_certs.clone();
        let revoked = trusted.revoked.clone();
        drop(trusted);

        let seen_nonces = self.seen_nonces.read();
        let seen_nonces = read_rwlock(seen_nonces, "seen_nonces");

        Ok(TrustedPeerStore {
            peers,
            tls_root_certs,
            revoked,
            seen_nonces: seen_nonces
                .iter()
                .map(|(node_id, nonces)| (node_id.clone(), nonces.iter().copied().collect()))
                .collect(),
            next_outbound_nonce: self.next_nonce.load(Ordering::SeqCst),
        })
    }

    pub(super) async fn persist_security_state_async(&self) -> Result<(), NodeError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(());
        };
        let store = self.capture_persisted_security_state()?;
        if let Some(worker) = &self.auth_state_worker {
            return worker.persist_state_async(store).await;
        }
        persist_trusted_peer_store_async(storage.clone(), store).await
    }

    pub(super) fn persist_security_state(&self) -> Result<(), NodeError> {
        // Keep the blocking variant for startup/admin callers and other sync-only edges. Async
        // request-serving code should use `persist_security_state_async()` instead.
        let Some(storage) = self.storage.as_ref() else {
            return Ok(());
        };
        let store = self.capture_persisted_security_state()?;
        if let Some(worker) = &self.auth_state_worker {
            return worker.persist_state_blocking(store);
        }
        persist_trusted_peer_store(storage, &store)
    }
}

enum AuthStateCommand {
    Persist {
        store: Box<TrustedPeerStore>,
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

    async fn persist_state_async(&self, store: TrustedPeerStore) -> Result<(), NodeError> {
        request_worker_operation_async(
            self.thread.sender(),
            |reply| AuthStateCommand::Persist {
                store: Box::new(store),
                reply,
            },
            NodeError::AuthStateWorkerUnavailable,
            NodeError::AuthStateWorkerTerminated,
        )
        .await
    }

    fn persist_state_blocking(&self, store: TrustedPeerStore) -> Result<(), NodeError> {
        request_worker_operation_blocking(
            self.thread.sender(),
            |reply| AuthStateCommand::Persist {
                store: Box::new(store),
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
            AuthStateCommand::Persist { store, reply } => {
                let _ = reply.send(persist_trusted_peer_store(&storage, &store));
            }
        }
    }
}

async fn persist_trusted_peer_store_async(
    storage: NodeStorage,
    store: TrustedPeerStore,
) -> Result<(), NodeError> {
    tokio::task::spawn_blocking(move || persist_trusted_peer_store(&storage, &store))
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
        return Ok(SigningKey::generate(&mut OsRng));
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

    let signing_key = SigningKey::generate(&mut OsRng);
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
        .map(|(node_id, nonces)| (node_id, nonces.into_iter().collect()))
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
