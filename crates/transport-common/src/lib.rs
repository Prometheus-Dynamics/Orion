#[cfg(feature = "tls")]
mod tls;
#[cfg(feature = "tls")]
pub use tls::{
    build_client_verifier, install_rustls_crypto_provider, parse_cert_chain, parse_private_key,
    root_store_from_pem, root_store_from_pem_iter,
};

use std::{
    hash::{Hash, Hasher},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    time::Duration,
};
use tokio::task::JoinSet;

pub const DEFAULT_TRANSPORT_SERVER_NAME: &str = "localhost";
pub const DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
pub const DEFAULT_HTTP_CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
pub const DEFAULT_TRANSPORT_IO_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_TRANSPORT_MAX_CONCURRENT_CONNECTIONS: usize = 1024;
pub const DEFAULT_QUIC_INSECURE_SELF_SIGNED_SAN_HOSTS: &[&str] =
    &[DEFAULT_TRANSPORT_SERVER_NAME, "node-b.local"];

pub fn loopback_ephemeral_socket_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

pub fn stable_fingerprint(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

pub fn fingerprint_file_state(
    path: &Path,
    hasher: &mut std::collections::hash_map::DefaultHasher,
    context: &str,
) -> Result<(), String> {
    path.hash(hasher);
    let metadata = std::fs::metadata(path).map_err(|err| format!("{context}: {err}"))?;
    metadata.len().hash(hasher);
    let modified = metadata
        .modified()
        .map_err(|err| format!("{context}: {err}"))?;
    modified
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|err| format!("{context}: {err}"))?
        .as_nanos()
        .hash(hasher);
    Ok(())
}

pub struct ConnectionTasks {
    tasks: JoinSet<()>,
}

impl ConnectionTasks {
    pub fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
        }
    }

    pub fn spawn<F, E>(&mut self, future: F)
    where
        F: std::future::Future<Output = Result<(), E>> + Send + 'static,
        E: Send + 'static,
    {
        self.tasks.spawn(async move {
            let _ = future.await;
        });
    }

    pub fn spawn_unit<F>(&mut self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.tasks.spawn(future);
    }

    pub fn reap_finished(&mut self) {
        while let Some(_result) = self.tasks.try_join_next() {}
    }

    pub async fn abort_all(&mut self) {
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }
}

impl Default for ConnectionTasks {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::ConnectionTasks;

    #[test]
    fn connection_tasks_reaps_completed_tasks_without_shutdown() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime should build")
            .block_on(async {
                let mut tasks = ConnectionTasks::new();
                tasks.spawn_unit(async {});
                tokio::task::yield_now().await;

                tasks.reap_finished();

                assert!(tasks.tasks.is_empty());
            });
    }
}
