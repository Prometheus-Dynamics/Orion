//! The recovery way in.
//!
//! A board is repaired by logging in to it. So when the comm core cannot start as it was
//! told to, it does not stop: it opens this, an SSH way in that needs nothing the board
//! might have broken.
//!
//! - **Nothing from the state directory.** Who may log in is a file of public keys in the
//!   image (`/etc/orion/recovery_keys`, one OpenSSH public key a line), read again while
//!   it runs. The trust store is not opened: it may be what is broken. The host key is the
//!   board's if it can be read, and one for the run if not.
//! - **Nothing from the config.** If the settings could not be read, nothing in them is
//!   believed: the addresses are the usual ones and security is on. A board whose settings
//!   were read and say security is off is as open here as it always is.
//! - **Nothing written.** A full disk is not a reason to keep someone out: nothing is
//!   saved, and a session needs no home directory and no shell but `/bin/sh`.
//! - **Never wider.** Recovery lets in who the recovery keys say and nobody else. A board
//!   with security on and no recovery keys is closed here, and says so.
//! - **Never backed off.** If no address can be listened on it tries again every second,
//!   for ever, at the same pace. The way in to a broken board does not get slower the
//!   longer the board has been broken.

#![cfg(target_os = "linux")]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use orion_check::Security;
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_ssh::{Helper, Ssh, SshConfig, Subsystem};
use orion_types::KeyId;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Where the recovery keys are unless something says otherwise.
pub const KEYS_FILE: &str = "/etc/orion/recovery_keys";

/// What the recovery way in is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryConfig {
    /// Where to listen: the first of these that can be, each time it tries.
    pub listen: Vec<SocketAddr>,
    /// The public keys that may log in, one OpenSSH public key a line.
    pub keys_file: PathBuf,
    /// The board's host key, if it can be read. One is made for the run if not.
    pub host_key: PathBuf,
    /// The program sessions run in.
    pub helper: Helper,
    /// The subsystems there are.
    pub subsystems: Vec<Subsystem>,
    /// Where the board's `passwd` and `group` are, if it still has them.
    pub etc: PathBuf,
    /// Off only if the board's settings were read and say so.
    pub security: Security,
    /// How long between tries to listen. It does not grow.
    pub retry: Duration,
    /// Why the comm core is in recovery, for the log and for whoever asks.
    pub why: String,
}

/// The keys in the text of a recovery keys file. A line that is not a plain ed25519 public
/// key lets nobody in: it is skipped, never half understood.
#[must_use]
pub fn keys_in(text: &str) -> Vec<([u8; 32], String)> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let key = russh::keys::PublicKey::from_openssh(line).ok()?;
            let bytes = key.key_data().ed25519()?.0;
            Some((bytes, key.comment().as_str_lossy().to_owned()))
        })
        .collect()
}

/// A trust store that holds the recovery keys and nothing else: each is an operator
/// trusted with everything, which is what logging in asks for.
fn trust(keys: &[([u8; 32], String)]) -> Option<Keys> {
    let mut store = Keys::open(KeysConfig::in_memory("recovery"), Security::On).ok()?;
    for (key, name) in keys {
        let name = if name.is_empty() { "recovery" } else { name };
        store.approve_operator(*key, name).ok()?;
        store.grant(&KeyId(*key), true, Vec::new(), true).ok()?;
    }
    Some(store)
}

struct Shared {
    config: RecoveryConfig,
    keys: SharedKeys,
    /// The keys file as it was last read.
    read: Mutex<Option<String>>,
    local_addr: Mutex<Option<SocketAddr>>,
    attempts: AtomicU64,
}

impl Shared {
    /// Reads the keys file again and, if it changed, trusts what it says now.
    fn reload(&self) {
        let text = std::fs::read_to_string(&self.config.keys_file).ok();
        let mut read = self.read.lock().unwrap_or_else(PoisonError::into_inner);
        if *read == text && read.is_some() {
            return;
        }
        let keys = keys_in(text.as_deref().unwrap_or_default());
        if let Some(store) = trust(&keys) {
            *self.keys.lock() = store;
        }
        if *read != text || read.is_none() {
            match (&text, keys.len(), self.config.security) {
                (_, _, Security::Off) => {}
                (None, _, _) => warn!(
                    "recovery: {} cannot be read, so nobody can log in",
                    self.config.keys_file.display()
                ),
                (Some(_), 0, _) => warn!(
                    "recovery: {} has no key in it, so nobody can log in",
                    self.config.keys_file.display()
                ),
                (Some(_), count, _) => info!("recovery: {count} keys may log in"),
            }
        }
        // An unreadable file is read again next time, in case it is back.
        *read = text.or(Some(String::new()));
    }
}

/// The recovery way in. Dropping it closes it.
pub struct Recovery {
    shared: Arc<Shared>,
    task: JoinHandle<()>,
}

impl std::fmt::Debug for Recovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Recovery({:?}: {})",
            self.local_addr(),
            self.shared.config.why
        )
    }
}

impl Drop for Recovery {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Recovery {
    /// Opens the recovery way in. This cannot fail: if nothing can be listened on yet, it
    /// goes on trying.
    #[must_use]
    pub fn start(config: RecoveryConfig) -> Self {
        error!("recovery: the comm core is in recovery: {}", config.why);
        // Until the file has been read, nobody is trusted.
        let empty = trust(&[]).map(SharedKeys::new);
        let keys = empty.unwrap_or_else(|| {
            // No random bytes for an identity nothing here uses: still nobody is trusted.
            SharedKeys::new(
                Keys::open(KeysConfig::in_memory("recovery"), Security::On)
                    .expect("a trust store that keeps nothing can always be made"),
            )
        });
        let shared = Arc::new(Shared {
            config,
            keys,
            read: Mutex::new(None),
            local_addr: Mutex::new(None),
            attempts: AtomicU64::new(0),
        });
        let running = shared.clone();
        let task = tokio::spawn(async move {
            let config = &running.config;
            loop {
                let tries = running.attempts.fetch_add(1, Ordering::AcqRel);
                running.reload();
                let mut last = None;
                for listen in &config.listen {
                    let ssh = SshConfig {
                        listen: *listen,
                        host_key: config.host_key.clone(),
                        helper: config.helper.clone(),
                        subsystems: config.subsystems.clone(),
                        forward: true,
                        etc: config.etc.clone(),
                        recovery: true,
                        max_connections: 16,
                        max_unauthenticated: 8,
                        login: Duration::from_secs(60),
                    };
                    match Ssh::start(ssh, config.security, running.keys.clone()).await {
                        Ok(server) => {
                            let at = server.local_addr();
                            info!("recovery: SSH is open on {at}");
                            *running
                                .local_addr
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner) = Some(at);
                            // Open for as long as this lives, with the keys file read again
                            // now and then: someone at the console may have just fixed it.
                            loop {
                                tokio::time::sleep(Duration::from_secs(2).min(config.retry * 2))
                                    .await;
                                running.reload();
                            }
                        }
                        Err(error) => last = Some((listen, error)),
                    }
                }
                // Said the first time and once a minute, tried every time.
                if let Some((listen, error)) = last
                    && tries.is_multiple_of(60)
                {
                    warn!("recovery: cannot listen on {listen}: {error}; trying again, and again");
                }
                // The same wait every time.
                tokio::time::sleep(config.retry).await;
            }
        });
        Self { shared, task }
    }

    /// Where SSH clients connect, once somewhere could be listened on.
    #[must_use]
    pub fn local_addr(&self) -> Option<SocketAddr> {
        *self
            .shared
            .local_addr
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// How many times it has tried to listen.
    #[must_use]
    pub fn attempts(&self) -> u64 {
        self.shared.attempts.load(Ordering::Acquire)
    }

    /// Why the comm core is in recovery.
    #[must_use]
    pub fn why(&self) -> &str {
        &self.shared.config.why
    }

    /// The file whose keys may log in.
    #[must_use]
    pub fn keys_file(&self) -> &Path {
        &self.shared.config.keys_file
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_plain_ed25519_keys_are_keys() {
        let good = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl ana@desk";
        let text = format!(
            "# who may repair this board\n\n{good}\n  {good}  \n\
             restrict {good}\n\
             ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC7 old@key\n\
             not a key at all\n\
             ssh-ed25519 AAAA truncated\n"
        );
        let keys = keys_in(&text);
        assert_eq!(keys.len(), 2);
        assert!(
            keys.iter()
                .all(|(key, name)| key == &keys[0].0 && name == "ana@desk")
        );
        assert!(keys_in("").is_empty());
        // Each is an operator who may log in, and nobody else is.
        let store = trust(&keys[..1]).expect("a store");
        assert!(
            store
                .operator(&KeyId(keys[0].0))
                .is_some_and(|operator| operator.all_actions)
        );
        assert!(store.operator(&KeyId([1; 32])).is_none());
    }
}
