//! The trust store: peers, operators, revoked keys, kept in a file.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use orion_config::{ConfigError, Security, Source};
use orion_msg_peers::OperatorState;
use orion_server::{Grants, Principal, What};
use orion_types::{KeyId, NodeName};

use crate::enroll_impl::{operator_proof, verify_proof};
use orion_identity::Identity;

const IDENTITY_FILE: &str = "node-identity.ed25519";
const TRUST_FILE: &str = "trust";
const HEADER: &str =
    "# Orion trust store. One entry per line; lines this build does not know are kept.";

/// The settings of the trust store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeysConfig {
    /// `state.dir`: where the identity and the trust store are kept. `None` keeps nothing:
    /// a new identity every start, for tests and for a board with no storage.
    pub state_dir: Option<PathBuf>,
    /// `cluster`: the cluster this node belongs to.
    pub cluster: String,
    /// `enrollment.key`: what a newcomer must prove it knows to enroll itself. `None`: only an
    /// administrator enrolls.
    pub enrollment_key: Option<Vec<u8>>,
    /// `operators.read`: whether a newly enrolled operator may read.
    pub operators_read: bool,
    /// `operators.actions`: the actions a newly enrolled operator may run. `*` at the end of a
    /// name matches any ending; `*` alone is every action.
    pub operators_actions: Vec<String>,
}

impl KeysConfig {
    /// Takes the settings out of `source`.
    ///
    /// # Errors
    ///
    /// A value that doesn't parse.
    pub fn from_source(source: &mut Source, state_dir: &Path) -> Result<Self, ConfigError> {
        Ok(Self {
            state_dir: Some(state_dir.to_owned()),
            cluster: source.get("cluster", "default".to_owned())?,
            enrollment_key: source
                .text("enrollment.key")
                .map(|key| key.as_bytes().to_vec()),
            operators_read: source.get("operators.read", true)?,
            operators_actions: source.list("operators.actions")?,
        })
    }

    /// Settings that keep nothing on disk.
    #[must_use]
    pub fn in_memory(cluster: &str) -> Self {
        Self {
            state_dir: None,
            cluster: cluster.to_owned(),
            enrollment_key: None,
            operators_read: true,
            operators_actions: Vec::new(),
        }
    }
}

/// Why the trust store couldn't be opened or saved.
#[derive(Debug)]
pub enum KeysError {
    /// The node's key is there but can't be used. It is never replaced.
    Identity(io::Error),
    /// The trust store can't be read or written.
    Store(io::Error),
}

impl fmt::Display for KeysError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => write!(f, "node identity: {error}"),
            Self::Store(error) => write!(f, "trust store: {error}"),
        }
    }
}

impl std::error::Error for KeysError {}

/// One operator: a key and what it may do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operator {
    /// Its key.
    pub key: KeyId,
    /// A name for people.
    pub name: String,
    /// Whether it is trusted.
    pub state: OperatorState,
    /// Whether it may read.
    pub read: bool,
    /// The actions it may run.
    pub actions: Vec<String>,
    /// Whether it may run every action, and write.
    pub all_actions: bool,
}

impl Operator {
    /// Whether this operator may do `what`.
    #[must_use]
    pub fn may(&self, what: What<'_>) -> bool {
        if self.state != OperatorState::Enrolled {
            return false;
        }
        match what {
            What::Read(_) | What::Open(_) => self.read,
            What::Run(name) => {
                self.all_actions || self.actions.iter().any(|pattern| matches(pattern, name))
            }
            What::Login => {
                self.all_actions || self.actions.iter().any(|pattern| matches(pattern, "login"))
            }
            // A shell is a login: granted by `login` for any account, or `shell:ACCOUNT`.
            What::Shell(account) => {
                self.all_actions
                    || self.actions.iter().any(|pattern| {
                        matches(pattern, "login")
                            || pattern
                                .strip_prefix("shell:")
                                .is_some_and(|pattern| matches(pattern, account))
                    })
            }
            // Changing what the board should be is for those trusted with everything.
            What::Write(_) => self.all_actions,
        }
    }
}

/// Whether an action `name` is covered by `pattern`: the name itself, a prefix ending in `*`,
/// or `*`.
fn matches(pattern: &str, name: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix),
        None => pattern == name,
    }
}

/// Lets the group read a key made owner-only by an older build, so node logic, in the group,
/// can act as the node. Anything else about the file's mode is left as it is.
fn group_may_read(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std::fs::metadata(path)?.permissions();
    if permissions.mode() & 0o777 == 0o600 {
        permissions.set_mode(0o640);
        std::fs::set_permissions(path, permissions)?;
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }
    let mut out = [0_u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Everything the node trusts.
#[derive(Debug)]
pub struct Keys {
    identity: Identity,
    config: KeysConfig,
    security: Security,
    peers: BTreeMap<NodeName, [u8; 32]>,
    operators: BTreeMap<[u8; 32], Operator>,
    revoked: BTreeSet<[u8; 32]>,
    /// Lines of the file this build did not understand, written back as they were.
    unknown: Vec<String>,
}

impl Keys {
    /// Opens the node's identity and trust store, making them if this is the first start.
    ///
    /// # Errors
    ///
    /// [`KeysError`]. A key or store that is there but unreadable is an error, never a reason
    /// to start afresh.
    pub fn open(config: KeysConfig, security: Security) -> Result<Self, KeysError> {
        let identity = match &config.state_dir {
            Some(dir) => {
                let path = dir.join(IDENTITY_FILE);
                let identity = Identity::load_or_create(&path).map_err(KeysError::Identity)?;
                group_may_read(&path).map_err(KeysError::Identity)?;
                identity
            }
            None => Identity::generate(),
        };
        let mut keys = Self {
            identity,
            config,
            security,
            peers: BTreeMap::new(),
            operators: BTreeMap::new(),
            revoked: BTreeSet::new(),
            unknown: Vec::new(),
        };
        if let Some(dir) = &keys.config.state_dir {
            match std::fs::read_to_string(dir.join(TRUST_FILE)) {
                Ok(text) => keys.parse(&text),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(KeysError::Store(error)),
            }
        }
        Ok(keys)
    }

    fn parse(&mut self, text: &str) {
        for line in text.lines() {
            let mut words = line.split_whitespace();
            let parsed = match words.next() {
                None | Some("#") => continue,
                Some(word) if word.starts_with('#') => continue,
                Some("peer") => (|| {
                    let name = NodeName::new(words.next()?).ok()?;
                    self.peers.insert(name, unhex(words.next()?)?);
                    Some(())
                })(),
                Some("revoked") => (|| {
                    self.revoked.insert(unhex(words.next()?)?);
                    Some(())
                })(),
                Some("operator") => (|| {
                    let key = unhex(words.next()?)?;
                    let state = match words.next()? {
                        "enrolled" => OperatorState::Enrolled,
                        "pending" => OperatorState::Pending,
                        "revoked" => OperatorState::Revoked,
                        _ => return None,
                    };
                    let read = words.next()? == "1";
                    let all_actions = words.next()? == "1";
                    let name = words.next()?.to_owned();
                    let actions = match words.next()? {
                        "-" => Vec::new(),
                        list => list.split(',').map(str::to_owned).collect(),
                    };
                    self.operators.insert(
                        key,
                        Operator {
                            key: KeyId(key),
                            name,
                            state,
                            read,
                            actions,
                            all_actions,
                        },
                    );
                    Some(())
                })(),
                Some(_) => None,
            };
            if parsed.is_none() {
                // Written by a newer build, or by hand. Kept, so an older node never destroys
                // what a newer one stored.
                self.unknown.push(line.to_owned());
            }
        }
    }

    fn save(&self) -> Result<(), KeysError> {
        let Some(dir) = &self.config.state_dir else {
            return Ok(());
        };
        let mut text = format!("{HEADER}\n");
        for (name, key) in &self.peers {
            text.push_str(&format!("peer {name} {}\n", hex(key)));
        }
        for (key, operator) in &self.operators {
            let state = match operator.state {
                OperatorState::Enrolled => "enrolled",
                OperatorState::Pending => "pending",
                _ => "revoked",
            };
            let actions = if operator.actions.is_empty() {
                "-".to_owned()
            } else {
                operator.actions.join(",")
            };
            text.push_str(&format!(
                "operator {} {state} {} {} {} {actions}\n",
                hex(key),
                u8::from(operator.read),
                u8::from(operator.all_actions),
                operator.name,
            ));
        }
        for key in &self.revoked {
            text.push_str(&format!("revoked {}\n", hex(key)));
        }
        for line in &self.unknown {
            text.push_str(line);
            text.push('\n');
        }
        // Written beside and renamed over, so a power cut leaves the old store or the new one.
        let write = || -> io::Result<()> {
            std::fs::create_dir_all(dir)?;
            let temporary = dir.join(format!("{TRUST_FILE}.tmp"));
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(text.as_bytes())?;
            file.sync_all()?;
            std::fs::rename(&temporary, dir.join(TRUST_FILE))?;
            std::fs::File::open(dir)?.sync_all()
        };
        write().map_err(KeysError::Store)
    }

    /// The node's identity.
    #[must_use]
    pub const fn identity(&self) -> &Identity {
        &self.identity
    }

    /// The cluster.
    #[must_use]
    pub fn cluster(&self) -> &str {
        &self.config.cluster
    }

    /// Whether security is on.
    #[must_use]
    pub const fn security(&self) -> Security {
        self.security
    }

    /// The enrollment key, if newcomers may enroll themselves.
    #[must_use]
    pub fn enrollment_key(&self) -> Option<&[u8]> {
        self.config.enrollment_key.as_deref()
    }

    /// Whether `key` has been revoked.
    #[must_use]
    pub fn is_revoked(&self, key: &[u8; 32]) -> bool {
        self.revoked.contains(key)
    }

    /// The key `peer` is pinned to.
    #[must_use]
    pub fn peer_key(&self, peer: &NodeName) -> Option<&[u8; 32]> {
        self.peers.get(peer)
    }

    /// Every trusted peer.
    pub fn peers(&self) -> impl Iterator<Item = (&NodeName, &[u8; 32])> {
        self.peers.iter()
    }

    /// Who a connection that proved it holds `key` is: the peer pinned to that key, or an
    /// operator. A revoked key is nobody.
    #[must_use]
    pub fn principal(&self, key: &[u8; 32]) -> Principal {
        if self.is_revoked(key) {
            return Principal::Anonymous;
        }
        match self.peers.iter().find(|(_, pinned)| *pinned == key) {
            Some((name, _)) => Principal::Peer(name.clone()),
            None => Principal::Operator(KeyId(*key)),
        }
    }

    /// Pins `peer` to `key`.
    ///
    /// # Errors
    ///
    /// [`KeysError::Store`] if it can't be saved. `Ok(false)` if the key is revoked or the peer
    /// is already pinned to another key: a peer's key does not change by asking.
    pub fn trust_peer(&mut self, peer: &NodeName, key: [u8; 32]) -> Result<bool, KeysError> {
        if self.is_revoked(&key) || self.peers.get(peer).is_some_and(|pinned| *pinned != key) {
            return Ok(false);
        }
        self.peers.insert(peer.clone(), key);
        self.save()?;
        Ok(true)
    }

    /// The operator with this key.
    #[must_use]
    pub fn operator(&self, key: &KeyId) -> Option<&Operator> {
        self.operators.get(&key.0)
    }

    /// Every operator.
    pub fn operators(&self) -> impl Iterator<Item = &Operator> {
        self.operators.values()
    }

    fn new_operator(&self, key: [u8; 32], name: &str, state: OperatorState) -> Operator {
        let name: String = name
            .chars()
            .map(|c| {
                if c.is_whitespace() || c.is_control() {
                    '_'
                } else {
                    c
                }
            })
            .take(64)
            .collect();
        Operator {
            key: KeyId(key),
            name: if name.is_empty() {
                "operator".to_owned()
            } else {
                name
            },
            state,
            read: self.config.operators_read,
            actions: self.config.operators_actions.clone(),
            all_actions: false,
        }
    }

    /// An administrator enrolls `key`, whatever it was before: this is the one way a refused
    /// or revoked operator comes back.
    ///
    /// # Errors
    ///
    /// [`KeysError::Store`] if it can't be saved.
    pub fn approve_operator(&mut self, key: [u8; 32], name: &str) -> Result<&Operator, KeysError> {
        self.revoked.remove(&key);
        let mut operator = self.new_operator(key, name, OperatorState::Enrolled);
        if let Some(had) = self.operators.get(&key) {
            // Approving keeps what it was given before; only the state changes.
            operator.read = had.read;
            operator.actions.clone_from(&had.actions);
            operator.all_actions = had.all_actions;
        }
        self.operators.insert(key, operator);
        self.save()?;
        Ok(&self.operators[&key])
    }

    /// An operator asks to be enrolled, with `proof` that it knows the enrollment key (or
    /// none). Returns where that leaves it:
    ///
    /// - already enrolled: enrolled, unchanged;
    /// - revoked or refused before: still revoked, whatever the proof;
    /// - a good proof: enrolled;
    /// - otherwise: pending, for an administrator.
    ///
    /// # Errors
    ///
    /// [`KeysError::Store`] if it can't be saved.
    pub fn operator_asks(
        &mut self,
        node: &NodeName,
        key: [u8; 32],
        name: &str,
        proof: &[u8],
    ) -> Result<Operator, KeysError> {
        if self.is_revoked(&key) {
            return Ok(self.new_operator(key, name, OperatorState::Revoked));
        }
        if let Some(operator) = self.operators.get(&key)
            && operator.state != OperatorState::Pending
        {
            return Ok(operator.clone());
        }
        let proven = self.enrollment_key().is_some_and(|secret| {
            verify_proof(secret, &operator_proof(node.as_str(), &key), proof)
        });
        let state = if proven {
            OperatorState::Enrolled
        } else {
            OperatorState::Pending
        };
        let operator = self.new_operator(key, name, state);
        self.operators.insert(key, operator.clone());
        self.save()?;
        Ok(operator)
    }

    /// Enrolls `key` as an operator that proved it knows the enrollment key, unless it was
    /// refused before: then it stays as it was.
    ///
    /// # Errors
    ///
    /// [`KeysError::Store`] if it can't be saved.
    pub fn approve_if_new(&mut self, key: [u8; 32], name: &str) -> Result<Operator, KeysError> {
        if self.is_revoked(&key) {
            return Ok(self.new_operator(key, name, OperatorState::Revoked));
        }
        match self.operators.get(&key) {
            Some(operator) if operator.state != OperatorState::Pending => Ok(operator.clone()),
            _ => {
                let operator = self.new_operator(key, name, OperatorState::Enrolled);
                self.operators.insert(key, operator.clone());
                self.save()?;
                Ok(operator)
            }
        }
    }

    /// Changes what an enrolled operator may do.
    ///
    /// # Errors
    ///
    /// [`KeysError::Store`] if it can't be saved. `Ok(false)` if there is no such operator.
    pub fn grant(
        &mut self,
        key: &KeyId,
        read: bool,
        actions: Vec<String>,
        all_actions: bool,
    ) -> Result<bool, KeysError> {
        let Some(operator) = self.operators.get_mut(&key.0) else {
            return Ok(false);
        };
        operator.read = read;
        operator.actions = actions;
        operator.all_actions = all_actions;
        self.save()?;
        Ok(true)
    }

    /// Stops trusting `key`, as a peer and as an operator, for good.
    ///
    /// # Errors
    ///
    /// [`KeysError::Store`] if it can't be saved.
    pub fn revoke(&mut self, key: [u8; 32]) -> Result<(), KeysError> {
        self.revoked.insert(key);
        self.peers.retain(|_, pinned| *pinned != key);
        if let Some(operator) = self.operators.get_mut(&key) {
            operator.state = OperatorState::Revoked;
        }
        self.save()
    }
}

/// The trust store, shared between the server's check and the services that change it.
#[derive(Clone, Debug)]
pub struct SharedKeys(Arc<Mutex<Keys>>);

impl SharedKeys {
    /// Shares `keys`.
    #[must_use]
    pub fn new(keys: Keys) -> Self {
        Self(Arc::new(Mutex::new(keys)))
    }

    /// The trust store. Hold it only for as long as it takes to read or change it.
    pub fn lock(&self) -> MutexGuard<'_, Keys> {
        // Nothing done under the lock can leave it half changed in a way that matters more
        // than refusing everyone would.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Grants for SharedKeys {
    fn operator_may(&self, key: &KeyId, what: What<'_>) -> bool {
        self.lock()
            .operator(key)
            .is_some_and(|operator| operator.may(what))
    }
}
