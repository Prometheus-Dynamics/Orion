//! The board identity document (contract 1), served by the node so a device running Orion needs
//! no separate identity endpoint (`docs/design/comm-core.md`, "Identity in the comm core").
//!
//! The fleet tool recognizes a board by `GET /.well-known/pd-device` on port 5899: a JSON object
//! whose readers ignore unknown properties and whose later versions only add properties. The
//! node serves it when `ORION_NODE_IDENTITY_ADDR` is set:
//!
//! - **Base document.** The device package's document (`ORION_NODE_IDENTITY_BASE`, for example
//!   `/run/board/identity.json`), passed through as it is. Without one, or when it cannot be
//!   read or is not a JSON object, the node builds a minimal document from host facts: the
//!   device-tree serial (lowercase hex), `os` from os-release, the interfaces' MAC addresses,
//!   and `null` for what only the package knows (`model`, `rev`, `device_package`,
//!   `manage_url`).
//! - **Live fields**, computed on every request and never cached: `hostname` (the kernel's),
//!   `time` (unix seconds) and, when `ORION_NODE_IDENTITY_SECURITY_FILES` is set, `security`:
//!   `BOARD_SECURITY` from those `KEY=value` files, where a later file wins over an earlier one
//!   (`1` is `"on"`, anything else `"off"`), and, when `ORION_NODE_SSH_HOST_KEYS` is set,
//!   `ssh_host_keys`: `[{"type": "ed25519", "sha256": "SHA256:..."}]`.
//!
//! The responder is a minimal HTTP/1.1 `GET` server, not the HTTP control stack, so builds
//! without `transport-http` can serve it. One request per connection, an 8 KiB header limit,
//! and a 5 s read deadline. Each connection is its own task: a slow or broken client affects
//! only itself.

mod ssh_host_keys;
pub use ssh_host_keys::{SshHostKey, read_ssh_host_keys, ssh_host_keys_dir_from_env};

use crate::NodeError;
use crate::host_facts::{firmware_string, parse_os_release};
use serde_json::{Map, Value, json};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// The path the identity contract serves the document at.
pub const IDENTITY_PATH: &str = "/.well-known/pd-device";
/// The contract version this node speaks.
pub const IDENTITY_CONTRACT: u64 = 1;
/// The longest request header the responder reads.
const MAX_REQUEST: usize = 8 * 1024;
/// How long a client may take to send its request.
const READ_DEADLINE: Duration = Duration::from_secs(5);
/// The security switch key in the security files.
const SECURITY_KEY: &str = "BOARD_SECURITY";

/// Where and how the node serves the identity document (`ORION_NODE_IDENTITY_*`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityConfig {
    /// `ORION_NODE_IDENTITY_ADDR`, for example `0.0.0.0:5899`.
    pub addr: SocketAddr,
    /// `ORION_NODE_IDENTITY_BASE`: the device package's document.
    pub base: Option<PathBuf>,
    /// `ORION_NODE_IDENTITY_SECURITY_FILES`: comma-separated, later files win.
    pub security_files: Vec<PathBuf>,
    /// `ORION_NODE_SSH_HOST_KEYS`: a directory of OpenSSH `*.pub` files.
    pub ssh_host_keys: Option<PathBuf>,
}

impl IdentityConfig {
    /// `None` unless `ORION_NODE_IDENTITY_ADDR` is set to an address (`off` or empty disables).
    pub fn try_from_env() -> Result<Option<Self>, NodeError> {
        let Some(addr) = std::env::var("ORION_NODE_IDENTITY_ADDR")
            .ok()
            .filter(|value| !value.is_empty() && !matches!(value.as_str(), "off" | "none"))
        else {
            return Ok(None);
        };
        let addr = addr.parse().map_err(|err| {
            NodeError::Config(format!(
                "ORION_NODE_IDENTITY_ADDR must be a socket address or `off`: {addr} ({err})"
            ))
        })?;
        let base = std::env::var_os("ORION_NODE_IDENTITY_BASE")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        let security_files = std::env::var("ORION_NODE_IDENTITY_SECURITY_FILES")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|path| !path.is_empty())
                    .map(PathBuf::from)
                    .collect()
            })
            .unwrap_or_default();
        Ok(Some(Self {
            addr,
            base,
            security_files,
            ssh_host_keys: ssh_host_keys_dir_from_env(),
        }))
    }
}

/// Builds the identity document from the base file, host facts and the live fields. Every path
/// is read below `root` (`/` in production, a temporary tree in tests).
#[derive(Clone, Debug)]
pub struct IdentitySource {
    root: PathBuf,
    base: Option<PathBuf>,
    security_files: Vec<PathBuf>,
    ssh_host_keys: Option<PathBuf>,
}

impl IdentitySource {
    pub fn new(config: &IdentityConfig) -> Self {
        Self::with_root("/", config)
    }

    pub fn with_root(root: impl Into<PathBuf>, config: &IdentityConfig) -> Self {
        Self {
            root: root.into(),
            base: config.base.clone(),
            security_files: config.security_files.clone(),
            ssh_host_keys: config.ssh_host_keys.clone(),
        }
    }

    fn path(&self, path: &Path) -> PathBuf {
        self.root.join(path.strip_prefix("/").unwrap_or(path))
    }

    fn read(&self, path: impl AsRef<Path>) -> Option<String> {
        std::fs::read_to_string(self.path(path.as_ref())).ok()
    }

    /// The document as of `now` (unix seconds).
    pub fn document(&self, now: u64) -> Value {
        let mut document = self.base_document().unwrap_or_else(|| self.fallback());
        document.insert("hostname".into(), self.hostname());
        document.insert("time".into(), json!(now));
        if let Some(security) = self.security() {
            document.insert("security".into(), json!(security));
        }
        if let Some(dir) = &self.ssh_host_keys {
            let keys: Vec<Value> = read_ssh_host_keys(&self.path(dir))
                .into_iter()
                .map(|key| json!({"type": key.kind, "sha256": key.sha256}))
                .collect();
            document.insert("ssh_host_keys".into(), Value::Array(keys));
        }
        Value::Object(document)
    }

    fn base_document(&self) -> Option<Map<String, Value>> {
        let path = self.base.as_ref()?;
        let text = self.read(path)?;
        // The package writes the document on one line; anything after it (a trailing newline,
        // a partial rewrite caught mid-way is not possible with an atomic rename) is ignored.
        match serde_json::from_str::<Value>(text.lines().next().unwrap_or("")) {
            Ok(Value::Object(map)) => Some(map),
            Ok(_) | Err(_) => match serde_json::from_str::<Value>(&text) {
                Ok(Value::Object(map)) => Some(map),
                _ => {
                    debug!(path = %path.display(), "identity base document is not a JSON object; serving the built-in one");
                    None
                }
            },
        }
    }

    /// The document a board without a device package gets.
    fn fallback(&self) -> Map<String, Value> {
        let os = self
            .read("/etc/os-release")
            .or_else(|| self.read("/usr/lib/os-release"))
            .map(|contents| parse_os_release(&contents))
            .unwrap_or_default();
        let serial = std::fs::read(self.path(Path::new("/proc/device-tree/serial-number")))
            .ok()
            .and_then(|bytes| firmware_string(&bytes))
            .map(|serial| serial.to_ascii_lowercase())
            .filter(|serial| !serial.is_empty() && serial.bytes().all(|b| b.is_ascii_hexdigit()));
        let mut document = Map::new();
        document.insert("contract".into(), json!(IDENTITY_CONTRACT));
        document.insert("model".into(), Value::Null);
        document.insert("rev".into(), Value::Null);
        document.insert("serial".into(), json!(serial));
        document.insert(
            "os".into(),
            json!({ "name": os.id, "version": os.version_id }),
        );
        document.insert(
            "device_package".into(),
            json!({ "version": null, "commit": null }),
        );
        document.insert("update_methods".into(), json!(["image-write"]));
        document.insert("manage_url".into(), Value::Null);
        document.insert("macs".into(), Value::Object(self.macs()));
        document
    }

    fn macs(&self) -> Map<String, Value> {
        let mut macs = Map::new();
        let Ok(entries) = std::fs::read_dir(self.path(Path::new("/sys/class/net"))) else {
            return macs;
        };
        let mut names: Vec<String> = entries
            .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
            .filter(|name| name != "lo")
            .collect();
        names.sort();
        for name in names {
            let Some(address) = self.read(format!("/sys/class/net/{name}/address")) else {
                continue;
            };
            let address = address.trim().to_ascii_lowercase();
            if is_mac(&address) && address != "00:00:00:00:00:00" {
                macs.insert(name, json!(address));
            }
        }
        macs
    }

    fn hostname(&self) -> Value {
        self.read("/proc/sys/kernel/hostname")
            .map(|name| name.trim().to_owned())
            .filter(|name| !name.is_empty())
            .map_or(Value::Null, Value::String)
    }

    /// `"on"`/`"off"` from the security files, or `None` when none is configured or readable.
    fn security(&self) -> Option<&'static str> {
        let mut value = None;
        for file in &self.security_files {
            let Some(contents) = self.read(file) else {
                continue;
            };
            for line in contents.lines() {
                if let Some(raw) = line.trim().strip_prefix(SECURITY_KEY)
                    && let Some(raw) = raw.trim_start().strip_prefix('=')
                {
                    value = Some(raw.trim().trim_matches(['"', '\'']) == "1");
                }
            }
        }
        value.map(|on| if on { "on" } else { "off" })
    }
}

fn is_mac(address: &str) -> bool {
    let bytes = address.as_bytes();
    bytes.len() == 17
        && bytes.iter().enumerate().all(|(i, b)| {
            if i % 3 == 2 {
                *b == b':'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

/// The running identity responder. [`IdentityServer::shutdown`] stops it.
#[derive(Debug)]
pub struct IdentityServer {
    addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl IdentityServer {
    /// Binds `config.addr` and serves until shut down.
    pub async fn start(config: &IdentityConfig) -> Result<Self, NodeError> {
        Self::start_with_source(config.addr, IdentitySource::new(config)).await
    }

    pub async fn start_with_source(
        addr: SocketAddr,
        source: IdentitySource,
    ) -> Result<Self, NodeError> {
        let listener = TcpListener::bind(addr).await.map_err(|err| {
            NodeError::Config(format!("identity endpoint cannot bind {addr}: {err}"))
        })?;
        let addr = listener.local_addr().unwrap_or(addr);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let source = Arc::new(source);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => return,
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _)) => {
                            tokio::spawn(answer(stream, source.clone()));
                        }
                        Err(err) => {
                            // EMFILE and friends: back off briefly instead of spinning.
                            warn!(error = %err, "identity endpoint accept failed");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    },
                }
            }
        });
        info!(%addr, "identity endpoint listening");
        Ok(Self {
            addr,
            shutdown_tx,
            task,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.task.await;
    }
}

async fn answer(mut stream: TcpStream, source: Arc<IdentitySource>) {
    let mut request = Vec::with_capacity(512);
    let head = tokio::time::timeout(READ_DEADLINE, read_head(&mut stream, &mut request)).await;
    let response = match head {
        Ok(Some(())) => respond(&request, &source),
        Ok(None) => return,
        Err(_) => response(408, "Request Timeout", "text/plain", b"request timeout\n"),
    };
    let _ = stream.write_all(&response).await;
    let _ = stream.shutdown().await;
}

/// Reads until the end of the request head. `None` when the client went away or sent more than
/// [`MAX_REQUEST`] bytes without finishing it.
async fn read_head(stream: &mut TcpStream, request: &mut Vec<u8>) -> Option<()> {
    let mut chunk = [0u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        request.extend_from_slice(&chunk[..read]);
        if request.windows(4).any(|w| w == b"\r\n\r\n") || request.windows(2).any(|w| w == b"\n\n")
        {
            return Some(());
        }
        if request.len() > MAX_REQUEST {
            return None;
        }
    }
}

/// The full HTTP response for one request head.
fn respond(request: &[u8], source: &IdentitySource) -> Vec<u8> {
    let line = request
        .split(|b| *b == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .unwrap_or("")
        .trim_end_matches('\r');
    let mut parts = line.split(' ');
    let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let path = target.split('?').next().unwrap_or("");
    match (method, path) {
        ("GET" | "HEAD", IDENTITY_PATH) => {
            let mut body = serde_json::to_vec(&source.document(unix_now())).unwrap_or_default();
            body.push(b'\n');
            let mut out = response(200, "OK", "application/json", &body);
            if method == "HEAD" {
                out.truncate(out.len() - body.len());
            }
            out
        }
        ("GET" | "HEAD", _) => response(404, "Not Found", "text/plain", b"not found\n"),
        _ => response(
            405,
            "Method Not Allowed",
            "text/plain",
            b"method not allowed\n",
        ),
    }
}

fn response(code: u16, reason: &str, content_type: &str, body: &[u8]) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body);
    out
}

#[cfg(test)]
mod tests;
