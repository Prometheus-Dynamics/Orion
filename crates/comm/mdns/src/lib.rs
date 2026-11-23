//! The board on the local network by name: an mDNS responder (RFC 6762) announcing the node's
//! services (RFC 6763).
//!
//! A node is `<name>.local`, and offers:
//!
//! - `_orion._tcp`: where it takes `orion+tcp` connections, with a TXT record saying what it
//!   is (see [`orion_txt`]);
//! - `_ssh._tcp` and `_sftp-ssh._tcp`: its SSH way in, when it has one.
//!
//! It answers what it is asked on each interface, with that interface's addresses, and claims
//! its name first: if another has it, it is `<name>-<4 hex of its key's fingerprint>`.
//!
//! **An announcement proves nothing.** Anyone on the network can announce any name and any
//! key; a client checks the key it is given when it connects, as it would with no mDNS.

pub mod packet;
mod respond;
mod socket;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};
use tracing::{info, warn};

pub use respond::{
    Announce, HOST_TTL, Interface, OTHER_TTL, PORT, Reach, Responder, Send, Service,
};
pub use socket::{GROUP, interfaces};

/// The `_orion._tcp` service.
pub const ORION: &str = "_orion._tcp";
/// The SSH way in.
pub const SSH: &str = "_ssh._tcp";
/// SFTP over the SSH way in.
pub const SFTP: &str = "_sftp-ssh._tcp";

/// The TXT layout's version. A key added keeps it; one whose meaning changes moves it.
pub const TXT_VERSION: &str = "2";
/// The longest cluster name a TXT string carries (255 bytes, less `cl=`).
pub const MAX_CLUSTER: usize = 252;
/// How often the interfaces are looked at.
pub const FOLLOW: Duration = Duration::from_secs(2);
/// The largest message read.
const MAX_MESSAGE: usize = 9000;

/// What `_orion._tcp`'s TXT record says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrionTxt<'a> {
    pub node: &'a str,
    pub key: &'a [u8; 32],
    pub wire: u8,
    pub revision: u16,
    pub level: u16,
    pub cluster: &'a str,
    pub port: u16,
    /// `running` or `flasher`.
    pub state: &'a str,
    /// `on`, `off` or `damaged`.
    pub security: &'a str,
    /// `synced` or `unset`, when the board's time sync measures; changed as the clock does.
    pub clock: Option<&'a str>,
}

/// `bytes` as lowercase hex.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// `_orion._tcp`'s TXT strings, or why it cannot be announced: a cluster name too long for
/// one.
///
/// # Errors
///
/// A cluster name longer than [`MAX_CLUSTER`].
pub fn orion_txt(txt: &OrionTxt<'_>) -> Result<Vec<Vec<u8>>, String> {
    if txt.cluster.len() > MAX_CLUSTER {
        return Err(format!(
            "the cluster name is {} bytes, and an announcement carries {MAX_CLUSTER} at most",
            txt.cluster.len()
        ));
    }
    Ok([
        ("v", TXT_VERSION.to_owned()),
        ("id", txt.node.to_owned()),
        ("pk", hex(txt.key)),
        ("wv", txt.wire.to_string()),
        ("rev", txt.revision.to_string()),
        ("cl", txt.cluster.to_owned()),
        ("tcp", txt.port.to_string()),
        ("level", txt.level.to_string()),
        ("state", txt.state.to_owned()),
        ("security", txt.security.to_owned()),
    ]
    .into_iter()
    .chain(txt.clock.map(|clock| ("clock", clock.to_owned())))
    .map(|(key, value)| format!("{key}={value}").into_bytes())
    .collect())
}

/// The 4 hex digits a node whose name is taken puts after it: the first of its key's
/// fingerprint, as `orion keys` shows it.
#[must_use]
pub fn tag(key: &[u8; 32]) -> String {
    let fingerprint = orion_identity::fingerprint(key);
    let digits = fingerprint.strip_prefix("sha256:").unwrap_or(&fingerprint);
    digits.chars().take(4).collect()
}

/// Where the responder answers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Settings {
    /// The interfaces by name; none is every one that is up, with an IPv4 address.
    pub interfaces: Vec<String>,
}

/// A socket on one interface, and what reads it.
struct Open {
    index: u32,
    name: String,
    socket: Rc<UdpSocket>,
    reader: JoinHandle<()>,
}

impl Drop for Open {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// What a reader hands on: the interface, the sender, the message.
type Heard = (u32, std::net::SocketAddr, Vec<u8>);

/// Where the responder's name is kept, to be shown.
#[derive(Clone, Debug, Default)]
pub struct Named(Rc<RefCell<Option<String>>>);

impl Named {
    /// The name it has, once it is its own.
    #[must_use]
    pub fn get(&self) -> Option<String> {
        self.0.borrow().clone()
    }
}

/// A service's TXT strings as they change. `txt` is asked each time the interfaces are looked
/// at ([`FOLLOW`]), and gives the strings when they have changed: they are announced again.
pub struct Retell {
    /// The service's type, as [`ORION`].
    pub kind: &'static str,
    pub txt: Box<dyn FnMut() -> Option<Vec<Vec<u8>>>>,
}

impl std::fmt::Debug for Retell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Retell({})", self.kind)
    }
}

/// Runs the responder until `stop` finishes, and says goodbye. Must be run inside a
/// `LocalSet`. With `retell`, a service's TXT strings are announced again when they change.
pub async fn run(
    announce: Announce,
    settings: Settings,
    named: Named,
    mut retell: Option<Retell>,
    stop: impl Future<Output = ()>,
) {
    let seed = announce
        .tag
        .bytes()
        .fold(u64::from(std::process::id()), |seed, byte| {
            seed.rotate_left(8) ^ u64::from(byte)
        });
    let wanted = announce.name.clone();
    let mut responder = Responder::new(announce, Instant::now().into_std(), seed);
    let (tell, mut heard) = mpsc::channel::<Heard>(64);
    let mut open: Vec<Open> = Vec::new();
    let mut follow = Instant::now();
    let mut told: Option<String> = None;
    tokio::pin!(stop);
    loop {
        if Instant::now() >= follow {
            follow = Instant::now() + FOLLOW;
            look(&settings, &mut open, &mut responder, &tell, &mut told);
            if let Some(retell) = &mut retell
                && let Some(txt) = (retell.txt)()
            {
                responder.retell(retell.kind, txt, Instant::now().into_std());
            }
        }
        let due = Instant::from_std(responder.due()).min(follow);
        tokio::select! {
            () = &mut stop => break,
            () = sleep_until(due) => {}
            got = heard.recv() => {
                let Some((index, from, bytes)) = got else { break };
                // Not mDNS, or not whole: dropped, as the network drops.
                if let Ok(message) = packet::Message::read(&bytes) {
                    responder.heard(index, from, &message, Instant::now().into_std());
                }
            }
        }
        let before = named.get();
        for send in responder.poll(Instant::now().into_std()) {
            deliver(&open, &send).await;
        }
        let now = responder.up().then(|| responder.name().to_owned());
        if now != before {
            match &now {
                Some(name) if *name != wanted => {
                    warn!("mDNS: {wanted}.local is taken here; announcing as {name}.local");
                }
                Some(name) => info!("mDNS: announcing {name}.local"),
                None => {}
            }
            *named.0.borrow_mut() = now;
        }
    }
    for send in responder.goodbye() {
        deliver(&open, &send).await;
    }
}

async fn deliver(open: &[Open], send: &Send) {
    let Some(on) = open.iter().find(|on| on.index == send.interface) else {
        return;
    };
    let bytes = send.message.write();
    let to = send.to.unwrap_or_else(socket::group);
    // Not sent (the interface going, a full queue): it is asked again, or announced again.
    let _ = on.socket.send_to(&bytes, to).await;
}

/// Looks at the interfaces: opens a socket on each new one, and lets go of those gone.
fn look(
    settings: &Settings,
    open: &mut Vec<Open>,
    responder: &mut Responder,
    tell: &mpsc::Sender<Heard>,
    told: &mut Option<String>,
) {
    let seen = match socket::interfaces(&settings.interfaces) {
        Ok(seen) => seen,
        Err(error) => {
            let words = format!("mDNS: the interfaces could not be read: {error}");
            if told.as_ref() != Some(&words) {
                warn!("{words}");
                *told = Some(words);
            }
            return;
        }
    };
    // Only where something can be reached: elsewhere, not even a socket.
    let seen: Vec<Interface> = seen.into_iter().filter(|i| responder.reaches(i)).collect();
    open.retain(|on| {
        seen.iter()
            .any(|s| s.index == on.index && s.name == on.name)
    });
    let mut answered = Vec::new();
    for each in seen {
        let index = each.index;
        if !open.iter().any(|on| on.index == index) {
            match socket::open(&each.name, index) {
                Ok(socket) => {
                    let socket = Rc::new(socket);
                    let reader =
                        tokio::task::spawn_local(read(index, socket.clone(), tell.clone()));
                    open.push(Open {
                        index,
                        name: each.name.clone(),
                        socket,
                        reader,
                    });
                }
                Err(error) => {
                    let words = format!("mDNS: not on {}: {error}", each.name);
                    if told.as_ref() != Some(&words) {
                        warn!("{words}");
                        *told = Some(words);
                    }
                    continue;
                }
            }
        }
        answered.push(each);
    }
    responder.interfaces(answered, Instant::now().into_std());
}

async fn read(index: u32, socket: Rc<UdpSocket>, tell: mpsc::Sender<Heard>) {
    let mut buffer = vec![0_u8; MAX_MESSAGE];
    loop {
        match socket.recv_from(&mut buffer).await {
            Ok((read, from)) => {
                // Full: dropped, as the network drops; it is asked again.
                let _ = tell.try_send((index, from, buffer[..read].to_vec()));
            }
            // An interface going: its socket is let go at the next look.
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}
