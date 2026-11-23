//! The board announced by name on the local network (`mdns`, `mdns.interface`): `<name>.local`,
//! `_orion._tcp` where the network way in is, and `_ssh._tcp` and `_sftp-ssh._tcp` where the
//! SSH way in is. The same with security on or off: the announcement says which.
//!
//! Recovery announces nothing: it is not built in.

use orion_mdns::{Announce, Named, OrionTxt, Reach, Retell, Service, Settings};
use tokio::sync::oneshot;
use tracing::warn;

use crate::CommSettings;

/// The responder, running: dropped, it says goodbye.
#[derive(Debug)]
pub(crate) struct Announcing {
    pub(crate) named: Named,
    _stop: oneshot::Sender<()>,
}

/// The word the hello document says of the board's security.
fn security(view: &orion_config::SecurityView) -> &'static str {
    match (view.fault.is_some(), view.security) {
        (true, _) => "damaged",
        (false, orion_config::Security::On) => "on",
        (false, orion_config::Security::Off) => "off",
    }
}

/// What the clock says, as the time sync has it.
#[cfg(feature = "time")]
type Clock = orion_time::TimeView;

/// No time sync built: nothing to say.
#[cfg(not(feature = "time"))]
#[derive(Debug)]
enum Clock {}

#[cfg(not(feature = "time"))]
impl Clock {
    fn word(&self) -> Option<&'static str> {
        match *self {}
    }
}

/// What `_orion._tcp`'s TXT strings are made of, but the clock.
struct TxtParts {
    node: String,
    key: [u8; 32],
    cluster: String,
    port: u16,
    state: &'static str,
    security: &'static str,
}

impl TxtParts {
    fn txt(&self, clock: Option<&str>) -> Result<Vec<Vec<u8>>, String> {
        orion_mdns::orion_txt(&OrionTxt {
            node: &self.node,
            key: &self.key,
            wire: orion_wire::PROTOCOL_VERSION,
            revision: orion_wire::INTERFACE_REVISION,
            level: orion_wire::INTERFACE_LEVEL,
            cluster: &self.cluster,
            port: self.port,
            state: self.state,
            security: self.security,
            clock,
        })
    }
}

/// The TXT strings again whenever the clock's word is no longer `said`.
#[cfg(feature = "time")]
fn follow(parts: TxtParts, clock: Clock, mut said: Option<&'static str>) -> Retell {
    Retell {
        kind: orion_mdns::ORION,
        txt: Box::new(move || {
            let now = clock.word();
            if now == said {
                return None;
            }
            said = now;
            parts.txt(now).ok()
        }),
    }
}

#[cfg(not(feature = "time"))]
fn follow(_: TxtParts, clock: Clock, _: Option<&'static str>) -> Retell {
    match clock {}
}

/// Why `_orion._tcp` is not announced, as the settings are: a cluster name too long for one.
pub(crate) fn withheld(settings: &CommSettings) -> Option<String> {
    let cluster = &settings.keys.cluster;
    (settings.mdns && cluster.len() > orion_mdns::MAX_CLUSTER).then(|| {
        format!(
            "setting `cluster` is {} bytes, and an announcement carries {} at most: \
             `_orion._tcp` is not announced; the board's name and SSH still are",
            cluster.len(),
            orion_mdns::MAX_CLUSTER
        )
    })
}

/// Starts the responder for `core`, unless `mdns = false` or there is nothing to announce.
/// Each way in is announced only on the interfaces it can be reached on: one listening on the
/// loopback address alone is not announced at all.
pub(crate) fn start(settings: &CommSettings, core: &crate::CommCore) -> Option<Announcing> {
    if !settings.mdns {
        return None;
    }
    let (keys, peer) = (&core.keys, core.peer_addr);
    // The SSH way in: its port once it listens (0 asks for any), and where it can be reached.
    #[cfg(feature = "ssh")]
    let ssh = settings.ssh.as_ref().and_then(|config| {
        let port = core
            .ssh_addr()
            .map_or(config.listen.port(), |addr| addr.port());
        let reach = Reach {
            addr: config.listen.ip(),
            interfaces: config.interfaces.clone(),
        };
        (port != 0).then_some((port, reach))
    });
    #[cfg(not(feature = "ssh"))]
    let ssh: Option<(u16, Reach)> = None;
    let node = settings.config.node_name.to_string();
    let key = keys.lock().identity().public();
    let mut services = Vec::new();
    let mut retell = None;
    #[cfg(feature = "time")]
    let clock = core.time().map(orion_time::TimeSync::view);
    #[cfg(not(feature = "time"))]
    let clock: Option<Clock> = None;
    if let Some(peer) = peer {
        let parts = TxtParts {
            node: node.clone(),
            key,
            cluster: settings.keys.cluster.clone(),
            port: peer.port(),
            state: settings.board_state.word(),
            security: security(&settings.config.security_view),
        };
        let word = clock.as_ref().and_then(Clock::word);
        let told = parts.txt(word);
        if let (Ok(_), Some(clock)) = (&told, clock) {
            retell = Some(follow(parts, clock, word));
        }
        match told {
            Ok(txt) => services.push(Service {
                kind: orion_mdns::ORION,
                port: peer.port(),
                txt,
                reach: Reach {
                    addr: peer.ip(),
                    interfaces: settings.peer_interfaces.clone(),
                },
            }),
            Err(_) => {
                if let Some(why) = withheld(settings) {
                    warn!("mDNS: {why}");
                }
            }
        }
    }
    if let Some((port, reach)) = ssh {
        #[cfg(feature = "ssh-sftp")]
        services.push(Service {
            kind: orion_mdns::SFTP,
            port,
            txt: Vec::new(),
            reach: reach.clone(),
        });
        services.push(Service {
            kind: orion_mdns::SSH,
            port,
            txt: Vec::new(),
            reach,
        });
    }
    if services
        .iter()
        .all(|service| service.reach.addr.is_loopback())
    {
        return None;
    }
    let announce = Announce {
        name: node,
        tag: orion_mdns::tag(&key),
        services,
    };
    let named = Named::default();
    let (stop, stopped) = oneshot::channel::<()>();
    let interfaces = Settings {
        interfaces: settings.mdns_interfaces.clone(),
    };
    // Not one of the core's tasks, which are cut off as it goes: this one is told, and says
    // goodbye.
    drop(tokio::task::spawn_local(orion_mdns::run(
        announce,
        interfaces,
        named.clone(),
        retell,
        async move {
            let _ = stopped.await;
        },
    )));
    Some(Announcing { named, _stop: stop })
}
