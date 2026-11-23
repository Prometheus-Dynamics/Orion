//! The board announced by name on the local network (`mdns`, `mdns.interface`): `<name>.local`,
//! `_orion._tcp` where the network way in is, and `_ssh._tcp` and `_sftp-ssh._tcp` where the
//! SSH way in is. The same with security on or off: the announcement says which.
//!
//! Recovery announces nothing: it is not built in.

use orion_mdns::{Announce, Named, OrionTxt, Reach, Service, Settings};
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
    if let Some(peer) = peer {
        let txt = OrionTxt {
            node: &node,
            key: &key,
            wire: orion_wire::PROTOCOL_VERSION,
            revision: orion_wire::INTERFACE_REVISION,
            level: orion_wire::INTERFACE_LEVEL,
            cluster: &settings.keys.cluster,
            port: peer.port(),
            state: settings.board_state.word(),
            security: security(&settings.config.security_view),
        };
        match orion_mdns::orion_txt(&txt) {
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
        async move {
            let _ = stopped.await;
        },
    )));
    Some(Announcing { named, _stop: stop })
}
