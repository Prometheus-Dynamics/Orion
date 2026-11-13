//! Everything the comm core is told, from one file and the environment.

use std::time::Duration;

use orion_broker::BrokerConfig;
use orion_config::{Config, ConfigError, Source};
use orion_keys::KeysConfig;
use orion_server::Limits;

/// The comm core's settings.
#[derive(Clone, Debug)]
pub struct CommSettings {
    /// What every build needs: the node's name, the security switch, the socket, the state
    /// directory and where other nodes connect.
    pub config: Config,
    /// `state.require_mount`: refuse to start when `state.dir` or `keys.dir` would be on the
    /// root file system, as on a board updated by slot whose state partition is not mounted.
    pub state_require_mount: bool,
    /// `hello.state`: what the hello document says the board is: `running`, or `flasher` for
    /// a flasher image.
    pub board_state: orion_hello::BoardState,
    /// `comm.*`: the server's limits.
    pub limits: Limits,
    /// `topics.*`: the broker's limits.
    pub broker: BrokerConfig,
    /// `outbound.max_bytes`: what may wait to be sent to one subscriber without a ring.
    #[cfg(feature = "outbound")]
    pub outbound: orion_outbound::OutboundLimits,
    /// `cluster`, `enrollment.key`, `operators.*`: whom the node trusts.
    pub keys: KeysConfig,
    /// `socket.mode`: the socket's permission bits, in octal. They decide who on the board can
    /// connect at all.
    pub socket_mode: u32,
    /// `socket.node_uids`: the users whose programs may be this board's node logic, besides
    /// root.
    pub node_uids: Vec<u32>,
    /// `status.interval_ms`: how often the board's vital signs are read while someone watches.
    pub status_interval: Duration,
    /// `time.*`: time sync.
    #[cfg(feature = "time")]
    pub time: orion_time::TimeConfig,
    /// `discovery`: `off` or `mdns`.
    #[cfg(feature = "discovery")]
    pub discovery: bool,
    /// `discovery.ttl_ms`: how long a peer stays listed without a fresh announcement.
    #[cfg(feature = "discovery")]
    pub discovery_ttl: Duration,
    /// `discovery.interfaces`: the interfaces or addresses to run mDNS on. Empty for all.
    #[cfg(feature = "discovery")]
    pub discovery_interfaces: Vec<String>,
    /// `ws.listen`: where browsers connect. Off unless it is said.
    #[cfg(feature = "ws")]
    pub ws_listen: Option<std::net::SocketAddr>,
    /// `ws.origins`: the pages that may connect, by origin. Empty for any.
    #[cfg(feature = "ws")]
    pub ws_origins: Vec<String>,
    /// `ws.send_buffer`: the most bytes the kernel may hold for one browser that has not
    /// taken them yet. Zero leaves it to the kernel.
    #[cfg(feature = "ws")]
    pub ws_send_buffer: usize,
    /// `ssh.*`: the SSH way in. `None` unless `ssh.listen` is said.
    #[cfg(feature = "ssh")]
    pub ssh: Option<orion_ssh::SshConfig>,
    /// `recovery.keys`: the public keys that may log in when the comm core is in recovery.
    #[cfg(feature = "recovery")]
    pub recovery_keys: std::path::PathBuf,
    /// `recovery.listen`: where recovery listens, after `ssh.listen`. Empty for the usual
    /// addresses.
    #[cfg(feature = "recovery")]
    pub recovery_listen: Vec<std::net::SocketAddr>,
    /// `ssh.sandbox`, `ssh.sandbox_user`: whether what SSH clients send is read in a child
    /// process with no rights, and the account that child becomes. `None` for no sandbox.
    #[cfg(feature = "ssh-sandbox")]
    pub ssh_sandbox: Option<orion_ssh_sandbox::SandboxConfig>,
    /// `nt4.*`: the NT4 gateway and its mapping. `None` unless `nt4.listen` is said.
    #[cfg(feature = "nt4")]
    pub nt4: Option<orion_nt4_gateway::Nt4Config>,
    /// `nt4.server` and the rest of `nt4.*`: the NT4 gateway as a client of a server
    /// somewhere else. `None` unless `nt4.server` is said.
    #[cfg(feature = "nt4-client")]
    pub nt4_client: Option<orion_nt4_client_gateway::Nt4ClientConfig>,
    /// `peer.nodes`: where the other nodes are, each `name=address:port`. A topic of one of
    /// them can be subscribed to here.
    #[cfg(feature = "topic-peers")]
    pub peer_nodes: Vec<orion_topic_peers::PeerNode>,
    /// `links.serial`: serial links, each `name=path@baud`.
    #[cfg(feature = "link-serial")]
    pub serial_links: Vec<orion_link_serial::SerialLink>,
    /// `links.sealed`: links brought over the network, each `name=address:port`.
    #[cfg(feature = "link-sealed")]
    pub sealed_links: Vec<SealedLink>,
    /// `links.can`: CAN buses, each `name=interface:device-to-host:host-to-device:addresses`,
    /// with `:fd` and `:extended` as needed.
    #[cfg(feature = "link-can")]
    pub can_links: Vec<orion_link_can::CanBus>,
    /// `links.usb`: the names of the USB links, whose handles are handed over at run time.
    #[cfg(feature = "link-usb")]
    pub usb_links: Vec<String>,
}

/// A link brought over the network: its name and where the board listens for it.
#[cfg(feature = "link-sealed")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedLink {
    /// The link's name.
    pub name: orion_types::DeviceName,
    /// Where the board listens.
    pub listen: std::net::SocketAddr,
}

#[cfg(feature = "link-sealed")]
impl std::str::FromStr for SealedLink {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let (name, listen) = text.split_once('=').ok_or("expected name=address:port")?;
        Ok(Self {
            name: name
                .trim()
                .parse()
                .map_err(|error| format!("{name:?}: {error}"))?,
            listen: listen
                .trim()
                .parse()
                .map_err(|error| format!("{listen:?}: {error}"))?,
        })
    }
}

impl CommSettings {
    /// Takes the comm core's settings out of `source`.
    ///
    /// # Errors
    ///
    /// A value that doesn't parse, or settings that cannot work together.
    pub fn from_source(source: &mut Source) -> Result<Self, ConfigError> {
        let error = |key: &str, message: String| ConfigError {
            key: key.into(),
            message,
        };
        let config = Config::from_source(source)?;
        let socket_mode = match source.text("socket.mode") {
            Some(mode) => u32::from_str_radix(mode.trim_start_matches("0o"), 8)
                .ok()
                .filter(|mode| *mode <= 0o777)
                .ok_or_else(|| {
                    error(
                        "socket.mode",
                        format!("{mode:?} is not permission bits in octal"),
                    )
                })?,
            None => 0o660,
        };
        let status_interval = Duration::from_millis(source.get("status.interval_ms", 1000_u64)?);
        if status_interval.is_zero() {
            return Err(error("status.interval_ms", "must be above 0".into()));
        }
        #[cfg(feature = "discovery")]
        let discovery = match source.text("discovery") {
            None | Some("" | "off") => false,
            Some("mdns") if cfg!(feature = "mdns") => true,
            Some("mdns") => return Err(error("discovery", "this build has no mDNS".into())),
            Some(other) => {
                return Err(error(
                    "discovery",
                    format!("expected off or mdns, got {other:?}"),
                ));
            }
        };
        #[cfg(feature = "discovery")]
        if discovery && config.peer_listen.is_none() {
            return Err(error(
                "discovery",
                "needs peer.listen: that is what is announced".into(),
            ));
        }
        let board_state = match source.text("hello.state") {
            None | Some("running") => orion_hello::BoardState::Running,
            Some("flasher") => orion_hello::BoardState::Flasher,
            Some(other) => {
                return Err(error(
                    "hello.state",
                    format!("expected running or flasher, got {other:?}"),
                ));
            }
        };
        Ok(Self {
            board_state,
            state_require_mount: source.get("state.require_mount", false)?,
            limits: Limits::from_source(source)?,
            broker: BrokerConfig::from_source(source)?,
            #[cfg(feature = "outbound")]
            outbound: orion_outbound::OutboundLimits::from_source(source)?,
            keys: KeysConfig::from_source(source, &config.keys_dir)?,
            socket_mode,
            node_uids: source.list("socket.node_uids")?,
            status_interval,
            #[cfg(feature = "time")]
            time: orion_time::TimeConfig::from_source(source)?,
            #[cfg(feature = "discovery")]
            discovery,
            #[cfg(feature = "discovery")]
            discovery_ttl: Duration::from_millis(source.get("discovery.ttl_ms", 120_000_u64)?),
            #[cfg(feature = "discovery")]
            discovery_interfaces: source.list("discovery.interfaces")?,
            #[cfg(feature = "ws")]
            ws_listen: source.optional("ws.listen")?,
            #[cfg(feature = "ws")]
            ws_origins: source.list("ws.origins")?,
            #[cfg(feature = "ws")]
            ws_send_buffer: source.get("ws.send_buffer", 256 * 1024_usize)?,
            #[cfg(feature = "recovery")]
            recovery_keys: source
                .optional("recovery.keys")?
                .unwrap_or_else(|| orion_recovery::KEYS_FILE.into()),
            #[cfg(feature = "recovery")]
            recovery_listen: source.list("recovery.listen")?,
            #[cfg(feature = "ssh-sandbox")]
            ssh_sandbox: if source.get("ssh.sandbox", true)? {
                Some(orion_ssh_sandbox::SandboxConfig {
                    user: source.get("ssh.sandbox_user", "nobody".to_owned())?,
                })
            } else {
                None
            },
            #[cfg(feature = "ssh")]
            ssh: {
                // Sessions run in this program, started again as the helper, unless
                // another is named.
                let program = match source.optional::<std::path::PathBuf>("ssh.helper")? {
                    Some(program) => program,
                    None => std::env::current_exe().map_err(|err| {
                        error("ssh.helper", format!("this program has no path: {err}"))
                    })?,
                };
                let helper = orion_ssh::Helper {
                    program,
                    args: Vec::new(),
                };
                #[allow(unused_mut)]
                let mut ssh = orion_ssh::SshConfig::from_source(source, &config.state_dir, helper)?;
                // sftp and scp: the same program, started as the sftp helper.
                #[cfg(feature = "ssh-sftp")]
                if let Some(ssh) = &mut ssh {
                    ssh.subsystems.push(orion_ssh::Subsystem {
                        name: "sftp".to_owned(),
                        program: vec![
                            ssh.helper.program.clone().into(),
                            orion_ssh_sftp::MODE.into(),
                        ],
                    });
                }
                ssh
            },
            #[cfg(feature = "nt4")]
            nt4: orion_nt4_gateway::Nt4Config::from_source(source)?,
            #[cfg(feature = "nt4-client")]
            nt4_client: orion_nt4_client_gateway::Nt4ClientConfig::from_source(source)?,
            #[cfg(feature = "topic-peers")]
            peer_nodes: source.list("peer.nodes")?,
            #[cfg(feature = "link-serial")]
            serial_links: source.list("links.serial")?,
            #[cfg(feature = "link-sealed")]
            sealed_links: source.list("links.sealed")?,
            #[cfg(feature = "link-can")]
            can_links: source.list("links.can")?,
            #[cfg(feature = "link-usb")]
            usb_links: source.list("links.usb")?,
            config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<(CommSettings, Vec<String>), ConfigError> {
        let mut source = Source::parse(text, Vec::new())?;
        let settings = CommSettings::from_source(&mut source)?;
        let unknown = source.warnings().iter().map(ToString::to_string).collect();
        Ok((settings, unknown))
    }

    #[test]
    fn nothing_set_is_a_comm_core_with_security_off_and_nothing_extra() {
        let (settings, unknown) = parse("").expect("settings");
        assert_eq!(
            settings.config.socket.to_str(),
            Some("/run/orion/orion.sock")
        );
        assert_eq!(settings.socket_mode, 0o660);
        assert_eq!(settings.status_interval, Duration::from_secs(1));
        assert_eq!(settings.config.peer_listen, None);
        assert!(unknown.is_empty(), "{unknown:?}");
    }

    #[test]
    fn every_part_reads_its_own_settings_and_a_typo_is_seen() {
        let (settings, unknown) = parse(
            "node.name = left\nsecurity = on\nsocket.mode = 0600\nsocket.node_uids = 990, 991\ntopics.max = 64\ncomm.max_sessions = 32\ncluster = lab\nstatus.interval_ms = 250\npeer.listen = 0.0.0.0:9200\ntopcis.max = 3\n",
        )
        .expect("settings");
        assert_eq!(settings.config.node_name.as_str(), "left");
        assert_eq!(
            (settings.socket_mode, settings.node_uids.as_slice()),
            (0o600, [990, 991].as_slice())
        );
        assert_eq!(
            (settings.broker.max_topics, settings.limits.max_sessions),
            (64, 32)
        );
        assert_eq!(settings.keys.cluster, "lab");
        assert_eq!(unknown.len(), 1, "{unknown:?}");
        assert!(unknown[0].contains("topcis.max"), "{unknown:?}");
    }

    #[test]
    fn settings_that_cannot_work_are_refused() {
        assert_eq!(
            parse("socket.mode = rw").expect_err("mode").key,
            "socket.mode"
        );
        assert_eq!(
            parse("socket.mode = 7777").expect_err("mode").key,
            "socket.mode"
        );
        assert_eq!(
            parse("status.interval_ms = 0").expect_err("interval").key,
            "status.interval_ms"
        );
        assert_eq!(
            parse("comm.max_sessions = 0").expect_err("limits").key,
            "comm.max_sessions"
        );
        assert_eq!(
            parse("hello.state = recovery").expect_err("state").key,
            "hello.state"
        );
    }

    #[test]
    fn a_flasher_image_says_so_and_a_board_is_running_unless_it_says() {
        use orion_hello::BoardState;
        assert_eq!(parse("").expect("none").0.board_state, BoardState::Running);
        assert_eq!(
            parse("hello.state = flasher")
                .expect("flasher")
                .0
                .board_state,
            BoardState::Flasher
        );
    }
}
