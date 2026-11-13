//! Orion's own protocol in recovery: the board's socket and its USB links, serving the hello
//! document only, which says the board is in recovery and why. A tool that finds the board
//! on its socket or over a USB cable is told what is wrong without logging in.
//!
//! **Nothing from the state directory.** The node's key is one made for the run: the hello
//! document is signed, but by a key no one has pinned, and a tool that pinned the board's own
//! key is told it is another (it is: the board is not itself). Security is on unless the
//! settings were read and say otherwise.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use orion_check::Security;
use orion_hello::{BoardState, Hello, SharedState};
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_server::{Comm, Limits};
use orion_unix::UnixCarrier;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::{Carriers, CommSettings};

/// Where the board's socket is when the settings cannot say.
const SOCKET: &str = "/run/orion/orion.sock";
/// How long between tries to open the socket.
const RETRY: std::time::Duration = std::time::Duration::from_millis(100);

/// Orion's protocol, served in recovery. Dropping it stops it.
#[derive(Debug)]
pub struct RecoveryComm {
    state: SharedState,
    tasks: Rc<RefCell<Vec<JoinHandle<()>>>>,
}

impl RecoveryComm {
    /// Serves `settings`' socket and USB links, if they were read; else the usual socket, and
    /// a USB link for any name root offers. `why` is what the hello document says. With
    /// `shell`, a shell may be opened over the session, by whom the check lets.
    #[must_use]
    pub fn start(
        settings: Option<&CommSettings>,
        why: &str,
        shell: Option<orion_ssh::SshConfig>,
    ) -> Self {
        let node = settings.map_or_else(
            || "node.local".parse().expect("a node name"),
            |settings| settings.config.node_name.clone(),
        );
        let security = settings.map_or(Security::On, |settings| settings.config.security);
        let cluster = settings.map_or("default", |settings| settings.keys.cluster.as_str());
        let keys = SharedKeys::new(
            Keys::open(KeysConfig::in_memory(cluster), security).expect("a key kept nowhere"),
        );
        let comm = Comm::new(
            node.clone(),
            security,
            Some(Rc::new(keys.clone())),
            settings.map_or_else(Limits::default, |settings| settings.limits),
        );
        let hello =
            Hello::new(node, keys.clone(), None).in_state(BoardState::Recovery(why.to_owned()));
        let state = hello.state();
        comm.add_service(hello);
        #[cfg(feature = "shell")]
        if let Some(config) = shell {
            let at = config.listen;
            let keeper = orion_ssh::Keeper::new(config, security, keys.clone(), at);
            comm.add_service(orion_shell::ShellService::new(&comm, Rc::new(keeper)));
        } else {
            comm.add_service(orion_shell::ShellOff);
        }
        #[cfg(not(feature = "shell"))]
        let _ = shell;
        let tasks: Rc<RefCell<Vec<JoinHandle<()>>>> = Rc::default();
        let serve = {
            let (comm, tasks) = (comm.clone(), tasks.clone());
            move |carrier| {
                let comm = comm.clone();
                tasks
                    .borrow_mut()
                    .push(tokio::task::spawn_local(async move {
                        comm.serve(carrier).await;
                    }));
            }
        };

        #[cfg(feature = "link-usb")]
        {
            let links = orion_link_usb::UsbLinks::new();
            comm.add_service(links.service());
            let named = settings.map(|settings| settings.usb_links.as_slice());
            for name in named.unwrap_or_default() {
                match name.parse() {
                    Ok(device) => serve(Carriers::Usb(orion_link_serial::StreamCarrier::new(
                        device,
                        links.link(name),
                    ))),
                    Err(error) => warn!("recovery: links.usb {name}: {error}"),
                }
            }
            if named.is_none() {
                // No list to go by: whatever link root hands over is served.
                let serve = serve.clone();
                links.any_name(move |name, link| match name.parse() {
                    Ok(device) => {
                        serve(Carriers::Usb(orion_link_serial::StreamCarrier::new(
                            device, link,
                        )));
                    }
                    Err(error) => warn!("recovery: a link named {name:?}: {error}"),
                });
            }
        }

        // The board's socket. A comm core that failed in this process may hold it a moment
        // longer, until its tasks are dropped: try again until it is free.
        let socket = settings.map_or_else(
            || PathBuf::from(SOCKET),
            |settings| settings.config.socket.clone(),
        );
        let mode = settings.map_or(0o660, |settings| settings.socket_mode);
        let node_uids = settings
            .map(|settings| settings.node_uids.clone())
            .unwrap_or_default();
        let comm_for_socket = comm.clone();
        tasks
            .borrow_mut()
            .push(tokio::task::spawn_local(async move {
                let mut said = false;
                let unix = loop {
                    match bind(&socket, mode, &node_uids) {
                        Ok(unix) => break unix,
                        Err(error) if !said => {
                            warn!(
                                "recovery: the socket {} cannot be opened yet: {error}",
                                socket.display()
                            );
                            said = true;
                        }
                        Err(_) => {}
                    }
                    tokio::time::sleep(RETRY).await;
                };
                info!("recovery: Orion's protocol on {}", socket.display());
                comm_for_socket.serve(Carriers::Unix(unix)).await;
            }));
        Self { state, tasks }
    }

    /// What the hello document says the board is doing.
    #[must_use]
    pub const fn state(&self) -> &SharedState {
        &self.state
    }
}

fn bind(socket: &Path, mode: u32, node_uids: &[u32]) -> std::io::Result<UnixCarrier> {
    if let Some(dir) = socket.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    UnixCarrier::bind(socket, Some(mode), node_uids)
}

impl Drop for RecoveryComm {
    fn drop(&mut self) {
        for task in self.tasks.borrow().iter() {
            task.abort();
        }
    }
}
