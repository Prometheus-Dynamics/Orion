//! The network way in: one listener on `peer.listen`, or one kept to each interface
//! `peer.interface` names, at that address. Each opens on its own: one whose interface is not
//! there yet, or never comes, holds up neither the others nor the comm core, and is said once,
//! by its name, and tried again.

use std::net::SocketAddr;

use orion_server::Comm;
use orion_tcp::{Listen, TcpCarrier, TcpOptions};

use crate::carriers::{Carriers, Tasks};
use crate::error::{StartError, failed};
use crate::settings::CommSettings;

/// Opens the network way in, if `peer.listen` says where, and returns the address to announce:
/// the first listener's, or the address as it was given while none is open yet.
pub(crate) fn open(
    settings: &CommSettings,
    options: &TcpOptions,
    comm: &Comm,
    tasks: &mut Tasks,
) -> Result<Option<SocketAddr>, StartError> {
    let Some(addr) = settings.config.peer_listen else {
        return Ok(None);
    };
    let interfaces: Vec<Option<String>> = if settings.peer_interfaces.is_empty() {
        vec![None]
    } else {
        settings.peer_interfaces.iter().cloned().map(Some).collect()
    };
    let retry = settings.listen_retry;
    let mut announced = None;
    for interface in interfaces {
        let listen = Listen { addr, interface };
        let comm = comm.clone();
        match TcpCarrier::listen(&listen, options.clone(), retry) {
            Ok(carrier) => {
                announced = announced.or_else(|| carrier.local_addr().ok());
                tasks.push(tokio::task::spawn_local(async move {
                    comm.serve(Carriers::Tcp(carrier)).await;
                }));
            }
            // A way in that cannot open yet is not a comm core that cannot start: it tries
            // again, and the rest runs meanwhile.
            Err(error) if orion_server::may_clear(&error) => {
                let options = options.clone();
                tasks.push(tokio::task::spawn_local(async move {
                    let carrier = TcpCarrier::listen_retrying(&listen, options, retry).await;
                    comm.serve(Carriers::Tcp(carrier)).await;
                }));
            }
            Err(error) => return Err(failed(format!("peer.listen {listen}"), error)),
        }
    }
    // Announced at the address it was given while none is open yet.
    Ok(announced.or(Some(addr)))
}
