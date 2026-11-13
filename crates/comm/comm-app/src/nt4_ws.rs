//! NT4 clients on the WebSocket listener (`nt4.listen = ws`): the connections that ask for an
//! NT4 subprotocol, handed to the gateway.

use orion_nt4_gateway::Nt4Listen;
#[cfg(feature = "ws")]
use orion_nt4_gateway::{Gateway, SUBPROTOCOL_V4_0, SUBPROTOCOL_V4_1};
#[cfg(feature = "ws")]
use tokio::task::JoinHandle;

use crate::{CommSettings, StartError, failed};

/// The two NT4 subprotocols' connections, taken from the listener before it runs.
#[cfg(feature = "ws")]
pub(crate) type Nt4OnWs = [orion_ws::Incoming; 2];

/// Whether the gateway shares the WebSocket listener; an error when there is none to share.
pub(crate) fn wanted(settings: &CommSettings) -> Result<bool, StartError> {
    let wants = settings
        .nt4
        .as_ref()
        .is_some_and(|config| config.listen == Nt4Listen::Ws);
    #[cfg(feature = "ws")]
    let no_ws = settings.ws_listen.is_none();
    #[cfg(not(feature = "ws"))]
    let no_ws = true;
    if wants && no_ws {
        return Err(failed("nt4.listen ws", "there is no ws.listen to share"));
    }
    Ok(wants)
}

/// Asks the listener for the connections that want NT4, newest version first.
#[cfg(feature = "ws")]
pub(crate) fn take_from(listener: &mut orion_ws::WsListener) -> Nt4OnWs {
    [
        listener.protocol(SUBPROTOCOL_V4_1),
        listener.protocol(SUBPROTOCOL_V4_0),
    ]
}

/// Hands each such connection to the gateway, for as long as the listener runs.
#[cfg(feature = "ws")]
pub(crate) fn forward(gateway: &Gateway, incoming: Nt4OnWs) -> JoinHandle<()> {
    let [mut new, mut old] = incoming;
    let taker = gateway.taker();
    tokio::task::spawn_local(async move {
        loop {
            tokio::select! {
                Some(socket) = new.next() => taker.take(socket),
                Some(socket) = old.next() => taker.take(socket),
                else => break,
            }
        }
    })
}
