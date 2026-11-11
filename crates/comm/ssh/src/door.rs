//! The door between the side that speaks SSH and the side that has the board's rights.
//!
//! Everything a client sends is read on one side. What needs rights (deciding a login,
//! starting a session as an account, connecting from the board) is asked for through a
//! [`Door`], in a few plain values, and what comes back is a yes or no, or descriptors.
//! In one process the door is [`Local`]: it calls the [`Keeper`]. With the sandbox the
//! side that speaks SSH is a process with no rights of its own and the door is a socket
//! to the keeper.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};

use crate::keeper::{Admitted, Keeper, Started, Terminal, Wanted};

/// What the side that speaks SSH may ask of the side that has the rights.
pub trait Door: Clone + Send + Sync + 'static {
    /// Whether a login with `key` could be let in at all.
    fn offered(&self, key: Option<[u8; 32]>) -> impl Future<Output = bool> + Send;

    /// Asks to be let in as the account `name`, holding `key`. Whether it was.
    fn login(&self, name: &str, key: Option<[u8; 32]>) -> impl Future<Output = bool> + Send;

    /// Starts what `wanted` says as the account that was let in.
    fn start(
        &self,
        wanted: Wanted,
        terminal: Option<Terminal>,
    ) -> impl Future<Output = std::io::Result<Started>> + Send;

    /// Connects from the board to `host` on `port`.
    fn connect(
        &self,
        host: &str,
        port: u32,
    ) -> impl Future<Output = Result<std::net::TcpStream, String>> + Send;
}

/// The door in one process: the keeper is called, on a thread that may block.
#[derive(Clone, Debug)]
pub struct Local {
    keeper: Arc<Keeper>,
    peer: Option<SocketAddr>,
    /// The login that was let in, once one was.
    admitted: Arc<Mutex<Option<Arc<Admitted>>>>,
}

impl Local {
    /// A door to `keeper` for the connection from `peer`.
    #[must_use]
    pub fn new(keeper: Arc<Keeper>, peer: Option<SocketAddr>) -> Self {
        Self {
            keeper,
            peer,
            admitted: Arc::default(),
        }
    }

    /// The login that was let in, if one was.
    #[must_use]
    pub fn admitted(&self) -> Option<Arc<Admitted>> {
        self.admitted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn not_in() -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::PermissionDenied, "not logged in")
    }
}

impl Door for Local {
    async fn offered(&self, key: Option<[u8; 32]>) -> bool {
        self.keeper.offered(key.as_ref())
    }

    async fn login(&self, name: &str, key: Option<[u8; 32]>) -> bool {
        // Reads the account files: not on the thread everything else runs on.
        let (keeper, peer, name) = (self.keeper.clone(), self.peer, name.to_owned());
        let admitted = tokio::task::spawn_blocking(move || keeper.login(&name, key.as_ref(), peer))
            .await
            .ok()
            .flatten();
        let Some(admitted) = admitted else {
            return false;
        };
        *self.admitted.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(admitted));
        true
    }

    async fn start(&self, wanted: Wanted, terminal: Option<Terminal>) -> std::io::Result<Started> {
        let admitted = self.admitted().ok_or_else(Self::not_in)?;
        let (keeper, peer) = (self.keeper.clone(), self.peer);
        tokio::task::spawn_blocking(move || {
            keeper.start(&admitted, wanted, terminal.as_ref(), peer)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    async fn connect(&self, host: &str, port: u32) -> Result<std::net::TcpStream, String> {
        let admitted = self.admitted().ok_or("not logged in")?;
        let (keeper, host) = (self.keeper.clone(), host.to_owned());
        tokio::task::spawn_blocking(move || keeper.connect(&admitted, &host, port))
            .await
            .map_err(|error| error.to_string())?
    }
}
