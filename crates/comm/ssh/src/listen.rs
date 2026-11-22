//! Where SSH listens: on its address, or kept to each of its interfaces at that address. A
//! listener kept to an interface follows it: one that is not there yet is waited for, and one
//! that goes, or is made again under the same name (a USB gadget's network, rebuilt), is
//! listened on again once it is there.

use std::net::SocketAddr;
use std::time::Duration;

use orion_server::{Retry, open_retrying};
use orion_tcp::{Listen, kept, still_kept};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::debug;

/// How often a listener waits for its interface, and looks whether it is still the one.
const LOOK: Duration = Duration::from_secs(1);

/// Listens on `listen`, and only on the network interface named `interface` if there is one.
///
/// # Errors
///
/// If it cannot be listened on; [`absent`] says whether that is for want of the interface.
pub fn listen(listen: SocketAddr, interface: Option<&str>) -> std::io::Result<TcpListener> {
    kept(listen, interface).map(|(listener, _)| listener)
}

/// Whether listening failed because the interface it was to be on is not there (yet).
#[must_use]
pub fn absent(error: &std::io::Error) -> bool {
    error.raw_os_error() == Some(rustix::io::Errno::NODEV.raw_os_error())
}

/// Listens as [`listen`] does, waiting for an interface that is not there yet: a USB
/// gadget's network comes up after the comm core does. Tries every second, and says so
/// once, by the interface's name, and once when it opens.
async fn when_there(at: SocketAddr, interface: &str) -> (TcpListener, u32) {
    let on = Listen {
        addr: at,
        interface: Some(interface.to_owned()),
    };
    let every = Retry {
        first: LOOK,
        most: LOOK,
    };
    open_retrying("ssh", &on, every, || {
        std::future::ready(kept(at, Some(interface)))
    })
    .await
}

/// Waits until the interface `listener` is kept to is no longer the one named `interface`.
async fn gone(listener: &TcpListener, interface: &str, index: u32) {
    loop {
        tokio::time::sleep(LOOK).await;
        if !still_kept(listener, interface, index) {
            return;
        }
    }
}

/// The listeners SSH has, each in a task of its own, and the connections they take. They stop
/// when this is dropped.
#[derive(Debug)]
pub struct Listeners {
    local_addr: SocketAddr,
    accepted: mpsc::Receiver<(TcpStream, SocketAddr)>,
    tasks: Vec<JoinHandle<()>>,
}

impl Listeners {
    /// Listens on `at`, or kept to each of `interfaces` at `at`. An interface that is not
    /// there does not stop the rest: it is waited for, and said once.
    ///
    /// # Errors
    ///
    /// If `at` cannot be listened on for any reason but a missing interface.
    pub fn start(at: SocketAddr, interfaces: &[String]) -> std::io::Result<Self> {
        start(at, interfaces)
    }

    /// Where SSH clients connect: the first listener's address, or the one it was given while
    /// none is open yet.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The next connection any of them took, and who from.
    pub async fn accept(&mut self) -> Option<(TcpStream, SocketAddr)> {
        self.accepted.recv().await
    }
}

impl Drop for Listeners {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn start(at: SocketAddr, interfaces: &[String]) -> std::io::Result<Listeners> {
    let names: Vec<Option<String>> = if interfaces.is_empty() {
        vec![None]
    } else {
        interfaces.iter().cloned().map(Some).collect()
    };
    let (taken, accepted) = mpsc::channel(16);
    let mut local_addr = None;
    let mut tasks = Vec::new();
    for name in names {
        let first = match kept(at, name.as_deref()) {
            Ok(opened) => Some(opened),
            Err(error) if name.is_some() && absent(&error) => None,
            Err(error) => return Err(error),
        };
        if let Some((listener, _)) = &first {
            let addr = listener.local_addr()?;
            local_addr = local_addr.or(Some(addr));
            // The address last: what reads this line for the port takes what follows the last ':'.
            tracing::info!("ssh: {}: open on {addr}", name.as_deref().unwrap_or("any"));
        }
        tasks.push(tokio::spawn(serve(at, name, first, taken.clone())));
    }
    Ok(Listeners {
        local_addr: local_addr.unwrap_or(at),
        accepted,
        tasks,
    })
}

/// Takes connections on one listener, for as long as there is someone to give them to,
/// listening again when its interface goes or is made again.
async fn serve(
    at: SocketAddr,
    interface: Option<String>,
    mut first: Option<(TcpListener, u32)>,
    taken: mpsc::Sender<(TcpStream, SocketAddr)>,
) {
    loop {
        let (listener, index) = match (first.take(), &interface) {
            (Some(opened), _) => opened,
            (None, Some(name)) => when_there(at, name).await,
            (None, None) => unreachable!("only a named interface is waited for"),
        };
        let gone = async {
            match &interface {
                Some(name) => gone(&listener, name, index).await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(gone);
        loop {
            tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok(accepted) => {
                        if taken.send(accepted).await.is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        debug!(%error, "ssh: a connection could not be accepted");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
                () = &mut gone => break,
            }
        }
        if let Some(name) = &interface {
            tracing::warn!(
                "ssh: {name} went, or was made again; listening on it again once it is there"
            );
        }
    }
}
