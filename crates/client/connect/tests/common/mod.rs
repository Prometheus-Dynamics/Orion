//! The harness for the connection tests: a comm core in its own thread, and halves of a link
//! that can be unplugged.

#![allow(dead_code)]

pub use std::os::fd::OwnedFd;
pub use std::path::PathBuf;
pub use std::time::{Duration, Instant};

pub use orion_connect::{Identity, Options, Session};
pub use orion_msg_session::{Identify, Identity as IdentityAnswer};
pub use orion_server::Security;
pub use orion_wire::{Bytes, kind};

use std::rc::Rc;
use std::sync::mpsc;

use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_link_serial::{Serial, StreamCarrier};
use orion_server::{Comm, Limits};
use orion_tcp::{TcpCarrier, TcpOptions};
use orion_unix::UnixCarrier;
use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};

pub const HEARTBEAT: Duration = Duration::from_millis(100);

/// A comm core in its own thread: the board's socket, the network, and a serial link whose
/// other end is the returned descriptor.
pub struct Board {
    pub socket: PathBuf,
    pub network: std::net::SocketAddr,
    pub key: [u8; 32],
    pub link: Option<OwnedFd>,
    pub keys: SharedKeys,
    /// How many sessions the board has open, as of a moment ago.
    pub live: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub stop: Option<tokio::sync::oneshot::Sender<()>>,
    pub thread: Option<std::thread::JoinHandle<()>>,
    pub dir: PathBuf,
}

impl Board {
    pub fn start(name: &str, security: Security) -> Self {
        let dir = std::env::temp_dir().join(format!("orion-connect-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        Self::start_in(dir, security, None)
    }

    /// Starts a board in `dir`, on `port` if it is to come back where it was.
    pub fn start_in(dir: PathBuf, security: Security, port: Option<u16>) -> Self {
        let socket = dir.join("orion.sock");
        let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY).expect("openpt");
        grantpt(&master).expect("grantpt");
        unlockpt(&master).expect("unlockpt");
        let slave = PathBuf::from(
            ptsname(&master, Vec::new())
                .expect("ptsname")
                .into_string()
                .expect("path"),
        );
        let keys =
            SharedKeys::new(Keys::open(KeysConfig::in_memory("lab"), security).expect("keys"));
        let key = keys.lock().identity().public();
        let (ready, started) = mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (for_board, path) = (keys.clone(), socket.clone());
        let for_link = keys.clone();
        let live = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting = live.clone();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let limits = Limits {
                    heartbeat_ms: HEARTBEAT.as_millis() as u32,
                    ..Limits::default()
                };
                let comm = Comm::new(
                    "board".parse().expect("name"),
                    security,
                    Some(Rc::new(for_board.clone())),
                    limits,
                );
                comm.add_service(orion_hello::Hello::new(
                    "board".parse().expect("name"),
                    for_board.clone(),
                    None,
                ));
                let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
                let options = TcpOptions {
                    security,
                    keys: for_board,
                    max_frame: 64 * 1024,
                    handshake: Duration::from_secs(5),
                };
                let listen = std::net::SocketAddr::from(([127, 0, 0, 1], port.unwrap_or(0)));
                let tcp = TcpCarrier::bind(listen, options).await.expect("listen");
                let network = tcp.local_addr().expect("addr");
                let link = StreamCarrier::new(
                    "uart0".parse().expect("name"),
                    Serial {
                        path: slave,
                        baud: 115_200,
                    },
                )
                .signed(for_link);
                let opened = link.stats();
                for carrier in [
                    tokio::task::spawn_local({
                        let comm = comm.clone();
                        async move { comm.serve(unix).await }
                    }),
                    tokio::task::spawn_local({
                        let comm = comm.clone();
                        async move { comm.serve(tcp).await }
                    }),
                    tokio::task::spawn_local({
                        let comm = comm.clone();
                        async move { comm.serve(link).await }
                    }),
                ] {
                    drop(carrier);
                }
                tokio::task::spawn_local({
                    let comm = comm.clone();
                    async move {
                        loop {
                            let open = comm.with(|sessions, _| sessions.iter().count());
                            counting.store(open, std::sync::atomic::Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    }
                });
                // The link is raw only once the board has opened it.
                while opened().opens == 0 {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                ready.send(network).expect("ready");
                let _ = stopped.await;
            });
        });
        let network = started.recv().expect("the board starts");
        Self {
            socket,
            network,
            key,
            link: Some(master),
            keys,
            live,
            stop: Some(stop),
            thread: Some(thread),
            dir,
        }
    }

    pub fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().expect("the board stops");
        }
    }
}

impl Drop for Board {
    fn drop(&mut self) {
        // The directory is left for the test to remove: a board started again in it must not
        // lose its socket when the one before it is dropped.
        self.stop();
    }
}

pub fn options(identity: Option<Identity>) -> Options {
    Options {
        identity,
        ..Options::default()
    }
}

/// Asks the board who it is and returns the name it gives.
pub fn who(session: &mut Session, nonce: &[u8]) -> String {
    let seq = session
        .send(&Identify {
            nonce: Bytes(nonce),
        })
        .expect("send");
    let answer = session.answer(seq, kind::IDENTITY).expect("an answer");
    let identity: IdentityAnswer<'_> = answer.body().expect("identity");
    assert_eq!((identity.request_seq, identity.nonce.0), (seq, nonce));
    identity.node.to_string()
}

/// What the two halves of one plug share.
#[derive(Default)]
struct Shared {
    unplugged: bool,
    /// Frames the write half was given.
    written: usize,
}

type Between = std::sync::Arc<(std::sync::Mutex<Shared>, std::sync::Condvar)>;

/// How a plug misbehaves.
#[derive(Clone, Default)]
pub struct Faults {
    /// The first this many frames written are lost on the way.
    pub lose: usize,
    /// Nothing is read until this many frames were written.
    pub hold: usize,
    /// Nothing is read until this other plug is unplugged: two readers on one link never race.
    pub after: Option<Unplugged>,
}

/// Says when a plug is unplugged.
#[derive(Clone)]
pub struct Unplugged(Between);

/// A read half that can be unplugged: dropping its write half wakes its reading thread, which
/// then reads no more, so the next halves on the same link get every byte after it.
pub struct Plug {
    pub link: std::fs::File,
    wake: OwnedFd,
    between: Between,
    hold: usize,
    after: Option<Unplugged>,
}

/// Its write half. Dropping it unplugs the read half.
pub struct Socket {
    pub link: std::fs::File,
    wake: Option<OwnedFd>,
    between: Between,
    lose: usize,
}

impl orion_connect::LinkRead for Plug {
    fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        use rustix::event::{PollFd, PollFlags, poll};
        let gone = || std::io::Error::other("unplugged");
        if self.hold > 0 {
            let (lock, written) = &*self.between;
            let shared = lock.lock().expect("lock");
            let _held = written
                .wait_while(shared, |shared| {
                    !shared.unplugged && shared.written < self.hold
                })
                .expect("lock");
            self.hold = 0;
        }
        if let Some(Unplugged(other)) = self.after.take() {
            let (lock, changed) = &*other;
            let shared = lock.lock().expect("lock");
            let _gone = changed
                .wait_while(shared, |shared| !shared.unplugged)
                .expect("lock");
        }
        loop {
            let mut waiting = [
                PollFd::new(&self.link, PollFlags::IN),
                PollFd::new(&self.wake, PollFlags::IN),
            ];
            match poll(&mut waiting, None) {
                Ok(_) | Err(rustix::io::Errno::INTR) => {}
                Err(error) => return Err(error.into()),
            }
            // Held while the check and the read are made, so a byte read here is one sent
            // before the halves were let go. Every half on the link reads under its own lock
            // only, so the link is looked at again here: the bytes the wait saw may have gone
            // to the half before, and a read with nothing there would wait holding the lock.
            let shared = self.between.0.lock().expect("lock");
            if shared.unplugged || !waiting[1].revents().is_empty() {
                return Err(gone());
            }
            let mut now = [PollFd::new(&self.link, PollFlags::IN)];
            if poll(&mut now, Some(&rustix::event::Timespec::default())).unwrap_or(0) > 0 {
                return std::io::Read::read(&mut self.link, into);
            }
        }
    }
}

impl orion_connect::LinkWrite for Socket {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        {
            let (lock, written) = &*self.between;
            lock.lock().expect("lock").written += 1;
            written.notify_all();
        }
        if self.lose > 0 {
            // Lost on the way, as on a link: nothing says so.
            self.lose -= 1;
            return Ok(());
        }
        std::io::Write::write_all(&mut self.link, bytes)
    }
}

impl Socket {
    /// What says when this plug is unplugged.
    pub fn unplugged(&self) -> Unplugged {
        Unplugged(self.between.clone())
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        let (lock, written) = &*self.between;
        lock.lock().expect("lock").unplugged = true;
        written.notify_all();
        // Closing the pipe's write end wakes the reading thread.
        drop(self.wake.take());
    }
}

/// Plugs into `link`: two halves over a copy of it.
pub fn plug(link: &OwnedFd) -> (Plug, Socket) {
    plug_with(link, Faults::default())
}

/// Plugs into `link` with `faults`.
pub fn plug_with(link: &OwnedFd, faults: Faults) -> (Plug, Socket) {
    let (wake, waker) = rustix::pipe::pipe().expect("pipe");
    let between = Between::default();
    let copy = || std::fs::File::from(link.try_clone().expect("the link again"));
    (
        Plug {
            link: copy(),
            wake,
            between: between.clone(),
            hold: faults.hold,
            after: faults.after,
        },
        Socket {
            link: copy(),
            wake: Some(waker),
            between,
            lose: faults.lose,
        },
    )
}
