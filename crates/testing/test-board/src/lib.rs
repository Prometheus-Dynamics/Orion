//! A whole board for a consumer's tests.
//!
//! [`TestBoard::start`] runs the real comm core and the real node logic in the test's own
//! process, each in a thread of its own, on a socket in a directory of its own. Nothing is stood
//! in for: what a client is answered, and refused, is what a board answers. A board in
//! recovery is the real recovery way in.
//!
//! - The settings are one `orion.conf` text, read by both, as on a board. [`Options::conf`]
//!   adds lines to it.
//! - The security switch is the board's record, written in the board's directory:
//!   [`Options::security`]. With it on, a client is checked as on a board, so a test sees the
//!   real refusals.
//! - [`TestBoard::stop_node`] and [`TestBoard::start_node`] take node logic away and bring it
//!   back, with what it saved, while the comm core stays.
//! - [`TestBoard::start_in_recovery`] starts recovery, with a reason, as the comm core does
//!   when it cannot start; settings that stop the comm core from starting do the same.
//!
//! `docs/testing.md` says when to use this and when [`orion-stubs`] is still the one.

#![cfg(target_os = "linux")]

use std::net::SocketAddr;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use orion::records::RecordClass;
use orion::{Address, Orion};
use orion_comm_app::{CommSettings, RecoverWith, Started, recover, start_or_recover};
use orion_config::Source;
use orion_node_app::{Node, NodeSettings};
use orion_scratch::Scratch;
use tokio::sync::oneshot;

mod held;
mod unanswered;

pub use held::HeldCommits;
pub use unanswered::HeldAction;

/// How long a start may take before the test is told it failed.
const START: Duration = Duration::from_secs(20);

/// The security switch, as the board's record says it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Security {
    /// No record: security is off.
    #[default]
    NoRecord,
    /// A record saying off.
    Off,
    /// A record saying on: every client is checked.
    On,
    /// A record that is not one: security is on, and the hello document says it is damaged.
    Damaged,
}

/// How a [`TestBoard`] starts.
#[derive(Clone, Debug)]
pub struct Options {
    /// The board's name. `board` by default.
    pub name: String,
    /// The security switch. No record, so off, by default.
    pub security: Security,
    /// The network and WebSocket ways in, on 127.0.0.1 at ports of their own. Off by default:
    /// the socket only.
    pub network: bool,
    /// `comm.heartbeat_ms`. The board's own default if `None`. A test on a loaded machine sets
    /// it long, so a slow moment is not taken for a lost session.
    pub heartbeat_ms: Option<u32>,
    /// Node logic, or the comm core alone. On by default.
    pub node: bool,
    /// An operator the board trusts with everything from the start: its public key, seeded as a
    /// flash seeds one (`operator.seed` in `keys.dir`). None by default.
    pub operator: Option<[u8; 32]>,
    /// Operators the board trusts with less than everything, from the start, as an
    /// administrator would have granted them. None by default.
    pub granted: Vec<Granted>,
    /// More `orion.conf` lines, read by both. Anything a board reads may be set: `leases.quiet_ms`,
    /// `enrollment.key`, `topics.max`. A key set here replaces the board's own line for it.
    pub conf: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            name: "board".to_owned(),
            security: Security::NoRecord,
            network: false,
            heartbeat_ms: None,
            node: true,
            operator: None,
            granted: Vec::new(),
            conf: String::new(),
        }
    }
}

/// An operator trusted with some things: to read or not, and the actions it may run (a name,
/// or a prefix ending in `*`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Granted {
    /// Its public key.
    pub key: [u8; 32],
    /// Whether it may read: records, topics, and files to fetch.
    pub read: bool,
    /// The actions it may run.
    pub actions: Vec<String>,
}

/// What the comm core's thread says once it is up.
enum Up {
    Running {
        network: Option<SocketAddr>,
        websocket: Option<SocketAddr>,
    },
    Recovery {
        why: String,
        ssh: Option<SocketAddr>,
    },
}

/// A thread that runs until it is told to stop.
struct Service {
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Service {
    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        self.stop();
    }
}

/// What the board is doing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    /// The comm core runs.
    Running,
    /// Recovery runs instead, for this reason.
    Recovery(String),
}

/// A board in the test's process, until it is dropped. Dropping it stops node logic, then the
/// comm core, and removes its directory.
pub struct TestBoard {
    socket: PathBuf,
    state: State,
    network: Option<SocketAddr>,
    websocket: Option<SocketAddr>,
    ssh: Option<SocketAddr>,
    node_settings: NodeSettings,
    node: Option<Service>,
    comm: Service,
    /// The board's directory, removed last, after both services have stopped.
    dir: Scratch,
}

impl std::fmt::Debug for TestBoard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestBoard({}, {:?})", self.socket.display(), self.state)
    }
}

impl TestBoard {
    /// Starts a board named `board`, with security off, node logic running and the socket its
    /// only way in. `name` keeps tests apart: it names the board's directory.
    ///
    /// # Panics
    ///
    /// If the board cannot start: a test has nothing to do then.
    #[must_use]
    pub fn start(name: &str) -> Self {
        Self::start_with(name, Options::default())
    }

    /// Starts a board as `options` say. If its settings stop the comm core from starting,
    /// recovery runs instead, as on a board, and [`TestBoard::state`] says why.
    ///
    /// # Panics
    ///
    /// If its settings do not parse (recovery would then serve the board's usual socket, not
    /// this one), or neither the comm core nor recovery starts.
    #[must_use]
    pub fn start_with(name: &str, options: Options) -> Self {
        Self::begin(name, &options, None)
    }

    /// Starts recovery, as the comm core does when it cannot start, with `why` in its hello
    /// document. There is no node logic in recovery.
    ///
    /// # Panics
    ///
    /// If recovery does not start.
    #[must_use]
    pub fn start_in_recovery(name: &str, options: Options, why: &str) -> Self {
        Self::begin(name, &options, Some(why.to_owned()))
    }

    fn begin(name: &str, options: &Options, forced: Option<String>) -> Self {
        let dir = Scratch::new(&format!("board-{name}"));
        let record = dir.join("security");
        match options.security {
            Security::NoRecord => {}
            Security::Off => std::fs::write(&record, "off\ngeneration 1\n").expect("record"),
            Security::On => std::fs::write(&record, "on\ngeneration 1\n").expect("record"),
            Security::Damaged => std::fs::write(&record, "perhaps\n").expect("record"),
        }
        if let Some(key) = options.operator {
            let keys = dir.join("keys");
            std::fs::create_dir_all(&keys).expect("the keys directory");
            let hex: String = key.iter().map(|byte| format!("{byte:02x}")).collect();
            std::fs::write(keys.join("operator.seed"), format!("{hex}\n")).expect("the seed");
        }
        if !options.granted.is_empty() {
            let keys = dir.join("keys");
            std::fs::create_dir_all(&keys).expect("the keys directory");
            let mut trust = "# Orion trust store.\n".to_owned();
            for (index, granted) in options.granted.iter().enumerate() {
                let hex: String = granted
                    .key
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect();
                let actions = if granted.actions.is_empty() {
                    "-".to_owned()
                } else {
                    granted.actions.join(",")
                };
                trust.push_str(&format!(
                    "operator {hex} enrolled {} 0 granted-{index} {actions}\n",
                    u8::from(granted.read)
                ));
            }
            std::fs::write(keys.join("trust"), trust).expect("the trust store");
        }
        let text = conf(&dir, options);
        // Settings that do not parse would send recovery to the board's usual socket, outside
        // this directory: a test is told instead.
        let comm_settings = Source::parse(&text, Vec::new())
            .and_then(|mut source| CommSettings::from_source_with_view(&mut source, &record))
            .unwrap_or_else(|error| panic!("the board's settings do not parse: {error}\n{text}"));
        let node_settings = node_settings(&text, &comm_settings);
        let socket = dir.join("orion.sock");
        let with = RecoverWith {
            listen: vec![SocketAddr::from(([127, 0, 0, 1], 0))],
            keys_file: dir.join("recovery.keys"),
            helper: None,
            retry: Duration::from_millis(50),
            last_good: dir.join("last-good"),
            security_view: record,
            login: Default::default(),
        };
        let (comm, up) = comm(comm_settings, with, forced);
        let mut board = Self {
            dir,
            socket,
            state: State::Running,
            network: None,
            websocket: None,
            ssh: None,
            node_settings,
            node: None,
            comm,
        };
        match up {
            Up::Running { network, websocket } => {
                board.network = network;
                board.websocket = websocket;
                if options.node {
                    board.start_node();
                }
            }
            Up::Recovery { why, ssh } => {
                board.state = State::Recovery(why);
                board.ssh = ssh;
            }
        }
        board
    }

    /// Starts node logic again after [`TestBoard::stop_node`], with what it saved, and waits
    /// until it answers. Nothing happens if it runs.
    ///
    /// # Panics
    ///
    /// If node logic cannot start, does not answer, or the board is in recovery.
    pub fn start_node(&mut self) {
        if self.node.is_some() {
            return;
        }
        assert_eq!(self.state, State::Running, "no node logic in recovery");
        let settings = self.node_settings.clone();
        let (stop, stopped) = oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("node logic".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("runtime");
                runtime.block_on(async move {
                    let mut node = Node::load(settings).expect("node logic starts");
                    node.run(async {
                        let _ = stopped.await;
                    })
                    .await;
                });
            })
            .expect("a thread");
        self.node = Some(Service {
            stop: Some(stop),
            thread: Some(thread),
        });
        self.until_node_answers();
    }

    /// Stops node logic, as when its service stops: what it serves is refused as unavailable,
    /// and the comm core goes on. It saves what it keeps first.
    pub fn stop_node(&mut self) {
        if let Some(mut node) = self.node.take() {
            node.stop();
        }
    }

    /// Node logic answers once it has attached: records are its to read.
    fn until_node_answers(&self) {
        let began = Instant::now();
        let mut last = String::new();
        while began.elapsed() < START {
            let asked = Orion::connect(&self.address(), "test-board").and_then(|orion| {
                orion
                    .records()
                    .get(RecordClass::Resource, "")
                    .map_err(orion::Error::from)
            });
            match asked {
                Ok(_) => return,
                Err(error) => last = error.to_string(),
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("node logic never answered: {last}");
    }

    /// The board's socket.
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// The board's socket as an address a client connects to.
    #[must_use]
    pub fn address(&self) -> Address {
        Address::Local(self.socket.clone())
    }

    /// Connects a client named `name` over the socket.
    ///
    /// # Errors
    ///
    /// As [`Orion::connect`].
    pub fn connect(&self, name: &str) -> Result<Orion, orion::Error> {
        Orion::connect(&self.address(), name)
    }

    /// Where the network way in listens, with [`Options::network`].
    #[must_use]
    pub const fn network(&self) -> Option<SocketAddr> {
        self.network
    }

    /// Where the WebSocket way in listens, with [`Options::network`].
    #[must_use]
    pub const fn websocket(&self) -> Option<SocketAddr> {
        self.websocket
    }

    /// Where recovery's SSH listens, in recovery.
    #[must_use]
    pub const fn recovery_ssh(&self) -> Option<SocketAddr> {
        self.ssh
    }

    /// What the board is doing.
    #[must_use]
    pub const fn state(&self) -> &State {
        &self.state
    }

    /// Whether node logic runs.
    #[must_use]
    pub const fn node_runs(&self) -> bool {
        self.node.is_some()
    }

    /// A program on the board that receives every file whose destination starts with `prefix`
    /// and answers each commit `hold` after it is asked, pumping its session meanwhile: a
    /// receiver whose device work at the commit takes that long. For a test of what a pushing
    /// client does with a late answer ([`orion::Reply`]). Node logic must run.
    ///
    /// # Panics
    ///
    /// If it cannot connect or become the receiver for `prefix`.
    #[must_use]
    pub fn hold_commits(&self, prefix: &str, hold: Duration) -> HeldCommits {
        held::start(&self.socket, prefix, hold)
    }

    /// A program on the board that registers the action `name` on `target`, is handed every
    /// run of it, and answers none, pumping its session meanwhile: a device whose call never
    /// comes back. For a test of how a caller bounds its own wait for a run while the board
    /// stays heard. Node logic must run.
    ///
    /// # Panics
    ///
    /// If it cannot connect or register the action.
    #[must_use]
    pub fn hold_action(&self, name: &str, target: orion::actions::Target) -> HeldAction {
        unanswered::start(&self.socket, name, target)
    }

    /// The board's directory: its state, its keys, its record and its socket.
    #[must_use]
    pub fn dir(&self) -> &Path {
        self.dir.path()
    }
}

impl Drop for TestBoard {
    fn drop(&mut self) {
        self.stop_node();
        self.comm.stop();
    }
}

/// The board's `orion.conf`: what keeps it in its directory, then what the test asked for.
fn conf(dir: &Path, options: &Options) -> String {
    // The user running the test is the board's node logic, as root or the service's user is
    // on a board.
    let uid = std::fs::metadata(dir).map_or(0, |meta| meta.uid());
    let mut text = format!(
        "node.name = {name}\nsocket = {dir}/orion.sock\nsocket.node_uids = {uid}\n\
         state.dir = {dir}/state\nkeys.dir = {dir}/keys\nfiles.dir = {dir}/files\n",
        name = options.name,
        dir = dir.display(),
    );
    if options.network {
        text.push_str("peer.listen = 127.0.0.1:0\nws.listen = 127.0.0.1:0\n");
    }
    if let Some(heartbeat) = options.heartbeat_ms {
        text.push_str(&format!("comm.heartbeat_ms = {heartbeat}\n"));
    }
    text.push_str(&options.conf);
    text.push('\n');
    text
}

/// Node logic's settings from the same text. Its switch is the comm core's, read from the same
/// record, as on a board.
fn node_settings(text: &str, comm: &CommSettings) -> NodeSettings {
    let mut source = Source::parse(text, Vec::new()).expect("orion.conf");
    let mut settings = NodeSettings::from_source(&mut source)
        .unwrap_or_else(|error| panic!("node logic's settings do not parse: {error}"));
    settings.config = comm.config.clone();
    settings
}

/// Starts the comm core, or recovery, in a thread of its own, and waits until it is up.
fn comm(settings: CommSettings, with: RecoverWith, forced: Option<String>) -> (Service, Up) {
    let (stop, stopped) = oneshot::channel::<()>();
    let (tell, told) = mpsc::channel::<Up>();
    let thread = std::thread::Builder::new()
        .name("comm core".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let started = match forced {
                    Some(why) => Started::Recovery(recover(Some(&settings), why, with)),
                    None => start_or_recover(Ok(settings), with).await,
                };
                let up = match &started {
                    Started::Core(core) => Up::Running {
                        network: core.peer_addr(),
                        websocket: core.ws_addr(),
                    },
                    Started::Recovery(recovery) => {
                        let began = Instant::now();
                        while recovery.local_addr().is_none() && began.elapsed() < START {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Up::Recovery {
                            why: recovery.why().to_owned(),
                            ssh: recovery.local_addr(),
                        }
                    }
                };
                let _ = tell.send(up);
                let _ = stopped.await;
                drop(started);
            });
        })
        .expect("a thread");
    let up = told
        .recv_timeout(START)
        .expect("the comm core or recovery starts");
    (
        Service {
            stop: Some(stop),
            thread: Some(thread),
        },
        up,
    )
}
