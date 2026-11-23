//! Node logic, assembled.
//!
//! Node logic is the comm core's first client. It connects to the board's socket like any
//! program, with the node role, and tells the comm core which kinds it answers. The comm core
//! checks each such request at the door and passes it on; node logic answers through the comm
//! core, which sends the answer to the session that asked. There is no other protocol between
//! the two, and being a client like any other, node logic publishes what it has to show as
//! topics.
//!
//! So when node logic dies, the comm core and everything on it stay up: links, topics, the
//! hello document. What is node logic's is refused with "unavailable" until it is back. It
//! then loads what it had saved, connects, and the requests that stand, watches and
//! registered actions, are passed to it again.
//!
//! Node logic has one action of its own, `files.send`, which sends a file it keeps on to a
//! program that receives such files, a device's firmware slot among them (see `send`).
//!
//! One thread. The store and the lists are plain data owned by it. The one slow thing,
//! writing the snapshot, is done by a helper thread that exists only while it writes.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg(target_os = "linux")]

mod clock;
mod logic;
mod peers;
#[cfg(feature = "pull")]
mod pull;
pub mod records;
mod send;
mod served;
#[cfg(feature = "slow")]
pub mod slow;

use std::cell::RefCell;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_audit::{Audit, AuditConfig};
use orion_config::{Config, ConfigError, Security, Source};
use orion_connect::{Address, Client, ClientError, Options, Roles, Session};
use orion_file_store::{Files, FilesConfig};
use orion_msg_session::NodeAttach;
use orion_persist::{Persist, Source as Loaded};
use orion_store::{Record, Store, StoreConfig};
use orion_topics::{Publisher, TopicDecl, Topics};
use orion_wire::Bytes;
use tokio::io::Interest;
use tokio::net::UnixDatagram;
use tracing::{info, warn};

use logic::{Direct, Logic};
pub use orion_action_list::Actions;
pub use orion_reply::{Asked, Out};
pub use records::Records;

/// The topic node logic publishes what happens to action runs on.
pub const ACTIONS_TOPIC: &str = "orion.actions";

/// What node logic is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSettings {
    /// What every build needs: the node's name, the socket, the state directory.
    pub config: Config,
    /// `records.save_after_ms`: how long after a change the records are saved. Changes made
    /// in that time go in the same save.
    pub save_after: Duration,
    /// `records.max_drift_ms`: a stamp from a peer further ahead of this node's clock than
    /// this is refused.
    pub max_drift_ms: u64,
    /// `files.dir`: where files sent to this node go. `files` in the state directory unless
    /// it is said.
    pub files_dir: PathBuf,
    /// `files.max_size`: the largest file taken, in bytes.
    pub files_max_size: u64,
    /// `files.root.<name> = <directory> rw|ro`: the roots, one line each, which clients reach
    /// as `roots/<name>/` ([`orion_file_roots`]). None unless the image names them.
    pub roots: Vec<orion_file_roots::Root>,
    /// `files.receivers`: the prefixes that belong to programs that receive files. A file for
    /// one of them is never stored here: with no receiver there yet, it is refused as
    /// unavailable. None unless the image names them.
    pub files_receivers: Vec<String>,
    /// `comm.outbox_bytes`, the comm core's, and never above its default: how much may wait
    /// to be sent to a program that receives files, which node logic keeps chunks within.
    /// The smaller of the two, so that a setting changed while only one of the comm core and
    /// node logic restarted does not have node logic send more than the comm core holds.
    pub outbox_bytes: u32,
    /// `workloads.stop_grace_ms`: how long a run that was asked to stop is given before
    /// it is told to end.
    pub stop_grace: Duration,
    /// `workloads.placement_grace_ms`: how long a node that would take over a workload whose
    /// node can no longer run it waits for that node first.
    pub placement_grace: Duration,
    /// `actions.max_detached_ms`: the longest deadline a detached run may have. A detached
    /// run must have one, so that it ends without its caller.
    pub max_detached_ms: u64,
    /// `actions.confirm_ms`: how long a program has to say how a run ended once node logic
    /// told it to stop (its deadline passed, or node logic is stopping). With no word by
    /// then the run ends `Indeterminate`. Also how long node logic's stop waits at most.
    pub confirm_ms: u64,
    /// `leases.quiet_ms`: how long a program on the board that holds a lease with no end must
    /// be quiet before another claimant may take it.
    pub lease_quiet_ms: u64,
    /// `audit.file`: the audit log. `audit.log` in the state directory unless it is said;
    /// `off` for none.
    pub audit: Option<AuditConfig>,
}

/// `files.receivers`: each a prefix as a receiver asks for one, one or more parts of a name
/// ending in `/`.
fn receivers(source: &mut Source) -> Result<Vec<String>, ConfigError> {
    let prefixes = source.list::<String>("files.receivers")?;
    match prefixes.iter().find(|prefix| {
        !prefix.ends_with('/') || prefix.split('/').any(|part| part == "..") || prefix.len() < 2
    }) {
        Some(bad) => Err(ConfigError {
            key: "files.receivers".into(),
            message: format!("{bad:?} is not a prefix: one or more parts of a name, ending in /"),
        }),
        None => Ok(prefixes),
    }
}

impl NodeSettings {
    /// Takes node logic's settings out of `source`.
    ///
    /// # Errors
    ///
    /// A value that doesn't parse.
    pub fn from_source(source: &mut Source) -> Result<Self, ConfigError> {
        let defaults = StoreConfig::default();
        let config = Config::from_source(source)?;
        let files_dir = source
            .optional("files.dir")?
            .unwrap_or_else(|| config.state_dir.join("files"));
        let audit_file = match source.text("audit.file") {
            Some("off") => None,
            Some(path) => Some(PathBuf::from(path)),
            None => Some(config.state_dir.join("audit.log")),
        };
        let audit = match audit_file {
            Some(path) => {
                let defaults = AuditConfig::new(&path);
                Some(AuditConfig {
                    max_bytes: source.get("audit.max_bytes", defaults.max_bytes)?,
                    keep: source.get("audit.keep", defaults.keep)?,
                    ..defaults
                })
            }
            None => None,
        };
        Ok(Self {
            config,
            audit,
            files_dir,
            files_max_size: source.get("files.max_size", 16_u64 << 30)?,
            roots: orion_file_roots::from_source(source)?,
            files_receivers: receivers(source)?,
            outbox_bytes: source
                .get("comm.outbox_bytes", orion_wire::OUTBOX_BYTES)?
                .min(orion_wire::OUTBOX_BYTES),
            stop_grace: Duration::from_millis(source.get("workloads.stop_grace_ms", 10_000_u64)?),
            placement_grace: Duration::from_millis(
                source.get("workloads.placement_grace_ms", 10_000_u64)?,
            ),
            max_detached_ms: source.get(
                "actions.max_detached_ms",
                orion_action_list::MAX_DETACHED_MS,
            )?,
            confirm_ms: source.get("actions.confirm_ms", orion_action_list::CONFIRM_MS)?,
            lease_quiet_ms: source.get("leases.quiet_ms", orion_resources::QUIET_MS)?,
            save_after: Duration::from_millis(source.get("records.save_after_ms", 500_u64)?),
            max_drift_ms: source.get("records.max_drift_ms", defaults.max_drift_ms)?,
        })
    }
}

/// Why node logic could not start.
#[derive(Debug)]
pub struct StartError {
    what: &'static str,
    error: io::Error,
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.what, self.error)
    }
}

impl std::error::Error for StartError {}

/// Node logic: what it keeps, and where it keeps it across a restart.
pub struct Node {
    settings: NodeSettings,
    logic: Rc<RefCell<Logic>>,
    /// `None` while a helper is writing with it.
    persist: Option<Persist>,
    saving: Option<tokio::task::JoinHandle<(Persist, io::Result<()>)>>,
    /// The list of what is served, being written.
    served_writing: Option<tokio::task::JoinHandle<()>>,
    /// When the oldest change not yet saved was made.
    unsaved_since: Option<Instant>,
    /// Why no file under a root or the store can be opened here, if that is so: said as the
    /// hello fact `files`.
    files_refused: Option<&'static str>,
    scratch: Vec<u8>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node({})", self.settings.config.node_name)
    }
}

impl Node {
    /// Loads what was saved in the state directory and makes node logic of it.
    ///
    /// # Errors
    ///
    /// If the state directory cannot be made or used, or the comm core's socket is at a
    /// path no socket can be at: connecting again would never help.
    pub fn load(settings: NodeSettings) -> Result<Self, StartError> {
        orion_connect::socket_path(&settings.config.socket).map_err(|error| StartError {
            what: "socket",
            error: io::Error::new(io::ErrorKind::InvalidInput, error.to_string()),
        })?;
        let mut persist =
            Persist::open(&settings.config.state_dir).map_err(|error| StartError {
                what: "state directory",
                error,
            })?;
        let loaded = persist.load();
        for problem in &loaded.problems {
            warn!("{problem}");
        }
        match loaded.source {
            Loaded::Current => info!("{} records loaded", loaded.records.len()),
            Loaded::Previous => warn!(
                "{} records loaded from the snapshot before the last",
                loaded.records.len()
            ),
            Loaded::Nothing => info!("no records were saved: starting empty"),
        }
        let config = StoreConfig {
            max_drift_ms: settings.max_drift_ms,
            ..StoreConfig::default()
        };
        let name = settings.config.node_name.clone();
        let store = Store::restored(name.clone(), config, loaded.records);
        let files = Files::open(FilesConfig {
            root: settings.files_dir.clone(),
            max_size: settings.files_max_size,
            own: None,
        })
        .map_err(|error| StartError {
            what: "files directory",
            error,
        })?;
        // The node's key and the peers it pins, which the comm core makes and keeps there.
        let keys_dir = settings.config.keys_dir.clone();
        let check = settings.config.security != Security::Off;
        let audit = match &settings.audit {
            Some(config) => Some(
                Audit::start(name.as_str(), config.clone()).map_err(|error| StartError {
                    what: "audit log",
                    error,
                })?,
            ),
            None => None,
        };
        let stop_grace = settings.stop_grace;
        // Peer sync, over a session to each node the records give an address for.
        let dialer = orion_dial::Dialer {
            me: name.clone(),
            keys_dir,
            check,
            timeout: Duration::from_secs(3),
        };
        let links = orion_peer_links::PeerLinks::new(dialer.clone(), peers::Peers::KINDS).map_err(
            |error| StartError {
                what: "peer links",
                error,
            },
        )?;
        let peers = peers::Peers::new(name.clone(), links, settings.config.state_dir.clone());
        let mut logic = Logic::new(store, name, files, audit, dialer, stop_grace, peers);
        logic.sinks = orion_files_sink::Sinks::new(settings.outbox_bytes)
            .with_receivers(settings.files_receivers.clone());
        logic.roots = orion_file_roots::Roots::new(settings.roots.clone(), settings.files_max_size);
        // Looked at once, here: where the sandbox refuses the call everything under a root or
        // the store is opened with, it is said by name, not found out one file at a time.
        let places = settings.roots.iter().map(|root| root.path.as_path());
        let files_refused = std::iter::once(settings.files_dir.as_path())
            .chain(places)
            .find_map(|dir| orion_file_store::probe(dir).err());
        if let Some(why) = files_refused {
            tracing::error!("files: {why}");
        }
        let (served, said) = served::Served::load(&settings.config.state_dir);
        info!("{said}");
        logic.served = served;
        logic
            .workloads
            .set_placement_grace(settings.placement_grace.as_millis() as u64);
        logic.actions.set_max_detached(settings.max_detached_ms);
        logic.actions.set_confirm(settings.confirm_ms);
        logic
            .records
            .resources_mut()
            .set_quiet_ms(settings.lease_quiet_ms);
        logic.records.resources_mut().bind_holders(check);
        Ok(Self {
            settings,
            logic: Rc::new(RefCell::new(logic)),
            persist: Some(persist),
            saving: None,
            served_writing: None,
            unsaved_since: None,
            files_refused,
            scratch: vec![0; 64 * 1024],
        })
    }

    /// How many records are kept, deletes included.
    #[must_use]
    pub fn records(&self) -> usize {
        self.logic.borrow().records.store().len()
    }

    /// Starts a save if there is something to save, it has waited long enough, and no save
    /// is under way.
    fn save_if_due(&mut self, now: Instant) {
        self.write_served();
        if self.logic.borrow_mut().records.take_dirty() && self.unsaved_since.is_none() {
            self.unsaved_since = Some(now);
        }
        let due = self
            .unsaved_since
            .is_some_and(|since| now.duration_since(since) >= self.settings.save_after);
        if !due || self.saving.is_some() {
            return;
        }
        let Some(mut persist) = self.persist.take() else {
            return;
        };
        let records: Vec<Record> = self.logic.borrow().records.store().all().collect();
        self.unsaved_since = None;
        self.saving = Some(tokio::task::spawn_blocking(move || {
            let result = persist.save(records.into_iter());
            (persist, result)
        }));
    }

    /// Writes the list of what is served, off this thread, if it changed. One write at a time.
    fn write_served(&mut self) {
        if self
            .served_writing
            .as_ref()
            .is_some_and(|writing| !writing.is_finished())
        {
            return;
        }
        let Some((path, text)) = self.logic.borrow_mut().served_due() else {
            return;
        };
        self.served_writing = Some(tokio::task::spawn_blocking(move || {
            if let Err(error) = served::write(&path, &text) {
                warn!("{} was not written: {error}", path.display());
            }
        }));
    }

    /// Takes back what a finished save was using.
    fn saved(&mut self, done: Result<(Persist, io::Result<()>), tokio::task::JoinError>) {
        self.saving = None;
        match done {
            Ok((persist, result)) => {
                self.persist = Some(persist);
                if let Err(error) = result {
                    warn!("the records were not saved: {error}");
                    // They are still to be saved.
                    self.unsaved_since.get_or_insert_with(Instant::now);
                }
            }
            Err(error) => warn!("the save did not finish: {error}"),
        }
    }

    /// Saves now, on this thread: at the end.
    async fn save_now(&mut self) {
        if let Some(writing) = self.served_writing.take() {
            let _ = writing.await;
        }
        let due = self.logic.borrow_mut().served_due();
        if let Some((path, text)) = due
            && let Err(error) = served::write(&path, &text)
        {
            warn!("{} was not written: {error}", path.display());
        }
        if let Some(saving) = self.saving.take() {
            let done = saving.await;
            self.saved(done);
        }
        if self.logic.borrow_mut().records.take_dirty() {
            self.unsaved_since.get_or_insert_with(Instant::now);
        }
        if self.unsaved_since.take().is_none() {
            return;
        }
        if let Some(persist) = &mut self.persist
            && let Err(error) = persist.save(self.logic.borrow().records.store().all())
        {
            warn!("the records were not saved: {error}");
        }
    }

    /// What is done between frames: deadlines, and telling the topic what runs did.
    fn turn(
        &mut self,
        client: &Client,
        progress: Option<&mut Publisher>,
    ) -> Result<(), ClientError> {
        let mut direct = Direct::new(client, &mut self.scratch);
        let events = {
            let mut logic = self.logic.borrow_mut();
            logic.clock_step();
            let now = logic.now_ms();
            logic.actions.tick(now, &mut direct);
            logic.files.poll(&mut direct);
            logic.roots.poll(&mut direct);
            logic.forwards.poll(&mut direct);
            logic.restarts(&mut direct);
            logic.fetches(&mut direct);
            logic.sync(&mut direct);
            logic.actions.take_events()
        };
        direct.result()?;
        {
            let mut logic = self.logic.borrow_mut();
            let now = logic.now_ms();
            logic.sends.tick(now, &mut send::Via::Client(client))?;
            logic.roots.gives.poll(&mut send::Via::Client(client))?;
        }
        if let Some(progress) = progress {
            logic::publish(progress, &events, self.logic.borrow().actions.running());
        }
        Ok(())
    }

    /// Node logic is stopping: every run's program is told to stop, saying so. The runs end
    /// as their programs say, or `Indeterminate` at the bound, while [`Node::serve`] goes on.
    fn stopping(&mut self, client: &Client) {
        let mut direct = Direct::new(client, &mut self.scratch);
        {
            let mut logic = self.logic.borrow_mut();
            let now = logic.now_ms();
            logic.actions.stopping(now, &mut direct);
        }
        if let Err(error) = direct.result() {
            warn!("runs were not all told node logic is stopping: {error}");
        }
    }

    /// Serves one session until it is lost, or `stop` is done (`None`).
    async fn serve<S: Future<Output = ()>>(
        &mut self,
        session: Session,
        stop: &mut Pin<&mut S>,
    ) -> Option<ClientError> {
        // Tokio waits on a duplicate of the session's descriptor. It is only ever asked
        // whether there is something to read.
        let waiter = session
            .as_fd()
            .try_clone_to_owned()
            .map(std::os::unix::net::UnixDatagram::from)
            .and_then(UnixDatagram::from_std);
        let waiter = match waiter {
            Ok(waiter) => waiter,
            Err(error) => return Some(error.into()),
        };
        // Rung when a link to another node heard something.
        let bell = self
            .logic
            .borrow()
            .peers
            .bell()
            .try_clone()
            .and_then(UnixDatagram::from_std);
        let bell = match bell {
            Ok(bell) => bell,
            Err(error) => return Some(error.into()),
        };
        let client = Client::new(session);
        client.add(self.logic.clone());
        if let Err(error) = client.send(&NodeAttach {
            kinds: Bytes(Logic::KINDS),
        }) {
            return Some(error);
        }
        // Node logic's own action, registered as any program's is: the relay then checks
        // each run against its policy.
        if let Some(register) = send::register(&self.settings.config.node_name)
            && let Err(error) = client.send(&register)
        {
            return Some(error);
        }
        // The roots, if the image named any: offered as a program offers a prefix, with their
        // actions registered as a program's are.
        let offer = self.logic.borrow().roots.offer();
        let offered = match offer {
            Some(offer) => match client.send(&offer) {
                Ok(seq) => Some(seq),
                Err(error) => return Some(error),
            },
            None => None,
        };
        if offered.is_some() {
            for register in orion_file_roots::actions::registers(&self.settings.config.node_name) {
                if let Err(error) = client.send(&register) {
                    return Some(error);
                }
            }
        }
        let mut offered = offered;
        // One handle on the session for every topic node logic declares.
        let topics = Topics::new(&client);
        let mut progress = match topics.declare(&TopicDecl {
            name: ACTIONS_TOPIC,
            fields: &logic::ACTION_FIELDS,
            history: 256,
            expiry_ms: 0,
            max_payload: 512,
            on_subscribe: Default::default(),
        }) {
            Ok(publisher) => Some(publisher),
            Err(error) => {
                warn!("action runs are not published as a topic: {error}");
                None
            }
        };
        // Where files cannot be opened, the board says so in its hello: a fact, for as long
        // as node logic is here.
        let _files_fact = self.files_refused.and_then(|why| {
            let declared = topics.declare(&TopicDecl {
                name: "hello.files",
                fields: &[orion_msg_topics::Field {
                    name: "files",
                    ty: orion_msg_topics::FieldType::Text,
                    unit: "",
                    type_name: "",
                }],
                history: 2,
                expiry_ms: 0,
                max_payload: 256,
                on_subscribe: Default::default(),
            });
            let published = declared.and_then(|mut fact| {
                fact.publish(
                    shm_ring::FieldId(0),
                    orion_types::SampleTime::now(),
                    why.as_bytes(),
                )?;
                Ok(fact)
            });
            published
                .inspect_err(|error| warn!("the hello does not say why files fail: {error}"))
                .ok()
        });
        // Set once `stop` is done: the runs' programs were told to stop, and this goes on
        // until each run has ended, which its bound caps. A comm core lost meanwhile ends it.
        let mut stopping = false;
        loop {
            if let Err(error) = client.drain() {
                return (!stopping).then_some(error);
            }
            if let Some(refused) = offered.and_then(|seq| client.refusal(seq)) {
                warn!("the roots are not given: {refused}");
                offered = None;
            }
            if let Err(error) = self.turn(&client, progress.as_mut()) {
                return (!stopping).then_some(error);
            }
            if stopping && self.logic.borrow().actions.running() == 0 {
                return None;
            }
            let now = Instant::now();
            self.save_if_due(now);
            // Never longer than the session's heartbeat needs to be looked after.
            let mut wait = Duration::from_millis(500);
            if let (Some(since), None) = (self.unsaved_since, &self.saving) {
                wait = wait.min((since + self.settings.save_after).saturating_duration_since(now));
            }
            if let Some(due) = self.logic.borrow().next_deadline() {
                wait = wait.min(due);
            }
            if self.logic.borrow().fetching() {
                // The node is fetching a file: how far it is is asked for now and then.
                wait = wait.min(Duration::from_millis(50));
            }
            if self.logic.borrow().sends.active() {
                // A file is being sent: a receiver that went quiet is begun again.
                wait = wait.min(Duration::from_millis(100));
            }
            if self.logic.borrow().forwards.active() {
                // A run is under way on another node: what its helper heard is asked for.
                wait = wait.min(Duration::from_millis(5));
            }
            if self.logic.borrow().roots.gives.active() {
                // A file is being given: a client that went quiet is let go.
                wait = wait.min(Duration::from_millis(1_000));
            }
            if self.logic.borrow().files.verifying() || self.logic.borrow().roots.busy() {
                // A helper is checking a digest: what it found is asked for soon.
                wait = wait.min(Duration::from_millis(5));
            }
            let wait = wait.max(Duration::from_millis(1));
            let readable = async {
                let either = async {
                    tokio::select! {
                        ready = waiter.ready(Interest::READABLE) => ready.map(|_| ()),
                        // What the bell says is taken in the next turn, which hushes it.
                        ready = bell.ready(Interest::READABLE) => ready.map(|_| ()),
                    }
                };
                if let Ok(Ok(())) = tokio::time::timeout(wait, either).await {
                    let _ = bell.try_io(Interest::READABLE, || {
                        Err::<(), _>(io::Error::from(io::ErrorKind::WouldBlock))
                    });
                    // Readiness is forgotten here and asked for again after the next look,
                    // so a frame that is only partly here does not spin.
                    let _ = waiter.try_io(Interest::READABLE, || {
                        Err::<(), _>(io::Error::from(io::ErrorKind::WouldBlock))
                    });
                }
            };
            let waiting = async {
                if let Some(saving) = &mut self.saving {
                    tokio::select! {
                        done = saving => self.saved(done),
                        () = readable => {}
                    }
                } else {
                    readable.await;
                }
            };
            // `stop` is waited on until it is done, and never again after.
            let stopped = if stopping {
                waiting.await;
                false
            } else {
                tokio::select! {
                    () = stop.as_mut() => true,
                    () = waiting => false,
                }
            };
            if stopped {
                self.stopping(&client);
                stopping = true;
            }
        }
    }

    /// Runs node logic until `stop` is done: connects to the comm core, answers, and connects
    /// again whenever the comm core goes away. What changed is saved before it returns.
    pub async fn run(&mut self, stop: impl Future<Output = ()>) {
        tokio::pin!(stop);
        let address = Address::Local(self.settings.config.socket.clone());
        let mut pause = Duration::from_millis(20);
        let mut said = false;
        'run: loop {
            let options = Options {
                roles: Roles::NODE.and(Roles::CLIENT).and(Roles::PROVIDER),
                ..Options::default()
            };
            // One try at a time: this loop paces the tries itself, and stays ready to stop
            // and to save between them.
            match Session::connect_once(&address, "node", options) {
                Ok(session) => {
                    info!("connected to the comm core as node logic");
                    pause = Duration::from_millis(20);
                    said = false;
                    // Whatever was watching, or had an action, came through a comm core that
                    // is gone, or is passed again by the one that stayed.
                    self.logic.borrow_mut().forget_sessions();
                    let Some(lost) = self.serve(session, &mut stop).await else {
                        break 'run;
                    };
                    warn!("the comm core is gone: {lost}");
                }
                Err(error) => {
                    if !std::mem::replace(&mut said, true) {
                        warn!(
                            "no comm core at {}: {error}",
                            self.settings.config.socket.display()
                        );
                    }
                }
            }
            self.save_if_due(Instant::now());
            tokio::select! {
                () = &mut stop => break 'run,
                () = tokio::time::sleep(pause) => {}
            }
            pause = (pause * 2).min(Duration::from_secs(1));
        }
        self.save_now().await;
    }
}

/// Node logic as a program: runs until `stop` is done.
///
/// # Errors
///
/// If the state directory cannot be used.
pub async fn run_until(
    settings: NodeSettings,
    stop: impl Future<Output = ()>,
) -> Result<(), StartError> {
    let mut node = Node::load(settings)?;
    node.run(stop).await;
    Ok(())
}

/// Node logic as a program: runs until `SIGTERM` or `SIGINT`. Settings nobody read are
/// logged, so a typo is seen.
///
/// # Errors
///
/// If the runtime could not start or the state directory cannot be used.
pub fn run(settings: NodeSettings, unknown: &[orion_config::Warning]) -> Result<(), StartError> {
    use tokio::signal::unix::{SignalKind, signal};
    for warning in unknown {
        tracing::error!("{warning}: it is not read, and changes nothing");
    }
    let view = &settings.config.security_view;
    if view.fault.is_some() {
        warn!("{view}");
    } else {
        info!("{view}");
    }
    let failed = |what| move |error| StartError { what, error };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .map_err(failed("runtime"))?;
    runtime.block_on(async {
        let mut terminate = signal(SignalKind::terminate()).map_err(failed("signals"))?;
        let mut interrupt = signal(SignalKind::interrupt()).map_err(failed("signals"))?;
        let stop = async {
            tokio::select! {
                _ = terminate.recv() => info!("asked to stop"),
                _ = interrupt.recv() => info!("interrupted"),
            }
        };
        run_until(settings, stop).await
    })
}
