//! Node logic, assembled.
//!
//! Node logic is the comm core's first client. It connects to the board's socket like any
//! program, with the node role, and tells the comm core which kinds it answers. The comm core
//! checks each such request at the door and passes it on; node logic answers through the comm
//! core, which sends the answer to the session that asked. There is no other protocol between
//! the two.
//!
//! So when node logic dies, the comm core and everything on it stay up: links, topics, the
//! hello document. Requests for records are refused with "unavailable" until node logic is
//! back. It then loads what it had saved, connects, and the watches programs had are passed
//! to it again.
//!
//! One thread. The store is plain data owned by it. The one slow thing, writing the snapshot,
//! is done by a helper thread that exists only while it writes.

#![cfg(target_os = "linux")]

pub mod records;

use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use orion_config::{Config, ConfigError, Source};
use orion_connect::{Address, ClientError, Options, Roles, Session};
use orion_msg_session::{NodeAnswer, NodeAttach, NodeGone, NodeRequest, Refusal};
use orion_persist::{Persist, Source as Loaded};
use orion_store::{Record, Store, StoreConfig};
use orion_types::Error;
use orion_wire::body::encode_fields;
use orion_wire::{Body, Bytes, Decode, kind};
use tracing::{info, warn};

pub use records::Records;

/// What node logic is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSettings {
    /// What every build needs: the node's name, the socket, the state directory.
    pub config: Config,
    /// `records.save_after`: how long after a change the records are saved. Changes made in
    /// that time go in the same save.
    pub save_after: Duration,
    /// `records.max_drift_ms`: a stamp from a peer further ahead of this node's clock than
    /// this is refused.
    pub max_drift_ms: u64,
}

impl NodeSettings {
    /// Takes node logic's settings out of `source`.
    ///
    /// # Errors
    ///
    /// A value that doesn't parse.
    pub fn from_source(source: &mut Source) -> Result<Self, ConfigError> {
        let defaults = StoreConfig::default();
        Ok(Self {
            config: Config::from_source(source)?,
            save_after: Duration::from_millis(source.get("records.save_after_ms", 500_u64)?),
            max_drift_ms: source.get("records.max_drift_ms", defaults.max_drift_ms)?,
        })
    }
}

/// One request, as the comm core passed it on.
#[derive(Clone, Copy, Debug)]
pub struct Asked<'a> {
    /// The session that asked.
    pub session: u32,
    /// The `seq` of its request.
    pub seq: u16,
    /// The kind of its request.
    pub kind: u8,
    payload: &'a [u8],
}

impl<'a> Asked<'a> {
    /// The request's body, as a `B`.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] if it does not decode.
    pub fn body<B: Body + Decode<'a>>(&self) -> Result<B, Error> {
        B::decode(self.payload)
    }
}

/// Where answers go: to a session, through the comm core.
pub trait Out {
    /// Sends a frame of `kind` with `payload` to `session`.
    ///
    /// # Errors
    ///
    /// If the comm core could not be sent to.
    fn frame(
        &mut self,
        session: u32,
        kind: u8,
        payload: &[u8],
        may_drop: bool,
    ) -> Result<(), ClientError>;

    /// Room to encode a body in.
    fn scratch(&mut self) -> &mut Vec<u8>;
}

impl dyn Out + '_ {
    /// Sends `body` to `session`.
    ///
    /// # Errors
    ///
    /// If the body does not fit a frame, or the comm core could not be sent to.
    pub fn answer<B: Body>(&mut self, session: u32, body: &B) -> Result<(), ClientError> {
        let mut scratch = std::mem::take(self.scratch());
        let result = encode_fields(body, &mut scratch)
            .map_err(|_| ClientError::TooLarge)
            .and_then(|len| self.frame(session, B::KIND, &scratch[..len], false));
        *self.scratch() = scratch;
        result
    }
}

/// Answers through a session with the comm core.
struct Through<'a> {
    session: &'a mut Session,
    scratch: &'a mut Vec<u8>,
}

impl Out for Through<'_> {
    fn frame(
        &mut self,
        session: u32,
        kind: u8,
        payload: &[u8],
        may_drop: bool,
    ) -> Result<(), ClientError> {
        self.session
            .send(&NodeAnswer {
                session,
                kind,
                may_drop,
                payload: Bytes(payload),
            })
            .map(|_| ())
    }

    fn scratch(&mut self) -> &mut Vec<u8> {
        self.scratch
    }
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64)
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
#[derive(Debug)]
pub struct Node {
    settings: NodeSettings,
    records: Records,
    /// `None` while a helper is writing with it.
    persist: Option<Persist>,
    saving: Option<tokio::task::JoinHandle<(Persist, io::Result<()>)>>,
    /// When the oldest change not yet saved was made.
    unsaved_since: Option<Instant>,
    scratch: Vec<u8>,
}

impl Node {
    /// Loads what was saved in the state directory and makes node logic of it.
    ///
    /// # Errors
    ///
    /// If the state directory cannot be made or used.
    pub fn load(settings: NodeSettings) -> Result<Self, StartError> {
        let dir: PathBuf = settings.config.state_dir.clone();
        let mut persist = Persist::open(&dir).map_err(|error| StartError {
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
        let store = Store::restored(settings.config.node_name.clone(), config, loaded.records);
        Ok(Self {
            settings,
            records: Records::new(store),
            persist: Some(persist),
            saving: None,
            unsaved_since: None,
            scratch: vec![0; 64 * 1024],
        })
    }

    /// The record service.
    #[must_use]
    pub const fn records(&self) -> &Records {
        &self.records
    }

    /// The kinds node logic answers.
    #[must_use]
    pub const fn kinds() -> &'static [u8] {
        Records::KINDS
    }

    /// Answers one request. Whatever is wrong with the request is the caller's to hear, as a
    /// refusal.
    ///
    /// # Errors
    ///
    /// If the comm core could not be sent to.
    pub fn request(&mut self, asked: &Asked<'_>, out: &mut dyn Out) -> Result<(), ClientError> {
        let mut sent = Ok(());
        let answered = self.records.request(asked, wall_ms(), out, &mut sent);
        sent?;
        if let Err(error) = answered {
            let detail = error.to_string();
            out.answer(
                asked.session,
                &Refusal {
                    request_seq: asked.seq,
                    code: error.code(),
                    detail: &detail,
                },
            )?;
        }
        if self.records.take_dirty() && self.unsaved_since.is_none() {
            self.unsaved_since = Some(Instant::now());
        }
        Ok(())
    }

    /// Takes one frame from the comm core. `payload` is a copy: the session is sent on while
    /// it is read.
    fn frame(
        &mut self,
        kind: u8,
        payload: &[u8],
        session: &mut Session,
    ) -> Result<(), ClientError> {
        const UNREADABLE: ClientError = ClientError::Protocol("a body that does not decode");
        match kind {
            kind::NODE_REQUEST => {
                let passed = NodeRequest::decode(payload).map_err(|_| UNREADABLE)?;
                let asked = Asked {
                    session: passed.session,
                    seq: passed.seq,
                    kind: passed.kind,
                    payload: passed.payload.0,
                };
                let mut scratch = std::mem::take(&mut self.scratch);
                let result = self.request(
                    &asked,
                    &mut Through {
                        session,
                        scratch: &mut scratch,
                    },
                );
                self.scratch = scratch;
                result
            }
            kind::NODE_GONE => {
                let gone = NodeGone::decode(payload).map_err(|_| UNREADABLE)?;
                self.records.gone(gone.session);
                Ok(())
            }
            kind::REFUSAL => {
                let refusal = Refusal::decode(payload).map_err(|_| UNREADABLE)?;
                warn!(
                    "the comm core refused node logic: {} ({})",
                    refusal.detail, refusal.code
                );
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Starts a save if there is something to save, it has waited long enough, and no save
    /// is under way.
    fn save_if_due(&mut self, now: Instant) {
        let due = self
            .unsaved_since
            .is_some_and(|since| now.duration_since(since) >= self.settings.save_after);
        if !due || self.saving.is_some() {
            return;
        }
        let Some(mut persist) = self.persist.take() else {
            return;
        };
        let records: Vec<Record> = self.records.store().all().collect();
        self.unsaved_since = None;
        self.saving = Some(tokio::task::spawn_blocking(move || {
            let result = persist.save(records.into_iter());
            (persist, result)
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
        if let Some(saving) = self.saving.take() {
            let done = saving.await;
            self.saved(done);
        }
        if self.unsaved_since.take().is_none() {
            return;
        }
        if let Some(persist) = &mut self.persist
            && let Err(error) = persist.save(self.records.store().all())
        {
            warn!("the records were not saved: {error}");
        }
    }

    /// Serves one session until it is lost.
    async fn serve(&mut self, session: &mut Session) -> ClientError {
        if let Err(error) = session.send(&NodeAttach {
            kinds: Bytes(Self::kinds()),
        }) {
            return error;
        }
        // A frame is copied out of the session's buffer, because answering it sends on the
        // session. The copy's room is kept.
        let mut held = Vec::new();
        let copy = |frame: Result<Option<orion_connect::Incoming<'_>>, ClientError>,
                    held: &mut Vec<u8>| {
            frame.map(|frame| {
                frame.map(|frame| {
                    held.clear();
                    held.extend_from_slice(frame.payload());
                    frame.kind()
                })
            })
        };
        loop {
            self.save_if_due(Instant::now());
            let wait = match (self.unsaved_since, &self.saving) {
                (Some(since), None) => (since + self.settings.save_after)
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_millis(1)),
                _ => Duration::from_secs(1),
            };
            let taken = if let Some(saving) = &mut self.saving {
                tokio::select! {
                    done = saving => {
                        self.saved(done);
                        continue;
                    }
                    frame = session.next_async(wait) => copy(frame, &mut held),
                }
            } else {
                copy(session.next_async(wait).await, &mut held)
            };
            match taken {
                Ok(Some(kind)) => {
                    if let Err(error) = self.frame(kind, &held, session) {
                        return error;
                    }
                }
                Ok(None) => {}
                Err(error) => return error,
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
                roles: Roles::NODE.and(Roles::CLIENT),
                ..Options::default()
            };
            match Session::connect(&address, "node", options) {
                Ok(mut session) => {
                    info!("connected to the comm core as node logic");
                    pause = Duration::from_millis(20);
                    said = false;
                    // Whatever was watching came through a comm core that is gone, or is
                    // passed again by the one that stayed.
                    self.records.forget_sessions();
                    let lost = tokio::select! {
                        () = &mut stop => break 'run,
                        lost = self.serve(&mut session) => lost,
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
        warn!("{warning}");
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
