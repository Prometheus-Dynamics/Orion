//! The Orion client.
//!
//! One crate and one handle. [`Orion::connect`] reaches a comm core on this board, across
//! the network or over a link, and the calls after it are the same on all three.
//!
//! ```no_run
//! use orion::{Address, Orion, fields};
//! use orion::topics::{FieldId, SampleTime, TopicDecl};
//!
//! let orion = Orion::connect(&Address::default(), "thermometer")?;
//! let mut reading = orion.declare(&TopicDecl {
//!     name: "cabin.temperature",
//!     fields: &fields![celsius: Float "C"],
//!     history: 64,
//!     expiry_ms: 0,
//!     max_payload: 8,
//! })?;
//! reading.publish(FieldId(0), SampleTime::from_nanos(1), &21.5_f64.to_le_bytes())?;
//! # Ok::<(), orion::Error>(())
//! ```
//!
//! What a program can do is the six things Orion is made of, each behind a cargo feature so
//! a program carries only what it uses:
//!
//! | Feature | What it gives |
//! | --- | --- |
//! | `topics` | publish and subscribe: [`Orion::declare`], [`Orion::subscribe`] |
//! | `buffers` | frames in shared memory: [`Orion::declare_buffers`], [`Orion::subscribe_buffers`] |
//! | `records` | named state: [`Orion::records`] |
//! | `actions` | do one thing once: [`Orion::run`], [`Orion::register_action`] |
//! | `files` | push a file, or ask the board to pull it: [`Orion::files`] |
//! | `shell` | a shell on the board, over the session: [`Orion::shell`] |
//! | `workloads` | say what should run: [`Orion::workloads`] |
//! | `executor` | run workloads for Orion: [`Orion::executor`] |
//! | `provider` | speak for a resource: [`Orion::provider`], [`Orion::resources`] |
//! | `tokio` | wait inside a runtime |
//!
//! `topics`, `records` and `actions` are on by default; `full` is all of them.
//!
//! **When the connection is lost**, a call fails with an error for which [`Error::is_lost`]
//! is true. [`Orion::reconnect`] connects again and says everything again: topics are
//! declared with their rings handed back, subscriptions and watches are asked for from where
//! they were, actions are registered. On the board, what was flowing through a ring never
//! stopped.

#![cfg(target_os = "linux")]

use std::fmt;
use std::time::Duration;

use orion_connect::{Client, Session};

pub use orion_connect::{Address, ClientError, Identity, Options, Roles, Welcomed};
pub use orion_types as types;

/// Publish and subscribe.
#[cfg(feature = "topics")]
pub mod topics {
    pub use orion_msg_topics::{Field, FieldType};
    pub use orion_topics::{Holding, Publisher, Status, Subscriber, TopicDecl, TopicError, Topics};
    pub use shm_ring::{FieldId, Flags, Next, Ring, Sample, SampleTime, Spec};
}

/// Frames in shared buffers.
#[cfg(feature = "buffers")]
pub mod buffers {
    pub use orion_buffers::{
        AdoptedBuffers, Batch, BufferDesc, BufferError, BufferIndex, BufferPublisher,
        BufferSubscriber, Buffers, Dropped, Held, HoldLimits, HoldTable, NextBatch, Pool, PoolView,
    };
}

/// Named pieces of state.
#[cfg(feature = "records")]
pub mod records {
    pub use orion_records::{Bytes, Record, RecordClass, RecordEntry, Records, Watch};
}

/// Do one thing once, through the check.
#[cfg(feature = "actions")]
pub mod actions {
    pub use orion_actions::{
        ActionCall, ActionDecl, Actions, Admits, Doing, Finished, Outcome, Policy, Registration,
        Request, Run, Target, Update,
    };
}

/// Files, pushed or pulled.
#[cfg(feature = "files")]
pub mod files {
    pub use orion_files::{FileState, Files, Handed, Pull, Sink, Transfer, digest};
}

/// Running workloads for Orion.
#[cfg(feature = "executor")]
pub mod executor {
    pub use orion_executor::{
        ExecError, Executor, Serving, WorkloadKind, WorkloadName, WorkloadState,
    };
}

/// Speaking for a resource.
#[cfg(feature = "provider")]
pub mod provider {
    pub use orion_provider::{
        Describing, Found, Provider, Resource, ResourceState, describe, resources,
    };
}

/// Who a board is, and what time it has.
pub mod node;
#[cfg(feature = "shell")]
pub mod shell;

/// Saying what should run, and hearing where it is.
#[cfg(feature = "workloads")]
pub mod workloads;

/// What a call on the client failed with.
#[derive(Debug)]
pub enum Error {
    /// The session: the connection broke, or the comm core said no.
    Client(ClientError),
    /// A topic: a sample that does not fit, a ring that cannot be mapped, a name that is not
    /// one.
    #[cfg(feature = "topics")]
    Topic(orion_topics::TopicError),
}

impl Error {
    /// Whether connecting again may help: the connection broke or the comm core went quiet,
    /// as opposed to the comm core saying no.
    #[must_use]
    pub const fn is_lost(&self) -> bool {
        match self {
            Self::Client(error) => error.is_lost(),
            #[cfg(feature = "topics")]
            Self::Topic(orion_topics::TopicError::Client(error)) => error.is_lost(),
            #[cfg(feature = "topics")]
            Self::Topic(_) => false,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => write!(f, "{error}"),
            #[cfg(feature = "topics")]
            Self::Topic(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<ClientError> for Error {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

#[cfg(feature = "topics")]
impl From<orion_topics::TopicError> for Error {
    fn from(error: orion_topics::TopicError) -> Self {
        match error {
            orion_topics::TopicError::Client(error) => Self::Client(error),
            other => Self::Topic(other),
        }
    }
}

/// A topic's fields, written as `name: Type "unit"`. The unit may be left out.
///
/// ```
/// use orion::fields;
/// let fields = fields![rpm: Float "rpm", spinning: Bool];
/// assert_eq!((fields[0].name, fields[0].unit, fields[1].name), ("rpm", "rpm", "spinning"));
/// ```
#[cfg(feature = "topics")]
#[macro_export]
macro_rules! fields {
    ($($name:ident : $ty:ident $($unit:literal)?),* $(,)?) => {
        [$($crate::topics::Field {
            name: stringify!($name),
            ty: $crate::topics::FieldType::$ty,
            unit: $crate::fields!(@unit $($unit)?),
            type_name: "",
        }),*]
    };
    (@unit $unit:literal) => { $unit };
    (@unit) => { "" };
}

/// A program's connection to Orion.
#[derive(Clone)]
pub struct Orion {
    client: Client,
    node: std::rc::Rc<std::cell::RefCell<node::State>>,
    #[cfg(feature = "topics")]
    topics: orion_topics::Topics,
    #[cfg(feature = "buffers")]
    buffers: orion_buffers::Buffers,
    #[cfg(feature = "records")]
    records: orion_records::Records,
    #[cfg(feature = "actions")]
    actions: orion_actions::Actions,
    #[cfg(feature = "files")]
    files: orion_files::Files,
    #[cfg(feature = "shell")]
    shells: std::rc::Rc<std::cell::RefCell<shell::State>>,
    #[cfg(feature = "workloads")]
    workloads: workloads::Workloads,
}

impl fmt::Debug for Orion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Orion({:?})", self.client)
    }
}

impl Orion {
    /// Connects to the comm core at `address` as the program `name`.
    ///
    /// # Errors
    ///
    /// If nothing answers there, or the comm core turns the session away.
    pub fn connect(address: &Address, name: &str) -> Result<Self, Error> {
        Self::connect_with(address, name, Options::default())
    }

    /// As [`Orion::connect`], saying which roles to ask for, and, across a network, with
    /// which key.
    ///
    /// # Errors
    ///
    /// As [`Orion::connect`].
    pub fn connect_with(address: &Address, name: &str, options: Options) -> Result<Self, Error> {
        Ok(Self::over(Session::connect(address, name, options)?))
    }

    /// Over a session that is already made.
    #[must_use]
    pub fn over(session: Session) -> Self {
        let client = Client::new(session);
        let node = std::rc::Rc::new(std::cell::RefCell::new(node::State::default()));
        client.add(node.clone());
        #[cfg(feature = "topics")]
        let topics = orion_topics::Topics::new(&client);
        Self {
            #[cfg(feature = "buffers")]
            buffers: orion_buffers::Buffers::new(&client, &topics),
            #[cfg(feature = "topics")]
            topics,
            #[cfg(feature = "records")]
            records: orion_records::Records::new(&client),
            #[cfg(feature = "actions")]
            actions: orion_actions::Actions::new(&client),
            #[cfg(feature = "files")]
            files: orion_files::Files::new(&client),
            #[cfg(feature = "shell")]
            shells: {
                let shells = std::rc::Rc::new(std::cell::RefCell::new(shell::State::default()));
                client.add(shells.clone());
                shells
            },
            #[cfg(feature = "workloads")]
            workloads: workloads::Workloads::new(&client),
            client,
            node,
        }
    }

    /// What the comm core said at hello: its name, the heartbeat, the largest frame.
    #[must_use]
    pub fn welcomed(&self) -> Welcomed {
        self.client.session(|session| session.welcomed().clone())
    }

    /// Who the board is: its name, its cluster and its key, with the facts programs on it
    /// have published about it. The answer is signed by the key it names and made for this
    /// question, and is checked here. Whether that key is the board the caller meant is the
    /// caller's to decide: compare the fingerprint.
    ///
    /// # Errors
    ///
    /// If the answer does not check, or the connection broke.
    pub fn node(&self) -> Result<node::Node, Error> {
        Ok(node::node(&self.client, &self.node)?)
    }

    /// How the board's clock compares with this program's.
    ///
    /// # Errors
    ///
    /// If the board does not answer, or the connection broke.
    pub fn clock(&self) -> Result<node::Clock, Error> {
        Ok(node::clock(&self.client, &self.node)?)
    }

    /// Where the board's sync with each other board is.
    ///
    /// # Errors
    ///
    /// Refused without read access to records; as [`Orion::clock`].
    pub fn sync_peers(&self) -> Result<Vec<node::SyncPeer>, Error> {
        Ok(node::peers(&self.client, &self.node, None)?)
    }

    /// Lets the board sync again with `peer`, which was away longer than deletes are
    /// remembered. What was deleted while it was away may come back. The peer has to forgive
    /// this board too. Answers where sync is with each peer after.
    ///
    /// # Errors
    ///
    /// Refused without write access to records, `not found` if `peer` is not one; as
    /// [`Orion::clock`].
    pub fn forgive(&self, peer: &str) -> Result<Vec<node::SyncPeer>, Error> {
        Ok(node::peers(&self.client, &self.node, Some(peer))?)
    }

    /// Asks the board to enrol `public_key` as an operator called `name`.
    ///
    /// Who may ask: an administrator on the board (root, on the board's own socket), for any
    /// key, which is enrolled at once; or the operator itself, on a session signed with that
    /// key (`Options::identity`), which is enrolled at once if it gives `enrollment_key`, the
    /// cluster's enrollment key, and is `Pending` until someone on the board approves it if not.
    /// A key that was revoked stays `Revoked`. Asking again is harmless: the answer says where
    /// the operator is.
    ///
    /// # Errors
    ///
    /// Refused `denied` for anyone else; as [`Orion::clock`].
    pub fn enroll(
        &self,
        public_key: [u8; 32],
        name: &str,
        enrollment_key: Option<&[u8]>,
    ) -> Result<node::Enrollment, Error> {
        let at = self.welcomed().node.to_string();
        Ok(node::enroll(
            &self.client,
            &self.node,
            &at,
            public_key,
            name,
            enrollment_key,
        )?)
    }

    /// The session itself, for what this handle does not cover.
    #[must_use]
    pub const fn client(&self) -> &Client {
        &self.client
    }

    /// Keeps the session and takes in what the comm core sends, waiting up to `wait`. A
    /// program that is otherwise idle calls this in its loop. `true` if something came.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn pump(&self, wait: Duration) -> Result<bool, Error> {
        Ok(self.client.pump(wait)?)
    }

    /// Connects again to the same address, for as long as `keep_trying` says, and then says
    /// everything again: topics are declared with their rings handed back, subscriptions and
    /// watches are asked for from where they were, and actions are registered.
    ///
    /// # Errors
    ///
    /// If `keep_trying` said to stop, or the comm core now refuses what it took before.
    pub fn reconnect(
        &self,
        keep_trying: impl FnMut(&orion_connect::Status) -> bool,
    ) -> Result<(), Error> {
        self.client
            .session(|session| session.reconnect(keep_trying))?;
        #[cfg(feature = "topics")]
        self.topics.restore()?;
        #[cfg(feature = "records")]
        self.records.restore()?;
        #[cfg(feature = "actions")]
        self.actions.restore()?;
        Ok(())
    }

    /// Declares a topic and returns its publisher.
    ///
    /// # Errors
    ///
    /// If the comm core refuses: another program publishes it, or there is no room.
    #[cfg(feature = "topics")]
    pub fn declare(&self, topic: &topics::TopicDecl<'_>) -> Result<topics::Publisher, Error> {
        Ok(self.topics.declare(topic)?)
    }

    /// Makes a ring the program made and writes itself a topic, so that Orion's subscribers
    /// read the same ring: see [`Topics::adopt`](topics::Topics::adopt).
    ///
    /// # Errors
    ///
    /// If this session is not on the board, or the comm core refuses: a ring of another shape
    /// than `topic` is `malformed`.
    #[cfg(feature = "topics")]
    pub fn adopt(
        &self,
        topic: &topics::TopicDecl<'_>,
        ring: &topics::Ring,
    ) -> Result<topics::Publisher, Error> {
        Ok(self.topics.adopt(topic, ring)?)
    }

    /// Subscribes to a topic. One nobody has declared yet is waited for.
    ///
    /// # Errors
    ///
    /// If the comm core refuses.
    #[cfg(feature = "topics")]
    pub fn subscribe(&self, topic: &str) -> Result<topics::Subscriber, Error> {
        Ok(self.topics.subscribe(topic, &[])?)
    }

    /// As [`Orion::subscribe`], with a request that goes to the publisher unread: what this
    /// subscriber would like of it.
    ///
    /// # Errors
    ///
    /// If the comm core refuses.
    #[cfg(feature = "topics")]
    pub fn subscribe_asking(
        &self,
        topic: &str,
        request: &[u8],
    ) -> Result<topics::Subscriber, Error> {
        Ok(self.topics.subscribe(topic, request)?)
    }

    /// Declares a topic of frames over `pool` and returns its publisher.
    ///
    /// # Errors
    ///
    /// If this session is not on the board, or the comm core refuses.
    #[cfg(feature = "buffers")]
    pub fn declare_buffers(
        &self,
        name: &str,
        history: u32,
        max_meta: u32,
        pool: buffers::Pool,
    ) -> Result<buffers::BufferPublisher, Error> {
        Ok(self.buffers.declare(name, history, max_meta, pool)?)
    }

    /// Makes a ring and pool the program made and writes itself a topic of frames: see
    /// [`Buffers::adopt`](buffers::Buffers::adopt).
    ///
    /// # Errors
    ///
    /// If this session is not on the board, or the comm core refuses.
    #[cfg(feature = "buffers")]
    pub fn adopt_buffers(
        &self,
        name: &str,
        history: u32,
        max_meta: u32,
        ring: &topics::Ring,
        pool: &buffers::Pool,
        reader_slots: u64,
    ) -> Result<buffers::AdoptedBuffers, Error> {
        Ok(self
            .buffers
            .adopt(name, history, max_meta, ring, pool, reader_slots)?)
    }

    /// Subscribes to a topic of frames.
    ///
    /// # Errors
    ///
    /// If this session is not on the board, or the comm core refuses.
    #[cfg(feature = "buffers")]
    pub fn subscribe_buffers(
        &self,
        topic: &str,
        request: &[u8],
        limits: buffers::HoldLimits,
    ) -> Result<buffers::BufferSubscriber, Error> {
        Ok(self.buffers.subscribe(topic, request, limits)?)
    }

    /// The records: put, get and watch.
    #[cfg(feature = "records")]
    #[must_use]
    pub const fn records(&self) -> &records::Records {
        &self.records
    }

    /// Starts an action and returns at once with the run.
    ///
    /// # Errors
    ///
    /// If the connection broke. A refusal is found on the run's first read.
    #[cfg(feature = "actions")]
    pub fn run(&self, action: &actions::ActionCall<'_>) -> Result<actions::Run, Error> {
        Ok(self.actions.run(action)?)
    }

    /// Comes back to a run that was started detached.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    #[cfg(feature = "actions")]
    pub fn attach(&self, call: u64) -> Result<actions::Run, Error> {
        Ok(self.actions.attach(call)?)
    }

    /// Adds an action of this program's to the list, with who may run it.
    ///
    /// # Errors
    ///
    /// If the comm core refuses: the name is taken, or this program may not add actions.
    #[cfg(feature = "actions")]
    pub fn register_action(
        &self,
        action: &actions::ActionDecl<'_>,
        policy: actions::Policy,
    ) -> Result<actions::Registration, Error> {
        Ok(self.actions.register(action, policy)?)
    }

    /// Opens a shell on the board as `account`: its login shell, or `command` for that shell
    /// to run, on `terminal` if one is given. With security on, a program on the board gets
    /// its own account (root any), and an operator one it was granted (`login`, or
    /// `shell:ACCOUNT`).
    ///
    /// # Errors
    ///
    /// Refused (`denied` for an account the caller may not be), the board has no shell
    /// service, or the session broke.
    #[cfg(feature = "shell")]
    pub fn shell(
        &self,
        account: &str,
        command: Option<&[u8]>,
        terminal: Option<&shell::Terminal>,
    ) -> Result<shell::Shell, Error> {
        Ok(shell::open(
            &self.client,
            &self.shells,
            account,
            command,
            terminal,
        )?)
    }

    /// The files: push and pull.
    #[cfg(feature = "files")]
    #[must_use]
    pub const fn files(&self) -> &files::Files {
        &self.files
    }

    /// The workloads: say what should run, and hear where it is.
    #[cfg(feature = "workloads")]
    #[must_use]
    pub const fn workloads(&self) -> &workloads::Workloads {
        &self.workloads
    }

    /// An executor on this session. Ask for the role `EXECUTOR` when connecting.
    #[cfg(feature = "executor")]
    #[must_use]
    pub fn executor(&self) -> executor::Serving {
        orion_executor::Serving::new(&self.client)
    }

    /// A provider on this session: describe resources with their topics and actions.
    #[cfg(feature = "provider")]
    #[must_use]
    pub fn provider(&self) -> provider::Provider {
        orion_provider::Provider::new(&self.client, &self.records, &self.topics, &self.actions)
    }

    /// Every resource whose name starts with `prefix`, with what each offers.
    ///
    /// # Errors
    ///
    /// If the store refuses, or the connection broke.
    #[cfg(feature = "provider")]
    pub fn resources(&self, prefix: &str) -> Result<Vec<provider::Found>, Error> {
        Ok(orion_provider::resources(&self.records, prefix)?)
    }
}
