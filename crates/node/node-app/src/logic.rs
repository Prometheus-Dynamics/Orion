//! What node logic keeps, and how a frame from the comm core reaches the part it is for.

use std::fmt::Write as _;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use orion_action_forward::Forwards;
use orion_action_list::{Actions, Event};
use orion_audit::{Audit, Entry, Who};
use orion_connect::{Client, ClientError, Cx, Handler, Incoming};
use orion_dial::Dialer;
use orion_executors::Workloads;
use orion_file_store::Files;
use orion_files_sink::Sinks;
use orion_msg_actions::{Cancel, Claim, Register, Run, Target};
use orion_msg_files::{FileBegin, FileCommit, FileStatus};
use orion_msg_peers::PeerForgive;
use orion_msg_records::{NodeRecord, Put, RecordClass, ResourceRecord};
use orion_msg_session::{NodeAnswer, NodeGone, NodeRequest};
use orion_msg_topics::{Field, FieldType};
use orion_msg_workloads::{WorkloadPut, WorkloadRemove};
use orion_reply::{Asked, Out};
use orion_ring::FieldId;
use orion_store::Store;
use orion_topics::Publisher;
use orion_types::{Error, NodeName, SampleTime};
use orion_wire::{Bytes, Decode, kind};

use crate::peers::Peers;
use crate::records::Records;
use crate::send::{Sends, Via};

/// The fields of the topic of action runs.
///
/// - `event`: one line for each thing that happens to a run:
///   `<run> <action> <node|resource>:<target> <percent> <outcome or -> <message>`.
/// - `running`: how many runs are under way.
pub(crate) const ACTION_FIELDS: [Field<'static>; 2] = [
    Field {
        name: "event",
        ty: FieldType::Text,
        unit: "",
        type_name: "",
    },
    Field {
        name: "running",
        ty: FieldType::Int,
        unit: "",
        type_name: "",
    },
];

fn wall() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

/// Says on the topic what happened to runs.
pub(crate) fn publish(topic: &mut Publisher, events: &[Event], running: usize) {
    if events.is_empty() {
        return;
    }
    let mut line = String::new();
    for event in events {
        line.clear();
        let (sort, target) = match &event.target {
            Target::Node(node) => ("node", node.as_str()),
            Target::Resource(resource) => ("resource", resource.as_str()),
        };
        let _ = write!(
            line,
            "{} {} {sort}:{target} {} ",
            event.run, event.name, event.percent
        );
        match event.outcome {
            Some(outcome) => {
                let _ = write!(line, "{outcome:?} ");
            }
            None => line.push_str("- "),
        }
        // Whatever does not fit is cut, at a character.
        let room = 500_usize.saturating_sub(line.len());
        let mut end = event.message.len().min(room);
        while !event.message.is_char_boundary(end) {
            end -= 1;
        }
        line.push_str(&event.message[..end]);
        let _ = topic.publish(FieldId(0), SampleTime::now(), line.as_bytes());
    }
    let _ = topic.publish(
        FieldId(1),
        SampleTime::now(),
        &(running as i64).to_le_bytes(),
    );
}

/// The files a workload may need, as the node's store has them.
struct OnBoard<'a>(&'a Files);

impl orion_executors::Board for OnBoard<'_> {
    fn has(&self, name: &orion_types::FileName, digest: &[u8; 32]) -> bool {
        self.0.has(name, digest)
    }
}

/// Everything node logic keeps.
pub(crate) struct Logic {
    pub(crate) records: Records,
    pub(crate) actions: Actions,
    pub(crate) files: Files,
    pub(crate) sinks: Sinks,
    /// The runs of `files.send`.
    pub(crate) sends: Sends,
    pub(crate) workloads: Workloads,
    /// When the workloads' files were last looked at: at how many commits, and when.
    reviewed_commits: u64,
    reviewed_ms: u64,
    #[cfg(feature = "pull")]
    pulling: crate::pull::Pulling,
    pub(crate) forwards: Forwards,
    pub(crate) peers: Peers,
    audit: Option<Audit>,
    started: Instant,
    scratch: Vec<u8>,
}

impl Logic {
    /// The kinds node logic answers.
    #[cfg(feature = "pull")]
    pub(crate) const KINDS: &'static [u8] = &[
        kind::PUT,
        kind::GET,
        kind::WATCH,
        kind::ACTION_REGISTER,
        kind::ACTION_RUN,
        kind::ACTION_PROGRESS,
        kind::ACTION_RESULT,
        kind::ACTION_CANCEL,
        kind::ACTION_ATTACH,
        kind::FILE_BEGIN,
        kind::FILE_CHUNK,
        kind::FILE_COMMIT,
        kind::FILE_SINK,
        kind::FILE_STATUS,
        kind::WORKLOAD_PUT,
        kind::WORKLOAD_REMOVE,
        kind::EXECUTOR_OFFER,
        kind::EXECUTOR_REPORT,
        kind::EXECUTOR_INVENTORY,
        kind::FILE_PULL,
        kind::PEER_SYNC,
        kind::PEER_OBSERVED,
        kind::PEER_STATUS,
        kind::PEER_FORGIVE,
    ];

    /// The kinds node logic answers.
    #[cfg(not(feature = "pull"))]
    pub(crate) const KINDS: &'static [u8] = &[
        kind::PUT,
        kind::GET,
        kind::WATCH,
        kind::ACTION_REGISTER,
        kind::ACTION_RUN,
        kind::ACTION_PROGRESS,
        kind::ACTION_RESULT,
        kind::ACTION_CANCEL,
        kind::ACTION_ATTACH,
        kind::FILE_BEGIN,
        kind::FILE_CHUNK,
        kind::FILE_COMMIT,
        kind::FILE_SINK,
        kind::FILE_STATUS,
        kind::WORKLOAD_PUT,
        kind::WORKLOAD_REMOVE,
        kind::EXECUTOR_OFFER,
        kind::EXECUTOR_REPORT,
        kind::EXECUTOR_INVENTORY,
        kind::PEER_SYNC,
        kind::PEER_OBSERVED,
        kind::PEER_STATUS,
        kind::PEER_FORGIVE,
    ];

    pub(crate) fn new(
        store: Store,
        node: NodeName,
        files: Files,
        audit: Option<Audit>,
        dialer: Dialer,
        stop_grace: Duration,
        peers: Peers,
    ) -> Self {
        // What should run here is in the store. It is adopted from the executors that are
        // still connected, or started, once they have said what they have.
        let workloads = Workloads::restored(
            node.clone(),
            orion_executors::RestartConfig {
                stop_grace_ms: stop_grace.as_millis() as u64,
                ..orion_executors::RestartConfig::default()
            },
            &store,
            &OnBoard(&files),
            0,
        );
        Self {
            workloads,
            reviewed_commits: 0,
            reviewed_ms: 0,
            records: Records::new(store),
            forwards: Forwards::new(dialer),
            peers,
            actions: Actions::new(node),
            files,
            sinks: Sinks::new(),
            sends: Sends::default(),
            #[cfg(feature = "pull")]
            pulling: crate::pull::Pulling::default(),
            audit,
            started: Instant::now(),
            scratch: vec![0; 64 * 1024],
        }
    }

    /// Milliseconds on a clock that only goes forward: what deadlines are kept on.
    pub(crate) fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// How long until the next deadline: a run that times out, a workload to start again.
    pub(crate) fn next_deadline(&self) -> Option<Duration> {
        [
            self.actions.next_deadline(),
            self.workloads.next_deadline(),
            self.workloads.placement_deadline(),
            self.records.reclaim_at(),
        ]
        .into_iter()
        .flatten()
        .min()
        .map(|due| Duration::from_millis(due.saturating_sub(self.now_ms())))
        .into_iter()
        .chain(self.peers.next_deadline().map(|due| {
            // Sync keeps its times on the wall clock.
            Duration::from_millis(due.saturating_sub(wall().as_millis() as u64))
        }))
        .min()
    }

    /// Starts again the workloads that are due.
    pub(crate) fn restarts(&mut self, out: &mut dyn Out) {
        let (now, wall_ms) = (self.now_ms(), wall().as_millis() as u64);
        self.records.reclaim(now, wall_ms, out);
        self.workloads
            .tick(self.records.store_mut(), now, wall_ms, out);
        // A workload starts only when its files are here. Looked at when a file has been
        // put in place, and every second besides, since a file can also go away.
        let commits = self.files.commits();
        if commits != self.reviewed_commits || now.saturating_sub(self.reviewed_ms) >= 1_000 {
            (self.reviewed_commits, self.reviewed_ms) = (commits, now);
            self.workloads.review(
                &OnBoard(&self.files),
                self.records.store_mut(),
                wall_ms,
                out,
            );
        }
        self.records.publish(out);
    }

    /// Peer sync: what the links heard, and what changed here.
    pub(crate) fn sync(&mut self, out: &mut dyn Out) {
        let wall_ms = wall().as_millis() as u64;
        self.peers.turn(&mut self.records, wall_ms, out);
        // Which workloads are this node's, now that the records may say otherwise.
        if self.records.take_placement_due() {
            self.workloads.records_changed();
        }
        let now = self.now_ms();
        if self.workloads.place_due()
            || self
                .workloads
                .placement_deadline()
                .is_some_and(|due| due <= now)
        {
            self.workloads.place(
                self.records.store_mut(),
                &OnBoard(&self.files),
                now,
                wall_ms,
                out,
            );
            // What placement said is told to watchers and peers at once.
            self.peers.turn(&mut self.records, wall_ms, out);
            let _ = self.records.take_placement_due();
        }
    }

    pub(crate) fn forget_sessions(&mut self) {
        let now = self.now_ms();
        self.peers
            .forget_sessions(self.records.store_mut(), wall().as_millis() as u64);
        self.records.forget_sessions(now);
        self.actions.forget_sessions();
        self.forwards.forget_sessions();
        self.sinks.forget_sessions();
        self.sends.forget();
        self.workloads.forget_sessions(now);
        #[cfg(feature = "pull")]
        self.pulling.forget_sessions();
    }

    #[cfg(feature = "pull")]
    fn pull(&mut self, asked: &Asked<'_>, out: &mut dyn Out) -> Result<(), Error> {
        self.pulling.request(asked, &mut self.files, out)
    }

    #[cfg(not(feature = "pull"))]
    fn pull(&mut self, _asked: &Asked<'_>, _out: &mut dyn Out) -> Result<(), Error> {
        Err(Error::Unsupported)
    }

    /// Takes what the fetches under way reported.
    pub(crate) fn fetches(&mut self, out: &mut dyn Out) {
        #[cfg(feature = "pull")]
        self.pulling.poll(&mut self.files, out);
        #[cfg(not(feature = "pull"))]
        let _ = out;
    }

    /// Whether the node is fetching a file.
    pub(crate) fn fetching(&self) -> bool {
        #[cfg(feature = "pull")]
        return self.pulling.active();
        #[cfg(not(feature = "pull"))]
        false
    }

    /// Carries a run to the node that has the action: the node the run names, or the node
    /// the resource it names is on.
    fn forward(&mut self, asked: &Asked<'_>) -> Result<(), Error> {
        let run: Run<'_> = asked.body()?;
        let store = self.records.store();
        let node = match &run.target {
            Target::Node(node) => node.clone(),
            Target::Resource(resource) => {
                let record = store
                    .get(RecordClass::Resource, resource.as_str())
                    .ok_or(Error::NotFound)?;
                ResourceRecord::decode(&record.value)?.node
            }
        };
        if node == *store.node() {
            // It would be here, and it is not.
            return Err(Error::NotFound);
        }
        let record = store
            .get(RecordClass::Node, node.as_str())
            .ok_or(Error::NotFound)?;
        let address = NodeRecord::decode(&record.value)?.address;
        if address.is_empty() {
            return Err(Error::NotFound);
        }
        self.forwards.run(asked, &node, address)
    }

    fn request(&mut self, asked: &Asked<'_>, out: &mut dyn Out) {
        // A forced remove drops a record whatever is true of the run: what it leaves behind
        // is looked at before it is gone, for the audit log.
        let mut left = (asked.kind == kind::WORKLOAD_REMOVE)
            .then(|| asked.body::<WorkloadRemove>().ok())
            .flatten()
            .filter(|remove| remove.force)
            .and_then(|remove| {
                let tracked = self.workloads.tracker().get(&remove.name)?;
                Some(if tracked.run {
                    format!("ok: a run was left {:?}", tracked.state)
                } else {
                    "ok: nothing was running".to_owned()
                })
            });
        let answered = if Records::KINDS.contains(&asked.kind) {
            self.records.request(asked, wall().as_millis() as u64, out)
        } else if Workloads::KINDS.contains(&asked.kind) {
            let (now, wall_ms) = (self.now_ms(), wall().as_millis() as u64);
            let answered = self.workloads.request(
                asked,
                self.records.store_mut(),
                &OnBoard(&self.files),
                now,
                wall_ms,
                out,
            );
            // What should run is a record: watchers are told and it is saved.
            self.records.publish(out);
            answered
        } else if Peers::KINDS.contains(&asked.kind) {
            self.peers
                .request(asked, &mut self.records, wall().as_millis() as u64, out)
        } else if Peers::ASKED.contains(&asked.kind) {
            self.peers
                .ask(asked, &self.records, wall().as_millis() as u64, out)
        } else if asked.kind == kind::FILE_PULL {
            self.pull(asked, out)
        } else if Sinks::KINDS.contains(&asked.kind) {
            self.sinks.request(asked, out)
        } else if Files::KINDS.contains(&asked.kind) {
            // A file for a receiver goes straight to it. Any other is the store's.
            match self.sinks.pass(asked, out) {
                Ok(true) => Ok(()),
                Ok(false) => self.files.request(asked, out),
                Err(error) => Err(error),
            }
        } else {
            let now = self.now_ms();
            match self.actions.request(asked, now, out) {
                // Not an action of this node's: another node's, perhaps.
                Err(Error::NotFound) if asked.kind == kind::ACTION_RUN => self.forward(asked),
                Err(Error::NotFound) if asked.kind == kind::ACTION_CANCEL => asked
                    .body::<Cancel>()
                    .ok()
                    .filter(|cancel| self.forwards.cancel(asked.session, cancel.call))
                    .map(|_| ())
                    .ok_or(Error::NotFound),
                answered => answered,
            }
        };
        if let Some(audit) = &self.audit
            && let Some((did, what)) = done(asked)
        {
            let result = answered.map_or_else(
                |error| error.to_string(),
                |()| left.take().unwrap_or_else(|| "ok".to_owned()),
            );
            audit.record(&Entry {
                time_ms: wall().as_millis() as u64,
                who: who(asked),
                did,
                what: &what,
                result: &result,
            });
        }
        if let Err(error) = answered {
            out.refuse(asked, error);
        }
    }
}

/// Who asked, as the comm core named them.
fn who<'a>(asked: &Asked<'a>) -> Who<'a> {
    // The bits of `orion_check::PrincipalClass`.
    match asked.class {
        2 | 4 => Who::Local(asked.uid),
        8 => Who::Operator(asked.caller),
        16 => Who::Peer(asked.caller),
        32 => Who::Device(asked.caller),
        _ => Who::Anonymous,
    }
}

fn target(target: &Target) -> String {
    match target {
        Target::Node(node) => format!("node:{node}"),
        Target::Resource(resource) => format!("resource:{resource}"),
    }
}

/// What a request does and to what, for the audit log. `None` for what changes nothing, and
/// for what is said many times in the course of one thing: a chunk, a word of progress.
fn done(asked: &Asked<'_>) -> Option<(&'static str, String)> {
    match asked.kind {
        kind::PUT => {
            let put: Put<'_> = asked.body().ok()?;
            let mut what = String::new();
            for (index, entry) in put.records.iter().flatten().enumerate() {
                if index == 4 {
                    let _ = write!(what, " and {} more", put.records.len() - 4);
                    break;
                }
                if index != 0 {
                    what.push(' ');
                }
                let verb = if entry.deleted { "-" } else { "" };
                let _ = write!(what, "{verb}{:?}/{}", entry.class, entry.key);
            }
            Some(("put", what))
        }
        kind::ACTION_REGISTER => {
            let register: Register<'_> = asked.body().ok()?;
            Some((
                "register",
                format!("{} {}", register.name, target(&register.target)),
            ))
        }
        kind::ACTION_RUN => {
            let run: Run<'_> = asked.body().ok()?;
            Some(("run", format!("{} {}", run.name, target(&run.target))))
        }
        kind::ACTION_CANCEL => {
            let cancel: Cancel = asked.body().ok()?;
            Some(("cancel", format!("run {}", cancel.call)))
        }
        kind::FILE_BEGIN => {
            let begin: FileBegin = asked.body().ok()?;
            Some((
                "file",
                format!("{} ({} bytes)", begin.destination, begin.size),
            ))
        }
        kind::FILE_COMMIT => {
            let commit: FileCommit = asked.body().ok()?;
            Some(("commit", format!("transfer {:016x}", commit.transfer)))
        }
        kind::WORKLOAD_PUT => {
            let put: WorkloadPut<'_> = asked.body().ok()?;
            Some(("workload", format!("{} ({})", put.name, put.kind)))
        }
        kind::WORKLOAD_REMOVE => {
            let remove: WorkloadRemove = asked.body().ok()?;
            let how = if remove.force { " by force" } else { "" };
            Some(("remove", format!("{}{how}", remove.name)))
        }
        kind::PEER_FORGIVE => {
            let forgive: PeerForgive = asked.body().ok()?;
            Some(("forgive", format!("node:{}", forgive.node)))
        }
        _ => None,
    }
}

impl Handler for Logic {
    fn kinds(&self) -> &'static [u8] {
        // Besides what the comm core passes on: what node logic is told as a client, as the
        // program behind `files.send`.
        &[
            kind::NODE_REQUEST,
            kind::NODE_GONE,
            kind::ACTION_CLAIM,
            kind::ACTION_CANCEL,
            kind::FILE_STATUS,
        ]
    }

    fn handle(&mut self, frame: &Incoming<'_>, cx: &mut Cx<'_>) -> Result<(), ClientError> {
        let now = self.now_ms();
        match frame.kind() {
            kind::ACTION_CLAIM => {
                let claim: Claim<'_> = frame.body()?;
                return self
                    .sends
                    .claim(&claim, &self.files, &self.sinks, now, &mut Via::Cx(cx));
            }
            kind::ACTION_CANCEL => {
                let cancel: Cancel = frame.body()?;
                return self.sends.cancel(cancel.call, &mut Via::Cx(cx));
            }
            kind::FILE_STATUS => {
                let status: FileStatus<'_> = frame.body()?;
                return self.sends.status(&status, now, &mut Via::Cx(cx));
            }
            _ => {}
        }
        let mut scratch = std::mem::take(&mut self.scratch);
        let mut through = Through {
            cx,
            scratch: &mut scratch,
            failed: None,
        };
        if frame.kind() == kind::NODE_REQUEST {
            let passed: NodeRequest<'_> = frame.body()?;
            self.request(&Asked::new(&passed), &mut through);
        } else {
            let gone: NodeGone = frame.body()?;
            self.records
                .gone(gone.session, wall().as_millis() as u64, &mut through);
            self.files.gone(gone.session);
            self.forwards.gone(gone.session);
            self.peers.gone(
                gone.session,
                self.records.store_mut(),
                wall().as_millis() as u64,
            );
            self.sinks.gone(gone.session, &mut through);
            let (now, wall_ms) = (self.now_ms(), wall().as_millis() as u64);
            self.workloads.gone(
                gone.session,
                self.records.store_mut(),
                now,
                wall_ms,
                &mut through,
            );
            self.records.publish(&mut through);
            #[cfg(feature = "pull")]
            self.pulling.gone(gone.session);
            let now = self.now_ms();
            self.actions.gone(gone.session, now, &mut through);
        }
        let failed = through.failed.take();
        self.scratch = scratch;
        failed.map_or(Ok(()), Err)
    }
}

/// Answers while a frame from the comm core is held.
struct Through<'a, 'b> {
    cx: &'a mut Cx<'b>,
    scratch: &'a mut Vec<u8>,
    failed: Option<ClientError>,
}

impl Out for Through<'_, '_> {
    fn frame(&mut self, session: u32, kind: u8, payload: &[u8], may_drop: bool) {
        if self.failed.is_some() {
            return;
        }
        let answer = NodeAnswer {
            session,
            kind,
            may_drop,
            payload: Bytes(payload),
        };
        if let Err(error) = self.cx.send(&answer) {
            self.failed = Some(error);
        }
    }

    fn scratch(&mut self) -> &mut Vec<u8> {
        self.scratch
    }
}

/// Sends between frames: what a deadline has node logic say.
pub(crate) struct Direct<'a> {
    client: &'a Client,
    scratch: &'a mut Vec<u8>,
    failed: Option<ClientError>,
}

impl<'a> Direct<'a> {
    pub(crate) const fn new(client: &'a Client, scratch: &'a mut Vec<u8>) -> Self {
        Self {
            client,
            scratch,
            failed: None,
        }
    }

    pub(crate) fn result(self) -> Result<(), ClientError> {
        self.failed.map_or(Ok(()), Err)
    }
}

impl Out for Direct<'_> {
    fn frame(&mut self, session: u32, kind: u8, payload: &[u8], may_drop: bool) {
        if self.failed.is_some() {
            return;
        }
        let answer = NodeAnswer {
            session,
            kind,
            may_drop,
            payload: Bytes(payload),
        };
        if let Err(error) = self.client.send(&answer) {
            self.failed = Some(error);
        }
    }

    fn scratch(&mut self) -> &mut Vec<u8> {
        self.scratch
    }
}
