//! What node logic keeps, and how a frame from the comm core reaches the part it is for.

use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use orion_action_forward::Forwards;
use orion_action_list::{Actions, Event};
use orion_audit::{Audit, Entry, Who};
use orion_connect::{Client, ClientError, Cx, Handler, Incoming};
use orion_file_store::Files;
use orion_files_sink::Sinks;
use orion_msg_actions::{Cancel, Register, Run, Target};
use orion_msg_files::{FileBegin, FileCommit};
use orion_msg_records::{NodeRecord, Put, RecordClass, ResourceRecord};
use orion_msg_session::{NodeAnswer, NodeGone, NodeRequest};
use orion_msg_topics::{Field, FieldType};
use orion_reply::{Asked, Out};
use orion_ring::FieldId;
use orion_store::Store;
use orion_topics::Publisher;
use orion_types::{Error, FrameTime, NodeName};
use orion_wire::{Bytes, Decode, kind};

use crate::records::Records;

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
        let time = FrameTime::from_nanos(wall().as_nanos() as u64);
        let _ = topic.publish(FieldId(0), time, line.as_bytes());
    }
    let time = FrameTime::from_nanos(wall().as_nanos() as u64);
    let _ = topic.publish(FieldId(1), time, &(running as i64).to_le_bytes());
}

/// Everything node logic keeps.
pub(crate) struct Logic {
    pub(crate) records: Records,
    pub(crate) actions: Actions,
    pub(crate) files: Files,
    pub(crate) sinks: Sinks,
    pub(crate) forwards: Forwards,
    audit: Option<Audit>,
    started: Instant,
    scratch: Vec<u8>,
}

impl Logic {
    /// The kinds node logic answers.
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
    ];

    pub(crate) fn new(
        store: Store,
        node: NodeName,
        files: Files,
        audit: Option<Audit>,
        identity: PathBuf,
    ) -> Self {
        Self {
            records: Records::new(store),
            forwards: Forwards::new(node.clone(), identity),
            actions: Actions::new(node),
            files,
            sinks: Sinks::new(),
            audit,
            started: Instant::now(),
            scratch: vec![0; 64 * 1024],
        }
    }

    /// Milliseconds on a clock that only goes forward: what deadlines are kept on.
    pub(crate) fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// How long until the next deadline.
    pub(crate) fn next_deadline(&self) -> Option<Duration> {
        self.actions
            .next_deadline()
            .map(|due| Duration::from_millis(due.saturating_sub(self.now_ms())))
    }

    pub(crate) fn forget_sessions(&mut self) {
        self.records.forget_sessions(wall().as_millis() as u64);
        self.actions.forget_sessions();
        self.forwards.forget_sessions();
        self.sinks.forget_sessions();
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
        let answered = if Records::KINDS.contains(&asked.kind) {
            self.records.request(asked, wall().as_millis() as u64, out)
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
            let result = answered.map_or_else(|error| error.to_string(), |()| "ok".to_owned());
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
        _ => None,
    }
}

impl Handler for Logic {
    fn kinds(&self) -> &'static [u8] {
        &[kind::NODE_REQUEST, kind::NODE_GONE]
    }

    fn handle(&mut self, frame: &Incoming<'_>, cx: &mut Cx<'_>) -> Result<(), ClientError> {
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
            self.sinks.gone(gone.session, &mut through);
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
