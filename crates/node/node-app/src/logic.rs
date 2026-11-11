//! What node logic keeps, and how a frame from the comm core reaches the part it is for.

use std::fmt::Write as _;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use orion_action_list::{Actions, Event};
use orion_connect::{Client, ClientError, Cx, Handler, Incoming};
use orion_msg_actions::Target;
use orion_msg_session::{NodeAnswer, NodeGone, NodeRequest};
use orion_msg_topics::{Field, FieldType};
use orion_reply::{Asked, Out};
use orion_ring::FieldId;
use orion_store::Store;
use orion_topics::Publisher;
use orion_types::{FrameTime, NodeName};
use orion_wire::{Bytes, kind};

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
    ];

    pub(crate) fn new(store: Store, node: NodeName) -> Self {
        Self {
            records: Records::new(store),
            actions: Actions::new(node),
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
        self.records.forget_sessions();
        self.actions.forget_sessions();
    }

    fn request(&mut self, asked: &Asked<'_>, out: &mut dyn Out) {
        let answered = if Records::KINDS.contains(&asked.kind) {
            self.records.request(asked, wall().as_millis() as u64, out)
        } else {
            let now = self.now_ms();
            self.actions.request(asked, now, out)
        };
        if let Err(error) = answered {
            out.refuse(asked, error);
        }
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
            self.records.gone(gone.session);
            self.actions.gone(gone.session, &mut through);
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
