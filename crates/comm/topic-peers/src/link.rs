//! The link to one other node: a session with its comm core, as a subscriber without a ring.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use orion_broker::{TopicId, Writing};
use orion_msg_peers::{CLOCK_MONOTONIC, TimeReply, TimeRequest};
use orion_msg_session::{Hello, Ping, Pong, Reject, RejectReason, Roles, Welcome};
use orion_msg_topics::{Declare, Opened, Sample, Subscribe, TopicState, TopicStatus, Unsubscribe};
use orion_server::{Connection, Error, FdSet, FrameRead, FrameWrite};
use orion_tcp::{TcpRead, TcpWrite};
use orion_types::{NodeName, SampleTime, TopicName, TopicRef};
use orion_wire::{
    Body, Bytes, Decode, INTERFACE_LEVEL, INTERFACE_REVISION, encode_body, frame, kind,
};
use shm_ring::{FieldId, Flags, Spec};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{Command, LinkStats, Shared};

/// How long between tries to reach a node that is asked for. Short and fixed: a subscriber
/// is waiting.
const RETRY: Duration = Duration::from_millis(200);

/// How often the two clocks are compared again.
const COMPARE: Duration = Duration::from_secs(10);

/// The time of day, in nanoseconds.
/// This board's monotonic clock, which sample times are on.
pub(crate) fn monotonic_ns() -> u64 {
    SampleTime::now().as_nanos()
}

/// What is added to the other board's times to get this board's: this clock less that one,
/// from one request and its answer. Zero if that is less than the round trip could hide.
pub(crate) fn offset(sent_ns: u64, reply: &TimeReply, arrived_ns: u64) -> i64 {
    let round = i128::from(arrived_ns) - i128::from(sent_ns);
    let held =
        (i128::from(reply.replied_ns) - i128::from(reply.received_ns)).clamp(0, round.max(0));
    let delay = (round - held).max(0);
    // The other clock read `replied_ns` when this one read `arrived_ns` less half the delay.
    let theirs_less_mine = i128::from(reply.replied_ns) - (i128::from(arrived_ns) - delay / 2);
    // Half the delay is what an uneven path could account for, and never less than a
    // fifth of a millisecond: below that the clocks agree as far as anyone can tell.
    let unsure = (delay / 2).max(200_000);
    if theirs_less_mine.abs() <= unsure {
        0
    } else {
        (-theirs_less_mine).clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    }
}

/// One topic asked for over this link.
struct Mirror {
    name: TopicName,
    /// Its number here.
    id: TopicId,
    /// The `seq` of the subscribe that asked for it on this connection.
    asked_seq: u16,
    /// Its handle on the other node, once that has answered.
    handle: Option<u32>,
    writing: Option<Writing>,
}

/// What the two halves of a link share.
struct Session {
    mirrors: Vec<Mirror>,
    /// The topic declaration most recently received: it comes just before the `Opened` it
    /// belongs to.
    declared: Vec<u8>,
    seq: u16,
    /// When the clocks were last asked to be compared, on this board's clock.
    compared_at: u64,
    offset_ns: i64,
}

async fn send<B: Body>(
    write: &mut TcpWrite,
    session: &RefCell<Session>,
    body: &B,
) -> std::io::Result<u16> {
    let seq = {
        let mut session = session.borrow_mut();
        session.seq = session.seq.wrapping_add(1);
        session.seq
    };
    let mut out = [0_u8; 512];
    let len = encode_body(body, seq, &mut out)
        .map_err(|_| std::io::Error::other("a frame that does not fit"))?;
    write.send(&out[..len], &[]).await?;
    Ok(seq)
}

/// Ends a task when it goes out of scope: a link that is dropped takes its helpers along.
struct Ending(tokio::task::JoinHandle<()>);

impl Drop for Ending {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// What is asked for of one node, and the connection to it, if there is one.
#[derive(Default)]
struct Asking {
    /// The topics asked for. This outlives any one connection.
    wanted: Vec<(TopicName, TopicId)>,
    /// The writing half of the connection there is now.
    connection: Option<mpsc::UnboundedSender<Out>>,
}

/// Runs the link to `node` for as long as something of it is asked for.
pub(crate) async fn run(
    shared: Rc<Shared>,
    node: NodeName,
    mut commands: mpsc::UnboundedReceiver<Command>,
    stats: Rc<RefCell<LinkStats>>,
) {
    let asking = Rc::new(RefCell::new(Asking::default()));
    // What the broker asks is taken as it comes, whatever the connection is doing: noted,
    // and if there is a connection, sent on it at once.
    let taking = asking.clone();
    let counting = stats.clone();
    let _taker = Ending(tokio::task::spawn_local(async move {
        while let Some(command) = commands.recv().await {
            let mut asking = taking.borrow_mut();
            let out = match command {
                Command::Subscribe(name, id) => {
                    asking.wanted.push((name.clone(), id));
                    Out::Subscribe(name, id)
                }
                Command::Unsubscribe(id) => {
                    asking.wanted.retain(|(_, wanted)| *wanted != id);
                    Out::Unsubscribe(id)
                }
            };
            counting.borrow_mut().topics = asking.wanted.len();
            if let Some(connection) = &asking.connection {
                let _ = connection.send(out);
            }
        }
    }));
    let mut said_unknown = false;
    loop {
        let Some(reach) = shared.resolve.reach(&node) else {
            if !std::mem::replace(&mut said_unknown, true) {
                warn!("a topic of {node} is asked for, and nobody knows where {node} is");
            }
            tokio::time::sleep(RETRY * 5).await;
            continue;
        };
        said_unknown = false;
        match orion_tcp::connect(reach.addr, &shared.options, reach.key).await {
            Ok(connection) => {
                let (read, write) = connection.split();
                if let Err(error) = serve(&shared, &node, read, write, &asking, &stats).await {
                    debug!("the link to {node} ended: {error}");
                }
                asking.borrow_mut().connection = None;
                stats.borrow_mut().connected = false;
                // The publishers there cannot be heard: the subscribers here are told, and
                // keep their rings.
                let wanted: Vec<TopicId> =
                    asking.borrow().wanted.iter().map(|(_, id)| *id).collect();
                shared.comm.with(|sessions, _| {
                    let mut topics = shared.topics.borrow_mut();
                    for id in wanted {
                        topics.mirror_lost(id, sessions);
                    }
                });
            }
            Err(error) => debug!("{node} was not reached at {}: {error}", reach.addr),
        }
        tokio::time::sleep(RETRY).await;
    }
}

/// One connection: says hello, asks for what is wanted, and carries what arrives until the
/// connection ends.
async fn serve(
    shared: &Rc<Shared>,
    node: &NodeName,
    mut read: TcpRead,
    mut write: TcpWrite,
    asking: &Rc<RefCell<Asking>>,
    stats: &Rc<RefCell<LinkStats>>,
) -> std::io::Result<()> {
    let session = Rc::new(RefCell::new(Session {
        mirrors: Vec::new(),
        declared: Vec::new(),
        seq: 0,
        compared_at: 0,
        offset_ns: 0,
    }));
    let name = format!("comm:{}", shared.me);
    send(
        &mut write,
        &session,
        &Hello {
            name: &name,
            roles: Roles::CLIENT,
            max_frame: shared.options.max_frame,
            revision: INTERFACE_REVISION,
            level: INTERFACE_LEVEL,
        },
    )
    .await?;
    let mut buf = vec![0_u8; shared.options.max_frame as usize + 64];
    let mut fds = FdSet::default();
    let len = read
        .recv(&mut buf, &mut fds)
        .await?
        .ok_or_else(|| std::io::Error::other("closed before it said hello"))?;
    let view = frame::decode(&buf[..len]).map_err(|_| std::io::Error::other("not a frame"))?;
    let heartbeat = match view.kind() {
        kind::WELCOME => {
            let welcome = Welcome::decode(view.payload())
                .map_err(|_| std::io::Error::other("a welcome that does not decode"))?;
            if welcome.revision != INTERFACE_REVISION {
                warn!(
                    "{node} speaks wire revision {} and this node {INTERFACE_REVISION}",
                    welcome.revision
                );
                return Err(std::io::Error::other("another wire revision"));
            }
            if welcome.node != *node {
                warn!("the node at {node}'s address says it is {}", welcome.node);
                return Err(std::io::Error::other("another node answered"));
            }
            Duration::from_millis(u64::from(welcome.heartbeat_ms.max(100)))
        }
        kind::REJECT => {
            let why = Reject::decode(view.payload()).map(|reject| match reject.reason {
                RejectReason::RevisionMismatch => format!(
                    "it speaks wire revision {} and this node {}",
                    reject.node_revision, reject.peer_revision
                ),
                reason => format!("{reason:?}"),
            });
            warn!("{node} turned this node away: {}", why.unwrap_or_default());
            return Err(std::io::Error::other("rejected"));
        }
        _ => return Err(std::io::Error::other("not a welcome")),
    };
    info!("connected to {node} for its topics");
    stats.borrow_mut().connected = true;

    // The writing half has its own task: reading a frame cannot be put down half way, so
    // nothing else may wait in the task that reads.
    let (to_write, mut writing) = mpsc::unbounded_channel::<Out>();
    {
        // Everything that is asked for is asked of this connection, and what is asked from
        // now on goes to it as it comes.
        let mut asking = asking.borrow_mut();
        for (name, id) in &asking.wanted {
            let _ = to_write.send(Out::Subscribe(name.clone(), *id));
        }
        asking.connection = Some(to_write.clone());
    }
    let _ = to_write.send(Out::Compare);
    let writer_session = session.clone();
    let writer_clock = shared.clone();
    let _writer = Ending(tokio::task::spawn_local(async move {
        let mut beat = tokio::time::interval(heartbeat);
        let mut compare = tokio::time::interval(COMPARE);
        loop {
            let out = tokio::select! {
                out = writing.recv() => match out {
                    Some(out) => out,
                    None => return,
                },
                _ = beat.tick() => Out::Ping,
                _ = compare.tick() => Out::Compare,
            };
            let sent = match out {
                Out::Subscribe(name, id) => {
                    let asked = send(
                        &mut write,
                        &writer_session,
                        &Subscribe {
                            topic: TopicRef::local(name.clone()),
                            request: Bytes(&[]),
                            max_held: 0,
                            max_hold_ms: 0,
                        },
                    )
                    .await;
                    if let Ok(asked_seq) = asked {
                        let mut session = writer_session.borrow_mut();
                        session.mirrors.retain(|mirror| mirror.id != id);
                        session.mirrors.push(Mirror {
                            name,
                            id,
                            asked_seq,
                            handle: None,
                            writing: None,
                        });
                    }
                    asked.map(|_| ())
                }
                Out::Unsubscribe(id) => {
                    let handle = {
                        let mut session = writer_session.borrow_mut();
                        let handle = session
                            .mirrors
                            .iter()
                            .find(|mirror| mirror.id == id)
                            .and_then(|mirror| mirror.handle);
                        session.mirrors.retain(|mirror| mirror.id != id);
                        handle
                    };
                    match handle {
                        Some(handle) => send(&mut write, &writer_session, &Unsubscribe { handle })
                            .await
                            .map(|_| ()),
                        None => Ok(()),
                    }
                }
                Out::Ping => send(&mut write, &writer_session, &Ping { now_ms: 0 })
                    .await
                    .map(|_| ()),
                Out::Pong(now_ms) => send(&mut write, &writer_session, &Pong { now_ms })
                    .await
                    .map(|_| ()),
                Out::Compare => {
                    let sent_ns = (writer_clock.clock)();
                    writer_session.borrow_mut().compared_at = sent_ns;
                    send(
                        &mut write,
                        &writer_session,
                        &TimeRequest {
                            sent_ns,
                            clock: CLOCK_MONOTONIC,
                        },
                    )
                    .await
                    .map(|_| ())
                }
            };
            if sent.is_err() {
                return;
            }
        }
    }));

    carry(
        shared, node, &mut read, &mut buf, &session, &to_write, stats,
    )
    .await
}

/// What the writing half is asked to send.
enum Out {
    Subscribe(TopicName, TopicId),
    Unsubscribe(TopicId),
    Ping,
    Pong(u64),
    Compare,
}

/// Reads the connection until it ends: each frame is a declaration, a sample, or news of a
/// topic.
async fn carry(
    shared: &Rc<Shared>,
    node: &NodeName,
    read: &mut TcpRead,
    buf: &mut [u8],
    session: &Rc<RefCell<Session>>,
    to_write: &mpsc::UnboundedSender<Out>,
    stats: &Rc<RefCell<LinkStats>>,
) -> std::io::Result<()> {
    let mut fds = FdSet::default();
    loop {
        let Some(len) = read.recv(buf, &mut fds).await? else {
            return Ok(());
        };
        let view = frame::decode(&buf[..len]).map_err(|_| std::io::Error::other("not a frame"))?;
        let payload = view.payload();
        match view.kind() {
            kind::TOPIC_SAMPLE => {
                let Ok(sample) = Sample::decode(payload) else {
                    continue;
                };
                if write_sample(shared, session, &sample).is_ok() {
                    stats.borrow_mut().samples += 1;
                }
            }
            kind::TOPIC_DECLARE => {
                let mut session = session.borrow_mut();
                session.declared.clear();
                session.declared.extend_from_slice(payload);
            }
            kind::TOPIC_OPENED => {
                let Ok(opened) = Opened::decode(payload) else {
                    continue;
                };
                declare(shared, node, session, opened.request_seq, opened.handle);
            }
            kind::TOPIC_STATE => {
                let Ok(state) = TopicState::decode(payload) else {
                    continue;
                };
                topic_state(shared, session, &state);
            }
            kind::TIME_REPLY => {
                let arrived = (shared.clock)();
                if let Ok(reply) = TimeReply::decode(payload) {
                    let mut session = session.borrow_mut();
                    if reply.sent_ns == session.compared_at {
                        session.offset_ns = offset(reply.sent_ns, &reply, arrived);
                        stats.borrow_mut().offset_ns = session.offset_ns;
                    }
                }
            }
            kind::PING => {
                if let Ok(ping) = Ping::decode(payload) {
                    let _ = to_write.send(Out::Pong(ping.now_ms));
                }
            }
            kind::REFUSAL => {
                if let Ok(refusal) = orion_msg_session::Refusal::decode(payload) {
                    let session = session.borrow();
                    if let Some(mirror) = session
                        .mirrors
                        .iter()
                        .find(|mirror| mirror.asked_seq == refusal.request_seq)
                    {
                        warn!(
                            "{node} refused a subscription to {}: {}",
                            mirror.name, refusal.detail
                        );
                    }
                }
            }
            _ => {}
        }
    }
}

/// The other node answered a subscribe with the topic's ring-less handle: the declaration
/// that came just before it makes the local copy.
fn declare(
    shared: &Rc<Shared>,
    node: &NodeName,
    session: &Rc<RefCell<Session>>,
    asked_seq: u16,
    handle: u32,
) {
    let mut session = session.borrow_mut();
    let declared = std::mem::take(&mut session.declared);
    let Some(mirror) = session
        .mirrors
        .iter_mut()
        .find(|mirror| mirror.asked_seq == asked_seq || mirror.handle == Some(handle))
    else {
        return;
    };
    mirror.handle = Some(handle);
    mirror.writing = None;
    let Ok(declaration) = Declare::decode(&declared) else {
        warn!(
            "{node} sent a declaration of {} that cannot be read",
            mirror.name
        );
        return;
    };
    let spec = Spec {
        fields: declaration.fields.len() as u32,
        history: declaration.history,
        max_payload: declaration.max_payload,
        expiry_ms: declaration.expiry_ms,
    };
    let id = mirror.id;
    let made = shared.comm.with(|sessions, _| {
        shared
            .topics
            .borrow_mut()
            .declare_mirror(id, &declared, &spec, sessions)
    });
    if let Err(error) = made {
        warn!(
            "no local copy of {node}/{} could be made: {error}",
            mirror.name
        );
    }
}

/// News of a topic from the other node: its publisher went, or is back.
fn topic_state(shared: &Rc<Shared>, session: &Rc<RefCell<Session>>, state: &TopicState) {
    let mut session = session.borrow_mut();
    let Some(mirror) = session.mirrors.iter_mut().find(|mirror| {
        mirror.handle == Some(state.handle) || mirror.asked_seq == state.request_seq
    }) else {
        return;
    };
    mirror.handle.get_or_insert(state.handle);
    let id = mirror.id;
    shared.comm.with(|sessions, _| {
        let mut topics = shared.topics.borrow_mut();
        match state.status {
            TopicStatus::PublisherGone => topics.mirror_lost(id, sessions),
            TopicStatus::Live => {
                // The same ring goes on: the declaration is the one there already.
                let again = topics.get(id).map(|topic| topic.declare.clone());
                if let Some(declared) = again
                    && let Ok(declaration) = Declare::decode(&declared)
                {
                    let spec = Spec {
                        fields: declaration.fields.len() as u32,
                        history: declaration.history,
                        max_payload: declaration.max_payload,
                        expiry_ms: declaration.expiry_ms,
                    };
                    let _ = topics.declare_mirror(id, &declared, &spec, sessions);
                }
            }
            // Nobody has declared it there yet. The declaration follows when someone does.
            _ => {}
        }
    });
}

/// Writes one sample into the local copy of its topic's ring, in this board's time.
fn write_sample(
    shared: &Rc<Shared>,
    session: &Rc<RefCell<Session>>,
    sample: &Sample<'_>,
) -> Result<(), Error> {
    let mut session = session.borrow_mut();
    let offset_ns = session.offset_ns;
    let mirror = session
        .mirrors
        .iter_mut()
        .find(|mirror| mirror.handle == Some(sample.handle))
        .ok_or(Error::NotFound)?;
    let topics = shared.topics.borrow();
    let topic = topics.get(mirror.id).ok_or(Error::NotFound)?;
    if mirror
        .writing
        .as_ref()
        .is_none_or(|writing| writing.epoch != topic.epoch)
    {
        // The ring or its subscribers changed since the last sample.
        mirror.writing = Some(Writing::new(topic)?);
    }
    drop(topics);
    let writing = mirror.writing.as_mut().ok_or(Error::Internal)?;
    let time = SampleTime::from_nanos(sample.time.as_nanos().saturating_add_signed(offset_ns));
    writing.publish(
        FieldId(sample.field),
        time,
        Flags(sample.flags.0),
        sample.payload.0,
    )?;
    writing.wake();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    fn reply(received_ns: u64, replied_ns: u64) -> TimeReply {
        TimeReply {
            sent_ns: 0,
            received_ns,
            replied_ns,
        }
    }

    #[test]
    fn clocks_that_agree_are_left_alone() {
        // Sent at 100 ms, a millisecond each way, answered at once: the other clock read
        // 101 ms when this one did.
        assert_eq!(offset(100 * MS, &reply(101 * MS, 101 * MS), 102 * MS), 0);
        // Off by less than the path could hide: still nothing.
        assert_eq!(
            offset(
                100 * MS,
                &reply(101 * MS + 400_000, 101 * MS + 400_000),
                102 * MS
            ),
            0
        );
        // A fast path and a small difference: under a fifth of a millisecond is nothing.
        assert_eq!(
            offset(
                100 * MS,
                &reply(100 * MS + 150_000, 100 * MS + 150_000),
                100 * MS + 20_000
            ),
            0
        );
    }

    #[test]
    fn a_clock_that_is_ahead_or_behind_is_measured() {
        // The other clock is five seconds behind this one.
        let behind = offset(10_000 * MS, &reply(5_001 * MS, 5_001 * MS), 10_002 * MS);
        assert_eq!(behind, (5_000 * MS) as i64);
        // And five seconds ahead: its times are moved back.
        let ahead = offset(10_000 * MS, &reply(15_001 * MS, 15_001 * MS), 10_002 * MS);
        assert_eq!(ahead, -((5_000 * MS) as i64));
        // The time the other node held the request is not part of the path.
        let held = offset(10_000 * MS, &reply(5_001 * MS, 5_051 * MS), 10_052 * MS);
        assert_eq!(held, (5_000 * MS) as i64);
    }

    #[test]
    fn nonsense_does_not_overflow() {
        assert_eq!(offset(u64::MAX, &reply(0, 0), 0), 0);
        let far = offset(0, &reply(u64::MAX, u64::MAX), 1);
        assert!(far < 0);
        let _ = offset(5, &reply(9, 3), 1);
    }
}
