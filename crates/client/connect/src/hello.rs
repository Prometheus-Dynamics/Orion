//! Saying hello, and waiting for the welcome.
//!
//! Over a link that is not signed, nothing says a frame was lost: a hello the far end never
//! had, or a welcome that never came back, is only silence. So there the hello is said again
//! on a capped backoff until the welcome comes or the wait is over: again after
//! [`FIRST_AGAIN`], the wait doubling to [`MOST_BETWEEN`]. A comm core takes a hello inside a
//! session on a link as the far end starting over, so a hello said twice is never refused. A
//! welcome to an earlier one may still come after the session began; the welcome to the later
//! one then follows, and the session takes it as what it now is (`Session::take`).

use std::time::{Duration, Instant};

use orion_msg_session::{Reject, RejectReason, Welcome};
use orion_wire::{Decode, FrameView, INTERFACE_REVISION, frame, kind};

use crate::fd::FdSet;
use crate::transport::{Step, Transport};
use crate::{ClientError, Welcomed};

/// How long a hello over a link goes unanswered before it is said again.
pub const FIRST_AGAIN: Duration = Duration::from_millis(250);
/// The longest wait between hellos over a link.
pub const MOST_BETWEEN: Duration = Duration::from_secs(2);

/// What `view`, a welcome, says.
pub(crate) fn welcome_of(
    view: &FrameView<'_>,
    transport: &Transport,
) -> Result<Welcomed, ClientError> {
    let welcome = Welcome::decode(view.payload()).map_err(|_| ClientError::Protocol("welcome"))?;
    if welcome.revision != INTERFACE_REVISION {
        return Err(ClientError::Revision {
            node: welcome.revision,
            client: INTERFACE_REVISION,
        });
    }
    let heartbeat = Duration::from_millis(u64::from(welcome.heartbeat_ms.max(1)));
    Ok(Welcomed {
        node: welcome.node,
        session: welcome.session,
        heartbeat,
        // A comm core that does not say gives up after three heartbeats.
        idle: match welcome.idle_ms {
            0 => heartbeat * 3,
            ms => Duration::from_millis(u64::from(ms)).max(heartbeat),
        },
        max_frame: welcome.max_frame,
        revision: welcome.revision,
        level: welcome.level,
        signed: transport.signed(),
        node_key: transport.node_key(),
    })
}

/// Says `hello`, a whole frame, and waits up to `timeout` for the welcome (`None`: for as long
/// as it takes); over a link that is not signed, says it again while there is none.
pub(crate) fn welcomed(
    transport: &mut Transport,
    hello: &[u8],
    inbox: &mut [u8],
    fds: &mut FdSet,
    timeout: Option<Duration>,
) -> Result<Welcomed, ClientError> {
    transport.send(hello, &[])?;
    let start = Instant::now();
    let deadline = timeout.map(|timeout| start + timeout);
    let again = transport.loses_frames();
    let mut between = FIRST_AGAIN;
    let mut next = start + between;
    loop {
        let len = match transport.take(inbox, fds)? {
            Step::Frame(len) => len,
            Step::Closed => return Err(ClientError::Lost),
            Step::Nothing => {
                let now = Instant::now();
                if deadline.is_some_and(|deadline| now >= deadline) {
                    return Err(ClientError::Io(std::io::ErrorKind::TimedOut.into()));
                }
                if again && now >= next {
                    transport.send(hello, &[])?;
                    between = (between * 2).min(MOST_BETWEEN);
                    next = now + between;
                }
                let until = match (deadline, again) {
                    (Some(deadline), true) => deadline.min(next),
                    (Some(deadline), false) => deadline,
                    (None, true) => next,
                    // No deadline and nothing to say again: wait for the welcome a while at a
                    // time.
                    (None, false) => now + MOST_BETWEEN,
                };
                transport.wait(until.saturating_duration_since(now))?;
                continue;
            }
        };
        let view =
            frame::decode(&inbox[..len]).map_err(|_| ClientError::Protocol("a damaged frame"))?;
        match view.kind() {
            kind::WELCOME => return welcome_of(&view, transport),
            kind::REJECT => {
                let reject =
                    Reject::decode(view.payload()).map_err(|_| ClientError::Protocol("reject"))?;
                if reject.reason == RejectReason::RevisionMismatch {
                    return Err(ClientError::Revision {
                        node: reject.node_revision,
                        client: INTERFACE_REVISION,
                    });
                }
                return Err(ClientError::Rejected(reject.reason));
            }
            _ => {}
        }
    }
}
