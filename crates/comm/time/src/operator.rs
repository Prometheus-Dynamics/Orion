//! Time over a session: the comm core answers anyone who asks what time it has, and takes the
//! time from an operator who offers theirs.
//!
//! An operator offers its clock with `TimeOffer`. Offering the time is the same power as
//! setting it, so the offer needs the grant `clock.set`. From then on the board asks when it
//! wants the time, with `TimeRequest` on that session, and measures the round trip on its own
//! monotonic clock: a board on a closed network needs no address for its time source.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Instant;

use orion_msg_peers::{TimeOffer, TimeReply, TimeRequest, TimeTurn};
use orion_server::{
    Admits, Checked, Comm, Cx, Delivery, Error, Incoming, Policy, Service, SessionId, Sessions,
    What,
};
use orion_wire::{FrameView, kind};
use tokio::sync::Notify;

use crate::sntp::{self, QUERY_TIMEOUT, Sample};

/// The entry in `time.servers` that names this source.
pub const OPERATOR_SOURCE: &str = "operator";
/// The grant an operator needs to offer its clock.
pub const CLOCK_SET: &str = "clock.set";

#[derive(Debug, Default)]
struct Exchange {
    /// Who offers, and at what stratum.
    offer: Option<(SessionId, u8)>,
    /// The request waiting for its answer: its token, and the wall clock when it was sent.
    asked: Option<(u64, i128)>,
    answer: Option<Sample>,
}

/// The operator source, shared by the service and the sync loop.
#[derive(Debug)]
pub(crate) struct OperatorTime {
    epoch: Instant,
    listening: Cell<bool>,
    poll_ms: Cell<u32>,
    exchange: RefCell<Exchange>,
    answered: Notify,
    /// Someone has just offered a clock.
    pub(crate) offered: Notify,
}

impl Default for OperatorTime {
    fn default() -> Self {
        Self {
            epoch: Instant::now(),
            listening: Cell::new(false),
            poll_ms: Cell::new(2_000),
            exchange: RefCell::default(),
            answered: Notify::new(),
            offered: Notify::new(),
        }
    }
}

impl OperatorTime {
    /// Takes offers from now on: `operator` is one of the sources.
    pub(crate) fn listen(&self) {
        self.listening.set(true);
    }

    pub(crate) fn set_poll(&self, poll: std::time::Duration) {
        self.poll_ms
            .set(u32::try_from(poll.as_millis()).unwrap_or(u32::MAX));
    }

    /// Nanoseconds on the monotonic clock. Never zero.
    fn token(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1)
    }

    /// Asks the offering operator for the time and waits for the answer.
    pub(crate) async fn ask(&self, comm: &Comm) -> Result<Sample, &'static str> {
        let (session, token) = {
            let mut exchange = self.exchange.borrow_mut();
            let (session, _) = exchange.offer.ok_or("no operator is offering its clock")?;
            let token = self.token();
            exchange.asked = Some((token, sntp::unix_now_ns()));
            exchange.answer = None;
            (session, token)
        };
        let request = TimeRequest { sent_ns: token };
        if comm
            .with(|sessions, _| sessions.send(session, &request, Delivery::Must))
            .is_err()
        {
            self.exchange.borrow_mut().asked = None;
            return Err("the operator offering its clock could not be asked");
        }
        let answer = tokio::time::timeout(QUERY_TIMEOUT, async {
            loop {
                if let Some(sample) = self.exchange.borrow_mut().answer.take() {
                    return sample;
                }
                self.answered.notified().await;
            }
        })
        .await;
        self.exchange.borrow_mut().asked = None;
        answer.map_err(|_| "the operator offering its clock did not answer")
    }

    fn offer(&self, session: SessionId, stratum: u8) -> Result<u32, Error> {
        if !self.listening.get() {
            // This board does not take its time from operators.
            return Err(Error::Unsupported);
        }
        self.exchange.borrow_mut().offer = Some((session, stratum.clamp(1, 15)));
        self.offered.notify_one();
        Ok(self.poll_ms.get())
    }

    fn reply(&self, session: SessionId, reply: &TimeReply) -> Result<(), Error> {
        let mut exchange = self.exchange.borrow_mut();
        let (Some((offering, stratum)), Some((token, sent_wall_ns))) =
            (exchange.offer, exchange.asked)
        else {
            return Err(Error::OutOfOrder);
        };
        if offering != session || reply.sent_ns != token {
            return Err(Error::OutOfOrder);
        }
        let round_ns = self.token().saturating_sub(token);
        let (received, replied) = (i128::from(reply.received_ns), i128::from(reply.replied_ns));
        let held_ns = (replied - received).clamp(0, i128::from(round_ns));
        let delay_ns = i128::from(round_ns) - held_ns;
        // The wall clock when the answer arrived, from when the request left and the round
        // trip on the monotonic clock: a clock stepped in between does not skew it.
        let arrived_wall_ns = sent_wall_ns + i128::from(round_ns);
        let offset_ns = ((received - sent_wall_ns) + (replied - arrived_wall_ns)) / 2;
        exchange.asked = None;
        exchange.answer = Some(Sample {
            offset_ns: i64::try_from(offset_ns).map_err(|_| Error::Malformed)?,
            delay_ns: i64::try_from(delay_ns).map_err(|_| Error::Malformed)?,
            stratum,
        });
        drop(exchange);
        self.answered.notify_one();
        Ok(())
    }

    fn gone(&self, session: SessionId) {
        let mut exchange = self.exchange.borrow_mut();
        if exchange
            .offer
            .is_some_and(|(offering, _)| offering == session)
        {
            exchange.offer = None;
        }
    }
}

/// The wall clock as a `u64` of nanoseconds since the Unix epoch.
fn wall_ns() -> u64 {
    u64::try_from(sntp::unix_now_ns()).unwrap_or(0)
}

/// Answers `TimeRequest`, and takes `TimeOffer` and the `TimeReply` that follows from it.
#[derive(Debug)]
pub(crate) struct TimeService(pub(crate) Rc<OperatorTime>);

impl Service for TimeService {
    fn kinds(&self) -> &'static [u8] {
        &[kind::TIME_REQUEST, kind::TIME_OFFER, kind::TIME_REPLY]
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        Ok(if frame.kind() == kind::TIME_REQUEST {
            // What time a board has is no secret: comparing clocks is how anyone lines up
            // what it publishes.
            (
                What::Read("time"),
                Policy::new(Admits::ANYONE).without_grant(),
            )
        } else {
            (What::Run(CLOCK_SET), Policy::new(Admits::OPERATOR))
        })
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let incoming = request.get();
        match incoming.kind() {
            kind::TIME_REQUEST => {
                let received_ns = wall_ns();
                let asked: TimeRequest = incoming.body()?;
                cx.reply(&TimeReply {
                    sent_ns: asked.sent_ns,
                    received_ns,
                    replied_ns: wall_ns(),
                })
            }
            kind::TIME_OFFER => {
                let offer: TimeOffer = incoming.body()?;
                let poll_ms = self.0.offer(cx.from(), offer.stratum)?;
                cx.reply(&TimeTurn {
                    request_seq: cx.request_seq(),
                    poll_ms,
                })
            }
            _ => self.0.reply(cx.from(), &incoming.body()?),
        }
    }

    fn closed(&mut self, session: SessionId, _sessions: &mut Sessions) {
        self.0.gone(session);
    }
}
