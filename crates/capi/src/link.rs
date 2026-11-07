//! A blocking client session on a node's `unix:` link (docs/link-protocol.md, client role):
//! status queries and watches, actions, record queries and watches, and stream subscriptions,
//! read from one socket the caller may poll.
//!
//! No thread and no runtime: the caller drives the session. [`LinkClient::next_event`] reads
//! what the node sent and pings when one is due; a caller that polls [`LinkClient::fd`] itself
//! wakes up at least every [`LinkClient::service_in`] so the session stays alive.

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

use orion_control_plane::{
    ActionQuery, ActionRequest, ActionResult, SeriesDecl, SeriesKind, StatusEntry, StatusKey,
    StatusQuery, StreamCursor, StreamResetReason, StreamSubscription,
};
use orion_link::message::{
    ActionFilterRequest, ActionResults, ActionRun, Hello, Message, Record, RecordFilter, RecordKey,
    RecordRequest, Records, RejectReason, Roles, StatusChanges, StatusRequest, StreamCatalog,
    StreamData, StreamSubscribe, Welcome,
};
use orion_link::{LINK_INTERFACE_REVISION, StreamDecoder, StreamEncoder};

/// Largest frame this client sends or takes. The node splits answers to the negotiated size.
pub const MAX_FRAME: usize = 4096;
/// Writes that make no progress for this long fail the session.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// What the node told the client, one item at a time.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// An entry set or replaced, in the answer to `req`.
    Status {
        /// The query or watch.
        req: u32,
        /// Part of a bootstrap answer (the whole current set), not a change.
        bootstrap: bool,
        /// The entry.
        entry: StatusEntry,
    },
    /// An entry that expired or was removed.
    Expired {
        /// The query or watch.
        req: u32,
        /// The entry's key.
        key: StatusKey,
    },
    /// The answer to `req` is complete: every entry of a query, or one change set of a watch.
    Answered {
        /// The query or watch.
        req: u32,
        /// The answer was a bootstrap.
        bootstrap: bool,
    },
    /// The node refused `req`, which ends.
    RequestError {
        /// The request.
        req: u32,
        /// The link's error code (`orion_link::message::ErrorCode`).
        code: u16,
        /// Why.
        reason: String,
    },
    /// A state of an action the session runs or watches.
    Action {
        /// The run or watch.
        req: u32,
        /// The action as the node reported it.
        result: Box<ActionResult>,
    },
    /// A record set or replaced, in the answer to `req`.
    Record {
        /// The query or watch.
        req: u32,
        /// Part of a bootstrap answer (the whole current set), not a change.
        bootstrap: bool,
        /// The record.
        record: Box<Record>,
    },
    /// A record gone from a watch's set.
    RecordRemoved {
        /// The watch.
        req: u32,
        /// The record's key.
        key: RecordKey,
    },
    /// One series of a subscribed stream: the catalog comes first in a subscription's answers,
    /// one event per series in index order, and again after a reset.
    StreamSeries {
        /// The subscription.
        req: u32,
        /// The publisher's epoch.
        epoch: u64,
        /// The series' index, which its samples carry.
        index: u16,
        /// The stream has a publisher.
        live: bool,
        /// The series.
        series: SeriesDecl,
    },
    /// Samples of one series.
    StreamSamples {
        /// The subscription.
        req: u32,
        /// The batch the samples belong to.
        cursor: StreamCursor,
        /// History (a backfill), not a live batch.
        backfill: bool,
        /// The series' index.
        series: u16,
        /// `(t_us, value)`, oldest first; times are microseconds since the Unix epoch.
        samples: Vec<(u64, f64)>,
    },
    /// The subscription's position no longer holds; the catalog and a backfill follow, unless
    /// the stream is gone.
    StreamReset {
        /// The subscription.
        req: u32,
        /// Why.
        reason: StreamResetReason,
    },
}

/// Why a session call failed.
#[derive(Debug)]
pub enum Error {
    /// The socket failed.
    Io(io::Error),
    /// The node refused the session.
    Rejected(RejectReason),
    /// The node closed the connection.
    Closed,
    /// No answer within the caller's timeout.
    TimedOut,
    /// The node sent something this client cannot take.
    Protocol(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "link socket: {error}"),
            Self::Rejected(reason) => write!(f, "the node refused the session: {reason:?}"),
            Self::Closed => f.write_str("the node closed the session"),
            Self::TimedOut => f.write_str("timed out"),
            Self::Protocol(what) => write!(f, "link protocol: {what}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// One client session.
pub struct LinkClient {
    stream: UnixStream,
    decoder: Box<StreamDecoder<MAX_FRAME>>,
    events: VecDeque<Event>,
    welcome: Welcome,
    welcomed: bool,
    seq: u16,
    next_req: u32,
    /// Pings are sent every half heartbeat of silence from this side.
    ping_every: Duration,
    last_sent: Instant,
    frame: Box<[u8; MAX_FRAME]>,
    out: Vec<u8>,
    read: Box<[u8; MAX_FRAME]>,
    /// Each subscription's series kinds, from its latest catalog, to decode its samples.
    kinds: HashMap<u32, Vec<SeriesKind>>,
}

impl LinkClient {
    /// Connects to the link socket at `path` as `device_name` and waits up to `timeout` for the
    /// node's `Welcome`.
    pub fn connect(path: &Path, device_name: &str, timeout: Duration) -> Result<Self, Error> {
        let stream = UnixStream::connect(path)?;
        stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
        let mut client = Self {
            stream,
            decoder: Box::new(StreamDecoder::new()),
            events: VecDeque::new(),
            welcome: Welcome {
                node_id: orion_core::NodeId::new("pending"),
                session_id: 0,
                heartbeat_ms: 0,
                max_frame: 0,
                revision: 0,
            },
            welcomed: false,
            seq: 0,
            next_req: 0,
            ping_every: Duration::from_millis(500),
            last_sent: Instant::now(),
            frame: Box::new([0; MAX_FRAME]),
            out: Vec::with_capacity(MAX_FRAME + MAX_FRAME / 128 + 2),
            read: Box::new([0; MAX_FRAME]),
            kinds: HashMap::new(),
        };
        client.send(&Message::Hello(Hello {
            device_name: device_name.into(),
            roles: Roles::CLIENT,
            max_frame: MAX_FRAME as u32,
            revision: LINK_INTERFACE_REVISION,
        }))?;
        let deadline = Instant::now() + timeout;
        while !client.welcomed {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(Error::TimedOut);
            }
            if wait_readable(client.fd(), Some(left))? {
                client.read_once()?;
            }
        }
        client.ping_every =
            Duration::from_millis(u64::from(client.welcome.heartbeat_ms.max(2)) / 2);
        Ok(client)
    }

    /// The session's socket: readable when the node sent something. Poll it for reading, and
    /// call [`Self::next_event`] when it is readable or [`Self::service_in`] has passed.
    pub fn fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }

    /// The node's `Welcome`.
    pub fn welcome(&self) -> &Welcome {
        &self.welcome
    }

    /// How long until the session needs a call (a ping is due), at the latest.
    pub fn service_in(&self) -> Duration {
        self.ping_every.saturating_sub(self.last_sent.elapsed())
    }

    /// Asks for the entries matching `filter` once. Returns the request id the answer carries.
    pub fn status_query(&mut self, filter: StatusQuery) -> Result<u32, Error> {
        let req = self.request_id();
        self.send(&Message::StatusQuery(StatusRequest { req, filter }))?;
        Ok(req)
    }

    /// Watches the entries matching `filter`: a bootstrap answer, then changes. One watch per
    /// session. Returns the request id the answers carry.
    pub fn status_watch(&mut self, filter: StatusQuery) -> Result<u32, Error> {
        let req = self.request_id();
        self.send(&Message::StatusWatch(StatusRequest { req, filter }))?;
        Ok(req)
    }

    /// Runs `request`. Its states come as [`Event::Action`] with the returned request id, until a
    /// final one. `request.action_id` names the action (resubmitting the same request follows
    /// the existing action); `requested_by` and `wait_ms` are not sent (the node names the
    /// requester, and the answer follows the action until it is final).
    pub fn action_run(&mut self, request: ActionRequest) -> Result<u32, Error> {
        let req = self.request_id();
        self.send(&Message::ActionRun(Box::new(ActionRun { req, request })))?;
        Ok(req)
    }

    /// Asks once for the actions `filter` selects, as [`Event::Action`]s.
    pub fn action_query(&mut self, filter: ActionQuery) -> Result<u32, Error> {
        let req = self.request_id();
        self.send(&Message::ActionQuery(ActionFilterRequest { req, filter }))?;
        Ok(req)
    }

    /// Watches the actions `filter` selects: their current states, then each change.
    pub fn action_watch(&mut self, filter: ActionQuery) -> Result<u32, Error> {
        let req = self.request_id();
        self.send(&Message::ActionWatch(ActionFilterRequest { req, filter }))?;
        Ok(req)
    }

    /// Asks once for the records `filter` selects, as [`Event::Record`]s.
    pub fn record_query(&mut self, filter: RecordFilter) -> Result<u32, Error> {
        let req = self.request_id();
        self.send(&Message::RecordQuery(RecordRequest { req, filter }))?;
        Ok(req)
    }

    /// Watches the records `filter` selects: the current set (a bootstrap), then each change.
    pub fn record_watch(&mut self, filter: RecordFilter) -> Result<u32, Error> {
        let req = self.request_id();
        self.send(&Message::RecordWatch(RecordRequest { req, filter }))?;
        Ok(req)
    }

    /// Subscribes to a stream: its catalog ([`Event::StreamSeries`]), the backfill, then each
    /// batch ([`Event::StreamSamples`]), until [`Self::end`].
    pub fn stream_subscribe(&mut self, subscription: StreamSubscription) -> Result<u32, Error> {
        let req = self.request_id();
        self.kinds.insert(req, Vec::new());
        self.send(&Message::StreamSubscribe(Box::new(StreamSubscribe {
            req,
            subscription,
        })))?;
        Ok(req)
    }

    /// Ends the watch or subscription `req`.
    pub fn end(&mut self, req: u32) -> Result<(), Error> {
        self.kinds.remove(&req);
        self.send(&Message::RequestEnd { req })
    }

    /// The next event, waiting up to `timeout` for one (`None` waits as long as it takes). Pings
    /// while it waits. The event stays first until [`Self::pop_event`].
    pub fn next_event(&mut self, timeout: Option<Duration>) -> Result<Option<&Event>, Error> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        loop {
            self.service()?;
            if !self.events.is_empty() {
                return Ok(self.events.front());
            }
            let left = deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
            let wait = match left {
                Some(left) => left.min(self.service_in()),
                None => self.service_in(),
            };
            if wait_readable(self.fd(), Some(wait))? {
                self.read_once()?;
            } else if left.is_some_and(|left| left <= wait) {
                self.service()?;
                return Ok(self.events.front());
            }
        }
    }

    /// Drops the first event (the one [`Self::next_event`] returned).
    pub fn pop_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    fn request_id(&mut self) -> u32 {
        self.next_req = self.next_req.wrapping_add(1).max(1);
        self.next_req
    }

    fn service(&mut self) -> Result<(), Error> {
        if self.last_sent.elapsed() >= self.ping_every {
            self.send(&Message::Ping { now_ms: 0 })?;
        }
        Ok(())
    }

    fn send(&mut self, message: &Message) -> Result<(), Error> {
        self.seq = self.seq.wrapping_add(1);
        let len = message
            .encode(self.seq, &mut self.frame[..])
            .map_err(|error| Error::Protocol(error.to_string()))?;
        self.out.clear();
        self.out
            .extend(StreamEncoder::for_frame(&self.frame[..len]));
        self.stream.write_all(&self.out)?;
        self.last_sent = Instant::now();
        Ok(())
    }

    /// One read of what the node sent; its messages become events.
    fn read_once(&mut self) -> Result<(), Error> {
        let n = match self.stream.read(&mut self.read[..]) {
            Ok(0) => return Err(Error::Closed),
            Ok(n) => n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let mut at = 0;
        while at < n {
            let (used, frame) = self.decoder.push_slice(&self.read[at..n]);
            at += used;
            // A corrupt frame is dropped (the decoder resynchronizes); the node resends state.
            let Ok(Some(frame)) = frame else { continue };
            let message =
                Message::decode(&frame).map_err(|error| Error::Protocol(error.to_string()));
            match message? {
                Message::Welcome(welcome) => {
                    self.welcome = welcome;
                    self.welcomed = true;
                }
                Message::Reject(reason) => return Err(Error::Rejected(reason)),
                Message::StatusChanges(changes) => self.take_changes(changes),
                Message::ActionResults(results) => self.take_actions(results),
                Message::Records(records) => self.take_records(records),
                Message::StreamCatalog(catalog) => self.take_catalog(catalog),
                Message::StreamData(data) => self.take_samples(data)?,
                Message::StreamReset(reset) => self.events.push_back(Event::StreamReset {
                    req: reset.req,
                    reason: reset.reason,
                }),
                Message::RequestError(error) => self.events.push_back(Event::RequestError {
                    req: error.req,
                    code: error.code.0,
                    reason: error.reason,
                }),
                // Pongs, acks and kinds this client does not use.
                _ => {}
            }
        }
        Ok(())
    }

    fn take_changes(&mut self, changes: StatusChanges) {
        let StatusChanges {
            req,
            bootstrap,
            more,
            updated,
            expired,
        } = changes;
        self.events
            .extend(updated.into_iter().map(|entry| Event::Status {
                req,
                bootstrap,
                entry,
            }));
        self.events
            .extend(expired.into_iter().map(|key| Event::Expired { req, key }));
        if !more {
            self.events.push_back(Event::Answered { req, bootstrap });
        }
    }

    fn take_actions(&mut self, answer: ActionResults) {
        let ActionResults { req, more, results } = answer;
        self.events
            .extend(results.into_iter().map(|result| Event::Action {
                req,
                result: Box::new(result),
            }));
        if !more {
            self.events.push_back(Event::Answered {
                req,
                bootstrap: false,
            });
        }
    }

    fn take_records(&mut self, answer: Records) {
        let Records {
            req,
            bootstrap,
            more,
            updated,
            removed,
        } = answer;
        self.events
            .extend(updated.into_iter().map(|record| Event::Record {
                req,
                bootstrap,
                record: Box::new(record),
            }));
        self.events.extend(
            removed
                .into_iter()
                .map(|key| Event::RecordRemoved { req, key }),
        );
        if !more {
            self.events.push_back(Event::Answered { req, bootstrap });
        }
    }

    fn take_catalog(&mut self, catalog: StreamCatalog) {
        let StreamCatalog {
            req,
            epoch,
            series,
            live,
        } = catalog;
        self.kinds
            .insert(req, series.iter().map(|series| series.kind).collect());
        self.events
            .extend(
                series
                    .into_iter()
                    .enumerate()
                    .map(|(index, series)| Event::StreamSeries {
                        req,
                        epoch,
                        index: u16::try_from(index).unwrap_or(u16::MAX),
                        live,
                        series,
                    }),
            );
    }

    fn take_samples(&mut self, data: StreamData) -> Result<(), Error> {
        let kinds = self.kinds.get(&data.req).map_or(&[][..], Vec::as_slice);
        for chunk in &data.chunks {
            let Some(&kind) = kinds.get(usize::from(chunk.series)) else {
                return Err(Error::Protocol(format!(
                    "samples of series {} before the catalog lists it",
                    chunk.series
                )));
            };
            let samples = chunk
                .samples(kind)
                .map_err(|error| Error::Protocol(error.to_string()))?;
            // A batch may hold no sample of a series (an empty backfill).
            if samples.is_empty() {
                continue;
            }
            self.events.push_back(Event::StreamSamples {
                req: data.req,
                cursor: data.cursor,
                backfill: data.backfill,
                series: chunk.series,
                samples,
            });
        }
        if !data.more {
            self.events.push_back(Event::Answered {
                req: data.req,
                bootstrap: data.backfill,
            });
        }
        Ok(())
    }
}

/// Whether `fd` became readable within `timeout`.
pub(crate) fn wait_readable(fd: RawFd, timeout: Option<Duration>) -> io::Result<bool> {
    let mut poll = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let ms = timeout.map_or(-1, |timeout| {
        // Round up, so a wait never returns before its time.
        i32::try_from(timeout.as_micros().div_ceil(1_000)).unwrap_or(i32::MAX)
    });
    loop {
        // SAFETY: one valid pollfd for the duration of the call.
        let ready = unsafe { libc::poll(&mut poll, 1, ms) };
        if ready >= 0 {
            return Ok(ready > 0);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}
