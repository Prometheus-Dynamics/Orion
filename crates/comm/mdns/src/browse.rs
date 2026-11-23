//! The other boards heard on the local network: a browse for `_orion._tcp` on the responder's
//! sockets, and what it has heard, kept while it is heard.
//!
//! - **What is sent.** One question, `_orion._tcp.local PTR`, to the group on each interface:
//!   when the interface comes, once more [`AGAIN`] after, and every [`ASK`] after that. Every
//!   board that hears it answers to the group with all it announces, so one question serves
//!   every board that browses: a board that hears another ask puts its own question off.
//!   The second question at the start is for the board that announced itself in the second
//!   before the first: it does not say again so soon what it has just said (RFC 6762 §6).
//! - **What is kept.** An instance with its SRV and TXT records, and the addresses of its
//!   host. It is dropped at its goodbye, and [`KEEP`] after it was last heard: two questions
//!   may go unanswered.
//! - **Bounded.** [`MOST`] instances; one more is not kept, and counted. An instance keeps
//!   [`MOST_ADDRESSES`] addresses, and a TXT record of [`MOST_TXT`] bytes at most.
//! - **A board this one knows has a place.** With [`Browser::known`] set, an instance whose
//!   TXT strings it says yes to takes the place of the one longest unheard that it says no
//!   to, when all places are taken: names nobody knows cannot keep a known board out. An
//!   announcement can say whatever it likes, a known board's name and key too, so this keeps
//!   known boards listed; it does not make what is listed true.
//!
//! Only what comes from the mDNS port is heard, as RFC 6762 §6 has it.
//!
//! **What is heard proves nothing**: anyone on the network can announce anything.

use std::net::IpAddr;
use std::time::{Duration, Instant};

use crate::packet::{Data, IN, Message, Name, PTR, Question, Record, UNICAST};
use crate::respond::Send;

/// How often the question is asked on an interface, when nobody else asked.
pub const ASK: Duration = Duration::from_secs(60);
/// How long after the first question on an interface the second is asked.
pub const AGAIN: Duration = Duration::from_secs(1);
/// How long an instance is kept after it was last heard.
pub const KEEP: Duration = Duration::from_secs(150);
/// The most instances kept.
pub const MOST: usize = 64;
/// The most addresses kept for an instance.
pub const MOST_ADDRESSES: usize = 8;
/// The longest TXT record kept, all its strings together.
pub const MOST_TXT: usize = 1024;

/// One board heard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    /// Its instance name: the label before `_orion._tcp.local`.
    pub instance: String,
    /// The port of its SRV record.
    pub port: u16,
    /// Its host's addresses, as heard.
    pub addresses: Vec<IpAddr>,
    /// Its TXT strings, each `key=value`.
    pub txt: Vec<Vec<u8>>,
    /// When it was last heard.
    pub heard: Instant,
}

/// Whether TXT strings are those of a board this one knows.
pub type Known = Box<dyn Fn(&[Vec<u8>]) -> bool>;

#[derive(Debug)]
struct Kept {
    instance: Name,
    /// The SRV record: the host and the port.
    at: Option<(Name, u16)>,
    txt: Option<Vec<Vec<u8>>>,
    addresses: Vec<(IpAddr, Instant)>,
    heard: Instant,
    /// Its TXT strings are a known board's.
    known: bool,
}

/// The browse.
pub struct Browser {
    service: Name,
    kept: Vec<Kept>,
    /// When the question is next asked on each interface, and how many of the two of its
    /// start are still to ask.
    asks: Vec<(u32, Instant, u8)>,
    refused: u64,
    /// One was refused in the message being heard: counted once for it.
    refusing: bool,
    changes: u64,
    random: u64,
    known: Option<Known>,
}

impl std::fmt::Debug for Browser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Browser({} kept)", self.kept.len())
    }
}

impl Browser {
    /// A browse for the service `kind` (as [`crate::ORION`]).
    #[must_use]
    pub fn new(kind: &str, seed: u64) -> Self {
        Self {
            service: Name::dotted(&format!("{kind}.local")).unwrap_or_default(),
            kept: Vec::new(),
            asks: Vec::new(),
            refused: 0,
            refusing: false,
            changes: 0,
            random: seed | 1,
            known: None,
        }
    }

    /// Which boards this one knows, by their TXT strings: they always have a place.
    pub fn known(&mut self, known: Known) {
        self.known = Some(known);
    }

    fn jitter(&mut self, low: u64, high: u64) -> Duration {
        self.random ^= self.random << 13;
        self.random ^= self.random >> 7;
        self.random ^= self.random << 17;
        Duration::from_millis(low + self.random % (high - low + 1))
    }

    /// The interfaces there are now: a new one is asked on shortly.
    pub fn interfaces(&mut self, interfaces: &[u32], now: Instant) {
        self.asks.retain(|(index, ..)| interfaces.contains(index));
        for &index in interfaces {
            if !self.asks.iter().any(|(asked, ..)| *asked == index) {
                let wait = self.jitter(20, 120);
                self.asks.push((index, now + wait, 2));
            }
        }
    }

    /// The instance a record of the service is of, if it is one.
    fn of(&self, name: &Name) -> bool {
        name.0
            .split_first()
            .is_some_and(|(_, rest)| Name(rest.to_vec()) == self.service)
    }

    fn drop_instance(&mut self, instance: &Name) {
        let before = self.kept.len();
        self.kept.retain(|kept| &kept.instance != instance);
        if self.kept.len() != before {
            self.changes += 1;
        }
    }

    /// The kept instance `instance`, made if there is room, or if it is `known` and one that
    /// is not can make room.
    fn keep(&mut self, instance: &Name, known: bool, now: Instant) -> Option<&mut Kept> {
        let at = match self.kept.iter().position(|k| &k.instance == instance) {
            Some(at) => at,
            None => {
                if self.kept.len() >= MOST {
                    let oldest = self
                        .kept
                        .iter()
                        .enumerate()
                        .filter(|(_, kept)| known && !kept.known)
                        .min_by_key(|(_, kept)| kept.heard)
                        .map(|(at, _)| at);
                    let Some(oldest) = oldest else {
                        self.refused += u64::from(!self.refusing);
                        self.refusing = true;
                        return None;
                    };
                    self.kept.remove(oldest);
                    self.changes += 1;
                }
                // In the order of their names, so what is found is.
                let at = self
                    .kept
                    .partition_point(|kept| kept.instance.0 < instance.0);
                self.kept.insert(
                    at,
                    Kept {
                        instance: instance.clone(),
                        at: None,
                        txt: None,
                        addresses: Vec::new(),
                        heard: now,
                        known,
                    },
                );
                at
            }
        };
        let kept = self.kept.get_mut(at)?;
        kept.heard = now;
        Some(kept)
    }

    fn record(&mut self, record: &Record, now: Instant) {
        match &record.data {
            Data::Ptr(instance) if record.name == self.service && record.ttl == 0 => {
                self.drop_instance(instance);
            }
            Data::Srv { .. } | Data::Txt(_) if record.ttl == 0 && self.of(&record.name) => {
                self.drop_instance(&record.name);
            }
            Data::Srv { port, target, .. } if self.of(&record.name) => {
                let new = Some((target.clone(), *port));
                let changed = self.keep(&record.name, false, now).is_some_and(|kept| {
                    let changed = kept.at != new;
                    if changed {
                        // Another host: the addresses were the other's.
                        kept.addresses.clear();
                        kept.at = new;
                    }
                    changed
                });
                self.changes += u64::from(changed);
            }
            Data::Txt(strings) if self.of(&record.name) => {
                if strings.iter().map(Vec::len).sum::<usize>() > MOST_TXT {
                    return;
                }
                let known = self.known.as_ref().is_some_and(|known| known(strings));
                let changed = self.keep(&record.name, known, now).is_some_and(|kept| {
                    let changed = kept.txt.as_ref() != Some(strings);
                    if changed {
                        kept.txt = Some(strings.clone());
                    }
                    kept.known = known;
                    changed
                });
                self.changes += u64::from(changed);
            }
            Data::A(_) | Data::Aaaa(_) => {
                let address = match record.data {
                    Data::A(v4) => IpAddr::from(v4),
                    Data::Aaaa(v6) => IpAddr::from(v6),
                    _ => return,
                };
                let mut changed = false;
                for kept in &mut self.kept {
                    if !kept
                        .at
                        .as_ref()
                        .is_some_and(|(host, _)| host == &record.name)
                    {
                        continue;
                    }
                    let known = kept.addresses.iter().position(|(a, _)| *a == address);
                    match (known, record.ttl) {
                        (Some(at), 0) => {
                            kept.addresses.remove(at);
                            changed = true;
                        }
                        (Some(at), _) => {
                            if let Some(entry) = kept.addresses.get_mut(at) {
                                entry.1 = now;
                            }
                        }
                        (None, 0) => {}
                        (None, _) if kept.addresses.len() < MOST_ADDRESSES => {
                            kept.addresses.push((address, now));
                            changed = true;
                        }
                        (None, _) => {}
                    }
                }
                self.changes += u64::from(changed);
            }
            _ => {}
        }
    }

    /// A message heard on `interface` from another: never this board's own.
    pub fn heard(&mut self, interface: u32, message: &Message, now: Instant) {
        if !message.response {
            // Another asked what this one asks, to the group and of everyone: its answers
            // are heard here too.
            let same = message.answers.is_empty()
                && message.questions.iter().any(|question| {
                    question.kind == PTR
                        && question.class & UNICAST == 0
                        && question.name == self.service
                });
            if same {
                let wait = ASK + self.jitter(0, 2000);
                for (index, next, start) in &mut self.asks {
                    // Those of its start are asked all the same: it has heard nothing yet.
                    if *index == interface && *start == 0 {
                        *next = now + wait;
                    }
                }
            }
            return;
        }
        // What an instance says of itself first: it says whether it is known, and has a
        // place. Then where it is, then the addresses of its host.
        self.refusing = false;
        let rank = |record: &Record| match record.data {
            Data::Txt(_) => 0,
            Data::A(_) | Data::Aaaa(_) => 2,
            _ => 1,
        };
        for wanted in 0..3 {
            let records = message.answers.iter().chain(&message.additional);
            for record in records.filter(|record| rank(record) == wanted) {
                self.record(record, now);
            }
        }
    }

    /// When it next has something to do.
    #[must_use]
    pub fn due(&self, now: Instant) -> Instant {
        let far = now + Duration::from_secs(3600);
        let asks = self.asks.iter().map(|(_, at, _)| *at);
        let kept = self.kept.iter().map(|kept| kept.heard + KEEP);
        asks.chain(kept).fold(far, Instant::min)
    }

    /// What it sends now; what was not heard in time is dropped.
    pub fn poll(&mut self, now: Instant) -> Vec<Send> {
        let before = self.kept.len();
        self.kept.retain(|kept| kept.heard + KEEP > now);
        if self.kept.len() != before {
            self.changes += 1;
        }
        for kept in &mut self.kept {
            kept.addresses.retain(|(_, heard)| *heard + KEEP > now);
        }
        let due: Vec<u32> = self
            .asks
            .iter()
            .filter(|(_, at, _)| *at <= now)
            .map(|(index, ..)| *index)
            .collect();
        let mut out = Vec::new();
        for interface in due {
            let wait = ASK + self.jitter(0, 2000);
            for (index, next, start) in &mut self.asks {
                if *index == interface {
                    *start = start.saturating_sub(1);
                    *next = now + if *start > 0 { AGAIN } else { wait };
                }
            }
            out.push(Send {
                interface,
                to: None,
                message: Message {
                    questions: vec![Question {
                        name: self.service.clone(),
                        kind: PTR,
                        class: IN,
                    }],
                    ..Message::default()
                },
            });
        }
        out
    }

    /// The boards heard whole, by name: with where they are and what they say of themselves.
    #[must_use]
    pub fn found(&self) -> Vec<Found> {
        self.kept
            .iter()
            .filter_map(|kept| {
                let (_, port) = kept.at.as_ref()?;
                Some(Found {
                    instance: String::from_utf8_lossy(kept.instance.0.first()?).into_owned(),
                    port: *port,
                    addresses: kept.addresses.iter().map(|(address, _)| *address).collect(),
                    txt: kept.txt.clone()?,
                    heard: kept.heard,
                })
            })
            .collect()
    }

    /// How many announcements were of an instance not kept because [`MOST`] were.
    #[must_use]
    pub const fn refused(&self) -> u64 {
        self.refused
    }

    /// A count that moves when what [`Browser::found`] gives may have changed, a board being
    /// heard again aside.
    #[must_use]
    pub const fn changes(&self) -> u64 {
        self.changes
    }
}
