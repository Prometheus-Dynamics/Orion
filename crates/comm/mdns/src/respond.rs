//! The responder, without its sockets: what it sends, given what it hears and the time.
//!
//! It claims its names as RFC 6762 §8 says: it waits up to 250 ms, probes three times 250 ms
//! apart, and announces twice a second apart. A name someone else answers for is lost: the
//! responder takes `<name>-<4 hex of its key's fingerprint>` and probes again. Two probing
//! for one name at once are told apart by their records (§8.2); the lesser waits a second and
//! probes again. Once its names are its own it answers questions for them, and defends them
//! against a probe by answering at once.
//!
//! Each interface is answered with its own addresses. What the responder hears from an
//! address of its own is its own, come back: it is not a rival.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};

use crate::packet::{
    A, AAAA, ANY, Data, FLUSH, IN, MAX_LABEL, Message, Name, Question, Record, SRV, TXT, UNICAST,
};

/// How long a host's addresses and a service's place may be kept (RFC 6762 §10).
pub const HOST_TTL: u32 = 120;
/// How long anything else may be.
pub const OTHER_TTL: u32 = 4500;
/// Between probes, and the longest wait before the first.
const PROBE: Duration = Duration::from_millis(250);
/// Between announcements, and after a probe that lost.
const SECOND: Duration = Duration::from_secs(1);
/// After this many conflicts in [`CONFLICT_WINDOW`], probing waits [`CONFLICT_WAIT`] (§8.1).
const CONFLICTS: usize = 15;
const CONFLICT_WINDOW: Duration = Duration::from_secs(10);
const CONFLICT_WAIT: Duration = Duration::from_secs(5);
/// The port of mDNS: a question from another is a plain DNS client's (§6.7).
pub const PORT: u16 = 5353;
/// The longest a plain DNS client is told to keep an answer.
const LEGACY_TTL: u32 = 10;

/// Where a service can be reached: the address it listens on, and the interfaces it is kept
/// to, if it is kept to some.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reach {
    /// Unspecified for every address.
    pub addr: IpAddr,
    /// None for every interface.
    pub interfaces: Vec<String>,
}

impl Reach {
    /// Every address of every interface.
    #[must_use]
    pub const fn everywhere() -> Self {
        Self {
            addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            interfaces: Vec::new(),
        }
    }

    /// Whether it can be reached on `interface`.
    #[must_use]
    pub fn on(&self, interface: &Interface) -> bool {
        (self.interfaces.is_empty() || self.interfaces.contains(&interface.name))
            && match self.addr {
                addr if addr.is_unspecified() => true,
                IpAddr::V4(v4) => interface.v4.contains(&v4),
                IpAddr::V6(v6) => interface.v6.contains(&v6),
            }
    }
}

/// A service the responder announces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Service {
    /// Its type, as `_orion._tcp`.
    pub kind: &'static str,
    pub port: u16,
    /// Its TXT strings, `key=value` each.
    pub txt: Vec<Vec<u8>>,
    /// Where it is announced: only where it can be reached.
    pub reach: Reach,
}

/// What the responder announces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Announce {
    /// The name it would have: the node's.
    pub name: String,
    /// The 4 hex digits a lost name takes after it.
    pub tag: String,
    pub services: Vec<Service>,
}

impl Announce {
    /// Whether anything is announced on `interface`: whether a service can be reached there.
    #[must_use]
    pub fn reaches(&self, interface: &Interface) -> bool {
        self.services
            .iter()
            .any(|service| service.reach.on(interface))
    }
}

/// A network interface it answers on, and its addresses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interface {
    pub name: String,
    pub index: u32,
    pub v4: Vec<Ipv4Addr>,
    pub v6: Vec<Ipv6Addr>,
}

/// A message to send on interface `interface`: to the group, or to one address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Send {
    pub interface: u32,
    pub to: Option<SocketAddr>,
    pub message: Message,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Probing, with this many probes sent.
    Probing(u8),
    /// Its names are its own.
    Up,
}

#[derive(Debug)]
struct Announcing {
    interface: u32,
    sent: u8,
    next: Instant,
}

/// The responder.
#[derive(Debug)]
pub struct Responder {
    announce: Announce,
    /// The name it has, or is probing for.
    label: String,
    /// How many names it has lost.
    lost: usize,
    phase: Phase,
    next: Instant,
    interfaces: Vec<Interface>,
    announcing: Vec<Announcing>,
    /// When each record was last sent to the group on an interface, by name and type.
    sent: Vec<(u32, Name, u16, Instant)>,
    conflicts: Vec<Instant>,
    /// Answers held back a little, as answers with shared records are (§6).
    held: Vec<(Instant, Send)>,
    random: u64,
}

/// The name a host may have: letters, digits and `-`.
fn host_label(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// The `.local` domain.
fn local() -> Name {
    Name(vec![b"local".to_vec()])
}

impl Responder {
    /// A responder that begins claiming its names at `now`; `seed` varies its waits.
    #[must_use]
    pub fn new(announce: Announce, now: Instant, seed: u64) -> Self {
        let label = announce.name.clone();
        let mut responder = Self {
            announce,
            label,
            lost: 0,
            phase: Phase::Probing(0),
            next: now,
            interfaces: Vec::new(),
            announcing: Vec::new(),
            sent: Vec::new(),
            conflicts: Vec::new(),
            held: Vec::new(),
            random: seed | 1,
        };
        responder.next = now + responder.jitter(0, 250);
        responder
    }

    /// A wait between `low` and `high` milliseconds.
    fn jitter(&mut self, low: u64, high: u64) -> Duration {
        // xorshift: the waits only need to differ between responders.
        self.random ^= self.random << 13;
        self.random ^= self.random >> 7;
        self.random ^= self.random << 17;
        Duration::from_millis(low + self.random % (high - low + 1))
    }

    /// Announces on `interface` from `now`, twice, from the first time again if it was.
    fn announce_on(&mut self, interface: u32, now: Instant) {
        self.announcing.retain(|a| a.interface != interface);
        self.announcing.push(Announcing {
            interface,
            sent: 0,
            next: now,
        });
    }

    /// Gives the service of type `kind` the TXT strings `txt`. Changed, they are announced
    /// again on every interface (RFC 6762 §8.4), once the names are its own.
    pub fn retell(&mut self, kind: &str, txt: Vec<Vec<u8>>, now: Instant) {
        let Some(service) = self
            .announce
            .services
            .iter_mut()
            .find(|service| service.kind == kind)
        else {
            return;
        };
        if service.txt == txt {
            return;
        }
        service.txt = txt;
        if self.up() {
            let on: Vec<u32> = self.interfaces.iter().map(|i| i.index).collect();
            for interface in on {
                self.announce_on(interface, now);
            }
        }
    }

    /// The name it has, or is probing for.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.label
    }

    /// Whether anything is announced on `interface`.
    #[must_use]
    pub fn reaches(&self, interface: &Interface) -> bool {
        self.announce.reaches(interface)
    }

    /// Whether its names are its own.
    #[must_use]
    pub fn up(&self) -> bool {
        self.phase == Phase::Up
    }

    /// The interfaces it answers on, as they are now. One that is new, or whose addresses
    /// changed, is announced on again.
    pub fn interfaces(&mut self, mut interfaces: Vec<Interface>, now: Instant) {
        interfaces.retain(|interface| self.announce.reaches(interface));
        for interface in &interfaces {
            let old = self
                .interfaces
                .iter()
                .find(|old| old.index == interface.index);
            if old != Some(interface) && self.up() {
                self.announce_on(interface.index, now);
            }
        }
        self.announcing
            .retain(|a| interfaces.iter().any(|i| i.index == a.interface));
        self.sent
            .retain(|(index, ..)| interfaces.iter().any(|i| i.index == *index));
        self.held
            .retain(|(_, send)| interfaces.iter().any(|i| i.index == send.interface));
        self.interfaces = interfaces;
    }

    fn host(&self) -> Name {
        let label = host_label(&self.label);
        local().under(label.as_bytes()).unwrap_or_default()
    }

    fn instance(&self, service: &Service) -> Name {
        Name::dotted(service.kind)
            .and_then(|kind| {
                let mut name = kind;
                name.0.push(b"local".to_vec());
                name.under(self.label.as_bytes())
            })
            .unwrap_or_default()
    }

    /// The names it claims as its own alone.
    fn unique(&self) -> Vec<Name> {
        let mut names = vec![self.host()];
        names.extend(
            self.announce
                .services
                .iter()
                .filter(|s| self.interfaces.iter().any(|i| s.reach.on(i)))
                .map(|s| self.instance(s)),
        );
        names
    }

    /// Its records, with the addresses of `interface`, or of all for `None`.
    fn records(&self, interface: Option<u32>) -> Vec<Record> {
        let host = self.host();
        let record = |name: &Name, class: u16, ttl: u32, data: Data| Record {
            name: name.clone(),
            class,
            ttl,
            data,
        };
        let mut records = Vec::new();
        for each in self
            .interfaces
            .iter()
            .filter(|each| interface.is_none_or(|index| index == each.index))
        {
            for v4 in &each.v4 {
                records.push(record(&host, IN | FLUSH, HOST_TTL, Data::A(v4.octets())));
            }
            for v6 in &each.v6 {
                records.push(record(&host, IN | FLUSH, HOST_TTL, Data::Aaaa(v6.octets())));
            }
        }
        let browse = Name::dotted("_services._dns-sd._udp.local").unwrap_or_default();
        let reached = |service: &Service| {
            self.interfaces
                .iter()
                .filter(|each| interface.is_none_or(|index| index == each.index))
                .any(|each| service.reach.on(each))
        };
        for service in self.announce.services.iter().filter(|s| reached(s)) {
            let Some(mut kind) = Name::dotted(service.kind) else {
                continue;
            };
            kind.0.push(b"local".to_vec());
            let instance = self.instance(service);
            records.push(record(&browse, IN, OTHER_TTL, Data::Ptr(kind.clone())));
            records.push(record(&kind, IN, OTHER_TTL, Data::Ptr(instance.clone())));
            let srv = Data::Srv {
                priority: 0,
                weight: 0,
                port: service.port,
                target: host.clone(),
            };
            records.push(record(&instance, IN | FLUSH, HOST_TTL, srv));
            let txt = Data::Txt(service.txt.clone());
            records.push(record(&instance, IN | FLUSH, OTHER_TTL, txt));
        }
        records
    }

    /// When it next has something to do.
    #[must_use]
    pub fn due(&self) -> Instant {
        let mut due = self.next;
        if self.up() {
            due = self
                .announcing
                .iter()
                .map(|a| a.next)
                .fold(due, Instant::min);
        }
        self.held.iter().map(|(at, _)| *at).fold(due, Instant::min)
    }

    /// Whether `from` is one of its own addresses.
    fn ours(&self, from: IpAddr) -> bool {
        self.interfaces.iter().any(|each| match from {
            IpAddr::V4(v4) => each.v4.contains(&v4),
            IpAddr::V6(v6) => each.v6.contains(&v6),
        })
    }

    /// What it sends now.
    pub fn poll(&mut self, now: Instant) -> Vec<Send> {
        let mut out = Vec::new();
        let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut self.held)
            .into_iter()
            .partition(|(at, _)| *at <= now);
        self.held = later;
        out.extend(due.into_iter().map(|(_, send)| send));
        match self.phase {
            Phase::Probing(sent) if now >= self.next => {
                if sent == 3 {
                    self.phase = Phase::Up;
                    let all: Vec<u32> = self.interfaces.iter().map(|i| i.index).collect();
                    for index in all {
                        self.announce_on(index, now);
                    }
                } else {
                    out.extend(self.probes(sent == 0));
                    self.phase = Phase::Probing(sent + 1);
                    self.next = now + PROBE;
                }
            }
            _ => {}
        }
        if self.up() {
            let mut announced = Vec::new();
            for announcing in &mut self.announcing {
                if announcing.next <= now && announcing.sent < 2 {
                    announcing.sent += 1;
                    announcing.next = now + SECOND;
                    announced.push(announcing.interface);
                }
            }
            self.announcing.retain(|announcing| announcing.sent < 2);
            for index in announced {
                let answers = self.records(Some(index));
                self.mark(index, &answers, now);
                out.push(Send {
                    interface: index,
                    to: None,
                    message: Message {
                        response: true,
                        answers,
                        ..Message::default()
                    },
                });
            }
            self.next = now + Duration::from_secs(3600);
        }
        out
    }

    fn probes(&self, first: bool) -> Vec<Send> {
        let class = if first { IN | UNICAST } else { IN };
        let questions: Vec<Question> = self
            .unique()
            .into_iter()
            .map(|name| Question {
                name,
                kind: ANY,
                class,
            })
            .collect();
        self.interfaces
            .iter()
            .map(|each| Send {
                interface: each.index,
                to: None,
                message: Message {
                    questions: questions.clone(),
                    authority: self
                        .records(Some(each.index))
                        .into_iter()
                        .filter(|r| r.class & FLUSH != 0)
                        .collect(),
                    ..Message::default()
                },
            })
            .collect()
    }

    fn mark(&mut self, interface: u32, records: &[Record], now: Instant) {
        for record in records {
            let kind = record.data.kind();
            match self.sent.iter_mut().find(|(on, name, sent, _)| {
                *on == interface && *sent == kind && *name == record.name
            }) {
                Some(found) => found.3 = now,
                None => self.sent.push((interface, record.name.clone(), kind, now)),
            }
        }
    }

    fn recently(&self, interface: u32, record: &Record, now: Instant, within: Duration) -> bool {
        let kind = record.data.kind();
        self.sent.iter().any(|(on, name, sent, at)| {
            *on == interface
                && *sent == kind
                && *name == record.name
                && now.duration_since(*at) < within
        })
    }

    /// Its name is taken: it takes the next, and probes for it.
    fn lose(&mut self, now: Instant) {
        self.lost += 1;
        self.conflicts
            .retain(|at| now.duration_since(*at) < CONFLICT_WINDOW);
        self.conflicts.push(now);
        let tag = &self.announce.tag;
        let room = MAX_LABEL - tag.len() - 1;
        let mut base: String = self.announce.name.clone();
        while base.len() > room {
            base.pop();
        }
        self.label = match self.lost {
            1 => format!("{base}-{tag}"),
            n => {
                let suffix = format!("-{tag}-{n}");
                while base.len() + suffix.len() > MAX_LABEL {
                    base.pop();
                }
                format!("{base}{suffix}")
            }
        };
        self.restart(now, Duration::ZERO);
    }

    fn restart(&mut self, now: Instant, wait: Duration) {
        let wait = if self.conflicts.len() >= CONFLICTS {
            CONFLICT_WAIT
        } else {
            wait
        };
        self.phase = Phase::Probing(0);
        self.announcing.clear();
        self.held.clear();
        self.next = now + wait + self.jitter(0, 250);
    }

    /// Whether `record` says something of one of its names that it does not say itself.
    fn rival(&self, record: &Record, unique: &[Name], ours: &[Record]) -> bool {
        record.ttl != 0
            && unique.contains(&record.name)
            && !ours
                .iter()
                .any(|own| own.name == record.name && own.data == record.data)
    }

    /// What it heard on `interface` from `from`.
    pub fn heard(&mut self, interface: u32, from: SocketAddr, message: &Message, now: Instant) {
        if from.port() == PORT && self.ours(from.ip()) {
            return;
        }
        let unique = self.unique();
        let ours = self.records(None);
        if message.response {
            let lost = message
                .answers
                .iter()
                .chain(&message.additional)
                .any(|record| self.rival(record, &unique, &ours));
            if lost {
                self.lose(now);
            }
            return;
        }
        if let Phase::Probing(_) = self.phase {
            self.tie(interface, message, &unique, now);
            return;
        }
        self.answer(interface, from, message, now);
    }

    /// Another probing for one of its names now (§8.2): the lesser records wait.
    fn tie(&mut self, interface: u32, message: &Message, unique: &[Name], now: Instant) {
        let ours = self.records(Some(interface));
        for name in unique {
            if !message.questions.iter().any(|q| &q.name == name) {
                continue;
            }
            let sorted = |records: &mut dyn Iterator<Item = &Record>| {
                let mut keyed: Vec<(u16, u16, Vec<u8>)> = records
                    .filter(|r| &r.name == name)
                    .map(|r| (r.class & !FLUSH, r.data.kind(), r.data.canonical()))
                    .collect();
                keyed.sort_unstable();
                keyed
            };
            let theirs = sorted(&mut message.authority.iter());
            if theirs.is_empty() {
                continue;
            }
            let mine = sorted(&mut ours.iter().filter(|r| r.class & FLUSH != 0));
            if mine < theirs {
                self.restart(now, SECOND);
                return;
            }
        }
    }

    fn answer(&mut self, interface: u32, from: SocketAddr, message: &Message, now: Instant) {
        let legacy = from.port() != PORT;
        let probe = !message.authority.is_empty();
        let records = self.records(Some(interface));
        let mut answers: Vec<Record> = Vec::new();
        let mut unicast = legacy;
        for question in &message.questions {
            unicast |= question.class & UNICAST != 0;
            for record in records.iter().filter(|r| question.asks(r)) {
                let known = message.answers.iter().any(|known| {
                    known.name == record.name
                        && known.data == record.data
                        && known.ttl >= record.ttl / 2
                });
                if !known && !answers.contains(record) {
                    answers.push(record.clone());
                }
            }
        }
        if !unicast {
            // A record is sent to the group once a second at most; a probe, four times.
            let within = if probe { PROBE } else { SECOND };
            answers.retain(|record| !self.recently(interface, record, now, within));
        }
        if answers.is_empty() {
            return;
        }
        let mut additional = Vec::new();
        for answer in &answers {
            let wanted: &[(u16, Option<&Name>)] = match &answer.data {
                Data::Ptr(instance) => &[(SRV, Some(instance)), (TXT, Some(instance))],
                Data::Srv { .. } => &[(A, None), (AAAA, None)],
                _ => &[],
            };
            for &(kind, name) in wanted {
                for record in &records {
                    let named = name.map_or(record.name == self.host(), |n| &record.name == n);
                    if named
                        && record.data.kind() == kind
                        && !answers.contains(record)
                        && !additional.contains(record)
                    {
                        additional.push(record.clone());
                    }
                }
            }
            if let Data::Ptr(_) = &answer.data {
                for record in records.iter().filter(|r| r.name == self.host()) {
                    if !answers.contains(record) && !additional.contains(record) {
                        additional.push(record.clone());
                    }
                }
            }
        }
        let shared = answers.iter().any(|record| record.class & FLUSH == 0);
        let mut reply = Message {
            response: true,
            answers,
            additional,
            ..Message::default()
        };
        if legacy {
            // A plain DNS client: its id and questions back, no cache flush, short lives.
            reply.id = message.id;
            reply.questions.clone_from(&message.questions);
            for record in reply.answers.iter_mut().chain(&mut reply.additional) {
                record.class &= !FLUSH;
                record.ttl = record.ttl.min(LEGACY_TTL);
            }
        }
        let send = Send {
            interface,
            to: unicast.then_some(from),
            message: reply,
        };
        if !unicast {
            self.mark(interface, &send.message.answers, now);
        }
        if shared && !probe && !legacy {
            let wait = self.jitter(20, 120);
            self.held.push((now + wait, send));
        } else {
            self.held.push((now, send));
        }
    }

    /// Its records, saying they are no longer (§10.1): sent as it stops.
    #[must_use]
    pub fn goodbye(&self) -> Vec<Send> {
        if !self.up() {
            return Vec::new();
        }
        self.interfaces
            .iter()
            .map(|each| {
                let mut answers = self.records(Some(each.index));
                answers.iter_mut().for_each(|record| record.ttl = 0);
                Send {
                    interface: each.index,
                    to: None,
                    message: Message {
                        response: true,
                        answers,
                        ..Message::default()
                    },
                }
            })
            .collect()
    }
}
