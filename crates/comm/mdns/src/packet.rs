//! DNS messages as mDNS carries them (RFC 1035 §4, RFC 6762 §18): what is read from the
//! network and what is written to it.
//!
//! Only what a responder needs is understood: questions, and records of the types it
//! announces (A, AAAA, PTR, SRV, TXT). Any other record is kept as its bytes, so a probe's
//! rival can still be compared with it. Names are read through compression pointers, never
//! more pointers than the message has bytes, and are written whole.

/// A host address.
pub const A: u16 = 1;
/// A pointer to a name: a service's instances.
pub const PTR: u16 = 12;
/// Text: a service's properties.
pub const TXT: u16 = 16;
/// A host address, IPv6.
pub const AAAA: u16 = 28;
/// Where a service is: host and port.
pub const SRV: u16 = 33;
/// Which types a name has (RFC 6762 §6.1): said with an answer, for those it has not.
pub const NSEC: u16 = 47;
/// Any type, asked for.
pub const ANY: u16 = 255;

/// The internet class.
pub const IN: u16 = 1;
/// In a question's class: the answer may be sent to the asker alone.
pub const UNICAST: u16 = 0x8000;
/// In a record's class: the record replaces what is cached under its name and type.
pub const FLUSH: u16 = 0x8000;

/// The longest name, in bytes, written as a DNS name is.
const MAX_NAME: usize = 255;
/// The longest label.
pub const MAX_LABEL: usize = 63;

/// A name: its labels, as they were said. Two names are the same when their labels are the
/// same in ASCII without regard to case.
#[derive(Clone, Debug, Default, Eq)]
pub struct Name(pub Vec<Vec<u8>>);

impl PartialEq for Name {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(&other.0)
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
    }
}

impl Name {
    /// The name of these labels, or `None` if one is empty or too long, or all are.
    #[must_use]
    pub fn new<I, L>(labels: I) -> Option<Self>
    where
        I: IntoIterator<Item = L>,
        L: AsRef<[u8]>,
    {
        let labels: Vec<Vec<u8>> = labels.into_iter().map(|l| l.as_ref().to_vec()).collect();
        let written: usize = labels.iter().map(|label| label.len() + 1).sum::<usize>() + 1;
        (labels
            .iter()
            .all(|label| !label.is_empty() && label.len() <= MAX_LABEL)
            && written <= MAX_NAME)
            .then_some(Self(labels))
    }

    /// The name of a dotted text (`_orion._tcp.local`), each part a label.
    #[must_use]
    pub fn dotted(text: &str) -> Option<Self> {
        Self::new(text.trim_end_matches('.').split('.'))
    }

    /// This name with `label` before it (an instance before its service).
    #[must_use]
    pub fn under(&self, label: &[u8]) -> Option<Self> {
        Self::new(std::iter::once(label).chain(self.0.iter().map(Vec::as_slice)))
    }

    /// The name in lowercase, as written: what RFC 6762 §8.2 compares.
    fn canonical(&self, out: &mut Vec<u8>) {
        for label in &self.0 {
            out.push(label.len() as u8);
            out.extend(label.iter().map(u8::to_ascii_lowercase));
        }
        out.push(0);
    }

    fn write(&self, out: &mut Vec<u8>) {
        for label in &self.0 {
            out.push(label.len() as u8);
            out.extend_from_slice(label);
        }
        out.push(0);
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for label in &self.0 {
            write!(f, "{}.", String::from_utf8_lossy(label))?;
        }
        Ok(())
    }
}

/// What a record says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Data {
    A([u8; 4]),
    Aaaa([u8; 16]),
    Ptr(Name),
    Srv {
        priority: u16,
        weight: u16,
        port: u16,
        target: Name,
    },
    /// Its strings, each at most 255 bytes.
    Txt(Vec<Vec<u8>>),
    /// The types its name has, all below 256.
    Nsec(Name, Vec<u16>),
    /// A type this responder does not read: its type, and its bytes as they came.
    Other(u16, Vec<u8>),
}

impl Data {
    /// Its type's number.
    #[must_use]
    pub const fn kind(&self) -> u16 {
        match self {
            Self::A(_) => A,
            Self::Aaaa(_) => AAAA,
            Self::Ptr(_) => PTR,
            Self::Srv { .. } => SRV,
            Self::Txt(_) => TXT,
            Self::Nsec(..) => NSEC,
            Self::Other(kind, _) => *kind,
        }
    }

    /// Written with names whole and in lowercase: what a probe's tie is broken on.
    #[must_use]
    pub fn canonical(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::Ptr(name) => name.canonical(&mut out),
            Self::Srv {
                priority,
                weight,
                port,
                target,
            } => {
                for number in [priority, weight, port] {
                    out.extend_from_slice(&number.to_be_bytes());
                }
                target.canonical(&mut out);
            }
            Self::Nsec(name, kinds) => {
                name.canonical(&mut out);
                bitmap(kinds, &mut out);
            }
            _ => self.write(&mut out),
        }
        out
    }

    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::A(address) => out.extend_from_slice(address),
            Self::Aaaa(address) => out.extend_from_slice(address),
            Self::Ptr(name) => name.write(out),
            Self::Srv {
                priority,
                weight,
                port,
                target,
            } => {
                for number in [priority, weight, port] {
                    out.extend_from_slice(&number.to_be_bytes());
                }
                target.write(out);
            }
            Self::Txt(strings) if strings.is_empty() => out.push(0),
            Self::Txt(strings) => {
                for string in strings {
                    out.push(string.len().min(255) as u8);
                    out.extend_from_slice(&string[..string.len().min(255)]);
                }
            }
            Self::Nsec(name, kinds) => {
                name.write(out);
                bitmap(kinds, out);
            }
            Self::Other(_, bytes) => out.extend_from_slice(bytes),
        }
    }
}

/// The types below 256 as NSEC's one window says them (RFC 4034 §4.1.2).
fn bitmap(kinds: &[u16], out: &mut Vec<u8>) {
    let mut bits = [0_u8; 32];
    for &kind in kinds.iter().filter(|&&kind| kind < 256) {
        bits[usize::from(kind / 8)] |= 0x80 >> (kind % 8);
    }
    let used = bits
        .iter()
        .rposition(|&byte| byte != 0)
        .map_or(0, |at| at + 1);
    out.extend_from_slice(&[0, used as u8]);
    out.extend_from_slice(&bits[..used]);
}

/// A record: a name, its class word (with [`FLUSH`]), how long it may be kept, and what it
/// says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub name: Name,
    pub class: u16,
    pub ttl: u32,
    pub data: Data,
}

/// A question: a name, a type and its class word (with [`UNICAST`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Question {
    pub name: Name,
    pub kind: u16,
    pub class: u16,
}

impl Question {
    /// Whether `record` answers it.
    #[must_use]
    pub fn asks(&self, record: &Record) -> bool {
        (self.kind == ANY || self.kind == record.data.kind())
            && matches!(self.class & !UNICAST, IN | ANY)
            && self.name == record.name
    }
}

/// A message: a query, a probe (a query with records in its authority section) or a
/// response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Message {
    pub id: u16,
    /// Whether it is a response.
    pub response: bool,
    pub questions: Vec<Question>,
    pub answers: Vec<Record>,
    pub authority: Vec<Record>,
    pub additional: Vec<Record>,
}

/// Why a message could not be read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unreadable(pub &'static str);

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], Unreadable> {
        let end = self
            .at
            .checked_add(count)
            .filter(|&end| end <= self.bytes.len())
            .ok_or(Unreadable("it ends early"))?;
        let taken = &self.bytes[self.at..end];
        self.at = end;
        Ok(taken)
    }

    fn u16(&mut self) -> Result<u16, Unreadable> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> Result<u32, Unreadable> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// A name at the reader, through any pointers: the reader is left after it as written.
    fn name(&mut self) -> Result<Name, Unreadable> {
        let mut labels = Vec::new();
        let mut written = 1;
        let mut at = self.at;
        let mut after = None;
        // A pointer per byte of the message at most: more is a loop.
        let mut jumps = 0;
        loop {
            let &length = self.bytes.get(at).ok_or(Unreadable("a name ends early"))?;
            match length {
                0 => break,
                1..=63 => {
                    let label = self
                        .bytes
                        .get(at + 1..at + 1 + usize::from(length))
                        .ok_or(Unreadable("a label ends early"))?;
                    written += label.len() + 1;
                    if written > MAX_NAME {
                        return Err(Unreadable("a name is too long"));
                    }
                    labels.push(label.to_vec());
                    at += 1 + usize::from(length);
                }
                0xC0..=0xFF => {
                    let &low = self
                        .bytes
                        .get(at + 1)
                        .ok_or(Unreadable("a pointer ends early"))?;
                    after.get_or_insert(at + 2);
                    jumps += 1;
                    if jumps > self.bytes.len() {
                        return Err(Unreadable("a name's pointers loop"));
                    }
                    at = usize::from(u16::from_be_bytes([length & 0x3F, low]));
                }
                _ => return Err(Unreadable("a label of an unknown kind")),
            }
        }
        self.at = after.unwrap_or(at + 1);
        Ok(Name(labels))
    }

    fn question(&mut self) -> Result<Question, Unreadable> {
        Ok(Question {
            name: self.name()?,
            kind: self.u16()?,
            class: self.u16()?,
        })
    }

    fn record(&mut self) -> Result<Record, Unreadable> {
        let name = self.name()?;
        let kind = self.u16()?;
        let class = self.u16()?;
        let ttl = self.u32()?;
        let length = usize::from(self.u16()?);
        let start = self.at;
        let end = start + length;
        if end > self.bytes.len() {
            return Err(Unreadable("a record ends early"));
        }
        let data = match kind {
            A => Data::A(self.take(4)?.try_into().map_err(|_| Unreadable("A"))?),
            AAAA => Data::Aaaa(self.take(16)?.try_into().map_err(|_| Unreadable("AAAA"))?),
            PTR => Data::Ptr(self.name()?),
            SRV => Data::Srv {
                priority: self.u16()?,
                weight: self.u16()?,
                port: self.u16()?,
                target: self.name()?,
            },
            TXT => {
                let mut strings = Vec::new();
                while self.at < end {
                    let length = usize::from(self.take(1)?[0]);
                    strings.push(self.take(length)?.to_vec());
                }
                strings.retain(|string| !string.is_empty());
                Data::Txt(strings)
            }
            _ => Data::Other(kind, self.take(length)?.to_vec()),
        };
        if self.at != end {
            return Err(Unreadable("a record's data is not its length"));
        }
        Ok(Record {
            name,
            class,
            ttl,
            data,
        })
    }
}

impl Message {
    /// Reads a message.
    ///
    /// # Errors
    ///
    /// What in it could not be read.
    pub fn read(bytes: &[u8]) -> Result<Self, Unreadable> {
        let mut reader = Reader { bytes, at: 0 };
        let id = reader.u16()?;
        let flags = reader.u16()?;
        let counts = [reader.u16()?, reader.u16()?, reader.u16()?, reader.u16()?];
        // Each takes at least five bytes: more than the message holds is a lie.
        if counts
            .iter()
            .map(|&count| usize::from(count))
            .sum::<usize>()
            * 5
            > bytes.len()
        {
            return Err(Unreadable("it counts more than it holds"));
        }
        let mut message = Self {
            id,
            response: flags & 0x8000 != 0,
            ..Self::default()
        };
        for _ in 0..counts[0] {
            message.questions.push(reader.question()?);
        }
        for (count, into) in counts[1..].iter().zip([
            &mut message.answers,
            &mut message.authority,
            &mut message.additional,
        ]) {
            for _ in 0..*count {
                into.push(reader.record()?);
            }
        }
        Ok(message)
    }

    /// The message as it is sent.
    #[must_use]
    pub fn write(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(512);
        out.extend_from_slice(&self.id.to_be_bytes());
        // A response is authoritative (RFC 6762 §18.4); a query says nothing more.
        let flags: u16 = if self.response { 0x8400 } else { 0 };
        out.extend_from_slice(&flags.to_be_bytes());
        for count in [
            self.questions.len(),
            self.answers.len(),
            self.authority.len(),
            self.additional.len(),
        ] {
            out.extend_from_slice(&(count.min(usize::from(u16::MAX)) as u16).to_be_bytes());
        }
        for question in &self.questions {
            question.name.write(&mut out);
            out.extend_from_slice(&question.kind.to_be_bytes());
            out.extend_from_slice(&question.class.to_be_bytes());
        }
        for record in self
            .answers
            .iter()
            .chain(&self.authority)
            .chain(&self.additional)
        {
            record.name.write(&mut out);
            out.extend_from_slice(&record.data.kind().to_be_bytes());
            out.extend_from_slice(&record.class.to_be_bytes());
            out.extend_from_slice(&record.ttl.to_be_bytes());
            let length_at = out.len();
            out.extend_from_slice(&[0, 0]);
            record.data.write(&mut out);
            let length = (out.len() - length_at - 2) as u16;
            out[length_at..length_at + 2].copy_from_slice(&length.to_be_bytes());
        }
        out
    }
}
