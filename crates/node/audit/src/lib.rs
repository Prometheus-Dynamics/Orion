//! The audit log: who did what.
//!
//! Every request that changes something, and passed the check, is one line in a file: when,
//! who asked, what they did, to what, and how it went. The file is for people and for tools
//! that read lines, so a line is one JSON object.
//!
//! - **Capped.** The file never grows past `max_bytes`. When the next line would, the file
//!   becomes `<name>.1`, the one that was `.1` becomes `.2`, and so on up to `keep`; the
//!   oldest is deleted. A board's flash is not filled by its own log.
//! - **On its own helper.** [`Audit::record`] formats the line and puts it in a bounded queue.
//!   It never touches the disk and never waits. A thread of its own writes. If the disk is
//!   slower than what is asked of the node, lines are dropped, counted, and the count is
//!   written as a line of its own when there is room again.
//! - **Whole lines.** A file is rotated between lines and never in one.

use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread::JoinHandle;

use tracing::warn;

/// What the log is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditConfig {
    /// The file.
    pub path: PathBuf,
    /// The most bytes the file holds.
    pub max_bytes: u64,
    /// How many files from before are kept beside it.
    pub keep: u32,
    /// How many lines may wait to be written.
    pub queue: usize,
}

impl AuditConfig {
    /// A megabyte at `path`, three files from before, a thousand lines waiting.
    #[must_use]
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_owned(),
            max_bytes: 1 << 20,
            keep: 3,
            queue: 1024,
        }
    }
}

/// Who asked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Who<'a> {
    /// A program on this board, by user id.
    Local(u32),
    /// A person's tool, by key.
    Operator(&'a str),
    /// Another node.
    Peer(&'a str),
    /// A device on a link.
    Device(&'a str),
    /// Nobody known.
    Anonymous,
    /// Orion itself.
    Orion,
}

/// One thing that was done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry<'a> {
    /// When, in milliseconds since 1970.
    pub time_ms: u64,
    /// Who asked.
    pub who: Who<'a>,
    /// What they did: `put`, `run`, `file`.
    pub did: &'a str,
    /// To what: a record's key, an action's name, a file's destination.
    pub what: &'a str,
    /// How it went: `ok`, or why not.
    pub result: &'a str,
}

fn quote(out: &mut String, text: &str) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", ch as u32);
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
}

impl Entry<'_> {
    /// The entry as one line, with its newline.
    #[must_use]
    pub fn line(&self, node: &str) -> String {
        let mut line = String::with_capacity(160);
        let _ = write!(line, "{{\"time_ms\":{},\"node\":", self.time_ms);
        quote(&mut line, node);
        line.push_str(",\"who\":");
        match self.who {
            Who::Local(uid) => {
                let _ = write!(line, "\"local:{uid}\"");
            }
            Who::Operator(key) => quote(&mut line, &format!("operator:{key}")),
            Who::Peer(name) => quote(&mut line, &format!("peer:{name}")),
            Who::Device(name) => quote(&mut line, &format!("device:{name}")),
            Who::Anonymous => line.push_str("\"anonymous\""),
            Who::Orion => line.push_str("\"orion\""),
        }
        line.push_str(",\"did\":");
        quote(&mut line, self.did);
        line.push_str(",\"what\":");
        quote(&mut line, self.what);
        line.push_str(",\"result\":");
        quote(&mut line, self.result);
        line.push_str("}\n");
        line
    }
}

enum Said {
    Line(String),
    /// Everything said before this is on disk when the sender hears back.
    Flush(SyncSender<()>),
}

/// The audit log of one node.
#[derive(Debug)]
pub struct Audit {
    node: String,
    lines: Option<SyncSender<Said>>,
    dropped: Arc<AtomicU64>,
    written: Arc<AtomicU64>,
    writer: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for Said {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Said")
    }
}

impl Audit {
    /// Opens the file, making its directory, and starts the thread that writes it.
    ///
    /// # Errors
    ///
    /// If the file cannot be opened or the thread started.
    pub fn start(node: &str, config: AuditConfig) -> io::Result<Self> {
        if let Some(parent) = config.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.path)?;
        let size = file.metadata()?.len();
        let (lines, waiting) = mpsc::sync_channel(config.queue.max(1));
        let (dropped, written) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let mut writer = Writer {
            node: node.to_owned(),
            config,
            file,
            size,
            dropped: dropped.clone(),
            told_dropped: 0,
            written: written.clone(),
            gathered: Vec::new(),
        };
        let thread = std::thread::Builder::new()
            .name("orion-audit".into())
            .spawn(move || writer.run(&waiting))?;
        Ok(Self {
            node: node.to_owned(),
            lines: Some(lines),
            dropped,
            written,
            writer: Some(thread),
        })
    }

    /// Records one thing that was done. It returns at once: if more is waiting to be written
    /// than there is room for, the entry is dropped and counted.
    pub fn record(&self, entry: &Entry<'_>) {
        let Some(lines) = &self.lines else {
            return;
        };
        match lines.try_send(Said::Line(entry.line(&self.node))) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// How many entries were dropped for want of room.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// How many entries were written.
    #[must_use]
    pub fn written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    /// Waits until everything recorded so far is in the file.
    pub fn flush(&self) {
        let Some(lines) = &self.lines else {
            return;
        };
        let (done, heard) = mpsc::sync_channel(1);
        if lines.send(Said::Flush(done)).is_ok() {
            let _ = heard.recv();
        }
    }
}

impl Drop for Audit {
    fn drop(&mut self) {
        // The writer ends when there is nobody left to say anything, having written what
        // was waiting.
        self.lines = None;
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

struct Writer {
    node: String,
    config: AuditConfig,
    file: File,
    size: u64,
    dropped: Arc<AtomicU64>,
    told_dropped: u64,
    written: Arc<AtomicU64>,
    /// Lines for the file that are not in it yet.
    gathered: Vec<u8>,
}

impl Writer {
    fn run(&mut self, waiting: &Receiver<Said>) {
        while let Ok(first) = waiting.recv() {
            // Whatever else is waiting goes in the same write.
            let mut said = Some(first);
            while let Some(next) = said.take() {
                match next {
                    Said::Line(line) => {
                        self.tell_dropped();
                        if self.write(&line) {
                            self.written.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Said::Flush(done) => {
                        self.tell_dropped();
                        self.put();
                        let _ = self.file.sync_data();
                        let _ = done.send(());
                    }
                }
                said = waiting.try_recv().ok();
            }
            self.put();
        }
        self.tell_dropped();
        self.put();
        let _ = self.file.sync_data();
    }

    /// Puts what was gathered in the file.
    fn put(&mut self) {
        if self.gathered.is_empty() {
            return;
        }
        if let Err(error) = self.file.write_all(&self.gathered) {
            warn!("the audit log could not be written: {error}");
        }
        self.gathered.clear();
    }

    /// Says in the log itself how many entries are missing from it.
    fn tell_dropped(&mut self) {
        let dropped = self.dropped.load(Ordering::Relaxed);
        if dropped == self.told_dropped {
            return;
        }
        let missing = format!("{} entries", dropped - self.told_dropped);
        let line = Entry {
            time_ms: 0,
            who: Who::Orion,
            did: "dropped",
            what: &missing,
            result: "the log could not keep up",
        }
        .line(&self.node);
        if self.write(&line) {
            self.told_dropped = dropped;
        }
    }

    /// Gathers a line to be written, rotating first if it would not fit.
    fn write(&mut self, line: &str) -> bool {
        let len = line.len() as u64;
        if self.size != 0
            && self.size + len > self.config.max_bytes
            && let Err(error) = self.rotate()
        {
            warn!("the audit log could not be rotated: {error}");
            return false;
        }
        self.gathered.extend_from_slice(line.as_bytes());
        self.size += len;
        true
    }

    fn numbered(&self, number: u32) -> PathBuf {
        let mut name = self.config.path.clone().into_os_string();
        name.push(format!(".{number}"));
        PathBuf::from(name)
    }

    /// The file becomes `.1`, `.1` becomes `.2`, and the oldest goes.
    fn rotate(&mut self) -> io::Result<()> {
        self.put();
        let _ = self.file.sync_data();
        if self.config.keep == 0 {
            self.file.set_len(0)?;
            self.size = 0;
            return Ok(());
        }
        let _ = fs::remove_file(self.numbered(self.config.keep));
        for number in (1..self.config.keep).rev() {
            let from = self.numbered(number);
            if from.exists() {
                fs::rename(from, self.numbered(number + 1))?;
            }
        }
        fs::rename(&self.config.path, self.numbered(1))?;
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.config.path)?;
        self.size = 0;
        Ok(())
    }
}
