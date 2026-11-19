//! Where the log goes: lines on standard error, which a console, a service manager or an
//! initramfs's own log all take. One line an event: its level, where it is from, what it
//! says, and its fields.
//!
//! The newest lines are also kept in memory, for the comm core to hand over as its own
//! `orion/log` on a board whose log went nowhere else: each [`Window`] shows up to its own
//! bytes of them.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

/// Writes events at `most` and above in importance to standard error.
struct Lines {
    most: Level,
    spans: AtomicU64,
}

/// An event's message first, then `name=value` for the rest.
#[derive(Default)]
struct Line {
    message: String,
    fields: String,
}

impl Visit for Line {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.fields, " {}={value:?}", field.name());
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            let _ = write!(self.fields, " {}={value}", field.name());
        }
    }
}

impl Subscriber for Lines {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= self.most
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        // Spans are not written; each still needs a number of its own.
        Id::from_u64(self.spans.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut line = Line::default();
        event.record(&mut line);
        let metadata = event.metadata();
        // One write, so that lines from two threads do not run into each other.
        let text = format!(
            "{:5} {}: {}{}\n",
            metadata.level().as_str(),
            metadata.target(),
            line.message,
            line.fields
        );
        let _ = std::io::stderr().lock().write_all(text.as_bytes());
        keep(&text);
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

/// Sends the log to standard error: what `ORION_LOG` names and above (`error`, `warn`,
/// `info`, `debug`, `trace`), or `info` and above.
pub fn install() {
    let most = std::env::var("ORION_LOG")
        .ok()
        .and_then(|level| level.parse().ok())
        .unwrap_or(Level::INFO);
    let lines = Lines {
        most,
        spans: AtomicU64::new(0),
    };
    // Only fails if a log is already going somewhere, which is as good.
    let _ = tracing::subscriber::set_global_default(lines);
}

/// How many bytes of the newest lines are kept while no [`Window`] says.
pub const WINDOW_BYTES: usize = 256 * 1024;

/// The newest lines, and the size of each window open on them.
struct Kept {
    lines: VecDeque<u8>,
    sizes: Vec<usize>,
}

impl Kept {
    /// As many bytes as the largest window wants, or [`WINDOW_BYTES`] while there is none.
    fn limit(&self) -> usize {
        self.sizes.iter().copied().max().unwrap_or(WINDOW_BYTES)
    }
}

static KEPT: Mutex<Kept> = Mutex::new(Kept {
    lines: VecDeque::new(),
    sizes: Vec::new(),
});

fn kept() -> std::sync::MutexGuard<'static, Kept> {
    KEPT.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The newest lines of this process's log, up to a size of its own (`log.window_bytes`).
///
/// The lines are the process's: one log, whoever wrote them. The size is each window's own,
/// so two comm cores in one process each read back as many bytes as they asked for. The lines
/// are kept for the largest window open, and for [`WINDOW_BYTES`] while none is; dropping a
/// window lets that go.
#[derive(Debug)]
pub struct Window {
    bytes: usize,
}

impl Window {
    /// A window of at most `bytes` of the newest lines; zero shows none.
    #[must_use]
    pub fn new(bytes: usize) -> Self {
        let mut kept = kept();
        kept.sizes.push(bytes);
        let limit = kept.limit();
        trim(&mut kept.lines, limit);
        Self { bytes }
    }

    /// How many bytes of the newest lines this window shows, at most.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The newest lines within this window's bytes, oldest first, each whole.
    #[must_use]
    pub fn read(&self) -> Vec<u8> {
        let kept = kept();
        let (front, back) = kept.lines.as_slices();
        let all = [front, back].concat();
        newest(&all, self.bytes).to_vec()
    }
}

impl Drop for Window {
    fn drop(&mut self) {
        let mut kept = kept();
        if let Some(at) = kept.sizes.iter().position(|bytes| *bytes == self.bytes) {
            kept.sizes.swap_remove(at);
        }
        let limit = kept.limit();
        trim(&mut kept.lines, limit);
    }
}

fn keep(line: &str) {
    let mut kept = kept();
    let limit = kept.limit();
    if line.len() > limit {
        return;
    }
    kept.lines.extend(line.as_bytes());
    trim(&mut kept.lines, limit);
}

/// Where whole lines within `limit` bytes start in `lines`: the line the first byte over is
/// on is left out whole.
fn cut(len: usize, limit: usize, newline_from: impl FnOnce(usize) -> Option<usize>) -> usize {
    match len.checked_sub(limit).filter(|over| *over > 0) {
        None => 0,
        Some(over) => newline_from(over - 1).map_or(len, |end| over + end),
    }
}

/// The newest whole lines of `lines` within `limit` bytes.
fn newest(lines: &[u8], limit: usize) -> &[u8] {
    let from = cut(lines.len(), limit, |at| {
        lines[at..].iter().position(|byte| *byte == b'\n')
    });
    &lines[from..]
}

/// Drops whole lines from the front until `lines` is at most `limit` bytes.
fn trim(lines: &mut VecDeque<u8>, limit: usize) {
    let from = cut(lines.len(), limit, |at| {
        lines.range(at..).position(|byte| *byte == b'\n')
    });
    lines.drain(..from);
}

#[cfg(test)]
mod tests {
    use super::*;

    // One test: the kept lines are the process's, and tests in one process share them.
    #[test]
    fn each_window_shows_the_newest_whole_lines_within_its_own_bytes() {
        let window = Window::new(32);
        for line in ["one 1111\n", "two 2222\n", "three 333\n", "four 4444\n"] {
            keep(line);
        }
        assert_eq!(window.read(), b"two 2222\nthree 333\nfour 4444\n");
        // A line longer than every window is not kept, and keeps nothing out.
        keep(&"x".repeat(40));
        assert_eq!(window.read(), b"two 2222\nthree 333\nfour 4444\n");
        drop(window);

        // A small window beside a large one: the lines are kept for the large one, and the
        // small one still never shows more than its own bytes.
        let large = Window::new(64);
        let small = Window::new(12);
        for line in ["five 5555\n", "six 66666\n", "seven 777\n"] {
            keep(line);
        }
        assert_eq!(small.bytes(), 12);
        assert_eq!(large.bytes(), 64);
        assert_eq!(small.read(), b"seven 777\n");
        assert!(small.read().len() <= 12);
        assert_eq!(
            large.read(),
            b"two 2222\nthree 333\nfour 4444\nfive 5555\nsix 66666\nseven 777\n"
        );
        // A line too long for the small window but not the large: kept for the large one only.
        keep("eight 888888888\n");
        assert_eq!(small.read(), b"");
        assert!(large.read().ends_with(b"eight 888888888\n"));

        // Dropping the large window lets its lines go.
        drop(large);
        assert!(kept().lines.len() <= 12);
        let none = Window::new(0);
        assert!(none.read().is_empty());
    }
}
