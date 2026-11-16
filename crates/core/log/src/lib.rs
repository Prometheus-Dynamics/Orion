//! Where the log goes: lines on standard error, which a console, a service manager or an
//! initramfs's own log all take. One line an event: its level, where it is from, what it
//! says, and its fields.
//!
//! The newest lines are also kept in memory, up to [`window_bytes`] of them, for the comm core
//! to hand over as its own `orion/log` on a board whose log went nowhere else ([`window`]).

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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

/// How many bytes of the newest lines are kept by default.
pub const WINDOW_BYTES: usize = 256 * 1024;

static WINDOW: Mutex<VecDeque<u8>> = Mutex::new(VecDeque::new());
static WINDOW_LIMIT: AtomicUsize = AtomicUsize::new(WINDOW_BYTES);

/// Keeps at most `bytes` of the newest lines from now on (`log.window_bytes`); zero keeps none.
pub fn set_window_bytes(bytes: usize) {
    WINDOW_LIMIT.store(bytes, Ordering::Relaxed);
    trim(
        &mut WINDOW
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        bytes,
    );
}

/// How many bytes of the newest lines are kept.
#[must_use]
pub fn window_bytes() -> usize {
    WINDOW_LIMIT.load(Ordering::Relaxed)
}

/// The newest lines kept, oldest first, each whole.
#[must_use]
pub fn window() -> Vec<u8> {
    let window = WINDOW
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (front, back) = window.as_slices();
    [front, back].concat()
}

fn keep(line: &str) {
    let limit = WINDOW_LIMIT.load(Ordering::Relaxed);
    if line.len() > limit {
        return;
    }
    let mut window = WINDOW
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    window.extend(line.as_bytes());
    trim(&mut window, limit);
}

/// Drops whole lines from the front until `window` is at most `limit` bytes: the line the
/// first byte to go is on goes whole.
fn trim(window: &mut VecDeque<u8>, limit: usize) {
    let Some(over) = window.len().checked_sub(limit).filter(|over| *over > 0) else {
        return;
    };
    match window.range(over - 1..).position(|byte| *byte == b'\n') {
        Some(end) => drop(window.drain(..over + end)),
        None => window.clear(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_window_keeps_the_newest_whole_lines_within_its_bytes() {
        set_window_bytes(32);
        for line in ["one 1111\n", "two 2222\n", "three 333\n", "four 4444\n"] {
            keep(line);
        }
        assert_eq!(window(), b"two 2222\nthree 333\nfour 4444\n");
        // A line longer than the window is not kept, and keeps nothing out.
        keep(&"x".repeat(40));
        assert_eq!(window(), b"two 2222\nthree 333\nfour 4444\n");
        set_window_bytes(12);
        assert_eq!(window(), b"four 4444\n");
        set_window_bytes(0);
        assert!(window().is_empty());
    }
}
