//! Where the log goes: lines on standard error, which a console, a service manager or an
//! initramfs's own log all take. One line an event: its level, where it is from, what it
//! says, and its fields.

use std::fmt::Write as _;
use std::io::Write as _;
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
