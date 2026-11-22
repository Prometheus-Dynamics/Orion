//! Node logic made slow to read, for a test of what the comm core does when node logic is
//! behind. Only with the `slow` feature, which no shipped build has.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static NODES: Mutex<Vec<(String, Arc<AtomicU64>)>> = Mutex::new(Vec::new());

/// How long node logic for the node `name` sleeps before each frame it handles, in
/// milliseconds: 0, the default, for not at all. It may be changed while node logic runs.
#[must_use]
pub fn delay(name: &str) -> Arc<AtomicU64> {
    let mut nodes = NODES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some((_, delay)) = nodes.iter().find(|(node, _)| node == name) {
        return delay.clone();
    }
    let delay = Arc::new(AtomicU64::new(0));
    nodes.push((name.to_owned(), delay.clone()));
    delay
}

/// Sleeps as long as `delay` says.
pub(crate) fn wait(delay: &AtomicU64) {
    let ms = delay.load(Ordering::Relaxed);
    if ms > 0 {
        std::thread::sleep(Duration::from_millis(ms));
    }
}
