//! Node logic made slow to read, or deaf to some requests, for a test of what the comm core
//! and a caller do when node logic is behind or stuck. Only with the `slow` feature, which no
//! shipped build has.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use orion_msg_records::Get;
use orion_msg_session::NodeRequest;
use orion_wire::{Decode, kind};

/// What is set for one node: by its name.
#[derive(Debug, Default)]
struct Node {
    delay: Arc<AtomicU64>,
    gets: Arc<Unanswered>,
}

static NODES: Mutex<Vec<(String, Arc<Node>)>> = Mutex::new(Vec::new());

fn node(name: &str) -> Arc<Node> {
    let mut nodes = NODES.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some((_, node)) = nodes.iter().find(|(node, _)| node == name) {
        return node.clone();
    }
    let node = Arc::new(Node::default());
    nodes.push((name.to_owned(), node.clone()));
    node
}

/// How long node logic for the node `name` sleeps before each frame it handles, in
/// milliseconds: 0, the default, for not at all. It may be changed while node logic runs.
#[must_use]
pub fn delay(name: &str) -> Arc<AtomicU64> {
    node(name).delay.clone()
}

/// The record gets node logic for the node `name` is passed and never answers, while it goes
/// on answering everything else: node logic stuck on one request, with the comm core alive
/// and answering the caller's pings. None, the default, until [`Unanswered::hold`].
#[must_use]
pub fn unanswered_gets(name: &str) -> Arc<Unanswered> {
    node(name).gets.clone()
}

/// The gets left unanswered: those whose prefix starts with the one held.
#[derive(Debug, Default)]
pub struct Unanswered {
    prefix: Mutex<Option<String>>,
    asked: AtomicUsize,
}

impl Unanswered {
    /// Leaves every get whose prefix starts with `prefix` unanswered from now on.
    pub fn hold(&self, prefix: &str) {
        *self.prefix.lock().unwrap_or_else(PoisonError::into_inner) = Some(prefix.to_owned());
    }

    /// Answers gets again. Those already left stay unanswered.
    pub fn release(&self) {
        *self.prefix.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// How many gets have been left unanswered so far.
    #[must_use]
    pub fn asked(&self) -> usize {
        self.asked.load(Ordering::Relaxed)
    }

    /// Whether `request` is a get to leave unanswered; counted if it is.
    pub(crate) fn takes(&self, request: &NodeRequest<'_>) -> bool {
        if request.kind != kind::GET {
            return false;
        }
        let held = self.prefix.lock().unwrap_or_else(PoisonError::into_inner);
        let left = held.as_deref().is_some_and(|prefix| {
            Get::decode(request.payload.0).is_ok_and(|get| get.prefix.starts_with(prefix))
        });
        if left {
            self.asked.fetch_add(1, Ordering::Relaxed);
        }
        left
    }
}

/// Sleeps as long as `delay` says.
pub(crate) fn wait(delay: &AtomicU64) {
    let ms = delay.load(Ordering::Relaxed);
    if ms > 0 {
        std::thread::sleep(Duration::from_millis(ms));
    }
}
