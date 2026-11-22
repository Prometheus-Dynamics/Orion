//! A program that is asked to run an action and never answers, while its session stays heard.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use orion::actions::{ActionDecl, Admits, Policy, Request, Target};
use orion::types::Error;
use orion::{Address, ClientError, Orion};

use crate::START;

/// A program on the board that has registered an action, takes every run of it it is handed,
/// and answers none: no progress, no end. A device whose call never comes back. It keeps
/// pumping its session meanwhile, so the comm core and node logic go on answering the caller,
/// its pings included, and the run stays open until the caller's own bound ends it. It goes
/// when dropped, and node logic then ends each run it left open as failed, "the program that
/// runs it went away". Made by
/// [`crate::TestBoard::hold_action`].
#[derive(Debug)]
pub struct HeldAction {
    stop: Arc<AtomicBool>,
    asked: Arc<AtomicUsize>,
    thread: Option<JoinHandle<()>>,
}

impl HeldAction {
    /// How many runs it has been handed, and left unanswered, so far.
    #[must_use]
    pub fn asked(&self) -> usize {
        self.asked.load(Ordering::Relaxed)
    }
}

impl Drop for HeldAction {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Starts the program on the board at `socket`, and returns once the action is registered.
pub(crate) fn start(socket: &Path, name: &str, target: Target) -> HeldAction {
    let stop = Arc::new(AtomicBool::new(false));
    let asked = Arc::new(AtomicUsize::new(0));
    let (socket, name) = (socket.to_owned(), name.to_owned());
    let (stopping, counting) = (stop.clone(), asked.clone());
    let (ready, up) = mpsc::channel();
    let thread = std::thread::Builder::new()
        .name("held action".to_owned())
        .spawn(move || {
            let orion = Orion::connect(&Address::Local(socket), "held-action").expect("connect");
            let decl = ActionDecl {
                name: &name,
                target,
                description: "Never answers",
            };
            // Node logic may still be starting: it is the one that keeps the list.
            let deadline = Instant::now() + START;
            let mut runs = loop {
                match orion.register_action(&decl, Policy::new(Admits::ANYONE)) {
                    Ok(runs) => break runs,
                    Err(orion::Error::Client(ClientError::Refused {
                        error: Error::Unavailable,
                        ..
                    })) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("registering {name}: {error}"),
                }
            };
            let _ = ready.send(());
            while !stopping.load(Ordering::Relaxed) {
                match runs.wait(Duration::from_millis(20)) {
                    Ok(Some(Request::Run { .. })) => {
                        counting.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(_) => {}
                    Err(_) => return,
                }
            }
        })
        .expect("a thread");
    up.recv_timeout(START)
        .expect("the program that holds the action registered it");
    HeldAction {
        stop,
        asked,
        thread: Some(thread),
    }
}
