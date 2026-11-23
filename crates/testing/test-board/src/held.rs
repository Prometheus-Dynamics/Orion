//! A receiver that answers a commit late, while its session stays heard.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use orion::files::{FileState, Handed, digest};
use orion::types::Error;
use orion::{Address, ClientError, Orion};

use crate::START;

/// A program on the board that receives every file under a prefix, as board management
/// does, and answers each commit `hold` after it was asked: a receiver whose check, or the
/// device work behind it, takes that long. It keeps pumping its session meanwhile, so the
/// comm core and node logic go on answering the pushing client, its pings included. It stops
/// when dropped. Made by [`crate::TestBoard::hold_commits`].
#[derive(Debug)]
pub struct HeldCommits {
    stop: Arc<AtomicBool>,
    answered: Arc<AtomicUsize>,
    thread: Option<JoinHandle<()>>,
}

impl HeldCommits {
    /// How many commits it has answered so far, committed or rejected: each is counted before
    /// it is told, so a pusher that has heard its answer finds it counted.
    #[must_use]
    pub fn answered(&self) -> usize {
        self.answered.load(Ordering::Relaxed)
    }
}

impl Drop for HeldCommits {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A file on its way: what it must be, and its bytes so far.
struct Receiving {
    size: u64,
    digest: [u8; 32],
    data: Vec<u8>,
}

/// A commit's answer, and when it is due.
struct Due {
    at: Instant,
    transfer: u64,
    received: u64,
    state: FileState,
    detail: &'static str,
}

/// Starts the receiver on the board at `socket`, and returns once it is the receiver.
pub(crate) fn start(socket: &Path, prefix: &str, hold: Duration) -> HeldCommits {
    let stop = Arc::new(AtomicBool::new(false));
    let answered = Arc::new(AtomicUsize::new(0));
    let (socket, prefix) = (socket.to_owned(), prefix.to_owned());
    let (stopping, counting) = (stop.clone(), answered.clone());
    let (ready, up) = mpsc::channel();
    let thread = std::thread::Builder::new()
        .name("held commits".to_owned())
        .spawn(move || {
            let orion = Orion::connect(&Address::Local(socket), "held-commits").expect("connect");
            // Node logic may still be starting: it is the one that takes a receiver.
            let deadline = Instant::now() + START;
            let mut sink = loop {
                match orion.files().sink(&prefix) {
                    Ok(sink) => break sink,
                    Err(ClientError::Refused {
                        error: Error::Unavailable,
                        ..
                    }) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("the receiver for {prefix}: {error}"),
                }
            };
            let _ = ready.send(());
            let mut receiving: BTreeMap<u64, Receiving> = BTreeMap::new();
            let mut due: Vec<Due> = Vec::new();
            while !stopping.load(Ordering::Relaxed) {
                let now = Instant::now();
                for answer in due.extract_if(.., |answer| answer.at <= now) {
                    // Counted before it is told: a pusher that hears it and asks how many were
                    // answered finds this one among them.
                    counting.fetch_add(1, Ordering::Relaxed);
                    let told = sink.status(
                        answer.transfer,
                        answer.received,
                        answer.state,
                        answer.detail,
                    );
                    if told.is_err() {
                        return;
                    }
                }
                let Ok(handed) = sink.next(Duration::from_millis(20)) else {
                    return;
                };
                let answer = match handed {
                    Some(Handed::Begin {
                        transfer,
                        size,
                        digest,
                        ..
                    }) => {
                        let file = receiving.entry(transfer).or_insert(Receiving {
                            size,
                            digest,
                            data: Vec::new(),
                        });
                        if file.size != size || file.digest != digest {
                            *file = Receiving {
                                size,
                                digest,
                                data: Vec::new(),
                            };
                        }
                        sink.status(transfer, file.data.len() as u64, FileState::Receiving, "")
                    }
                    Some(Handed::Chunk {
                        transfer,
                        offset,
                        data,
                    }) => {
                        let Some(file) = receiving.get_mut(&transfer) else {
                            continue;
                        };
                        if offset == file.data.len() as u64 {
                            file.data.extend_from_slice(&data);
                        }
                        sink.status(transfer, file.data.len() as u64, FileState::Receiving, "")
                    }
                    Some(Handed::Commit { transfer }) => {
                        let (received, state, detail) = match receiving.remove(&transfer) {
                            None => (0, FileState::Rejected, "no such transfer"),
                            Some(file) => {
                                let found = digest(&mut Cursor::new(&file.data)).ok();
                                let received = file.data.len() as u64;
                                if found == Some((file.digest, file.size)) {
                                    (received, FileState::Committed, "")
                                } else {
                                    (received, FileState::Rejected, "the digest does not match")
                                }
                            }
                        };
                        due.push(Due {
                            at: Instant::now() + hold,
                            transfer,
                            received,
                            state,
                            detail,
                        });
                        Ok(())
                    }
                    None => Ok(()),
                };
                if answer.is_err() {
                    return;
                }
            }
        })
        .expect("a thread");
    up.recv_timeout(START)
        .expect("the receiver that holds commits is the receiver");
    HeldCommits {
        stop,
        answered,
        thread: Some(thread),
    }
}
