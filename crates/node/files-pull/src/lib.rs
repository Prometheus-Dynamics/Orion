//! Fetching a file from a URL.
//!
//! Instead of being sent a file, a node can be told where to get it. It then fetches the file
//! itself, into a part file, and the file is checked against its digest and put in place like
//! any other. Nothing of the file crosses the connection that asked.
//!
//! - **It goes on where it was.** If part of the file is already here, the server is asked for
//!   the rest with a `Range` header. A server that honours it answers `206` and the bytes are
//!   appended. One that does not answers `200` with the whole file, and the part is started
//!   over. A fetch that breaks off leaves what it had, for the next try.
//! - **A helper for each fetch.** Waiting on a server would hold node logic up, so a fetch has
//!   a thread of its own that ends with it. [`Pulls::poll`] collects what the helpers report.
//! - **Bounded.** The body is read in blocks and written as it comes. A server that sends
//!   more than the file's size is cut off there.
//!
//! This is the one place in Orion that speaks HTTP, and with feature `https` the one place
//! that uses rustls.

use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant};

use tracing::{info, warn};
use ureq::http::StatusCode;

/// The most fetches under way at once. One more is refused.
pub const MAX_PULLS: usize = 4;

/// How long connecting to the server may take.
const CONNECT: Duration = Duration::from_secs(10);

/// How long the server may say nothing in the middle of a file.
const SILENCE: Duration = Duration::from_secs(30);

/// How often a fetch says how far it is.
const REPORT: Duration = Duration::from_millis(250);

/// One file to fetch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetch {
    /// The transfer it belongs to.
    pub transfer: u64,
    /// Where to get it: `http://` or, with feature `https`, `https://`.
    pub url: String,
    /// The part file to fetch into. What is already in it is kept if the server allows.
    pub part: PathBuf,
    /// The file's size.
    pub size: u64,
}

/// What a fetch reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Report {
    /// So many bytes are in the part file.
    Received {
        /// The transfer.
        transfer: u64,
        /// Bytes in the part file.
        received: u64,
    },
    /// Every byte is in the part file. It is not checked yet.
    Fetched {
        /// The transfer.
        transfer: u64,
    },
    /// It broke off. What it had is still in the part file.
    Failed {
        /// The transfer.
        transfer: u64,
        /// Bytes in the part file.
        received: u64,
        /// Why, for people.
        why: String,
    },
}

/// Whether `url` is one this build fetches.
#[must_use]
pub fn fetchable(url: &str) -> bool {
    url.starts_with("http://") || (cfg!(feature = "https") && url.starts_with("https://"))
}

/// The fetches of one node.
#[derive(Debug)]
pub struct Pulls {
    reports: Receiver<Report>,
    reporter: Sender<Report>,
    running: BTreeSet<u64>,
}

impl Default for Pulls {
    fn default() -> Self {
        Self::new()
    }
}

impl Pulls {
    /// No fetches.
    #[must_use]
    pub fn new() -> Self {
        let (reporter, reports) = mpsc::channel();
        Self {
            reports,
            reporter,
            running: BTreeSet::new(),
        }
    }

    /// Whether a fetch is under way: [`Pulls::poll`] will have something.
    #[must_use]
    pub fn active(&self) -> bool {
        !self.running.is_empty()
    }

    /// Whether `transfer` is being fetched now.
    #[must_use]
    pub fn fetching(&self, transfer: u64) -> bool {
        self.running.contains(&transfer)
    }

    /// Starts a fetch on a helper of its own. `false` if there is no room for another, the
    /// transfer is already being fetched, or no helper could be started.
    pub fn start(&mut self, fetch: Fetch) -> bool {
        if self.running.len() >= MAX_PULLS || self.running.contains(&fetch.transfer) {
            return false;
        }
        let (transfer, reporter) = (fetch.transfer, self.reporter.clone());
        let spawned = std::thread::Builder::new()
            .name("orion-file-pull".into())
            .spawn(move || {
                let report = run(&fetch, &reporter);
                let _ = reporter.send(report);
            });
        match spawned {
            Ok(_) => {
                self.running.insert(transfer);
                true
            }
            Err(error) => {
                warn!("no helper for a fetch could be started: {error}");
                false
            }
        }
    }

    /// What the fetches reported since the last call, in order.
    pub fn poll(&mut self) -> Vec<Report> {
        let mut reports = Vec::new();
        while let Ok(report) = self.reports.try_recv() {
            if let Report::Fetched { transfer } | Report::Failed { transfer, .. } = &report {
                self.running.remove(transfer);
            }
            reports.push(report);
        }
        reports
    }
}

fn failed(fetch: &Fetch, received: u64, why: impl Into<String>) -> Report {
    Report::Failed {
        transfer: fetch.transfer,
        received,
        why: why.into(),
    }
}

/// Fetches one file. Returns the last report: fetched, or failed with why.
fn run(fetch: &Fetch, reporter: &Sender<Report>) -> Report {
    let mut part = match OpenOptions::new()
        .create(true)
        .append(true)
        .open(&fetch.part)
    {
        Ok(part) => part,
        Err(error) => return failed(fetch, 0, format!("the part file cannot be opened: {error}")),
    };
    let mut received = part.metadata().map_or(0, |metadata| metadata.len());
    if received > fetch.size {
        // More than the file: not that file.
        if part.set_len(0).is_err() {
            return failed(fetch, received, "the part file cannot be emptied");
        }
        received = 0;
    }
    if received == fetch.size {
        return Report::Fetched {
            transfer: fetch.transfer,
        };
    }
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_connect(Some(CONNECT))
        .timeout_recv_body(Some(SILENCE))
        // A status that is not 2xx is an answer to read, not an error to unwrap.
        .http_status_as_error(false)
        .build()
        .into();
    let mut request = agent.get(&fetch.url);
    if received != 0 {
        request = request.header("Range", format!("bytes={received}-"));
    }
    let mut response = match request.call() {
        Ok(response) => response,
        Err(error) => {
            return failed(
                fetch,
                received,
                format!("the server was not reached: {error}"),
            );
        }
    };
    match response.status() {
        StatusCode::PARTIAL_CONTENT => {
            info!("fetching {} from byte {received}", fetch.url);
        }
        StatusCode::OK => {
            // The whole file, whatever was asked: what was here is not used.
            if received != 0 {
                if part.set_len(0).is_err() {
                    return failed(fetch, received, "the part file cannot be emptied");
                }
                received = 0;
            }
        }
        status => {
            return failed(
                fetch,
                received,
                format!("the server answered {}", status.as_u16()),
            );
        }
    }
    let mut body = response.body_mut().as_reader();
    let mut block = vec![0_u8; 64 * 1024];
    let mut said = Instant::now();
    loop {
        let read = match body.read(&mut block) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) => return failed(fetch, received, format!("the fetch broke off: {error}")),
        };
        if received + read as u64 > fetch.size {
            return failed(fetch, received, "the server sent more than the file's size");
        }
        if let Err(error) = part.write_all(&block[..read]) {
            return failed(
                fetch,
                received,
                format!("the part file cannot be written: {error}"),
            );
        }
        received += read as u64;
        if said.elapsed() >= REPORT {
            said = Instant::now();
            let _ = reporter.send(Report::Received {
                transfer: fetch.transfer,
                received,
            });
        }
    }
    if received == fetch.size {
        Report::Fetched {
            transfer: fetch.transfer,
        }
    } else {
        failed(
            fetch,
            received,
            format!("the server sent {received} of {} bytes", fetch.size),
        )
    }
}
