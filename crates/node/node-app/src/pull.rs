//! Files the node fetches itself: the request, the helper's reports, and the file store.

use std::collections::BTreeMap;

use orion_file_store::Files;
use orion_files_pull::{Fetch, Pulls, Report, fetchable};
use orion_msg_files::{FilePull, FileState, FileStatus};
use orion_reply::{Asked, Out};
use orion_types::Error;

/// The fetches under way, and who asked for each.
#[derive(Debug, Default)]
pub(crate) struct Pulling {
    pulls: Pulls,
    /// The session to tell about each transfer. `None` once it has gone.
    asked_by: BTreeMap<u64, Option<u32>>,
}

const fn status(transfer: u64, received: u64, state: FileState, detail: &str) -> FileStatus<'_> {
    FileStatus {
        transfer,
        received,
        state,
        detail,
    }
}

impl Pulling {
    /// Whether a fetch is under way.
    pub(crate) fn active(&self) -> bool {
        self.pulls.active()
    }

    /// Answers a `FilePull`.
    pub(crate) fn request(
        &mut self,
        asked: &Asked<'_>,
        files: &mut Files,
        out: &mut dyn Out,
    ) -> Result<(), Error> {
        let pull: FilePull<'_> = asked.body()?;
        let transfer = pull.transfer;
        if !fetchable(pull.url) {
            return out.send(
                asked.session,
                &status(
                    transfer,
                    0,
                    FileState::Rejected,
                    "not a URL this node fetches",
                ),
            );
        }
        if self.pulls.fetching(transfer) {
            // Asked again while it runs: whoever asks now is who is told.
            self.asked_by.insert(transfer, Some(asked.session));
            let received = files.received(transfer).unwrap_or(0);
            return out.send(
                asked.session,
                &status(transfer, received, FileState::Receiving, ""),
            );
        }
        let (part, received) =
            match files.reserve(transfer, pull.destination, pull.size, pull.digest) {
                Ok(reserved) => reserved,
                Err(why) => {
                    return out.send(
                        asked.session,
                        &status(transfer, 0, FileState::Rejected, &why),
                    );
                }
            };
        let started = self.pulls.start(Fetch {
            transfer,
            url: pull.url.to_owned(),
            part,
            size: pull.size,
        });
        if !started {
            files.fetched(transfer, received, true);
            return out.send(
                asked.session,
                &status(
                    transfer,
                    received,
                    FileState::Rejected,
                    "too many fetches are under way",
                ),
            );
        }
        self.asked_by.insert(transfer, Some(asked.session));
        out.send(
            asked.session,
            &status(transfer, received, FileState::Receiving, ""),
        )
    }

    /// Takes what the helpers reported: how far each fetch is, and how it ended.
    pub(crate) fn poll(&mut self, files: &mut Files, out: &mut dyn Out) {
        for report in self.pulls.poll() {
            match report {
                Report::Received { transfer, received } => {
                    files.fetched(transfer, received, false);
                    if let Some(Some(session)) = self.asked_by.get(&transfer) {
                        let _ = out.push(
                            *session,
                            &status(transfer, received, FileState::Receiving, ""),
                        );
                    }
                }
                Report::Fetched { transfer } => {
                    // Every byte is here. It is checked and put in place like any file, and
                    // whoever asked is told how that ends.
                    files.fetched(transfer, u64::MAX, true);
                    let session = self.asked_by.remove(&transfer).flatten();
                    let received = files.received(transfer).unwrap_or(0);
                    if files.commit_fetched(transfer, session)
                        && let Some(session) = session
                    {
                        let _ = out.send(
                            session,
                            &status(transfer, received, FileState::Verifying, ""),
                        );
                    }
                }
                Report::Failed {
                    transfer,
                    received,
                    why,
                } => {
                    // What it had stays, for the next try.
                    files.fetched(transfer, received, true);
                    if let Some(session) = self.asked_by.remove(&transfer).flatten() {
                        let _ = out.send(
                            session,
                            &status(transfer, received, FileState::Rejected, &why),
                        );
                    }
                }
            }
        }
    }

    /// A session is gone: its fetches go on, with nobody to tell.
    pub(crate) fn gone(&mut self, session: u32) {
        for asked_by in self.asked_by.values_mut() {
            if *asked_by == Some(session) {
                *asked_by = None;
            }
        }
    }

    /// Forgets who asked: the comm core they came through is gone.
    pub(crate) fn forget_sessions(&mut self) {
        for asked_by in self.asked_by.values_mut() {
            *asked_by = None;
        }
    }
}
