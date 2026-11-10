//! A record store: keeps what is put, answers gets, pushes changes to watchers.

use std::collections::BTreeMap;

use orion_msg_records::{Changed, Get, Put, RecordClass, RecordEntry, Watch};
use orion_server::{
    Admits, Checked, Cx, Delivery, Error, Incoming, Policy, Service, SessionId, Sessions, What,
};
use orion_types::HlcTimestamp;
use orion_wire::{Bytes, FrameView, List, kind};

/// How many records go in one message.
const PAGE: usize = 50;

#[derive(Clone, Debug)]
struct Kept {
    class: RecordClass,
    key: String,
    stamp: HlcTimestamp,
    deleted: bool,
    value: Vec<u8>,
}

/// The store.
#[derive(Debug, Default)]
pub struct Store {
    records: BTreeMap<(u8, String), Kept>,
    clock: u64,
    watchers: Vec<(SessionId, u16, RecordClass, String)>,
}

fn send(
    sessions: &mut Sessions,
    to: SessionId,
    request_seq: u16,
    records: &[&Kept],
) -> Result<(), Error> {
    let mut pages = records.chunks(PAGE).peekable();
    if pages.peek().is_none() {
        let empty = Changed {
            request_seq,
            records: List::new(&[]),
            more: false,
        };
        return sessions.send(to, &empty, Delivery::Must);
    }
    while let Some(page) = pages.next() {
        let entries: Vec<RecordEntry<'_>> = page
            .iter()
            .map(|kept| RecordEntry {
                class: kept.class,
                key: &kept.key,
                stamp: kept.stamp,
                deleted: kept.deleted,
                value: Bytes(&kept.value),
            })
            .collect();
        let changed = Changed {
            request_seq,
            records: List::new(&entries),
            more: pages.peek().is_some(),
        };
        sessions.send(to, &changed, Delivery::Must)?;
    }
    Ok(())
}

impl Service for Store {
    fn kinds(&self) -> &'static [u8] {
        &[kind::PUT, kind::GET, kind::WATCH]
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        let what = if frame.kind() == kind::PUT {
            What::Write("records")
        } else {
            What::Read("records")
        };
        Ok((what, Policy::new(Admits::ANYONE).without_grant()))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let incoming = request.get();
        let (from, seq) = (cx.from(), cx.request_seq());
        match incoming.kind() {
            kind::PUT => {
                let put: Put<'_> = incoming.body()?;
                let mut stored = Vec::new();
                for entry in put.records.iter().flatten() {
                    self.clock += 1;
                    let kept = Kept {
                        class: entry.class,
                        key: entry.key.to_owned(),
                        stamp: HlcTimestamp::new(self.clock, 0, 1),
                        deleted: entry.deleted,
                        value: entry.value.0.to_vec(),
                    };
                    self.records
                        .insert((entry.class.code(), kept.key.clone()), kept.clone());
                    stored.push(kept);
                }
                let sessions = cx.sessions();
                send(sessions, from, seq, &stored.iter().collect::<Vec<_>>())?;
                for (watcher, watch_seq, class, prefix) in &self.watchers {
                    let theirs: Vec<&Kept> = stored
                        .iter()
                        .filter(|kept| {
                            kept.class == *class && kept.key.starts_with(prefix.as_str())
                        })
                        .collect();
                    if !theirs.is_empty() {
                        let _ = send(sessions, *watcher, *watch_seq, &theirs);
                    }
                }
                Ok(())
            }
            kind::GET => {
                let get: Get<'_> = incoming.body()?;
                let found: Vec<&Kept> = self
                    .records
                    .values()
                    .filter(|kept| {
                        kept.class == get.class && !kept.deleted && kept.key.starts_with(get.prefix)
                    })
                    .collect();
                send(cx.sessions(), from, seq, &found)
            }
            _ => {
                let watch: Watch<'_> = incoming.body()?;
                self.watchers.retain(|(session, _, class, prefix)| {
                    !(*session == from && *class == watch.class && prefix == watch.prefix)
                });
                self.watchers
                    .push((from, seq, watch.class, watch.prefix.to_owned()));
                let mut newer: Vec<&Kept> = self
                    .records
                    .values()
                    .filter(|kept| {
                        kept.class == watch.class
                            && kept.key.starts_with(watch.prefix)
                            && kept.stamp > watch.after
                    })
                    .collect();
                newer.sort_by_key(|kept| kept.stamp);
                if newer.is_empty() {
                    return Ok(());
                }
                send(cx.sessions(), from, seq, &newer)
            }
        }
    }

    fn closed(&mut self, session: SessionId, _sessions: &mut Sessions) {
        self.watchers.retain(|(watcher, ..)| *watcher != session);
    }
}
