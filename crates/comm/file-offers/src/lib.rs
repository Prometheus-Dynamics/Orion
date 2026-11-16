//! Files from a board.
//!
//! A program on the board offers every file under a prefix (`FileOffer`): a log keeper offers
//! `logs/`, the program that manages the board offers `system/`. A client asks for a file by
//! name (`FileFetch`), from wherever the client is, and the comm core hands the request to the
//! program offering the longest prefix of that name. The program gives the file
//! (`FileGive`, then `FileGiveChunk`s), paced by what the client says it has (`FileTaken`), or
//! refuses it, saying why. Orion knows nothing of what a file holds: it routes.
//!
//! **A fetch resumes.** The client says how much it has (`from`); the program gives the rest
//! and the file's digest, which the client checks over all of it.
//!
//! **One program a prefix.** The first to offer a prefix holds it until its session ends; another
//! is refused as busy, naming the holder, and the refusal is counted where the list of offers
//! says it ([`FileOffersList::clashes`]). A clash is seen, never silent.
//!
//! **Each side is told when the other goes.** A fetch whose program goes is rejected to its
//! client; one whose client goes is rejected to its program, which stops giving it.
//!
//! The comm core rather than node logic routes, so that a board in recovery, where no node
//! logic runs, can still hand over its own files.

#![cfg(target_os = "linux")]

mod own;

pub use own::{OwnFiles, OwnName, SNAPSHOT_MS};

use orion_msg_files::{
    FETCH_WINDOW, FileFetch, FileGive, FileGiveChunk, FileOffer, FileOffered, FileOffers,
    FileOffersList, FileState, FileTaken, OfferEntry, OfferedBy,
};
use orion_server::{
    Admits, Checked, Cx, Delivery, Error, Incoming, Policy, Service, SessionId, Sessions, What,
};
use orion_types::FileName;
use orion_wire::{FrameView, List, kind};
use tracing::warn;

/// The most prefixes offered at once. One more is refused as busy.
pub const MAX_OFFERS: usize = 64;

/// The most fetches under way at once. One more is refused as busy.
pub const MAX_FETCHES: usize = 256;

/// The bytes of a chunk's frame that are not the file's.
const CHUNK_OVERHEAD: u32 = 64;

#[derive(Debug)]
struct Offer {
    prefix: String,
    session: SessionId,
    since_ms: u64,
}

/// A few entries by number, kept in a list: there are never many, and a list costs a board
/// far less code than a tree.
#[derive(Debug)]
struct Table<V>(Vec<(u64, V)>);

impl<V> Default for Table<V> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<V> Table<V> {
    fn get(&self, key: &u64) -> Option<&V> {
        self.0
            .iter()
            .find(|(at, _)| at == key)
            .map(|(_, value)| value)
    }

    fn get_mut(&mut self, key: &u64) -> Option<&mut V> {
        self.0
            .iter_mut()
            .find(|(at, _)| at == key)
            .map(|(_, value)| value)
    }

    fn insert(&mut self, key: u64, value: V) {
        match self.get_mut(&key) {
            Some(known) => *known = value,
            None => self.0.push((key, value)),
        }
    }

    fn remove(&mut self, key: &u64) -> Option<V> {
        let at = self.0.iter().position(|(known, _)| known == key)?;
        Some(self.0.swap_remove(at).1)
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn iter(&self) -> impl Iterator<Item = (&u64, &V)> {
        self.0.iter().map(|(key, value)| (key, value))
    }
}

#[derive(Debug)]
struct Fetch {
    client: SessionId,
    /// The client's number for it.
    transfer: u64,
    offerer: SessionId,
}

/// The service. Add it to the comm core.
#[derive(Debug)]
pub struct FileOffersService {
    offer: Policy,
    fetch: Policy,
    offers: Vec<Offer>,
    /// Each fetch under way, by the number the program knows it by.
    fetches: Table<Fetch>,
    next: u64,
    clashes: u64,
    /// The comm core's own files, if it offers any.
    own: Option<own::Own>,
}

impl FileOffersService {
    /// A service where `offer` says who may offer a prefix, and `fetch` who may fetch a file
    /// or list what is offered.
    #[must_use]
    pub const fn new(offer: Policy, fetch: Policy) -> Self {
        Self {
            offer,
            fetch,
            offers: Vec::new(),
            fetches: Table(Vec::new()),
            next: 1,
            clashes: 0,
            own: None,
        }
    }

    /// Offers `files` as the comm core's own: their prefix is never a program's, and a fetch
    /// under it is the comm core's to give.
    #[must_use]
    pub fn with_own(mut self, files: impl OwnFiles + 'static) -> Self {
        self.own = Some(own::Own::new(Box::new(files)));
        self
    }

    /// Who may as the node has it: programs on the board and devices wired to it offer, as
    /// they are receivers; programs on the board, operators and other nodes fetch, as they
    /// send files. Both need the grant where grants are kept.
    #[must_use]
    pub const fn standard() -> Self {
        Self::new(
            Policy::new(Admits::LOCAL.and(Admits::DEVICE)),
            Policy::new(Admits::LOCAL.and(Admits::OPERATOR).and(Admits::PEER)),
        )
    }

    /// Offers refused because another held the prefix.
    #[must_use]
    pub const fn clashes(&self) -> u64 {
        self.clashes
    }

    /// The offer of the longest prefix of `name`.
    fn offering(&self, name: &str) -> Option<&Offer> {
        self.offers
            .iter()
            .filter(|offer| name.starts_with(offer.prefix.as_str()))
            .max_by_key(|offer| offer.prefix.len())
    }

    fn offer(&mut self, request: &FileOffer<'_>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let prefix = request.prefix;
        if !prefix.ends_with('/') || FileName::new(prefix.trim_end_matches('/')).is_err() {
            return cx.refuse(
                Error::InvalidName,
                &format!("{prefix:?} is not a prefix: one or more parts of a name, ending in /"),
            );
        }
        if let Some(own) = &self.own
            && own.has(prefix)
        {
            self.clashes += 1;
            let holder = own.prefix().to_owned();
            return cx.refuse(
                Error::Busy,
                &format!("{prefix} is under {holder}, which the comm core offers"),
            );
        }
        let from = cx.from();
        match self.offers.iter().find(|offer| offer.prefix == prefix) {
            Some(offer) if offer.session == from => {}
            Some(offer) => {
                self.clashes += 1;
                let holder = cx
                    .sessions()
                    .get(offer.session)
                    .map(|info| format!("{} ({})", info.name, info.principal))
                    .unwrap_or_default();
                let asker = format!("{} ({})", cx.caller().name, cx.caller().principal);
                warn!("{asker} offered {prefix}, which {holder} offers: refused");
                return cx.refuse(Error::Busy, &format!("{prefix} is offered by {holder}"));
            }
            None if self.offers.len() >= MAX_OFFERS => return Err(Error::Busy),
            None => self.offers.push(Offer {
                prefix: prefix.to_owned(),
                session: from,
                since_ms: cx.now_ms(),
            }),
        }
        let request_seq = cx.request_seq();
        cx.reply(&FileOffered { request_seq })
    }

    /// The chunks a fetch asks for: a window of them fits in the client's outbox twice over,
    /// so a client that is slow to read is never closed for what it was allowed to be sent.
    fn chunk_for(cx: &mut Cx<'_>) -> u32 {
        let fits = cx.sessions().outbox_bytes() / (2 * FETCH_WINDOW);
        cx.caller()
            .max_frame
            .min(fits)
            .saturating_sub(CHUNK_OVERHEAD)
            .max(1)
    }

    fn fetch(&mut self, request: &FileFetch<'_>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let name = request.name.as_str();
        if let Some(own) = &mut self.own
            && own.has(name)
        {
            let chunk = Self::chunk_for(cx) as usize;
            let (client, now_ms) = (cx.from(), cx.now_ms());
            let fetch = (client, request.transfer, name, request.from);
            return match own.give(fetch, chunk, now_ms, cx.sessions()) {
                Ok(()) => Ok(()),
                Err(own::NotGiven::Busy) => Err(Error::Busy),
                Err(own::NotGiven::Rejected(why)) => {
                    let _ = cx.sessions().send(
                        client,
                        &FileTaken {
                            transfer: request.transfer,
                            received: 0,
                            state: FileState::Rejected,
                            detail: &why,
                        },
                        Delivery::Must,
                    );
                    Ok(())
                }
            };
        }
        let Some(offerer) = self.offering(name).map(|offer| offer.session) else {
            return cx.refuse(Error::NotFound, &format!("nothing offers {name}"));
        };
        let client = cx.from();
        // A client that asks again for a fetch it has under way, after hearing nothing, goes
        // on with the same one.
        let number = match self
            .fetches
            .iter()
            .find(|(_, fetch)| fetch.client == client && fetch.transfer == request.transfer)
        {
            Some((number, _)) => *number,
            None if self.fetches.len() >= MAX_FETCHES => return Err(Error::Busy),
            None => {
                let number = self.next;
                self.next += 1;
                number
            }
        };
        self.fetches.insert(
            number,
            Fetch {
                client,
                transfer: request.transfer,
                offerer,
            },
        );
        let caller = cx.caller().principal.to_string();
        let max_chunk = Self::chunk_for(cx);
        let passed = cx.sessions().send(
            offerer,
            &FileFetch {
                transfer: number,
                name: request.name.clone(),
                from: request.from,
                caller: &caller,
                max_chunk,
            },
            Delivery::Must,
        );
        if passed.is_err() {
            self.fetches.remove(&number);
            return cx.refuse(
                Error::Unavailable,
                &format!("the program that offers {name} is not answering"),
            );
        }
        Ok(())
    }

    /// The fetch `number`, if `from` is its program.
    fn given(&self, number: u64, from: SessionId) -> Result<&Fetch, Error> {
        match self.fetches.get(&number) {
            Some(fetch) if fetch.offerer == from => Ok(fetch),
            Some(_) => Err(Error::Denied),
            None => Err(Error::NotFound),
        }
    }

    /// Something the program gave reached nobody: its client is gone. The program is told.
    fn client_gone(&mut self, number: u64, sessions: &mut Sessions) {
        if let Some(fetch) = self.fetches.remove(&number) {
            let _ = sessions.send(
                fetch.offerer,
                &FileTaken {
                    transfer: number,
                    received: 0,
                    state: FileState::Rejected,
                    detail: "the client went",
                },
                Delivery::Must,
            );
        }
    }

    fn give(&mut self, give: &FileGive, cx: &mut Cx<'_>) -> Result<(), Error> {
        let fetch = self.given(give.transfer, cx.from())?;
        let to = fetch.client;
        let passed = cx.sessions().send(
            to,
            &FileGive {
                transfer: fetch.transfer,
                ..*give
            },
            Delivery::Must,
        );
        if passed.is_err() {
            self.client_gone(give.transfer, cx.sessions());
        }
        Ok(())
    }

    fn chunk(&mut self, chunk: &FileGiveChunk<'_>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let fetch = self.given(chunk.transfer, cx.from())?;
        let to = fetch.client;
        // A chunk is the program's to pace: it keeps a few on their way, as the client's
        // answers say, so a session's outbox holds them.
        let passed = cx.sessions().send(
            to,
            &FileGiveChunk {
                transfer: fetch.transfer,
                ..*chunk
            },
            Delivery::Must,
        );
        if passed.is_err() {
            self.client_gone(chunk.transfer, cx.sessions());
        }
        Ok(())
    }

    fn taken(&mut self, taken: &FileTaken<'_>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let from = cx.from();
        if let Some(own) = &mut self.own
            && own.taken(from, taken, cx.sessions())
        {
            return Ok(());
        }
        let over = matches!(taken.state, FileState::Committed | FileState::Rejected);
        // From the program: the number is its own. From the client: the client's.
        if let Some(fetch) = self.fetches.get(&taken.transfer)
            && fetch.offerer == from
        {
            let to = fetch.client;
            let transfer = fetch.transfer;
            if over {
                self.fetches.remove(&taken.transfer);
            }
            let _ = cx
                .sessions()
                .send(to, &FileTaken { transfer, ..*taken }, Delivery::Must);
            return Ok(());
        }
        let Some((&number, fetch)) = self
            .fetches
            .iter()
            .find(|(_, fetch)| fetch.client == from && fetch.transfer == taken.transfer)
        else {
            return Err(Error::NotFound);
        };
        let to = fetch.offerer;
        if over {
            self.fetches.remove(&number);
        }
        let _ = cx.sessions().send(
            to,
            &FileTaken {
                transfer: number,
                ..*taken
            },
            Delivery::Must,
        );
        Ok(())
    }

    fn list(&self, cx: &mut Cx<'_>) -> Result<(), Error> {
        let named: Vec<(String, String)> = self
            .offers
            .iter()
            .map(|offer| {
                cx.sessions()
                    .get(offer.session)
                    .map(|info| (info.name.clone(), info.principal.to_string()))
                    .unwrap_or_default()
            })
            .collect();
        let own = self.own.as_ref().map(|own| OfferEntry {
            prefix: own.prefix(),
            by: OfferedBy::CommCore,
            name: "",
            principal: "",
            since_ms: 0,
        });
        let entries: Vec<OfferEntry<'_>> = own
            .into_iter()
            .chain(
                self.offers
                    .iter()
                    .zip(&named)
                    .map(|(offer, (name, principal))| OfferEntry {
                        prefix: &offer.prefix,
                        by: OfferedBy::Program,
                        name,
                        principal,
                        since_ms: offer.since_ms,
                    }),
            )
            .collect();
        let request_seq = cx.request_seq();
        cx.reply(&FileOffersList {
            request_seq,
            clashes: self.clashes,
            offers: List::new(&entries),
        })
    }
}

/// Whoever is part of a fetch may send what it is due: which fetch is theirs is checked in
/// `handle`.
const PARTY: Policy = Policy::new(Admits::ANYONE).without_grant();

impl Service for FileOffersService {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::FILE_OFFER,
            kind::FILE_FETCH,
            kind::FILE_GIVE,
            kind::FILE_GIVE_CHUNK,
            kind::FILE_TAKEN,
            kind::FILE_OFFERS,
        ]
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        Ok(match frame.kind() {
            // As every file request is checked: against "files".
            kind::FILE_OFFER => (What::Write("files"), self.offer),
            kind::FILE_FETCH | kind::FILE_OFFERS => (What::Read("files"), self.fetch),
            _ => (What::Write("files"), PARTY),
        })
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let incoming = request.into_inner();
        match incoming.kind() {
            kind::FILE_OFFER => self.offer(&incoming.body()?, cx),
            kind::FILE_FETCH => self.fetch(&incoming.body()?, cx),
            kind::FILE_GIVE => self.give(&incoming.body()?, cx),
            kind::FILE_GIVE_CHUNK => self.chunk(&incoming.body()?, cx),
            kind::FILE_TAKEN => self.taken(&incoming.body()?, cx),
            _ => {
                let _: FileOffers = incoming.body()?;
                self.list(cx)
            }
        }
    }

    fn closed(&mut self, session: SessionId, sessions: &mut Sessions) {
        self.offers.retain(|offer| offer.session != session);
        if let Some(own) = &mut self.own {
            own.closed(session);
        }
        let ended: Vec<u64> = self
            .fetches
            .iter()
            .filter(|(_, fetch)| fetch.client == session || fetch.offerer == session)
            .map(|(number, _)| *number)
            .collect();
        for number in ended {
            let Some(fetch) = self.fetches.remove(&number) else {
                continue;
            };
            let (to, transfer, detail) = if fetch.client == session {
                (fetch.offerer, number, "the client went")
            } else {
                (
                    fetch.client,
                    fetch.transfer,
                    "the program that offers it went",
                )
            };
            let _ = sessions.send(
                to,
                &FileTaken {
                    transfer,
                    received: 0,
                    state: FileState::Rejected,
                    detail,
                },
                Delivery::Must,
            );
        }
    }

    fn quiet(&mut self, _session: SessionId, _quiet_ms: u64, _sessions: &mut Sessions) {}
}
