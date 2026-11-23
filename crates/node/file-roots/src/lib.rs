//! The directories an image lets clients reach: its roots.
//!
//! The image names each root in one line of the settings, `files.root.<name> = <path> rw`
//! (or `ro`). A file is then `roots/<name>/<path>`, in the namespace that push and fetch
//! already use, and nothing outside a root is reachable, with security on or off.
//!
//! - **Fetch.** Node logic offers `roots/` as a program offers a prefix, and gives each file
//!   as a program does ([`give`]). `roots/<name>/index` is the root's index: every file, in
//!   the format of `orion_files::index`.
//! - **Push.** Node logic receives `roots/<name>/<path>` itself: the file is kept in the
//!   root's own `.orion` directory until every byte is here and its digest matches, and is
//!   then renamed into place, so a push that breaks off never replaces the file it was for.
//! - **The rest** are node logic's actions: `files.list`, `files.remove`, `files.mkdir` and
//!   `files.rename` ([`actions`]).
//!
//! Every path is opened beneath the root's directory (`path`), and a root marked `ro` refuses
//! every change, whoever asks.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub mod actions;
pub mod give;
mod path;
mod walk;

use std::collections::BTreeMap;
use std::path::PathBuf;

use orion_config::{ConfigError, Source};
use orion_connect::ClientError;
use orion_file_store::{Files, FilesConfig};
use orion_msg_files::{
    FileBegin, FileChunk, FileCommit, FileFetch, FileOffer, FileState, FileStatus,
};
use orion_reply::{Asked, Out};
use orion_types::Error;
use orion_wire::{Body, kind};

pub use path::{OWN, PREFIX};

/// One root, as the image named it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Root {
    /// Its name: `roots/<name>/`.
    pub name: String,
    /// Its directory.
    pub path: PathBuf,
    /// Whether it takes changes.
    pub writable: bool,
}

impl Root {
    /// Reads one setting: the name from the key, `<path> rw` or `<path> ro` from its text.
    ///
    /// # Errors
    ///
    /// A name or a text that is not one.
    pub fn parse(name: &str, text: &str) -> Result<Self, ConfigError> {
        let wrong = |message: String| ConfigError {
            key: format!("files.root.{name}"),
            message,
        };
        if name.is_empty()
            || name.len() > 32
            || !name.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
            })
        {
            return Err(wrong(format!(
                "{name:?} is not a root's name: 1 to 32 of a-z, 0-9, - and _"
            )));
        }
        let mut words = text.split_whitespace();
        let (Some(path), Some(mode), None) = (words.next(), words.next(), words.next()) else {
            return Err(wrong(format!(
                "{text:?} is not \"<directory> rw\" or \"<directory> ro\""
            )));
        };
        let writable = match mode {
            "rw" => true,
            "ro" => false,
            _ => return Err(wrong(format!("{mode:?} is neither rw nor ro"))),
        };
        if !path.starts_with('/') {
            return Err(wrong(format!("{path:?} is not a whole path")));
        }
        Ok(Self {
            name: name.to_owned(),
            path: PathBuf::from(path),
            writable,
        })
    }
}

/// The roots the settings name, one `files.root.<name>` line each.
///
/// # Errors
///
/// A line that is not a root.
pub fn from_source(source: &mut Source) -> Result<Vec<Root>, ConfigError> {
    source
        .prefixed("files.root.")
        .iter()
        .map(|(name, text)| Root::parse(name, text))
        .collect()
}

/// The way to the comm core, as node logic sends to it as a client.
pub trait Link {
    /// Sends `body`.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    fn send<B: Body>(&mut self, body: &B) -> Result<(), ClientError>;
}

/// The roots, and what is under way in them.
#[derive(Debug)]
pub struct Roots {
    roots: Vec<Root>,
    max_size: u64,
    /// A store for each root that took a push: its parts, kept in the root's `.orion`.
    stores: BTreeMap<String, Files>,
    /// The files being given.
    pub gives: give::Gives,
}

const fn status(transfer: u64, state: FileState, detail: &str) -> FileStatus<'_> {
    FileStatus {
        transfer,
        received: 0,
        state,
        detail,
    }
}

impl Roots {
    /// The roots, each taking files of up to `max_size` bytes.
    #[must_use]
    pub fn new(roots: Vec<Root>, max_size: u64) -> Self {
        Self {
            roots,
            max_size,
            stores: BTreeMap::new(),
            gives: give::Gives::default(),
        }
    }

    /// The roots.
    #[must_use]
    pub fn roots(&self) -> &[Root] {
        &self.roots
    }

    /// Whether there are roots to offer.
    #[must_use]
    pub fn any(&self) -> bool {
        !self.roots.is_empty()
    }

    /// The store of `root`, opened the first time it is pushed to.
    fn store(&mut self, root: &Root) -> Result<&mut Files, String> {
        if !self.stores.contains_key(&root.name) {
            // The root's own directory must be one, not a link to somewhere else.
            let own = root.path.join(OWN);
            match std::fs::symlink_metadata(&own) {
                Ok(found) if !found.is_dir() => {
                    return Err(format!("the root's {OWN} is not a directory"));
                }
                _ => {}
            }
            let files = Files::open(FilesConfig::hidden(&root.path, OWN, self.max_size))
                .map_err(|error| format!("the root cannot take files: {error}"))?;
            self.stores.insert(root.name.clone(), files);
        }
        self.stores
            .get_mut(&root.name)
            .ok_or_else(|| "the root cannot take files".to_owned())
    }

    /// Takes a request of a push to `roots/`: `Ok(false)` if it is not one, for the node's
    /// own store to answer.
    ///
    /// # Errors
    ///
    /// [`Error`] for the caller, as a refusal.
    pub fn pass(&mut self, asked: &Asked<'_>, out: &mut dyn Out) -> Result<bool, Error> {
        let transfer = match asked.kind {
            kind::FILE_BEGIN => {
                let begin: FileBegin = asked.body()?;
                if !begin.destination.as_str().starts_with(PREFIX) {
                    return Ok(false);
                }
                self.begin(asked.session, begin, out)?;
                return Ok(true);
            }
            kind::FILE_CHUNK => asked.body::<FileChunk<'_>>()?.transfer,
            kind::FILE_COMMIT => asked.body::<FileCommit>()?.transfer,
            _ => return Ok(false),
        };
        match self
            .stores
            .values_mut()
            .find(|store| store.received(transfer).is_some())
        {
            Some(store) => store.request(asked, out).map(|()| true),
            None => Ok(false),
        }
    }

    fn begin(&mut self, session: u32, begin: FileBegin, out: &mut dyn Out) -> Result<(), Error> {
        let transfer = begin.transfer;
        let found = path::find(&self.roots, begin.destination.as_str()).and_then(|at| {
            if !at.root.writable {
                return Err("the root is read-only".to_owned());
            }
            if at.path.is_empty() {
                return Err("a root is not a file".to_owned());
            }
            let name = at
                .path
                .parse()
                .map_err(|_| "not a file's name".to_owned())?;
            Ok((at.root.clone(), name))
        });
        let (root, destination) = match found {
            Ok(found) => found,
            Err(words) => return out.send(session, &status(transfer, FileState::Rejected, &words)),
        };
        match self.store(&root) {
            Ok(store) => store.begin_as(
                session,
                FileBegin {
                    destination,
                    ..begin
                },
                out,
            ),
            Err(words) => out.send(session, &status(transfer, FileState::Rejected, &words)),
        }
    }

    /// What node logic offers, to be handed every fetch under `roots/`: `None` with no roots.
    #[must_use]
    pub fn offer(&self) -> Option<FileOffer<'static>> {
        self.any().then_some(FileOffer { prefix: PREFIX })
    }

    /// Begins giving what `fetch` asks for, or says why not.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn fetch(
        &mut self,
        fetch: &FileFetch<'_>,
        link: &mut impl Link,
    ) -> Result<(), ClientError> {
        self.gives.fetch(&self.roots, fetch, link)
    }

    /// Whether a helper is checking or reading something: [`Roots::poll`] wants calling soon.
    #[must_use]
    pub fn busy(&self) -> bool {
        self.stores.values().any(Files::verifying) || self.gives.hashing()
    }

    /// Tells whoever pushed how the check of its file ended.
    pub fn poll(&mut self, out: &mut dyn Out) {
        for store in self.stores.values_mut() {
            store.poll(out);
        }
    }

    /// A session is gone.
    pub fn gone(&mut self, session: u32) {
        for store in self.stores.values_mut() {
            store.gone(session);
        }
    }
}
