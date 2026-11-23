//! Reading a root's directories: for a listing, and for the root's index.
//!
//! A directory is read from a descriptor opened beneath the root, and each directory under
//! it is opened beneath the one before with no link followed, so a link swapped in while a
//! root is walked leads nowhere.

use std::fmt::Write as _;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};

use rustix::fs::{AtFlags, Dir, FileType, OFlags, Stat, statat};
use rustix::io::Errno;
use tracing::warn;

use crate::Root;
use crate::path::{self, OWN, PREFIX};

/// The most files an index lists. A root with more is listed, not indexed.
pub const MAX_INDEXED: usize = 65_536;

/// The deepest an index goes, in directories under the root.
pub const MAX_DEPTH: usize = 32;

/// What an entry of a directory is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    File,
    Dir,
    Link,
    Other,
}

impl Kind {
    pub(crate) const fn word(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Dir => "dir",
            Self::Link => "link",
            Self::Other => "other",
        }
    }
}

/// Calls `each` for every entry of `dir` but `.` and `..`, with its name if it is text, and
/// what it is.
pub(crate) fn read(
    fd: &OwnedFd,
    mut each: impl FnMut(BorrowedFd<'_>, Option<&str>, Kind, &Stat),
) -> Result<(), Errno> {
    let mut entries = Dir::read_from(fd)?;
    while let Some(entry) = entries.read() {
        let entry = entry?;
        let raw = entry.file_name();
        if matches!(raw.to_bytes(), b"." | b"..") {
            continue;
        }
        let Ok(stat) = statat(fd, raw, AtFlags::SYMLINK_NOFOLLOW) else {
            // Gone since it was read.
            continue;
        };
        let kind = match FileType::from_raw_mode(stat.st_mode) {
            FileType::RegularFile => Kind::File,
            FileType::Directory => Kind::Dir,
            FileType::Symlink => Kind::Link,
            _ => Kind::Other,
        };
        each(fd.as_fd(), raw.to_str().ok(), kind, &stat);
    }
    Ok(())
}

/// Opens the directory `name` in `dir` to read, following no link.
pub(crate) fn open_dir(dir: BorrowedFd<'_>, name: &str) -> Result<OwnedFd, Errno> {
    rustix::fs::openat2(
        dir,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
        rustix::fs::ResolveFlags::BENEATH | rustix::fs::ResolveFlags::NO_SYMLINKS,
    )
}

struct Walk<'a> {
    root: &'a Root,
    files: Vec<(String, u64)>,
    /// Files whose names Orion does not carry, or that are too deep.
    left_out: usize,
    too_many: bool,
}

impl Walk<'_> {
    fn dir(&mut self, dir: &OwnedFd, at: &str, depth: usize) {
        // The directories under it, by name: each is opened when it is walked, so no more
        // are open at once than the walk is deep.
        let mut under = Vec::new();
        let result = read(dir, |_, name, kind, stat| {
            let Some(name) = name else {
                self.left_out += usize::from(kind == Kind::File);
                return;
            };
            if at.is_empty() && name == OWN {
                return;
            }
            let whole = format!("{PREFIX}{}/{at}{name}", self.root.name);
            match kind {
                Kind::File if orion_types::FileName::new(&whole).is_ok() => {
                    if self.files.len() >= MAX_INDEXED {
                        self.too_many = true;
                    } else {
                        self.files.push((whole, stat.st_size as u64));
                    }
                }
                Kind::File => self.left_out += 1,
                Kind::Dir if depth < MAX_DEPTH => under.push(name.to_owned()),
                Kind::Dir => self.left_out += 1,
                // A link, a pipe, a device: not a file to fetch.
                Kind::Link | Kind::Other => {}
            }
        });
        if let Err(error) = result {
            warn!(
                "{PREFIX}{}/{at} could not be read for its index: {error}",
                self.root.name
            );
        }
        for name in under {
            if self.too_many {
                return;
            }
            if let Ok(opened) = open_dir(dir.as_fd(), &name) {
                self.dir(&opened, &format!("{at}{name}/"), depth + 1);
            }
        }
    }
}

/// The index of `root`: every file under it, its size as its bound, in the format of
/// `orion_files::index`. In the order its directories are read, a directory's own files
/// before those under it: not sorted, which an index does not ask for.
///
/// # Errors
///
/// The words a client is told: the root cannot be read, or it has too many files.
pub(crate) fn index(root: &Root) -> Result<Vec<u8>, String> {
    let fd = path::open_root(root)
        .and_then(|fd| path::beneath(&fd, "", OFlags::RDONLY | OFlags::DIRECTORY))
        .map_err(|error| format!("the root: {}", path::words(error)))?;
    let mut walk = Walk {
        root,
        files: Vec::new(),
        left_out: 0,
        too_many: false,
    };
    walk.dir(&fd, "", 0);
    if walk.too_many {
        return Err(format!(
            "the root has more than {MAX_INDEXED} files, too many for one index: list it"
        ));
    }
    if walk.left_out != 0 {
        warn!(
            "the index of {PREFIX}{} leaves out {} files: their names are not ones Orion \
             carries, or they are more than {MAX_DEPTH} directories down",
            root.name, walk.left_out
        );
    }
    let total: u64 = walk.files.iter().map(|(_, size)| size).sum();
    let mut text = format!("orion-index 1\ntotal {total}\n");
    for (name, size) in &walk.files {
        let _ = writeln!(text, "{name} {size} user-data");
    }
    Ok(text.into_bytes())
}
