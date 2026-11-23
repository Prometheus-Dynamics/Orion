//! A name under `roots/`, and the opening of what it names beneath its root's directory.
//!
//! Every open goes through `openat2` with `RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS` from the
//! root's directory: a `..`, an absolute link or a magic link that leads out of the root is
//! refused by the kernel. The names are checked as well, so that what is refused says why.

use std::ffi::OsString;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStringExt as _;
use std::path::PathBuf;

use rustix::fs::{Mode, OFlags, ResolveFlags, openat2};
use rustix::io::Errno;

use crate::Root;

/// The namespace the roots are in: `roots/<root>/<path>`.
pub const PREFIX: &str = "roots/";

/// The directory under each root that holds the roots' own things: the parts of pushes on
/// their way. It is never listed, and no name may take it.
pub const OWN: &str = ".orion";

/// What a name under `roots/` comes to.
#[derive(Debug)]
pub(crate) struct At<'a> {
    pub(crate) root: &'a Root,
    /// The path under the root: empty for the root itself.
    pub(crate) path: &'a str,
}

impl<'a> At<'a> {
    /// The directory that holds what this names, and its last part. `None` for the root.
    pub(crate) fn split(&self) -> Option<(&'a str, &'a str)> {
        if self.path.is_empty() {
            return None;
        }
        Some(match self.path.rsplit_once('/') {
            Some((parent, name)) => (parent, name),
            None => ("", self.path),
        })
    }
}

/// Finds what `name`, under `roots/`, names. `name` may end in `/`.
///
/// # Errors
///
/// The words a client is told.
pub(crate) fn find<'a>(roots: &'a [Root], name: &'a str) -> Result<At<'a>, String> {
    let rest = name
        .strip_prefix(PREFIX)
        .ok_or_else(|| format!("{name} is not under {PREFIX}"))?;
    let rest = rest.strip_suffix('/').unwrap_or(rest);
    let (root, path) = rest.split_once('/').unwrap_or((rest, ""));
    let root = roots
        .iter()
        .find(|known| known.name == root)
        .ok_or_else(|| format!("no such root: {root}"))?;
    if !path.is_empty() {
        // The parts a file name may have: no `..`, no `.`, nothing empty.
        if orion_types::FileName::new(name.strip_suffix('/').unwrap_or(name)).is_err()
            || path.split('/').any(|part| part == "." || part == "..")
        {
            return Err(format!("{name} is not a name Orion carries"));
        }
        if path.split('/').next() == Some(OWN) {
            return Err(format!("{OWN} is the root's own"));
        }
    }
    Ok(At { root, path })
}

/// What the kernel said, as a client is told it.
pub(crate) fn words(error: Errno) -> String {
    match error {
        Errno::NOENT => "not found".to_owned(),
        Errno::XDEV | Errno::LOOP => "outside the root".to_owned(),
        Errno::NOTDIR => "not a directory".to_owned(),
        Errno::ISDIR => "a directory".to_owned(),
        Errno::EXIST => "there already".to_owned(),
        Errno::NOTEMPTY => "the directory is not empty".to_owned(),
        Errno::ACCESS | Errno::PERM => "the file's permissions do not allow it".to_owned(),
        Errno::ROFS => "the root's file system is read-only".to_owned(),
        Errno::NOSPC => "the root's file system is full".to_owned(),
        error => error.to_string(),
    }
}

/// The root's directory, for opening beneath.
pub(crate) fn open_root(root: &Root) -> Result<OwnedFd, Errno> {
    rustix::fs::open(
        &root.path,
        OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
}

/// The bytes a name, or part of one, as it is carried stands for: what the kernel is given.
pub(crate) fn raw(name: &str) -> PathBuf {
    PathBuf::from(OsString::from_vec(orion_types::unescape(name).collect()))
}

/// Opens `path` (as it is carried) under `root` with `flags`, beneath it.
pub(crate) fn beneath(root: &OwnedFd, path: &str, flags: OFlags) -> Result<OwnedFd, Errno> {
    let path = if path.is_empty() { "." } else { path };
    openat2(
        root,
        raw(path),
        flags | OFlags::CLOEXEC,
        Mode::empty(),
        ResolveFlags::BENEATH | ResolveFlags::NO_MAGICLINKS,
    )
}

/// The directory `path` under `root`, as a place to make, remove and rename in.
pub(crate) fn directory(root: &OwnedFd, path: &str) -> Result<OwnedFd, Errno> {
    beneath(root, path, OFlags::PATH | OFlags::DIRECTORY)
}
