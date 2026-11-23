//! The helper that checks a file against its digest and puts it in place, beneath the store.

use std::fs::File;
use std::io::Read;
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;

use rustix::fs::{
    AtFlags, CWD, FileType, Mode, OFlags, ResolveFlags, fsync, mkdirat, openat2, renameat, statat,
};
use rustix::io::Errno;

use sha2::{Digest, Sha256};

/// One file to check.
pub(crate) struct Job {
    pub(crate) transfer: u64,
    pub(crate) part: PathBuf,
    pub(crate) destination: PathBuf,
    pub(crate) root: PathBuf,
    pub(crate) digest: [u8; 32],
    pub(crate) found: Sender<Found>,
}

/// What the helper found.
pub(crate) struct Found {
    pub(crate) transfer: u64,
    /// `Err` with why the file was not put in place.
    pub(crate) result: Result<(), String>,
}

/// Starts a thread for `job`. It ends when the file is in place or refused.
pub(crate) fn start(job: Job) {
    let (transfer, found) = (job.transfer, job.found.clone());
    let spawned = std::thread::Builder::new()
        .name("orion-file-verify".into())
        .spawn(move || {
            let result = check(&job);
            let _ = job.found.send(Found {
                transfer: job.transfer,
                result,
            });
        });
    if let Err(error) = spawned {
        let _ = found.send(Found {
            transfer,
            result: Err(format!("no helper could be started: {error}")),
        });
    }
}

fn check(job: &Job) -> Result<(), String> {
    let failed = |what: &str| {
        let what = what.to_owned();
        move |error: std::io::Error| format!("{what}: {error}")
    };
    let mut part = File::open(&job.part).map_err(failed("the part cannot be read"))?;
    let mut hasher = Sha256::new();
    let mut block = vec![0_u8; 256 * 1024];
    loop {
        let read = part
            .read(&mut block)
            .map_err(failed("the part cannot be read"))?;
        if read == 0 {
            break;
        }
        hasher.update(&block[..read]);
    }
    let digest: [u8; 32] = hasher.finalize().into();
    if digest != job.digest {
        return Err("the digest does not match".to_owned());
    }
    // On disk before it is named: a file at its destination is whole.
    part.sync_all()
        .map_err(failed("the file cannot be synced"))?;
    drop(part);
    let relative = job
        .destination
        .strip_prefix(&job.root)
        .map_err(|_| "the destination is outside the store".to_owned())?;
    place(&job.part, &job.root, relative)
}

/// What is said where the kernel is not let answer `openat2`, which is how anything under a
/// root or the store is opened (it is what keeps a path inside one). A service manager's
/// sandbox does this: systemd answers `openat2` with "not implemented" for a unit with
/// `RestrictSUIDSGID=yes`, on purpose, because it cannot look at the call's flags.
pub const NO_OPENAT2: &str = "openat2 is refused here (a unit with RestrictSUIDSGID=yes does \
    that): nothing under a root or the store can be opened";

/// Whether a path under `dir` can be opened at all: `Err`, with [`NO_OPENAT2`], if `openat2`
/// is refused here. A directory that is not there, or cannot be read, is not that fault.
///
/// # Errors
///
/// [`NO_OPENAT2`].
pub fn probe(dir: &Path) -> Result<(), &'static str> {
    let Ok(root) = rustix::fs::open(
        dir,
        OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    ) else {
        return Ok(());
    };
    match beneath(&root, Path::new(""), false) {
        Err(Errno::NOSYS) => Err(NO_OPENAT2),
        _ => Ok(()),
    }
}

/// Where a step of the way out of the store ends: refused, as outside.
fn outside(error: Errno) -> bool {
    matches!(error, Errno::XDEV | Errno::LOOP)
}

/// Opens `relative`, a directory under `root`, beneath it: a link, `..` or magic link that
/// leads out is refused by the kernel, not by a look at the names.
fn beneath(root: &OwnedFd, relative: &Path, read: bool) -> Result<OwnedFd, Errno> {
    let flags = if read {
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC
    } else {
        OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC
    };
    let relative = if relative.as_os_str().is_empty() {
        Path::new(".")
    } else {
        relative
    };
    openat2(
        root,
        relative,
        flags,
        Mode::empty(),
        ResolveFlags::BENEATH | ResolveFlags::NO_MAGICLINKS,
    )
}

/// Puts `part` at `relative` under `root`, making the directories on the way, each beneath
/// the store, and syncs the directory it went into.
pub(crate) fn place(part: &Path, root: &Path, relative: &Path) -> Result<(), String> {
    let failed = |what: &'static str| move |error: Errno| format!("{what}: {error}");
    let named = |error: Errno| {
        if outside(error) {
            "the destination is outside the store".to_owned()
        } else if error == Errno::NOSYS {
            NO_OPENAT2.to_owned()
        } else {
            format!("its directory cannot be made: {error}")
        }
    };
    let store = rustix::fs::open(
        root,
        OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(failed("the store"))?;
    let name = relative
        .file_name()
        .ok_or_else(|| "the destination has no name".to_owned())?;
    let parent = relative.parent().unwrap_or(Path::new(""));
    let directory = match beneath(&store, parent, false) {
        Ok(directory) => directory,
        Err(Errno::NOENT) => {
            // Each step is made in the one before it, which was opened beneath the store.
            let mut at = PathBuf::new();
            let mut directory = beneath(&store, &at, false).map_err(named)?;
            for step in parent.components() {
                at.push(step);
                directory = match beneath(&store, &at, false) {
                    Ok(next) => next,
                    Err(Errno::NOENT) => {
                        // As the file is made: what the process's umask leaves.
                        match mkdirat(&directory, step.as_os_str(), Mode::from_raw_mode(0o777)) {
                            Ok(()) | Err(Errno::EXIST) => {}
                            Err(error) => return Err(named(error)),
                        }
                        beneath(&store, &at, false).map_err(named)?
                    }
                    Err(error) => return Err(named(error)),
                };
            }
            directory
        }
        Err(error) => return Err(named(error)),
    };
    if statat(&directory, name, AtFlags::SYMLINK_NOFOLLOW)
        .is_ok_and(|stat| FileType::from_raw_mode(stat.st_mode) == FileType::Directory)
    {
        return Err("the destination is a directory".to_owned());
    }
    renameat(CWD, part, &directory, name).map_err(failed("it cannot be put in place"))?;
    if let Ok(synced) = beneath(&store, parent, true) {
        let _ = fsync(synced);
    }
    Ok(())
}
