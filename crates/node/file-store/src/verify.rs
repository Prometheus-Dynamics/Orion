//! The helper that checks a file against its digest and puts it in place.

use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;

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
    let parent = job
        .destination
        .parent()
        .ok_or_else(|| "the destination has no directory".to_owned())?;
    fs::create_dir_all(parent).map_err(failed("its directory cannot be made"))?;
    inside(parent, &job.root)?;
    if job.destination.is_dir() {
        return Err("the destination is a directory".to_owned());
    }
    fs::rename(&job.part, &job.destination).map_err(failed("it cannot be put in place"))?;
    if let Ok(directory) = File::open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}

/// Refuses a destination that a link on the way takes out of the store.
fn inside(parent: &Path, root: &Path) -> Result<(), String> {
    let (parent, root) = (
        fs::canonicalize(parent).map_err(|error| format!("its directory: {error}"))?,
        fs::canonicalize(root).map_err(|error| format!("the store: {error}"))?,
    );
    if parent.starts_with(&root) {
        Ok(())
    } else {
        Err("the destination is outside the store".to_owned())
    }
}
