//! The few things persistence asks of a disk, so they can be asked of a disk that fails.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

/// What persistence asks of a disk.
pub trait Disk {
    /// Everything in the file at `path`.
    ///
    /// # Errors
    ///
    /// `NotFound` if there is none; anything else the disk fails with.
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;

    /// Makes the file at `path` hold exactly `bytes`. Not safe against power loss until
    /// [`Disk::sync`].
    ///
    /// # Errors
    ///
    /// Whatever the disk fails with.
    fn write(&mut self, path: &Path, bytes: &[u8]) -> io::Result<()>;

    /// Makes the contents of the file at `path` safe against power loss.
    ///
    /// # Errors
    ///
    /// Whatever the disk fails with.
    fn sync(&mut self, path: &Path) -> io::Result<()>;

    /// Gives the file at `from` the name `to`, replacing what was there, in one step. Not
    /// safe against power loss until [`Disk::sync_dir`].
    ///
    /// # Errors
    ///
    /// Whatever the disk fails with.
    fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()>;

    /// Makes the names in the directory `dir` safe against power loss.
    ///
    /// # Errors
    ///
    /// Whatever the disk fails with.
    fn sync_dir(&mut self, dir: &Path) -> io::Result<()>;

    /// Removes the file at `path`. A file that is not there is not an error.
    ///
    /// # Errors
    ///
    /// Whatever the disk fails with.
    fn remove(&mut self, path: &Path) -> io::Result<()>;
}

/// The real disk.
#[derive(Clone, Copy, Debug, Default)]
pub struct RealDisk;

impl Disk for RealDisk {
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn write(&mut self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        File::create(path)?.write_all(bytes)
    }

    fn sync(&mut self, path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()
    }

    fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn sync_dir(&mut self, dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }

    fn remove(&mut self, path: &Path) -> io::Result<()> {
        match std::fs::remove_file(path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }
}
