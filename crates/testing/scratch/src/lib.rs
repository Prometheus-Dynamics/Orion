//! A directory for one test: empty when it is made, gone when the test ends, whether it passed
//! or panicked. A test that leaves its files in the shared temporary directory fills it, run
//! after run, until unrelated tests fail for want of space; the gate fails a step that leaves
//! anything there.

use std::ffi::OsStr;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A directory under the temporary directory, removed with everything in it when dropped.
///
/// It dereferences to its [`Path`], so it goes wherever a `&Path` does. Keep it alive for as
/// long as anything uses what is in it: bind it to a name in the test, not to `_`.
#[derive(Debug)]
pub struct Scratch {
    path: PathBuf,
}

impl Scratch {
    /// Makes `orion-<name>-<process id>-<n>` under the temporary directory, empty, where `n`
    /// counts the scratch directories this process has made: two tests in one process that
    /// give the same name get two directories.
    ///
    /// The name is kept short by its callers: a socket in it must fit in 107 bytes.
    ///
    /// # Panics
    ///
    /// If the directory cannot be made, saying why: one already there is never shared.
    #[must_use]
    pub fn new(name: &str) -> Self {
        static MADE: AtomicUsize = AtomicUsize::new(0);
        let n = MADE.fetch_add(1, Ordering::Relaxed);
        Self::at(std::env::temp_dir().join(format!("orion-{name}-{}-{n}", std::process::id())))
    }

    /// Makes `path`, which must not be there yet, to be removed when dropped.
    ///
    /// # Panics
    ///
    /// If the directory cannot be made, saying why: one already there is never shared.
    #[must_use]
    pub fn at(path: PathBuf) -> Self {
        if let Err(error) = std::fs::create_dir(&path) {
            panic!("making {}: {error}", path.display());
        }
        Self { path }
    }

    /// The directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Deref for Scratch {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for Scratch {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<OsStr> for Scratch {
    fn as_ref(&self) -> &OsStr {
        self.path.as_os_str()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Nothing to say if it cannot go: the test has its verdict, and the gate names what
        // is left.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::Scratch;

    #[test]
    fn it_is_empty_when_made_and_gone_when_dropped() {
        let path = {
            let scratch = Scratch::new("scratch-gone");
            assert_eq!(std::fs::read_dir(&*scratch).expect("read").count(), 0);
            std::fs::create_dir(scratch.join("inner")).expect("inner");
            std::fs::write(scratch.join("inner/file"), "x").expect("write");
            scratch.to_path_buf()
        };
        assert!(!path.exists(), "{} is still there", path.display());
    }

    #[test]
    fn the_same_name_twice_in_one_process_is_two_directories() {
        let first = Scratch::new("scratch-twice");
        let second = Scratch::new("scratch-twice");
        assert_ne!(first.path(), second.path());
        assert!(first.is_dir() && second.is_dir());
    }

    #[test]
    #[should_panic(expected = "making")]
    fn one_already_there_is_never_shared() {
        let first = Scratch::new("scratch-shared");
        let _second = Scratch::at(first.to_path_buf());
    }

    #[test]
    fn it_is_gone_when_its_test_panics() {
        let made = std::sync::mpsc::channel();
        let panicked = std::thread::spawn(move || {
            let scratch = Scratch::new("scratch-panic");
            std::fs::write(scratch.join("file"), "x").expect("write");
            made.0.send(scratch.to_path_buf()).expect("send");
            panic!("the test failed");
        })
        .join();
        assert!(panicked.is_err());
        let path = made.1.recv().expect("made");
        assert!(!path.exists(), "{} is still there", path.display());
    }
}
