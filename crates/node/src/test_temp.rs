//! One temporary directory per test process, for every path the unit tests make: state
//! directories, socket paths and files. It is removed when the process exits (also after failed
//! tests: the harness exits normally), and a test process removes the directories of earlier test
//! processes that are gone (killed or aborted), so nothing piles up in a RAM-backed `/tmp`.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

const PREFIX: &str = "orion-test-";

static ROOT: OnceLock<PathBuf> = OnceLock::new();

/// This process's directory, under the system temporary directory.
pub(crate) fn dir() -> &'static Path {
    ROOT.get_or_init(|| {
        let base = std::env::temp_dir();
        sweep(&base);
        let root = base.join(format!("{PREFIX}{}", std::process::id()));
        std::fs::create_dir_all(&root).expect("the test temporary directory should be created");
        #[cfg(unix)]
        // SAFETY: `remove_root` is an `extern "C" fn()` that does not unwind.
        unsafe {
            libc::atexit(remove_root);
        }
        root
    })
}

#[cfg(unix)]
extern "C" fn remove_root() {
    if let Some(root) = ROOT.get() {
        let _ = std::fs::remove_dir_all(root);
    }
}

/// A path in this process's directory that no other call returns: `<name>-<n><suffix>`. Short,
/// so socket paths stay within `sun_path`.
pub(crate) fn unique(name: &str, suffix: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    dir().join(format!("{name}-{n}{suffix}"))
}

/// Removes the directories of test processes that no longer run.
fn sweep(base: &Path) {
    #[cfg(unix)]
    {
        let Ok(entries) = std::fs::read_dir(base) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(pid) = name
                .to_str()
                .and_then(|name| name.strip_prefix(PREFIX))
                .and_then(|pid| pid.parse::<libc::pid_t>().ok())
            else {
                continue;
            };
            // SAFETY: signal 0 only checks that the process exists.
            let gone = unsafe { libc::kill(pid, 0) } != 0
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
            if gone {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }
    #[cfg(not(unix))]
    let _ = base;
}
