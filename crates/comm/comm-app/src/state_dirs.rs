//! The directories the comm core keeps things in, made and tried before anything is kept in
//! them. A directory that cannot be made or written to, or one on the root file system when a
//! mount was asked for, stops the start with a message that says which and why, and the board
//! goes to recovery instead of failing later, one write at a time.

use std::fs;
use std::io::{self, Write as _};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

/// Makes `dirs` (each with the setting it comes from), and writes and removes a file in each.
/// With `require_mount`, refuses a directory that would be on the same file system as `root`:
/// on a board updated by slot, the state is on its own partition, and a directory made on the
/// root file system while that partition is not mounted would be lost on the next update.
///
/// # Errors
///
/// What is wrong, in words: the setting, the directory and the cause.
pub fn prepare(dirs: &[(&str, &Path)], require_mount: bool, root: &Path) -> Result<(), String> {
    let mut seen: Vec<&Path> = Vec::new();
    for &(setting, dir) in dirs {
        if seen.contains(&dir) {
            continue;
        }
        seen.push(dir);
        if require_mount {
            on_its_own_mount(setting, dir, root)?;
        }
        fs::create_dir_all(dir)
            .map_err(|error| format!("{setting} {} cannot be made: {error}", dir.display()))?;
        try_writing(dir).map_err(|error| {
            format!("{setting} {} cannot be written to: {error}", dir.display())
        })?;
    }
    Ok(())
}

/// Refuses `dir` if it is, or would be when made, on `root`'s file system. Checked before it
/// is made, on the nearest part of the path that is there.
fn on_its_own_mount(setting: &str, dir: &Path, root: &Path) -> Result<(), String> {
    let there =
        nearest_existing(dir).map_err(|error| format!("{setting} {}: {error}", dir.display()))?;
    let device = |path: &Path| {
        fs::metadata(path)
            .map(|meta| meta.dev())
            .map_err(|error| format!("{setting}: {} cannot be read: {error}", path.display()))
    };
    if device(&there)? == device(root)? {
        return Err(format!(
            "{setting} {} is on the root file system and state.require_mount is on: \
             the file system it belongs on is not mounted",
            dir.display()
        ));
    }
    Ok(())
}

/// Whether `path`, made now, would land on `root`'s file system. Unknown counts as yes.
#[cfg_attr(not(feature = "recovery"), allow(dead_code))]
pub fn on_root(path: &Path, root: &Path) -> bool {
    let device = |path: &Path| fs::metadata(path).map(|meta| meta.dev()).ok();
    match nearest_existing(path).ok().and_then(|there| device(&there)) {
        Some(there) => Some(there) == device(root),
        None => true,
    }
}

/// `path` or the nearest of its parents that is there.
fn nearest_existing(path: &Path) -> io::Result<PathBuf> {
    let mut at = path;
    loop {
        match fs::symlink_metadata(at) {
            Ok(_) => return Ok(at.to_owned()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => match at.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => at = parent,
                _ => return Ok(PathBuf::from(".")),
            },
            Err(error) => return Err(error),
        }
    }
}

/// Writes a file in `dir`, makes sure it reached the disk, and removes it: a read-only file
/// system, a full disk or the wrong owner shows here, not in the middle of a save.
fn try_writing(dir: &Path) -> io::Result<()> {
    let path = dir.join(format!(".orion-write-test.{}", std::process::id()));
    let wrote = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.write_all(b"orion\n")?;
        file.sync_all()
    })();
    let removed = fs::remove_file(&path);
    wrote?;
    match removed {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("orion-state-dirs-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn missing_directories_are_made_and_left_empty() {
        let base = temp("made");
        let state = base.join("state");
        let keys = base.join("keys/deeper");
        prepare(
            &[("state.dir", &state), ("keys.dir", &keys)],
            false,
            Path::new("/"),
        )
        .unwrap();
        assert!(state.is_dir() && keys.is_dir());
        assert_eq!(
            fs::read_dir(&state).unwrap().count(),
            0,
            "the test file is gone"
        );
        // Once more, with them there.
        prepare(
            &[("state.dir", &state), ("keys.dir", &keys)],
            false,
            Path::new("/"),
        )
        .unwrap();
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn a_directory_that_cannot_be_made_is_named_with_its_setting() {
        let base = temp("blocked");
        fs::create_dir_all(&base).unwrap();
        // A file where a directory has to go.
        fs::write(base.join("file"), b"").unwrap();
        let error = prepare(
            &[("keys.dir", &base.join("file/keys"))],
            false,
            Path::new("/"),
        )
        .unwrap_err();
        assert!(error.starts_with("keys.dir "), "{error}");
        assert!(error.contains("cannot be made"), "{error}");
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn a_directory_that_cannot_be_written_to_is_refused() {
        use std::os::unix::fs::PermissionsExt as _;
        if rustix::process::geteuid().is_root() {
            // Root writes anywhere; the case is for the service's own user.
            return;
        }
        let base = temp("read-only");
        fs::create_dir_all(&base).unwrap();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o555)).unwrap();
        let error = prepare(&[("state.dir", &base)], false, Path::new("/")).unwrap_err();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(error.contains("cannot be written to"), "{error}");
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn with_a_mount_required_the_root_file_system_is_refused_before_anything_is_made() {
        let base = temp("mount");
        fs::create_dir_all(&base).unwrap();
        let state = base.join("data/orion");
        // `base` stands for the root file system: the state directory would be on it.
        let error = prepare(&[("state.dir", &state)], true, &base).unwrap_err();
        assert!(error.contains("state.require_mount"), "{error}");
        assert!(
            !base.join("data").exists(),
            "nothing was made on the root file system"
        );
        // A root on another file system (proc is always its own): the directory is made.
        assert!(on_root(&state, &base));
        assert!(!on_root(&state, Path::new("/proc")));
        prepare(&[("state.dir", &state)], true, Path::new("/proc")).unwrap();
        assert!(state.is_dir());
        fs::remove_dir_all(base).unwrap();
    }
}
