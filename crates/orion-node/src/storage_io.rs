use crate::NodeError;
use crate::blocking::run_possibly_blocking;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

pub(crate) fn storage_path_error(
    action: &'static str,
    path: &Path,
    err: impl std::fmt::Display,
) -> NodeError {
    NodeError::StoragePath {
        action,
        path: path.to_path_buf(),
        message: err.to_string(),
    }
}

pub(crate) fn blocking_read_file(path: &Path, action: &'static str) -> Result<Vec<u8>, NodeError> {
    // These helpers centralize intentionally synchronous storage I/O so request-serving code can
    // isolate the blocking boundary instead of scattering raw `std::fs` calls.
    run_possibly_blocking(|| {
        std::fs::read(path).map_err(|err| storage_path_error(action, path, err))
    })
}

pub(crate) fn atomic_write_file(
    path: &Path,
    bytes: &[u8],
    create_dir_action: &'static str,
    write_action: &'static str,
    install_action: &'static str,
) -> Result<(), NodeError> {
    let parent = path.parent().ok_or_else(|| NodeError::StoragePath {
        action: write_action,
        path: path.to_path_buf(),
        message: "path has no parent directory".into(),
    })?;

    // This remains a blocking path because it provides durable local state writes with explicit
    // `sync_all` and rename semantics.
    run_possibly_blocking(|| {
        std::fs::create_dir_all(parent)
            .map_err(|err| storage_path_error(create_dir_action, parent, err))?;

        let tmp_path = path.with_extension(format!(
            "{}.{}.{}.tmp",
            path.extension()
                .and_then(|ext| ext.to_str())
                .unwrap_or("write"),
            std::process::id(),
            std::thread::current().name().unwrap_or("thread"),
        ));
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|err| storage_path_error(write_action, &tmp_path, err))?;
        tmp_file
            .write_all(bytes)
            .map_err(|err| storage_path_error(write_action, &tmp_path, err))?;
        tmp_file
            .sync_all()
            .map_err(|err| storage_path_error(write_action, &tmp_path, err))?;
        drop(tmp_file);

        std::fs::rename(&tmp_path, path)
            .map_err(|err| storage_path_error(install_action, path, err))?;
        sync_parent_directory(parent, install_action)?;
        Ok(())
    })
}

pub(crate) fn sync_parent_directory(path: &Path, action: &'static str) -> Result<(), NodeError> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|dir| dir.sync_all())
            .map_err(|err| storage_path_error(action, path, err))?;
    }
    #[cfg(not(unix))]
    {
        let _ = (path, action);
    }
    Ok(())
}
