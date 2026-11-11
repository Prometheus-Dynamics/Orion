//! The sftp subsystem.
//!
//! `sftp` and `scp` both speak the SFTP protocol to a program on the board. That program
//! is this crate, run as a helper process: the SSH server starts it for a client that asks
//! for the `sftp` subsystem, after the session helper has become the user who logged in.
//!
//! - **As the user.** Every file is opened, made, renamed and removed by a process that is
//!   the user, so what the user may not do the kernel refuses. Nothing here decides who
//!   may read what.
//! - **A process of its own.** It speaks on its input and output and ends when the client
//!   does. A fault in it ends one transfer.
//! - **Protocol version 3**, the one OpenSSH speaks, with its `posix-rename`, `fsync` and
//!   `hardlink` extensions.

#![cfg(unix)]

use std::collections::HashMap;
use std::fs::{File, Metadata, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, UNIX_EPOCH};

use russh_sftp::protocol::{
    Attrs, Data, File as Entry, FileAttributes, Handle, Name, OpenFlags, Packet, Status,
    StatusCode, Version,
};
use russh_sftp::server::StatusReply;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Notify;

/// The word that says a command line is for this helper.
pub const MODE: &str = "ssh-sftp";

/// The most bytes read for one request, whatever the client asks for.
const MAX_READ: u32 = 256 * 1024;
/// The most names given for one request to read a directory.
const NAMES_AT_ONCE: usize = 128;
/// The most files and directories a client may hold open.
const MAX_OPEN: usize = 256;

/// What a handle is.
enum Open {
    File(File),
    /// A directory: what of it has not been given yet.
    Dir(std::vec::IntoIter<Entry>),
}

/// One client's sftp session.
#[derive(Default)]
pub struct Sftp {
    open: HashMap<String, Open>,
    next: u64,
}

fn refusal(error: &std::io::Error) -> StatusReply {
    let code = match error.kind() {
        std::io::ErrorKind::NotFound => StatusCode::NoSuchFile,
        std::io::ErrorKind::PermissionDenied => StatusCode::PermissionDenied,
        _ => StatusCode::Failure,
    };
    code.with_message(error.to_string())
}

fn ok(id: u32) -> Status {
    Status {
        id,
        status_code: StatusCode::Ok,
        error_message: "Ok".to_owned(),
        language_tag: "en-US".to_owned(),
    }
}

/// A file's attributes as the protocol carries them. The mode has the kind of file in it.
fn attributes(metadata: &Metadata) -> FileAttributes {
    FileAttributes {
        size: Some(metadata.len()),
        uid: Some(metadata.uid()),
        user: None,
        gid: Some(metadata.gid()),
        group: None,
        permissions: Some(metadata.mode()),
        atime: Some(metadata.atime().clamp(0, i64::from(u32::MAX)) as u32),
        mtime: Some(metadata.mtime().clamp(0, i64::from(u32::MAX)) as u32),
    }
}

/// `path` with its `.` and `..` worked out, from the directory the helper is in if it is
/// not from the root: for a path that is not there to ask the kernel about.
fn absolute(path: &Path) -> PathBuf {
    let mut out = if path.is_absolute() {
        PathBuf::from("/")
    } else {
        std::env::current_dir().unwrap_or_else(|_| "/".into())
    };
    for part in path.components() {
        match part {
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(name) => out.push(name),
            Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
        }
    }
    out
}

/// Sets what `attrs` names on the file at `path` or behind `file`.
fn set(path: Option<&Path>, file: Option<&File>, attrs: &FileAttributes) -> std::io::Result<()> {
    let opened;
    let file = match (file, path) {
        (Some(file), _) => Some(file),
        // Size and times need the file open; the rest goes by name.
        (None, Some(path)) if attrs.size.is_some() || attrs.mtime.is_some() => {
            opened = OpenOptions::new().write(true).open(path)?;
            Some(&opened)
        }
        _ => None,
    };
    if let (Some(size), Some(file)) = (attrs.size, file) {
        file.set_len(size)?;
    }
    if let Some(mode) = attrs.permissions {
        let permissions = std::fs::Permissions::from_mode(mode & 0o7777);
        match (file, path) {
            (Some(file), _) => file.set_permissions(permissions)?,
            (None, Some(path)) => std::fs::set_permissions(path, permissions)?,
            (None, None) => {}
        }
    }
    if attrs.uid.is_some() || attrs.gid.is_some() {
        match (file, path) {
            (Some(file), _) => std::os::unix::fs::fchown(file, attrs.uid, attrs.gid)?,
            (None, Some(path)) => std::os::unix::fs::chown(path, attrs.uid, attrs.gid)?,
            (None, None) => {}
        }
    }
    if let (Some(mtime), Some(file)) = (attrs.mtime, file) {
        let at = |seconds: u32| UNIX_EPOCH + Duration::from_secs(u64::from(seconds));
        let times = std::fs::FileTimes::new()
            .set_modified(at(mtime))
            .set_accessed(at(attrs.atime.unwrap_or(mtime)));
        file.set_times(times)?;
    }
    Ok(())
}

/// Two strings from the front of an extension's bytes: each its length, then itself.
fn two_strings(data: &[u8]) -> Option<(String, String)> {
    let one = |data: &[u8]| -> Option<(String, usize)> {
        let length = u32::from_be_bytes(data.get(..4)?.try_into().ok()?) as usize;
        let text = String::from_utf8(data.get(4..4 + length)?.to_vec()).ok()?;
        Some((text, 4 + length))
    };
    let (first, used) = one(data)?;
    let (second, _) = one(&data[used..])?;
    Some((first, second))
}

impl Sftp {
    fn hold(&mut self, open: Open) -> Result<String, StatusReply> {
        if self.open.len() >= MAX_OPEN {
            return Err(StatusCode::Failure.with_message("too many files open"));
        }
        self.next += 1;
        let handle = self.next.to_string();
        self.open.insert(handle.clone(), open);
        Ok(handle)
    }

    fn file(&self, handle: &str) -> Result<&File, StatusReply> {
        match self.open.get(handle) {
            Some(Open::File(file)) => Ok(file),
            _ => Err(StatusCode::Failure.with_message("not an open file")),
        }
    }
}

impl russh_sftp::server::Handler for Sftp {
    type Error = StatusReply;

    fn unimplemented(&self) -> Self::Error {
        StatusCode::OpUnsupported.into()
    }

    async fn init(
        &mut self,
        _version: u32,
        _extensions: HashMap<String, String>,
    ) -> Result<Version, Self::Error> {
        let mut version = Version::new();
        for extension in [
            "posix-rename@openssh.com",
            "fsync@openssh.com",
            "hardlink@openssh.com",
        ] {
            version
                .extensions
                .insert(extension.to_owned(), "1".to_owned());
        }
        Ok(version)
    }

    async fn open(
        &mut self,
        id: u32,
        filename: String,
        pflags: OpenFlags,
        attrs: FileAttributes,
    ) -> Result<Handle, Self::Error> {
        let mut options = OpenOptions::new();
        options
            .read(pflags.contains(OpenFlags::READ))
            .write(pflags.contains(OpenFlags::WRITE))
            .append(pflags.contains(OpenFlags::APPEND))
            .truncate(pflags.contains(OpenFlags::TRUNCATE))
            // The mode the client asks for, less what the user's mask takes away.
            .mode(attrs.permissions.unwrap_or(0o666) & 0o7777);
        if pflags.contains(OpenFlags::CREATE) {
            if pflags.contains(OpenFlags::EXCLUDE) {
                options.create_new(true);
            } else {
                options.create(true);
            }
        }
        let file = options.open(&filename).map_err(|error| refusal(&error))?;
        if file.metadata().is_ok_and(|metadata| metadata.is_dir()) {
            return Err(StatusCode::Failure.with_message("a directory, not a file"));
        }
        Ok(Handle {
            id,
            handle: self.hold(Open::File(file))?,
        })
    }

    async fn close(&mut self, id: u32, handle: String) -> Result<Status, Self::Error> {
        match self.open.remove(&handle) {
            Some(_) => Ok(ok(id)),
            None => Err(StatusCode::Failure.with_message("not open")),
        }
    }

    async fn read(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        len: u32,
    ) -> Result<Data, Self::Error> {
        let file = self.file(&handle)?;
        let mut data = vec![0_u8; len.min(MAX_READ) as usize];
        let count = file
            .read_at(&mut data, offset)
            .map_err(|error| refusal(&error))?;
        if count == 0 && len != 0 {
            return Err(StatusCode::Eof.into());
        }
        data.truncate(count);
        Ok(Data { id, data })
    }

    async fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<Status, Self::Error> {
        self.file(&handle)?
            .write_all_at(&data, offset)
            .map_err(|error| refusal(&error))?;
        Ok(ok(id))
    }

    async fn lstat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        let metadata = std::fs::symlink_metadata(&path).map_err(|error| refusal(&error))?;
        Ok(Attrs {
            id,
            attrs: attributes(&metadata),
        })
    }

    async fn stat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        let metadata = std::fs::metadata(&path).map_err(|error| refusal(&error))?;
        Ok(Attrs {
            id,
            attrs: attributes(&metadata),
        })
    }

    async fn fstat(&mut self, id: u32, handle: String) -> Result<Attrs, Self::Error> {
        let metadata = self
            .file(&handle)?
            .metadata()
            .map_err(|error| refusal(&error))?;
        Ok(Attrs {
            id,
            attrs: attributes(&metadata),
        })
    }

    async fn setstat(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> Result<Status, Self::Error> {
        set(Some(Path::new(&path)), None, &attrs).map_err(|error| refusal(&error))?;
        Ok(ok(id))
    }

    async fn fsetstat(
        &mut self,
        id: u32,
        handle: String,
        attrs: FileAttributes,
    ) -> Result<Status, Self::Error> {
        set(None, Some(self.file(&handle)?), &attrs).map_err(|error| refusal(&error))?;
        Ok(ok(id))
    }

    async fn opendir(&mut self, id: u32, path: String) -> Result<Handle, Self::Error> {
        let mut entries = Vec::new();
        // The directory itself and the one above, as every listing has them.
        for name in [".", ".."] {
            if let Ok(metadata) = std::fs::metadata(Path::new(&path).join(name)) {
                entries.push(Entry::new(name, attributes(&metadata)));
            }
        }
        for entry in std::fs::read_dir(&path).map_err(|error| refusal(&error))? {
            let entry = entry.map_err(|error| refusal(&error))?;
            // A name that is not text cannot be said in this protocol's strings.
            let (Ok(name), Ok(metadata)) = (entry.file_name().into_string(), entry.metadata())
            else {
                continue;
            };
            entries.push(Entry::new(name, attributes(&metadata)));
        }
        Ok(Handle {
            id,
            handle: self.hold(Open::Dir(entries.into_iter()))?,
        })
    }

    async fn readdir(&mut self, id: u32, handle: String) -> Result<Name, Self::Error> {
        let Some(Open::Dir(entries)) = self.open.get_mut(&handle) else {
            return Err(StatusCode::Failure.with_message("not an open directory"));
        };
        let files: Vec<Entry> = entries.take(NAMES_AT_ONCE).collect();
        if files.is_empty() {
            return Err(StatusCode::Eof.into());
        }
        Ok(Name { id, files })
    }

    async fn remove(&mut self, id: u32, filename: String) -> Result<Status, Self::Error> {
        std::fs::remove_file(&filename).map_err(|error| refusal(&error))?;
        Ok(ok(id))
    }

    async fn mkdir(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> Result<Status, Self::Error> {
        std::fs::DirBuilder::new()
            .mode(attrs.permissions.unwrap_or(0o777) & 0o7777)
            .create(&path)
            .map_err(|error| refusal(&error))?;
        Ok(ok(id))
    }

    async fn rmdir(&mut self, id: u32, path: String) -> Result<Status, Self::Error> {
        std::fs::remove_dir(&path).map_err(|error| refusal(&error))?;
        Ok(ok(id))
    }

    async fn realpath(&mut self, id: u32, path: String) -> Result<Name, Self::Error> {
        let path = if path.is_empty() { "." } else { path.as_str() };
        // What it really is, if it is there; where it would be, if it is not.
        let real = std::fs::canonicalize(path).unwrap_or_else(|_| absolute(Path::new(path)));
        let real = real
            .into_os_string()
            .into_string()
            .map_err(|_| StatusCode::Failure.with_message("a path that is not text"))?;
        Ok(Name {
            id,
            files: vec![Entry::dummy(real)],
        })
    }

    async fn rename(
        &mut self,
        id: u32,
        oldpath: String,
        newpath: String,
    ) -> Result<Status, Self::Error> {
        // This version of the protocol's rename never replaces: OpenSSH's own extension
        // is the one that does.
        if std::fs::symlink_metadata(&newpath).is_ok() {
            return Err(StatusCode::Failure.with_message("the new name is taken"));
        }
        std::fs::rename(&oldpath, &newpath).map_err(|error| refusal(&error))?;
        Ok(ok(id))
    }

    async fn readlink(&mut self, id: u32, path: String) -> Result<Name, Self::Error> {
        let target = std::fs::read_link(&path)
            .map_err(|error| refusal(&error))?
            .into_os_string()
            .into_string()
            .map_err(|_| StatusCode::Failure.with_message("a path that is not text"))?;
        Ok(Name {
            id,
            files: vec![Entry::dummy(target)],
        })
    }

    async fn symlink(
        &mut self,
        id: u32,
        linkpath: String,
        targetpath: String,
    ) -> Result<Status, Self::Error> {
        // OpenSSH sends the two the other way round from what the draft says, and every
        // client follows OpenSSH: the first is what the link points at, the second the link.
        std::os::unix::fs::symlink(&linkpath, &targetpath).map_err(|error| refusal(&error))?;
        Ok(ok(id))
    }

    async fn extended(
        &mut self,
        id: u32,
        request: String,
        data: Vec<u8>,
    ) -> Result<Packet, Self::Error> {
        let malformed = || StatusReply::from(StatusCode::BadMessage);
        match request.as_str() {
            "posix-rename@openssh.com" => {
                let (old, new) = two_strings(&data).ok_or_else(malformed)?;
                std::fs::rename(old, new).map_err(|error| refusal(&error))?;
            }
            "hardlink@openssh.com" => {
                let (old, new) = two_strings(&data).ok_or_else(malformed)?;
                std::fs::hard_link(old, new).map_err(|error| refusal(&error))?;
            }
            "fsync@openssh.com" => {
                let length = data
                    .get(..4)
                    .and_then(|length| length.try_into().ok())
                    .map(u32::from_be_bytes)
                    .ok_or_else(malformed)? as usize;
                let handle = data
                    .get(4..4 + length)
                    .and_then(|handle| std::str::from_utf8(handle).ok())
                    .ok_or_else(malformed)?;
                self.file(handle)?
                    .sync_all()
                    .map_err(|error| refusal(&error))?;
            }
            _ => return Err(self.unimplemented()),
        }
        Ok(Packet::Status(ok(id)))
    }
}

/// The helper's input and output as one stream, which says when the input has ended.
struct Stdio {
    input: tokio::io::Stdin,
    output: tokio::io::Stdout,
    ended: Arc<Notify>,
}

impl AsyncRead for Stdio {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let read = Pin::new(&mut self.input).poll_read(cx, buf);
        if let Poll::Ready(result) = &read
            && (result.is_err() || buf.filled().len() == before)
        {
            // The client is gone. Kept until it is asked for, so it cannot be missed.
            self.ended.notify_one();
        }
        read
    }
}

impl AsyncWrite for Stdio {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.output).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.output).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.output).poll_shutdown(cx)
    }
}

/// The helper's whole program, for a binary to call when its first argument is [`MODE`]:
/// serves one client on its input and output until the client ends, as whoever this
/// process is. Returns the exit code.
#[must_use]
pub fn main() -> i32 {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("orion: sftp could not start: {error}");
            return 1;
        }
    };
    runtime.block_on(async {
        let ended = Arc::new(Notify::new());
        let stream = Stdio {
            input: tokio::io::stdin(),
            output: tokio::io::stdout(),
            ended: ended.clone(),
        };
        russh_sftp::server::run(stream, Sftp::default()).await;
        ended.notified().await;
    });
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_that_is_not_there_is_still_worked_out() {
        assert_eq!(
            absolute(Path::new("/a/b/../c/./d")),
            PathBuf::from("/a/c/d")
        );
        assert_eq!(absolute(Path::new("/../..")), PathBuf::from("/"));
        let here = std::env::current_dir().expect("a directory");
        assert_eq!(absolute(Path::new("x/../y")), here.join("y"));
    }

    #[test]
    fn an_extension_carries_two_strings() {
        let mut data = Vec::new();
        for text in ["old name", "new"] {
            data.extend_from_slice(&(text.len() as u32).to_be_bytes());
            data.extend_from_slice(text.as_bytes());
        }
        assert_eq!(
            two_strings(&data),
            Some(("old name".to_owned(), "new".to_owned()))
        );
        for cut in 0..data.len() {
            assert_eq!(two_strings(&data[..cut]), None, "{cut}");
        }
        assert_eq!(two_strings(&[0xFF, 0xFF, 0xFF, 0xFF, 1]), None);
    }
}
