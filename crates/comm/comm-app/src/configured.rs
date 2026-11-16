//! Files the settings name: what a board without a program to offer them still hands over,
//! in running and in recovery, as a file read as it is or a command's output. Each prefix is
//! served while no program offers it; a program that does takes it over.
//!
//! `offers.file` (`/etc/orion/offers.conf`) holds them, one a line, `#` beginning a comment:
//!
//! ```text
//! system/ kernel = command /usr/bin/dmesg bound 262144 timeout 5000
//! system/ os-release = path /etc/os-release bound 4096
//! system/ journal = command /usr/bin/journalctl -b -n 2000 bound 1048576 timeout 10000 user-data
//! ```
//!
//! - a path is read as it is, up to its bound, and never waited on: what a device has not
//!   yet is not read;
//! - a command is a fixed program and its arguments, run with no shell, as `offers.user`
//!   (the comm core's own user unless it is said); its output up to its bound is the file,
//!   and it is ended at its bound, or at its timeout in milliseconds, which rejects the fetch;
//! - `user-data` marks a file that may hold a user's data, in the prefix's index.
//!
//! **Only root may say what runs.** The file must be root's and written to by no one else, or
//! none of it is served: the comm core never runs a command another could have put there.

use std::io::Read as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use orion_file_offers::{OwnFiles, OwnName, Reading, Ready};
use orion_server::Sessions;

/// What a configured file is made from.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Made {
    Path(PathBuf),
    Command { argv: Vec<String>, timeout_ms: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    name: String,
    made: Made,
    bound: u64,
    user_data: bool,
}

/// Whom commands run as, when not the comm core's own user: this program, started again as
/// the helper ([`MODE`]), becomes them and then the command.
#[derive(Clone, Debug)]
pub(crate) struct RunAs {
    helper: PathBuf,
    uid: u32,
    gid: u32,
    groups: Vec<u32>,
}

/// The word that starts this program as the helper that runs a command as `offers.user`.
pub const MODE: &str = "offers-command";

/// The files of one prefix.
#[derive(Debug)]
pub(crate) struct Prefix {
    prefix: String,
    entries: Vec<Entry>,
    run_as: Option<RunAs>,
}

/// Every prefix `file` names, if it is root's (`owner`, for tests) and no one else may write
/// it; none if there is no file. Commands run as `user`.
///
/// # Errors
///
/// Why none of it is served.
pub(crate) fn load(file: &Path, owner: u32, user: Option<&str>) -> Result<Vec<Prefix>, String> {
    let meta = match std::fs::metadata(file) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("{}: {error}", file.display())),
    };
    if meta.uid() != owner {
        return Err(format!(
            "{} is user {}'s, not root's: none of it is served",
            file.display(),
            meta.uid()
        ));
    }
    if meta.mode() & 0o022 != 0 {
        return Err(format!(
            "{} may be written by others than root (mode {:o}): none of it is served",
            file.display(),
            meta.mode() & 0o777
        ));
    }
    let text =
        std::fs::read_to_string(file).map_err(|error| format!("{}: {error}", file.display()))?;
    let entries = parse(&text).map_err(|why| format!("{}: {why}", file.display()))?;
    let run_as = run_as(user)?;
    let mut prefixes: Vec<Prefix> = Vec::new();
    for (prefix, entry) in entries {
        match prefixes.iter_mut().find(|known| known.prefix == prefix) {
            Some(known) => known.entries.push(entry),
            None => prefixes.push(Prefix {
                prefix,
                entries: vec![entry],
                run_as: run_as.clone(),
            }),
        }
    }
    Ok(prefixes)
}

/// The user commands run as, if not this process's.
fn run_as(user: Option<&str>) -> Result<Option<RunAs>, String> {
    let Some(user) = user else {
        return Ok(None);
    };
    #[cfg(feature = "ssh")]
    {
        let found = orion_ssh::users::lookup(Path::new("/etc"), user)
            .ok_or_else(|| format!("offers.user: no account {user:?}"))?;
        let me = rustix::process::getuid();
        if me.as_raw() == found.uid {
            return Ok(None);
        }
        if !me.is_root() {
            return Err(format!(
                "offers.user: the comm core is user {} and cannot run commands as {user}",
                me.as_raw()
            ));
        }
        let helper = std::env::current_exe()
            .map_err(|error| format!("offers.user: this program has no path: {error}"))?;
        Ok(Some(RunAs {
            helper,
            uid: found.uid,
            gid: found.gid,
            groups: found.groups,
        }))
    }
    #[cfg(not(feature = "ssh"))]
    Err(format!(
        "offers.user: {user:?} cannot be looked up in this build"
    ))
}

/// The entries of the file's text, each with its prefix, or which line is wrong and why.
fn parse(text: &str) -> Result<Vec<(String, Entry)>, String> {
    let mut entries: Vec<(String, Entry)> = Vec::new();
    for (at, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        let bad = |why: &str| format!("line {}: {why}", at + 1);
        let (head, made) = line
            .split_once('=')
            .ok_or_else(|| bad("not `<prefix> <name> = path|command ...`"))?;
        let mut head = head.split_whitespace();
        let (Some(prefix), Some(last), None) = (head.next(), head.next(), head.next()) else {
            return Err(bad("not `<prefix> <name> =`"));
        };
        if !prefix.ends_with('/')
            || orion_types::FileName::new(prefix.trim_end_matches('/')).is_err()
        {
            return Err(bad(
                "the prefix is one or more parts of a name, ending in /",
            ));
        }
        let name = format!("{prefix}{last}");
        if last == "index" || last.contains('/') || orion_types::FileName::new(&name).is_err() {
            return Err(bad("not a name: one part, not `index`"));
        }
        let mut words: Vec<&str> = made.split_whitespace().collect();
        let user_data = words.last() == Some(&"user-data");
        if user_data {
            words.pop();
        }
        // `bound` and `timeout` end the line, in either order.
        let (mut bound, mut timeout_ms) = (None, None);
        while let [.., word, value] = words.as_slice()
            && matches!(*word, "bound" | "timeout")
        {
            let value: u64 = value
                .parse()
                .map_err(|_| bad(&format!("`{word}` is not followed by a number")))?;
            if *word == "bound" {
                bound = Some(value);
            } else {
                timeout_ms = Some(value);
            }
            words.truncate(words.len() - 2);
        }
        let bound = bound.ok_or_else(|| bad("no `bound <bytes>` at the end"))?;
        let made = match (words.as_slice(), timeout_ms) {
            (["path", path], None) if path.starts_with('/') => Made::Path(path.into()),
            (["path", ..], _) => return Err(bad("not `path /<absolute path> bound <bytes>`")),
            (["command", program, ..], Some(timeout_ms)) if program.starts_with('/') => {
                Made::Command {
                    argv: words[1..].iter().map(|word| (*word).to_owned()).collect(),
                    timeout_ms,
                }
            }
            (["command", ..], _) => {
                return Err(bad(
                    "not `command /<absolute program> [args] bound <bytes> timeout <ms>`",
                ));
            }
            _ => return Err(bad("neither `path` nor `command`")),
        };
        if entries.iter().any(|(_, known)| known.name == name) {
            return Err(bad("a name twice"));
        }
        entries.push((
            prefix.to_owned(),
            Entry {
                name,
                made,
                bound,
                user_data,
            },
        ));
    }
    Ok(entries)
}

impl OwnFiles for Prefix {
    fn prefix(&self) -> &str {
        &self.prefix
    }

    fn names(&self) -> Vec<OwnName> {
        self.entries
            .iter()
            .map(|entry| OwnName {
                name: entry.name.clone(),
                max_bytes: entry.bound,
                user_data: entry.user_data,
            })
            .collect()
    }

    fn read(&mut self, name: &str, _: &Sessions, ready: Ready) -> Reading {
        let Some(entry) = self.entries.iter().find(|entry| entry.name == name) else {
            return Reading::Not(format!("{name}: no such file"));
        };
        match &entry.made {
            Made::Path(path) => match read_path(path, entry.bound) {
                Ok(bytes) => Reading::Now(bytes),
                Err(error) => Reading::Not(format!("{name}: {}: {error}", path.display())),
            },
            Made::Command { argv, timeout_ms } => run(
                name,
                argv,
                entry.bound,
                *timeout_ms,
                self.run_as.clone(),
                ready,
            ),
        }
    }
}

/// Up to `bound` bytes of `path`, never waiting for more.
fn read_path(path: &Path, bound: u64) -> std::io::Result<Vec<u8>> {
    let flags = rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::NOCTTY;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(flags.bits() as i32)
        .open(path)?;
    let mut bytes = Vec::new();
    match file.take(bound).read_to_end(&mut bytes) {
        Err(error) if error.kind() != std::io::ErrorKind::WouldBlock => Err(error),
        _ => Ok(bytes),
    }
}

/// Runs `argv` on a thread of its own and hands its output to `ready`.
fn run(
    name: &str,
    argv: &[String],
    bound: u64,
    timeout_ms: u64,
    run_as: Option<RunAs>,
    ready: Ready,
) -> Reading {
    let mut command = match &run_as {
        None => Command::new(&argv[0]),
        Some(who) => {
            let groups: Vec<String> = who.groups.iter().map(u32::to_string).collect();
            let mut command = Command::new(&who.helper);
            command
                .args([MODE, &who.uid.to_string(), &who.gid.to_string()])
                .arg(groups.join(","))
                .arg(&argv[0]);
            command
        }
    };
    command
        .args(&argv[1..])
        .env_clear()
        .env("PATH", "/usr/sbin:/usr/bin:/sbin:/bin")
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return Reading::Not(format!("{name}: {} cannot be started: {error}", argv[0]));
        }
    };
    let (told, heard) = tokio::sync::oneshot::channel();
    let (name, program) = (name.to_owned(), argv[0].clone());
    std::thread::spawn(move || {
        let made = output(&mut child, bound, Duration::from_millis(timeout_ms));
        let _ = child.kill();
        let status = child.wait();
        let made = match (made, status) {
            (Err(why), _) => Err(format!("{name}: {program} {why}")),
            // Ended at its bound: what it wrote is the file.
            (Ok((bytes, true)), _) => Ok(bytes),
            (Ok((bytes, false)), Ok(status)) if status.success() => Ok(bytes),
            (Ok(_), Ok(status)) => Err(format!("{name}: {program} {status}")),
            (Ok(_), Err(error)) => Err(format!("{name}: {program}: {error}")),
        };
        let _ = told.send(made);
    });
    tokio::task::spawn_local(async move {
        let made = heard
            .await
            .unwrap_or_else(|_| Err("the command was lost".into()));
        ready.done(made);
    });
    Reading::Later
}

/// What `child` writes, up to `bound` bytes, and whether it reached it; or why not, if it is
/// still writing at `timeout`.
fn output(
    child: &mut std::process::Child,
    bound: u64,
    timeout: Duration,
) -> Result<(Vec<u8>, bool), String> {
    let Some(mut out) = child.stdout.take() else {
        return Err("has no output".into());
    };
    let deadline = Instant::now() + timeout;
    let mut bytes = Vec::new();
    let mut buf = [0_u8; 8192];
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let mut fds = [rustix::event::PollFd::new(
            &out,
            rustix::event::PollFlags::IN,
        )];
        let timespec = rustix::event::Timespec::try_from(left).unwrap_or_default();
        match rustix::event::poll(&mut fds, Some(&timespec)) {
            Ok(0) => return Err(format!("did not finish in {} ms", timeout.as_millis())),
            Ok(_) | Err(rustix::io::Errno::INTR) => {}
            Err(error) => return Err(format!("output: {error}")),
        }
        let room = usize::try_from(bound.saturating_sub(bytes.len() as u64))
            .unwrap_or(usize::MAX)
            .min(buf.len());
        match out.read(&mut buf[..room]) {
            Ok(0) => return Ok((bytes, false)),
            Ok(read) => {
                bytes.extend_from_slice(&buf[..read]);
                if bytes.len() as u64 >= bound {
                    return Ok((bytes, true));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(format!("output: {error}")),
        }
    }
}

/// The helper's whole program, for a binary to call when its first argument is [`MODE`]:
/// `<uid> <gid> <groups,...> <program> [args]`. Becomes the user, its groups first, checks it
/// cannot go back, then is the program. Gives an exit code only if that fails.
#[cfg(feature = "ssh")]
#[must_use]
pub fn command_main(args: impl IntoIterator<Item = std::ffi::OsString>) -> i32 {
    use std::os::unix::process::CommandExt as _;
    let args: Vec<std::ffi::OsString> = args.into_iter().collect();
    match become_user(&args) {
        Ok(()) => {
            let error = Command::new(&args[3]).args(&args[4..]).exec();
            eprintln!("{MODE}: {}: {error}", args[3].to_string_lossy());
        }
        Err(why) => eprintln!("{MODE}: {why}"),
    }
    127
}

#[cfg(feature = "ssh")]
fn become_user(args: &[std::ffi::OsString]) -> Result<(), String> {
    use rustix::process::{Gid, Uid};
    let number = |at: usize| -> Result<u32, String> {
        args.get(at)
            .and_then(|arg| arg.to_str())
            .and_then(|arg| arg.parse().ok())
            .ok_or_else(|| "usage: <uid> <gid> <groups,...> <program> [args]".to_owned())
    };
    let (uid, gid) = (Uid::from_raw(number(0)?), Gid::from_raw(number(1)?));
    if args.len() < 4 {
        return Err("no program".into());
    }
    let groups: Vec<Gid> = args[2]
        .to_str()
        .unwrap_or_default()
        .split(',')
        .filter_map(|group| group.parse().ok())
        .map(Gid::from_raw)
        .collect();
    // One thread: what is set for it is set for the process. Groups, then the group, then
    // the user: the other way round the right to set the first two is gone.
    rustix::thread::set_thread_groups(&groups).map_err(|error| format!("groups: {error}"))?;
    rustix::thread::set_thread_res_gid(gid, gid, gid).map_err(|error| format!("group: {error}"))?;
    rustix::thread::set_thread_res_uid(uid, uid, uid).map_err(|error| format!("user: {error}"))?;
    if rustix::process::getuid() != uid
        || rustix::process::geteuid() != uid
        || rustix::process::getgid() != gid
        || (!uid.is_root()
            && rustix::thread::set_thread_res_uid(Uid::ROOT, Uid::ROOT, Uid::ROOT).is_ok())
    {
        return Err("not the user it was to become, or could go back".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_form_of_entry_reads() {
        let text = "# what a board hands over\n\
                    system/ kernel = command /usr/bin/dmesg --raw bound 262144 timeout 5000\n\
                    \n\
                    system/ release = path /etc/os-release bound 4096   # its name\n\
                    logs/ journal = command /usr/bin/journalctl -b bound 1048576 timeout 10000 user-data\n";
        let entries = parse(text).expect("entries");
        assert_eq!(
            entries,
            [
                (
                    "system/".into(),
                    Entry {
                        name: "system/kernel".into(),
                        made: Made::Command {
                            argv: vec!["/usr/bin/dmesg".into(), "--raw".into()],
                            timeout_ms: 5000
                        },
                        bound: 262_144,
                        user_data: false,
                    }
                ),
                (
                    "system/".into(),
                    Entry {
                        name: "system/release".into(),
                        made: Made::Path("/etc/os-release".into()),
                        bound: 4096,
                        user_data: false,
                    }
                ),
                (
                    "logs/".into(),
                    Entry {
                        name: "logs/journal".into(),
                        made: Made::Command {
                            argv: vec!["/usr/bin/journalctl".into(), "-b".into()],
                            timeout_ms: 10_000
                        },
                        bound: 1_048_576,
                        user_data: true,
                    }
                ),
            ]
        );
    }

    #[test]
    fn an_entry_that_is_not_one_says_its_line_and_why() {
        let cases = [
            (
                "system/ kernel path /x bound 1",
                "not `<prefix> <name> = path|command ...`",
            ),
            (
                "system kernel = path /x bound 1",
                "the prefix is one or more parts of a name, ending in /",
            ),
            (
                "system/ index = path /x bound 1",
                "not a name: one part, not `index`",
            ),
            (
                "system/ a/b = path /x bound 1",
                "not a name: one part, not `index`",
            ),
            ("system/ k = path /x", "no `bound <bytes>` at the end"),
            (
                "system/ k = path /x bound lots",
                "`bound` is not followed by a number",
            ),
            (
                "system/ k = path x bound 1",
                "not `path /<absolute path> bound <bytes>`",
            ),
            (
                "system/ k = command dmesg bound 1 timeout 1",
                "not `command /<absolute program> [args] bound <bytes> timeout <ms>`",
            ),
            (
                "system/ k = command /bin/dmesg bound 1",
                "not `command /<absolute program> [args] bound <bytes> timeout <ms>`",
            ),
            (
                "system/ k = path /x bound 1 timeout 9",
                "not `path /<absolute path> bound <bytes>`",
            ),
            (
                "system/ k = shell dmesg bound 1",
                "neither `path` nor `command`",
            ),
        ];
        for (line, why) in cases {
            assert_eq!(parse(line), Err(format!("line 1: {why}")), "{line}");
        }
        assert_eq!(
            parse("a/ k = path /x bound 1\na/ k = path /y bound 1\n"),
            Err("line 2: a name twice".into())
        );
    }

    #[cfg(feature = "ssh")]
    #[test]
    fn commands_run_as_offers_user_only_where_the_comm_core_may_become_them() {
        let me = rustix::process::getuid();
        let mine = orion_ssh::users::lookup_uid(Path::new("/etc"), me.as_raw()).expect("me");
        assert!(run_as(None).expect("no one").is_none());
        assert!(
            run_as(Some(&mine.name)).expect("myself").is_none(),
            "no helper for myself"
        );
        let nobody = run_as(Some("no-such-account-here")).expect_err("no account");
        assert_eq!(nobody, "offers.user: no account \"no-such-account-here\"");
        if !me.is_root() {
            let root = run_as(Some("root")).expect_err("not root");
            assert!(root.contains("cannot run commands as root"), "{root}");
        }
        // The helper says how it is used, and becomes no one, given less.
        assert_eq!(command_main(["1000".into()]), 127);
    }

    #[test]
    fn a_file_others_could_write_is_not_served() {
        let dir = std::env::temp_dir().join(format!("orion-offers-conf-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let file = dir.join("offers.conf");
        std::fs::write(&file, "a/ k = path /etc/hostname bound 100\n").expect("file");
        let me = rustix::process::getuid().as_raw();
        let not_root = load(&file, me + 1, None).expect_err("not the owner's");
        assert!(not_root.contains("not root's"), "{not_root}");
        let set = |mode| {
            std::fs::set_permissions(&file, std::os::unix::fs::PermissionsExt::from_mode(mode))
                .expect("mode");
        };
        set(0o664);
        let group = load(&file, me, None).expect_err("the group may write it");
        assert!(group.contains("may be written by others"), "{group}");
        set(0o646);
        assert!(load(&file, me, None).is_err(), "anyone may write it");
        set(0o644);
        let prefixes = load(&file, me, None).expect("served");
        assert_eq!(prefixes.len(), 1);
        assert!(
            load(&dir.join("none"), me, None)
                .expect("no file")
                .is_empty()
        );
    }
}
