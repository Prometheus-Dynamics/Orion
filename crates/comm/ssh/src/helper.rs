//! The helper a session runs in: a process of its own that becomes the user who logged in
//! and then becomes their shell.
//!
//! Becoming someone else has to happen in the process that will be them, after it is
//! started and before the shell is. The server starts this helper with the terminal as its
//! input and output; the helper makes itself the head of a new session with that terminal,
//! gives up every right the server has that the user does not, checks that they are gone,
//! and runs the shell in its place. Nothing here runs in the server's process.

use std::ffi::OsString;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;

use rustix::process::{getgid, getuid};
use rustix::thread::{Gid, Uid};

/// The word that says a command line is for the helper.
pub const MODE: &str = "ssh-session";

/// What a session is to run as and with.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Launch {
    /// The user id to become.
    pub uid: u32,
    /// The group id to become.
    pub gid: u32,
    /// Every group to be in.
    pub groups: Vec<u32>,
    /// Where to start.
    pub home: PathBuf,
    /// Whether input is a terminal to take as the session's own.
    pub tty: bool,
    /// The environment, all of it.
    pub env: Vec<(String, String)>,
    /// The shell.
    pub shell: PathBuf,
    /// The command for the shell to run, or `None` for a login shell.
    pub command: Option<OsString>,
    /// A program and its arguments to become instead of the shell: a subsystem. Empty for
    /// the shell.
    pub program: Vec<OsString>,
}

impl Launch {
    /// The arguments that say this to the helper, after [`MODE`].
    #[must_use]
    pub fn to_args(&self) -> Vec<OsString> {
        let mut args: Vec<OsString> = vec![
            "--uid".into(),
            self.uid.to_string().into(),
            "--gid".into(),
            self.gid.to_string().into(),
            "--groups".into(),
            self.groups
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(",")
                .into(),
            "--home".into(),
            self.home.clone().into(),
        ];
        if self.tty {
            args.push("--tty".into());
        }
        for (name, value) in &self.env {
            args.push("--env".into());
            args.push(format!("{name}={value}").into());
        }
        args.push("--shell".into());
        args.push(self.shell.clone().into());
        if let Some(command) = &self.command {
            args.push("--command".into());
            args.push(command.clone());
        }
        for part in &self.program {
            args.push("--program".into());
            args.push(part.clone());
        }
        args
    }

    /// Reads what [`Launch::to_args`] wrote.
    ///
    /// # Errors
    ///
    /// What was wrong with the arguments.
    pub fn from_args(args: impl IntoIterator<Item = OsString>) -> Result<Self, String> {
        let mut launch = Self::default();
        let mut args = args.into_iter();
        let text = |value: Option<OsString>, flag: &str| {
            value
                .and_then(|value| value.into_string().ok())
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        let number = |value: String, flag: &str| {
            value
                .parse::<u32>()
                .map_err(|_| format!("{flag}: {value:?} is not a number"))
        };
        let (mut uid, mut gid, mut shell) = (None, None, None);
        while let Some(flag) = args.next() {
            match flag.to_str() {
                Some("--uid") => uid = Some(number(text(args.next(), "--uid")?, "--uid")?),
                Some("--gid") => gid = Some(number(text(args.next(), "--gid")?, "--gid")?),
                Some("--groups") => {
                    launch.groups = text(args.next(), "--groups")?
                        .split(',')
                        .filter(|group| !group.is_empty())
                        .map(|group| number(group.to_owned(), "--groups"))
                        .collect::<Result<_, _>>()?;
                }
                Some("--home") => launch.home = args.next().ok_or("--home needs a value")?.into(),
                Some("--tty") => launch.tty = true,
                Some("--env") => {
                    let pair = text(args.next(), "--env")?;
                    let (name, value) = pair.split_once('=').ok_or("--env needs NAME=VALUE")?;
                    launch.env.push((name.to_owned(), value.to_owned()));
                }
                Some("--shell") => shell = Some(args.next().ok_or("--shell needs a value")?),
                Some("--command") => {
                    launch.command = Some(args.next().ok_or("--command needs a value")?);
                }
                Some("--program") => {
                    launch
                        .program
                        .push(args.next().ok_or("--program needs a value")?);
                }
                _ => return Err(format!("unknown argument {}", flag.to_string_lossy())),
            }
        }
        launch.uid = uid.ok_or("--uid is missing")?;
        launch.gid = gid.ok_or("--gid is missing")?;
        launch.shell = shell.ok_or("--shell is missing")?.into();
        Ok(launch)
    }
}

/// Becomes the user, and checks that it has.
fn become_user(launch: &Launch) -> Result<(), String> {
    let (uid, gid) = (Uid::from_raw(launch.uid), Gid::from_raw(launch.gid));
    if getuid() == uid && getgid() == gid && !getuid().is_root() {
        // Already them, and with no right to be anyone else: the comm core runs as the
        // user it lets in.
        return Ok(());
    }
    if !getuid().is_root() {
        return Err(format!(
            "this process is user {} and cannot become user {}",
            getuid().as_raw(),
            launch.uid
        ));
    }
    let groups: Vec<Gid> = launch
        .groups
        .iter()
        .map(|group| Gid::from_raw(*group))
        .collect();
    // The helper has one thread, so what is set for the thread is set for the process.
    // Groups first, then the group, then the user: the other way round the right to set
    // the first two is already gone.
    rustix::thread::set_thread_groups(&groups).map_err(|error| format!("groups: {error}"))?;
    rustix::thread::set_thread_res_gid(gid, gid, gid).map_err(|error| format!("group: {error}"))?;
    rustix::thread::set_thread_res_uid(uid, uid, uid).map_err(|error| format!("user: {error}"))?;
    // It must have stuck, and there must be no way back.
    let stuck = getuid() == uid
        && getgid() == gid
        && rustix::process::geteuid() == uid
        && rustix::process::getegid() == gid;
    if !stuck {
        return Err("the process is not the user it was to become".into());
    }
    if launch.uid != 0
        && rustix::thread::set_thread_res_uid(Uid::ROOT, Uid::ROOT, Uid::ROOT).is_ok()
    {
        return Err("the process could become root again".into());
    }
    Ok(())
}

/// Runs the session `launch` says: becomes the user and then the shell. Does not return
/// if it works.
///
/// # Errors
///
/// Why the shell was not started.
pub fn run(launch: &Launch) -> String {
    // A session of its own, so that the terminal can be its terminal and so that the whole
    // session can be ended together. Already the head of one if it was started as one.
    let _ = rustix::process::setsid();
    if launch.tty
        && let Err(error) = rustix::process::ioctl_tiocsctty(rustix::stdio::stdin())
    {
        return format!("the terminal cannot be taken: {error}");
    }
    if let Err(error) = become_user(launch) {
        return error;
    }
    let subsystem = launch.program.split_first();
    let mut command = match subsystem {
        Some((program, args)) => {
            let mut command = std::process::Command::new(program);
            command.args(args);
            command
        }
        None => std::process::Command::new(&launch.shell),
    };
    command
        .env_clear()
        .envs(launch.env.iter().map(|(name, value)| (name, value)));
    // A home that is not there is not a reason to keep someone out.
    command.current_dir(if launch.home.is_dir() {
        launch.home.as_path()
    } else {
        "/".as_ref()
    });
    match &launch.command {
        _ if subsystem.is_some() => {}
        Some(wanted) => {
            command.arg("-c").arg(wanted);
        }
        None => {
            // A shell whose name starts with a dash is a login shell.
            let name = launch.shell.file_name().unwrap_or(launch.shell.as_os_str());
            let mut dashed = OsString::from("-");
            dashed.push(name);
            command.arg0(dashed);
        }
    }
    let what = command.get_program().to_string_lossy().into_owned();
    format!("{what}: {}", command.exec())
}

/// The helper's whole program, for a binary to call when its first argument is [`MODE`]:
/// `args` are the ones after it. Returns the exit code if the shell could not be started.
#[must_use]
pub fn main(args: impl IntoIterator<Item = OsString>) -> i32 {
    let why = match Launch::from_args(args) {
        Ok(launch) => run(&launch),
        Err(why) => why,
    };
    eprintln!("orion: the session could not start: {why}");
    // What a shell says when a command cannot be run.
    126
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_is_said_to_the_helper_is_what_it_reads() {
        let launch = Launch {
            uid: 1000,
            gid: 1000,
            groups: vec![1000, 44, 20],
            home: "/home/ana".into(),
            tty: true,
            env: vec![
                ("HOME".into(), "/home/ana".into()),
                ("TERM".into(), "xterm=odd".into()),
            ],
            shell: "/bin/zsh".into(),
            command: Some("echo 'a b' --uid 0".into()),
            program: vec!["/usr/bin/orion".into(), "ssh-sftp".into()],
        };
        assert_eq!(Launch::from_args(launch.to_args()), Ok(launch.clone()));
        let plain = Launch {
            tty: false,
            command: None,
            program: Vec::new(),
            groups: Vec::new(),
            env: Vec::new(),
            ..launch
        };
        assert_eq!(Launch::from_args(plain.to_args()), Ok(plain));
        for bad in [
            vec!["--uid", "x", "--gid", "1", "--shell", "/bin/sh"],
            vec!["--gid", "1", "--shell", "/bin/sh"],
            vec!["--uid", "1", "--gid", "1"],
            vec!["--uid", "1", "--gid", "1", "--shell", "/bin/sh", "--root"],
            vec![
                "--uid", "1", "--gid", "1", "--shell", "/bin/sh", "--env", "NOVALUE",
            ],
        ] {
            assert!(
                Launch::from_args(bad.iter().map(OsString::from)).is_err(),
                "{bad:?}"
            );
        }
    }
}
