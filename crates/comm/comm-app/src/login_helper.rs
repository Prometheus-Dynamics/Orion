//! `orion login-helper [settings file]`: the login helper, the one process that becomes
//! other users ([`orion_ssh::login`]).
//!
//! The system starts it when the comm core first connects (`orion-login.socket`, which hands
//! the listening socket over as its input), and it exits when it has been idle for
//! `login.idle_ms`. Run by hand, it listens at `login.helper` itself.
//!
//! It reads the settings the comm core reads, the same way: when they cannot be read it
//! answers as recovery does, from the last good start's record if there is one, with the
//! recovery keys deciding who comes in. So a board whose settings are broken still lets in
//! whoever the recovery keys name.

use std::os::fd::AsFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::time::Duration;

use orion_config::ConfigError;
use orion_keys::{Keys, KeysConfig};
use orion_ssh::login::{HelperConfig, RecoveryLogin, serve};

use crate::{CommSettings, RecoverWith};

/// Where the login helper listens unless `login.helper` says otherwise, and where a comm core
/// that is not root and cannot read its settings looks for it.
pub const LOGIN_SOCKET: &str = "/run/orion-login/login.sock";

/// `login.*`: the login helper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoginSettings {
    /// `login.helper`: where the login helper listens. `None` starts sessions in the comm
    /// core, which must then be root to let anyone in but itself.
    pub helper: Option<PathBuf>,
    /// `login.comm_user`: the comm core's user, the only one the helper answers. A name or a
    /// number.
    pub comm_user: String,
    /// `login.idle_ms`: how long the helper stays with nothing running and nothing asked.
    pub idle: Duration,
}

impl LoginSettings {
    /// Takes the settings out of `source`.
    ///
    /// # Errors
    ///
    /// A value that does not parse.
    pub fn from_source(source: &mut orion_config::Source) -> Result<Self, ConfigError> {
        Ok(Self {
            helper: source.optional("login.helper")?,
            comm_user: source.get("login.comm_user", "orion-comm".to_owned())?,
            idle: Duration::from_millis(source.get("login.idle_ms", 10_000_u64)?),
        })
    }
}

impl Default for LoginSettings {
    fn default() -> Self {
        Self {
            helper: None,
            comm_user: "orion-comm".to_owned(),
            idle: Duration::from_secs(10),
        }
    }
}

/// The login helper a comm core in recovery asks: the settings' if they were read, else the
/// usual one if this process cannot start sessions itself and the helper's socket is there.
#[must_use]
pub fn recovery_helper(settings: Option<&CommSettings>, with: &RecoverWith) -> Option<PathBuf> {
    match settings {
        Some(settings) => settings.login.helper.clone(),
        None => {
            let path = with.login_socket();
            (!rustix::process::getuid().is_root() && path.exists()).then_some(path)
        }
    }
}

/// The user id `user` names: a number, or an account in `etc`.
fn uid_of(user: &str, etc: &Path) -> Option<u32> {
    user.parse()
        .ok()
        .or_else(|| orion_ssh::users::lookup(etc, user).map(|user| user.uid))
}

/// The recovery way in's reading of a keys file.
fn recovery_keys(path: &Path) -> Option<Keys> {
    let text = std::fs::read_to_string(path).ok()?;
    orion_recovery::trusted(&orion_recovery::keys_in(&text))
}

/// The SSH settings a recovery session starts with, as recovery starts them: the board's own
/// helper and subsystems if its settings say, else this program and sftp.
fn recovery_ssh(settings: Option<&CommSettings>, program: PathBuf) -> orion_ssh::SshConfig {
    let ssh = settings.and_then(|settings| settings.ssh.as_ref());
    let helper = ssh.map_or_else(
        || orion_ssh::Helper {
            program,
            args: Vec::new(),
        },
        |ssh| ssh.helper.clone(),
    );
    #[allow(unused_mut)]
    let mut subsystems = ssh.map(|ssh| ssh.subsystems.clone()).unwrap_or_default();
    #[cfg(feature = "ssh-sftp")]
    if subsystems.is_empty() {
        subsystems.push(orion_ssh::Subsystem {
            name: "sftp".to_owned(),
            program: vec![helper.program.clone().into(), orion_ssh_sftp::MODE.into()],
        });
    }
    orion_ssh::SshConfig {
        listen: std::net::SocketAddr::from(([0, 0, 0, 0], 0)),
        interface: None,
        host_key: PathBuf::new(),
        helper,
        subsystems,
        forward: false,
        etc: "/etc".into(),
        recovery: true,
        max_connections: 0,
        max_unauthenticated: 0,
        login: Duration::from_secs(60),
    }
}

/// Serves logins for the comm core under `settings` until idle. Returns the exit code.
#[must_use]
pub fn run(settings: Result<CommSettings, ConfigError>, with: &RecoverWith) -> i32 {
    let fail = |why: String| {
        eprintln!("orion: the login helper could not start: {why}");
        1
    };
    let view = orion_config::SecurityView::read(&with.security_view);
    // As recovery reads them: the settings, or the last good start's if they cannot be.
    let (settings, broken) = match settings {
        Ok(settings) => (Some(settings), false),
        Err(_) => (crate::last_good::read(&with.last_good, &view), true),
    };
    let login = settings
        .as_ref()
        .map_or_else(|| with.login.clone(), |settings| settings.login.clone());
    let helper_program = with
        .helper
        .clone()
        .or_else(|| std::env::current_exe().ok())
        .unwrap_or_else(|| "/usr/bin/orion".into());
    let recovery_ssh = recovery_ssh(settings.as_ref(), helper_program);
    let etc = recovery_ssh.etc.clone();
    let Some(comm_uid) = uid_of(&login.comm_user, &etc) else {
        return fail(format!(
            "login.comm_user {} is not a user here",
            login.comm_user
        ));
    };
    let (ssh, shell, keys, security_view, sandbox_user) = match (&settings, broken) {
        (Some(settings), false) => (
            settings.ssh.clone(),
            shell_of(settings),
            settings.keys.clone(),
            settings.config.security_view.path.clone(),
            sandbox_user_of(settings),
        ),
        // Settings that cannot be read are believed in nothing but recovery: only a comm core
        // in recovery is answered.
        _ => (
            None,
            None,
            KeysConfig::in_memory("recovery"),
            with.security_view.clone(),
            "nobody".to_owned(),
        ),
    };
    let config = HelperConfig {
        ssh,
        shell,
        sandbox_user,
        keys,
        security_view,
        comm_uid,
        idle: login.idle,
        recovery: RecoveryLogin {
            ssh: recovery_ssh,
            keys_file: settings.as_ref().map_or_else(
                || with.keys_file.clone(),
                |settings| settings.recovery_keys.clone(),
            ),
            trust: recovery_keys,
        },
    };
    let listener = match given() {
        Some(listener) => listener,
        None => {
            let path = login.helper.clone().unwrap_or_else(|| with.login_socket());
            match bind(&path, comm_uid) {
                Ok(listener) => listener,
                Err(error) => return fail(format!("{}: {error}", path.display())),
            }
        }
    };
    match serve(&config, &listener) {
        Ok(()) => 0,
        Err(error) => fail(error.to_string()),
    }
}

/// The shell's way of starting sessions, in a build that has the shell.
#[cfg(feature = "shell")]
fn shell_of(settings: &CommSettings) -> Option<orion_ssh::SshConfig> {
    settings.shell.clone()
}

#[cfg(not(feature = "shell"))]
const fn shell_of(_: &CommSettings) -> Option<orion_ssh::SshConfig> {
    None
}

/// The account an SSH connection's child becomes: `ssh.sandbox_user`, or `nobody`.
#[cfg(feature = "ssh-sandbox")]
fn sandbox_user_of(settings: &CommSettings) -> String {
    settings
        .ssh_sandbox
        .as_ref()
        .map_or_else(|| "nobody".to_owned(), |sandbox| sandbox.user.clone())
}

#[cfg(not(feature = "ssh-sandbox"))]
fn sandbox_user_of(_: &CommSettings) -> String {
    "nobody".to_owned()
}

/// The listening socket the system handed over as input, if it did.
fn given() -> Option<UnixListener> {
    let stdin = rustix::stdio::stdin();
    rustix::net::sockopt::socket_acceptconn(stdin)
        .ok()
        .filter(|listening| *listening)?;
    stdin
        .as_fd()
        .try_clone_to_owned()
        .ok()
        .map(UnixListener::from)
}

/// Listens at `path`, for the comm core's user alone.
fn bind(path: &Path, comm_uid: u32) -> std::io::Result<UnixListener> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if std::os::unix::fs::FileTypeExt::is_socket(&meta.file_type()) => {
            std::fs::remove_file(path)?;
        }
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "something that is not a socket is there",
            ));
        }
        Err(_) => {}
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    if rustix::process::getuid().as_raw() != comm_uid {
        rustix::fs::chown(path, Some(rustix::fs::Uid::from_raw(comm_uid)), None)?;
    }
    Ok(listener)
}
