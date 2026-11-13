//! Starting the comm core, or, if it cannot be started as it was told to, the recovery way
//! in. A board that cannot be reached cannot be repaired, so a broken config, a broken
//! state directory or a full disk does not end the program: it ends in recovery.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use orion_check::Security;
use orion_config::ConfigError;
use orion_recovery::{Recovery, RecoveryConfig};
use tracing::info;

use crate::{CommCore, CommSettings, RecoveryComm, StartError, systemd};

/// Recovery's host key when the state directory must not be written: in /run, for one boot.
const RUN_HOST_KEY: &str = "/run/orion-recovery/ssh_host_ed25519_key";

/// What recovery falls back on when the settings say nothing it can use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoverWith {
    /// Where to listen, after the address the settings name if they could be read.
    pub listen: Vec<SocketAddr>,
    /// The recovery keys, if the settings could not be read.
    pub keys_file: PathBuf,
    /// The program sessions run in, if the settings name none: this program.
    pub helper: Option<PathBuf>,
    /// How long between tries to listen.
    pub retry: Duration,
    /// The record of the last good start, opened when the settings cannot be read.
    pub last_good: PathBuf,
}

impl Default for RecoverWith {
    fn default() -> Self {
        Self {
            listen: vec![
                SocketAddr::from(([0, 0, 0, 0], 22)),
                SocketAddr::from(([0, 0, 0, 0], 2222)),
            ],
            keys_file: orion_recovery::KEYS_FILE.into(),
            helper: None,
            retry: Duration::from_secs(1),
            last_good: crate::last_good::USUAL.into(),
        }
    }
}

impl RecoverWith {
    /// The defaults, with what the environment says: `ORION_RECOVERY_KEYS` (the keys file)
    /// and `ORION_RECOVERY_LISTEN` (addresses joined by `,`). The environment is still there
    /// when the settings file is what is broken.
    #[must_use]
    pub fn from_env() -> Self {
        let mut with = Self {
            last_good: crate::last_good::path_from_env(),
            ..Self::default()
        };
        if let Some(keys) = std::env::var_os("ORION_RECOVERY_KEYS") {
            with.keys_file = keys.into();
        }
        if let Ok(listen) = std::env::var("ORION_RECOVERY_LISTEN") {
            let told: Vec<_> = listen
                .split(',')
                .filter_map(|addr| addr.trim().parse().ok())
                .collect();
            if !told.is_empty() {
                with.listen = told;
            }
        }
        with
    }
}

/// What was started.
#[derive(Debug)]
pub enum Started {
    /// The comm core, as it was told to be.
    Core(Box<CommCore>),
    /// The recovery way in, because the comm core could not be.
    Recovery(InRecovery),
}

/// Recovery: SSH, and Orion's own protocol saying the board is in recovery. Dropping it
/// stops both.
#[derive(Debug)]
pub struct InRecovery {
    /// The SSH way in.
    pub ssh: Recovery,
    /// The board's socket and USB links, with the hello document only.
    pub comm: RecoveryComm,
}

impl std::ops::Deref for InRecovery {
    type Target = Recovery;

    fn deref(&self) -> &Recovery {
        &self.ssh
    }
}

/// The recovery way in for a comm core that could not start because of `why`. `settings`
/// are the board's, if they could be read: only then is anything in them believed. If they
/// could not, the ways in of the last good start are opened, from its record, if there is
/// one; with neither, the usual socket, and a USB link for any name root hands over.
#[must_use]
pub fn recover(settings: Option<&CommSettings>, why: String, with: RecoverWith) -> InRecovery {
    let record = settings
        .is_none()
        .then(|| crate::last_good::read(&with.last_good))
        .flatten();
    let why = match &record {
        Some(_) => format!(
            "{why}; the ways in are the last good start's ({})",
            with.last_good.display()
        ),
        None => why,
    };
    let settings = settings.or(record.as_ref());
    let comm = RecoveryComm::start(settings, &why);
    let ssh = settings.and_then(|settings| settings.ssh.as_ref());
    let helper = ssh
        .map(|ssh| ssh.helper.clone())
        .unwrap_or_else(|| orion_ssh::Helper {
            program: with
                .helper
                .clone()
                .or_else(|| std::env::current_exe().ok())
                .unwrap_or_else(|| "/usr/bin/orion".into()),
            args: Vec::new(),
        });
    #[allow(unused_mut)]
    let mut subsystems = ssh.map(|ssh| ssh.subsystems.clone()).unwrap_or_default();
    #[cfg(feature = "ssh-sftp")]
    if subsystems.is_empty() {
        // Files can be copied off a broken board, and a fixed one copied on.
        subsystems.push(orion_ssh::Subsystem {
            name: "sftp".to_owned(),
            program: vec![helper.program.clone().into(), orion_ssh_sftp::MODE.into()],
        });
    }
    // The address the board's SSH has, then the ones recovery is told, then the usual.
    let mut listen: Vec<SocketAddr> = ssh.map(|ssh| ssh.listen).into_iter().collect();
    let told = settings
        .map(|settings| settings.recovery_listen.as_slice())
        .filter(|told| !told.is_empty())
        .unwrap_or(&with.listen);
    for addr in told {
        if !listen.contains(addr) {
            listen.push(*addr);
        }
    }
    let state_dir = settings.map_or_else(
        || PathBuf::from("/var/lib/orion"),
        |settings| settings.config.state_dir.clone(),
    );
    let mut host_key = ssh.map_or_else(
        || state_dir.join("ssh_host_ed25519_key"),
        |ssh| ssh.host_key.clone(),
    );
    // A state partition that is not mounted is not written to, not even for the host key:
    // recovery keeps one in /run, for this boot.
    if settings.is_some_and(|settings| settings.state_require_mount)
        && crate::state_dirs::on_root(&host_key, std::path::Path::new("/"))
    {
        host_key = PathBuf::from(RUN_HOST_KEY);
    }
    let ssh = Recovery::start(RecoveryConfig {
        listen,
        // Only from settings that were read: ones that could not be say nothing.
        interface: ssh.and_then(|ssh| ssh.interface.clone()),
        keys_file: settings.map_or(with.keys_file, |settings| settings.recovery_keys.clone()),
        host_key,
        helper,
        subsystems,
        etc: "/etc".into(),
        // Settings that could not be read say nothing: security is on.
        security: settings.map_or(Security::On, |settings| settings.config.security),
        retry: with.retry,
        why,
    });
    InRecovery { ssh, comm }
}

/// Starts the comm core `settings` describe. If the settings could not be read, or the
/// comm core cannot start as they say, starts the recovery way in instead. Must be called
/// inside a `LocalSet` on a current-thread runtime.
pub async fn start_or_recover(
    settings: Result<CommSettings, ConfigError>,
    with: RecoverWith,
) -> Started {
    let settings = match settings {
        Ok(settings) => settings,
        Err(error) => {
            return Started::Recovery(recover(
                None,
                format!("the settings cannot be read: {error}"),
                with,
            ));
        }
    };
    match CommCore::start(settings.clone()).await {
        Ok(core) => Started::Core(Box::new(core)),
        Err(error) => Started::Recovery(recover(Some(&settings), error.to_string(), with)),
    }
}

/// Runs the comm core until it is told to stop, as [`crate::run`] does, and if it cannot
/// start, runs the recovery way in until it is told to stop instead of ending.
///
/// # Errors
///
/// Only if there is no runtime or no signals to wait on: nothing a board's own state can
/// cause.
pub fn run_or_recover(
    settings: Result<CommSettings, ConfigError>,
    unknown: &[orion_config::Warning],
    with: RecoverWith,
) -> Result<(), StartError> {
    use tokio::signal::unix::{SignalKind, signal};
    for warning in unknown {
        tracing::warn!("{warning}");
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| crate::failed("runtime", error))?;
    tokio::task::LocalSet::new().block_on(&runtime, async {
        let mut terminate =
            signal(SignalKind::terminate()).map_err(|error| crate::failed("signals", error))?;
        let mut interrupt =
            signal(SignalKind::interrupt()).map_err(|error| crate::failed("signals", error))?;
        let notifier = systemd::Notifier::from_env();
        let recovery = match settings {
            Ok(settings) => {
                let stop = async {
                    tokio::select! {
                        _ = terminate.recv() => info!("asked to stop"),
                        _ = interrupt.recv() => info!("interrupted"),
                    }
                };
                let ran = crate::run_until(
                    settings.clone(),
                    systemd::Notifier::from_env(),
                    systemd::watchdog_interval_from_env(),
                    stop,
                )
                .await;
                match ran {
                    Ok(()) => return Ok(()),
                    Err(error) => recover(Some(&settings), error.to_string(), with),
                }
            }
            Err(error) => recover(None, format!("the settings cannot be read: {error}"), with),
        };
        // Up, as far as the service manager is concerned: it is not to be restarted into
        // the same failure, and it is not to be given up on.
        if let Some(notifier) = &notifier {
            notifier.ready(&format!("recovery: {}", recovery.why()));
        }
        tokio::select! {
            _ = terminate.recv() => info!("asked to stop"),
            _ = interrupt.recv() => info!("interrupted"),
        }
        if let Some(notifier) = &notifier {
            notifier.stopping("stopping");
        }
        drop(recovery);
        Ok(())
    })
}
