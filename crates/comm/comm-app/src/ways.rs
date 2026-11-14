//! The ways in that listen on an address of their own. One that cannot open yet, because its
//! address is taken or not on this machine, is tried again, and the rest of the comm core runs
//! meanwhile. Settings that cannot work are not tried again: they are the start's error.

use std::fmt::Display;
use std::io;

use orion_server::{Retry, may_clear, open_retrying};
use tokio::task::JoinHandle;

use crate::carriers::Tasks;

/// What `start` opens, with `then` called on it. Or, when it cannot open yet, `None`, and a
/// task in `tasks` that tries again as `retry` says, calls `then` once it opens, and keeps
/// both for as long as the comm core runs. An error that trying again does not help is
/// returned.
pub(crate) async fn now_or_later<T: 'static>(
    way: &'static str,
    at: impl Display + 'static,
    retry: Retry,
    tasks: &mut Tasks,
    mut start: impl AsyncFnMut() -> io::Result<T> + 'static,
    then: impl FnOnce(&T) -> Option<JoinHandle<()>> + 'static,
) -> io::Result<Option<T>> {
    match start().await {
        Ok(server) => {
            tasks.extend(then(&server));
            Ok(Some(server))
        }
        Err(error) if may_clear(&error) => {
            tasks.push(tokio::task::spawn_local(async move {
                let server = open_retrying(way, &at, retry, start).await;
                let task = then(&server).map(Abort);
                // Held until the comm core's tasks are aborted, which drops them.
                let _kept = (server, task);
                std::future::pending::<()>().await;
            }));
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// A task that ends when this is dropped.
struct Abort(JoinHandle<()>);

impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The SSH way in: with a child for each connection when the settings say so, and in the comm
/// core itself on an image with no account for the children to become.
#[cfg(feature = "ssh")]
pub(crate) async fn ssh(
    config: orion_ssh::SshConfig,
    #[cfg(feature = "ssh-sandbox")] sandbox: Option<orion_ssh_sandbox::SandboxConfig>,
    security: orion_server::Security,
    keys: orion_keys::SharedKeys,
) -> io::Result<crate::SshServer> {
    use crate::SshServer;
    #[cfg(feature = "ssh-sandbox")]
    if let Some(sandbox) = sandbox {
        match orion_ssh_sandbox::SandboxedSsh::start(
            config.clone(),
            sandbox,
            security,
            keys.clone(),
        )
        .await
        {
            Ok(server) => return Ok(SshServer::Sandboxed(server)),
            // An image with no account for the children to become. Reading in this process
            // lets in nobody it would not have: a way in is worth more than none. An address
            // that cannot be listened on is not helped by trying again without.
            Err(error) if error.kind() == io::ErrorKind::Other => {
                tracing::warn!(
                    "ssh: no child processes for connections ({error}): they are read in the \
                     comm core itself"
                );
            }
            Err(error) => return Err(error),
        }
    }
    orion_ssh::Ssh::start(config, security, keys)
        .await
        .map(SshServer::Plain)
}
