//! The key the board shows a client so that it is known to be the same board next time.

use std::io::Read;
use std::path::Path;

use russh::keys::PrivateKey;
use russh::keys::ssh_key::LineEnding;
use russh::keys::ssh_key::private::Ed25519Keypair;
use tracing::{info, warn};

fn random_seed() -> std::io::Result<[u8; 32]> {
    let mut seed = [0_u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut seed)?;
    Ok(seed)
}

fn generate() -> std::io::Result<PrivateKey> {
    Ok(PrivateKey::from(Ed25519Keypair::from_seed(&random_seed()?)))
}

fn write(path: &Path, key: &PrivateKey) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let text = key
        .to_openssh(LineEnding::LF)
        .map_err(std::io::Error::other)?;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    // Beside and renamed over: never half a key where the key belongs.
    let beside = path.with_extension("new");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&beside)?;
    file.write_all(text.as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&beside, path)
}

/// The host key at `path`, in OpenSSH's format, made if it is not there.
///
/// A key that is there but cannot be read as one is moved aside and another made, and that
/// is said loudly: clients will be told the board has changed. A key that cannot be kept,
/// because the disk is full or cannot be written, is used for this run only. Whatever is
/// wrong with the disk, there is a key and the way in stays open.
///
/// # Errors
///
/// Only if the kernel gives no random bytes.
pub fn load_or_make(path: &Path) -> std::io::Result<PrivateKey> {
    match std::fs::read_to_string(path) {
        Ok(text) => match PrivateKey::from_openssh(&text) {
            Ok(key) => return Ok(key),
            Err(error) => {
                let aside = path.with_extension(format!(
                    "corrupt-{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |since| since.as_millis())
                ));
                warn!(
                    "ssh: the host key {} is not a key ({error}): moved to {} and another made; \
                     clients will see the board's key change",
                    path.display(),
                    aside.display()
                );
                let _ = std::fs::rename(path, &aside);
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => warn!(
            "ssh: the host key {} cannot be read: {error}",
            path.display()
        ),
    }
    let key = generate()?;
    match write(path, &key) {
        Ok(()) => info!("ssh: made the host key {}", path.display()),
        Err(error) => warn!(
            "ssh: the host key cannot be kept at {} ({error}): this run has one of its own",
            path.display()
        ),
    }
    Ok(key)
}
