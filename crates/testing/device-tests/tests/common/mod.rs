//! What the device tests share: the device's end of a pseudo-terminal, a board directory,
//! node logic in a thread, and waiting for something to be true.

#![allow(dead_code)]

use std::fs::File;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use orion_node_app::{Node, NodeSettings};
use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};
use tokio::sync::oneshot;

/// A pseudo-terminal: the device's end, and the path of the end the board opens.
pub fn pty() -> (File, PathBuf) {
    let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY).expect("openpt");
    grantpt(&master).expect("grantpt");
    unlockpt(&master).expect("unlockpt");
    let path = ptsname(&master, Vec::new()).expect("ptsname");
    rustix::io::ioctl_fionbio(&master, true).expect("non-blocking");
    (
        File::from(master),
        PathBuf::from(path.into_string().expect("path")),
    )
}

pub fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-device-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("directory");
    dir
}

pub fn settings(dir: &Path) -> NodeSettings {
    let mut source = orion_config::Source::parse("", []).expect("no settings");
    let mut settings = NodeSettings::from_source(&mut source).expect("defaults");
    settings.config.socket = dir.join("orion.sock");
    settings.config.state_dir = dir.join("state");
    settings.files_dir = dir.join("files");
    settings.audit = Some(orion_audit::AuditConfig::new(&dir.join("state/audit.log")));
    settings.config.node_name = "board".parse().expect("name");
    settings
}

/// Node logic in a thread of its own, until dropped.
pub struct Logic {
    kill: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Logic {
    pub fn start(settings: NodeSettings) -> Self {
        let (kill, killed) = oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let mut node = Node::load(settings).expect("node logic starts");
                tokio::select! {
                    () = node.run(std::future::pending()) => {}
                    _ = killed => {}
                }
            });
        });
        Self {
            kill: Some(kill),
            thread: Some(thread),
        }
    }
}

impl Drop for Logic {
    fn drop(&mut self) {
        if let (Some(kill), Some(thread)) = (self.kill.take(), self.thread.take()) {
            let _ = kill.send(());
            let _ = thread.join();
        }
    }
}

pub fn until<T>(what: &str, mut done: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(value) = done() {
            return value;
        }
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}
