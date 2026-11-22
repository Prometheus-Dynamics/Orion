//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

#[path = "comm.rs"]
mod comm;
#[cfg(all(feature = "offers", feature = "recovery"))]
#[path = "configured.rs"]
mod configured;
#[cfg(all(feature = "tcp", feature = "ssh"))]
#[path = "interfaces.rs"]
mod interfaces;
#[cfg(all(feature = "offers", feature = "recovery"))]
#[path = "own_files.rs"]
mod own_files;
#[cfg(feature = "recovery")]
#[path = "precheck.rs"]
mod precheck;
#[cfg(all(
    feature = "recovery",
    feature = "ssh-sftp",
    feature = "ssh-sandbox",
    feature = "nt4"
))]
#[path = "recovery.rs"]
mod recovery;
#[cfg(all(feature = "shell", feature = "recovery"))]
#[path = "shell.rs"]
mod shell;
#[cfg(all(feature = "shell", feature = "recovery", feature = "link-usb"))]
#[path = "signed_link.rs"]
mod signed_link;
#[cfg(all(feature = "ws", feature = "ssh", feature = "nt4"))]
#[path = "ways.rs"]
mod ways;
