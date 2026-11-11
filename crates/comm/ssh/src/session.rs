//! One session: a shell or a command, in the helper, with its output going to the client
//! and the client's input going to it.

use std::os::fd::OwnedFd;
use std::process::Stdio;
use std::time::Duration;

use russh::ChannelId;
use russh::server::Handle;
use rustix::fs::{Mode, OFlags};
use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};
use rustix::termios::{Winsize, tcsetwinsize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::Helper;
use crate::helper::{Launch, MODE};

/// The terminal a client asked for.
#[derive(Clone, Debug)]
pub(crate) struct Terminal {
    pub term: String,
    pub cols: u32,
    pub rows: u32,
}

/// What comes from the client.
pub(crate) enum Input {
    Data(Vec<u8>),
    /// The client will send no more.
    End,
}

/// A session that is running. Dropping it ends it.
pub(crate) struct Running {
    input: mpsc::Sender<Input>,
    /// The terminal, for changing its size.
    window: Option<OwnedFd>,
    stop: Option<oneshot::Sender<()>>,
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

fn winsize(cols: u32, rows: u32) -> Winsize {
    Winsize {
        ws_row: rows.min(u32::from(u16::MAX)) as u16,
        ws_col: cols.min(u32::from(u16::MAX)) as u16,
        ws_xpixel: 0,
        ws_ypixel: 0,
    }
}

impl Running {
    /// What the client typed or sent.
    pub(crate) async fn feed(&self, input: Input) {
        let _ = self.input.send(input).await;
    }

    /// The client's window changed size.
    pub(crate) fn resize(&self, cols: u32, rows: u32) {
        if let Some(window) = &self.window {
            let _ = tcsetwinsize(window, winsize(cols, rows));
        }
    }
}

/// Sends what `from` gives to the client, as output or (with `stream` 1) as errors.
async fn pump(
    mut from: impl AsyncRead + Unpin,
    to: Handle,
    channel: ChannelId,
    stream: Option<u32>,
) {
    let mut bytes = vec![0_u8; 32 * 1024];
    // A terminal whose other end has closed reads as an error, not as an end: both end it.
    while let Ok(count @ 1..) = from.read(&mut bytes).await {
        let chunk = bytes[..count].to_vec();
        let sent = match stream {
            Some(stream) => to.extended_data(channel, stream, chunk).await,
            None => to.data(channel, chunk).await,
        };
        if sent.is_err() {
            break;
        }
    }
}

/// Writes what the client sends to `to`. With `ends`, the client's end is its end too.
async fn feed(mut to: impl AsyncWrite + Unpin, mut input: mpsc::Receiver<Input>, ends: bool) {
    while let Some(input) = input.recv().await {
        match input {
            Input::Data(bytes) => {
                if to.write_all(&bytes).await.is_err() || to.flush().await.is_err() {
                    break;
                }
            }
            Input::End if ends => break,
            Input::End => {}
        }
    }
    let _ = to.shutdown().await;
}

/// A terminal: the server's end, and the name of the end the session gets.
fn open_terminal(terminal: &Terminal) -> std::io::Result<(OwnedFd, OwnedFd)> {
    let ours = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY | OpenptFlags::CLOEXEC)?;
    grantpt(&ours)?;
    unlockpt(&ours)?;
    let name = ptsname(&ours, Vec::new())?;
    tcsetwinsize(&ours, winsize(terminal.cols, terminal.rows))?;
    // Not this process's terminal: the helper takes it as its own.
    let theirs = rustix::fs::open(
        name.as_c_str(),
        OFlags::RDWR | OFlags::NOCTTY | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    Ok((ours, theirs))
}

fn file(fd: &OwnedFd) -> std::io::Result<tokio::fs::File> {
    Ok(tokio::fs::File::from_std(std::fs::File::from(
        fd.try_clone()?,
    )))
}

/// Starts the session `launch` describes in the helper, on a terminal if the client asked
/// for one, and carries its input and output on `channel` until it ends. When it ends the
/// client is told how, and the channel is closed.
pub(crate) fn start(
    helper: &Helper,
    launch: &Launch,
    terminal: Option<&Terminal>,
    client: Handle,
    channel: ChannelId,
) -> std::io::Result<Running> {
    let mut command = tokio::process::Command::new(&helper.program);
    command
        .args(&helper.args)
        .arg(MODE)
        .args(launch.to_args())
        .kill_on_drop(true);
    let (input, from_client) = mpsc::channel(64);
    let (stop, stopped) = oneshot::channel();
    let mut tasks = Vec::new();
    let window;
    let mut child;
    if let Some(terminal) = terminal {
        let (ours, theirs) = open_terminal(terminal)?;
        command
            .stdin(Stdio::from(theirs.try_clone()?))
            .stdout(Stdio::from(theirs.try_clone()?))
            .stderr(Stdio::from(theirs));
        child = command.spawn()?;
        // The session's end of the terminal is the session's alone now: when the last
        // program on it goes, reading this end ends.
        drop(command);
        tasks.push(tokio::spawn(pump(
            file(&ours)?,
            client.clone(),
            channel,
            None,
        )));
        tokio::spawn(feed(file(&ours)?, from_client, false));
        window = Some(ours);
    } else {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        child = command.spawn()?;
        drop(command);
        if let Some(out) = child.stdout.take() {
            tasks.push(tokio::spawn(pump(out, client.clone(), channel, None)));
        }
        if let Some(errors) = child.stderr.take() {
            tasks.push(tokio::spawn(pump(errors, client.clone(), channel, Some(1))));
        }
        if let Some(stdin) = child.stdin.take() {
            tokio::spawn(feed(stdin, from_client, true));
        }
        window = None;
    }
    tokio::spawn(async move {
        let status = tokio::select! {
            status = child.wait() => status,
            _ = stopped => {
                // The client is gone, or closed the channel: the session goes with it.
                let _ = child.kill().await;
                return;
            }
        };
        // Everything it wrote, first. A program left running on the terminal keeps it
        // open: it is not waited for.
        for task in tasks {
            let mut task = task;
            if tokio::time::timeout(Duration::from_secs(2), &mut task)
                .await
                .is_err()
            {
                task.abort();
            }
        }
        let code = match status {
            Ok(status) => {
                use std::os::unix::process::ExitStatusExt;
                // Ended by a signal: what a shell would report.
                status.code().map_or_else(
                    || 128 + status.signal().unwrap_or(0) as u32,
                    |code| code as u32,
                )
            }
            Err(_) => 255,
        };
        let _ = client.exit_status_request(channel, code).await;
        let _ = client.eof(channel).await;
        let _ = client.close(channel).await;
    });
    Ok(Running {
        input,
        window,
        stop: Some(stop),
    })
}
