//! Carrying one session: its output to the client, the client's input to it, and how it
//! ended. The session itself was started by the keeper; this side holds only descriptors.

use std::os::fd::OwnedFd;
use std::time::Duration;

use russh::ChannelId;
use russh::server::Handle;
use rustix::termios::tcsetwinsize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::keeper::{Io, Started, winsize};

/// What comes from the client.
pub(crate) enum Input {
    Data(Vec<u8>),
    /// The client will send no more.
    End,
}

/// A session that is being carried. Dropping it ends it.
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

fn file(fd: OwnedFd) -> tokio::fs::File {
    tokio::fs::File::from_std(std::fs::File::from(fd))
}

/// Carries the session `started` on `channel` until it ends. When it ends the client is
/// told how, and the channel is closed.
pub(crate) fn carry(
    started: Started,
    client: Handle,
    channel: ChannelId,
) -> std::io::Result<Running> {
    let (input, from_client) = mpsc::channel(64);
    let (stop, stopped) = oneshot::channel();
    let mut tasks = Vec::new();
    let window = match started.io {
        Io::Terminal(ours) => {
            tasks.push(tokio::spawn(pump(
                file(ours.try_clone()?),
                client.clone(),
                channel,
                None,
            )));
            tokio::spawn(feed(file(ours.try_clone()?), from_client, false));
            Some(ours)
        }
        Io::Pipes {
            input,
            output,
            errors,
        } => {
            tasks.push(tokio::spawn(pump(
                file(output),
                client.clone(),
                channel,
                None,
            )));
            tasks.push(tokio::spawn(pump(
                file(errors),
                client.clone(),
                channel,
                Some(1),
            )));
            tokio::spawn(feed(file(input), from_client, true));
            None
        }
    };
    let status = std::os::unix::net::UnixStream::from(started.status);
    status.set_nonblocking(true)?;
    let mut status = tokio::net::UnixStream::from_std(status)?;
    tokio::spawn(async move {
        let mut code = [0_u8; 4];
        let ended = tokio::select! {
            ended = status.read_exact(&mut code) => ended.is_ok(),
            // The client is gone, or closed the channel. Letting go of `status` ends the
            // session.
            _ = stopped => return,
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
        // No code: whoever kept the session is gone.
        let code = if ended { u32::from_le_bytes(code) } else { 255 };
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
