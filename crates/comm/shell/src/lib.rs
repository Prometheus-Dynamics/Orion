//! A shell on the board, over an Orion session.
//!
//! What SSH gives a person, for a tool on whatever address its session reaches the board: the
//! socket, the network, a USB link. The shell runs as the account asked for, started by the
//! same helper as an SSH login ([`orion_ssh::Keeper`]), so becoming the account happens in the
//! helper's process and never in the comm core's.
//!
//! **Who.** The check decides (`What::Shell(account)`): with security on, root and programs on
//! the board, and operators granted `login` or `shell:ACCOUNT`. A program on the board is let in
//! as its own account only, or as anyone if it is root: nobody on the board becomes someone
//! else through Orion.
//!
//! **Under credit, both ways.** Each side may have at most its window of bytes in flight that
//! the other has not taken, and says what it has taken with `ShellCredit`. A shell that prints
//! faster than the caller reads waits; it never fills the session, so the session's other
//! calls and topics go on beside it. Input past the window ends the shell.
//!
//! **Ends.** `ShellExit` comes after the last of the output, with the exit code (255 if there
//! was none). `ShellClose`, or the session ending, ends the shell, whatever it was doing.

#![cfg(target_os = "linux")]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::os::fd::OwnedFd;
use std::rc::Rc;
use std::time::Duration;

use orion_msg_session::{
    ShellClose, ShellCredit, ShellData, ShellEof, ShellExit, ShellOpen, ShellOpened, ShellResize,
};
use orion_server::{
    Admits, Checked, Comm, Cx, Delivery, Error, Incoming, Policy, Principal, Service, SessionId,
    Sessions, WeakComm, What,
};
use orion_ssh::{Io, Keeper, Started, Terminal, Wanted};
use orion_wire::{Bytes, Decode, FrameView, kind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::{info, warn};

/// The most shells one session may have open.
pub const MAX_SHELLS: usize = 8;
/// The output window when the caller says none, and the most it may ask for.
pub const OUTPUT_WINDOW: u32 = 64 * 1024;
/// The most output window a caller may ask for.
pub const MAX_OUTPUT_WINDOW: u32 = 1024 * 1024;
/// The input window the board gives.
pub const INPUT_WINDOW: u32 = 64 * 1024;
/// The most bytes in one `ShellData`, before the session's own frame limit.
const CHUNK: usize = 16 * 1024;
/// How long the last output is waited for once the shell has ended.
const DRAIN: Duration = Duration::from_secs(2);

/// Who may ask: root and programs on the board, and operators granted the account.
const POLICY: Policy = Policy::new(Admits::ROOT.and(Admits::LOCAL).and(Admits::OPERATOR));
/// What a shell already open is asked to do: its own session's, checked by number.
const OWN: Policy = Policy::new(Admits::ANYONE).without_grant();

/// Output credit: how many bytes may still be sent.
#[derive(Default)]
struct Credit {
    bytes: Cell<u32>,
    given: Notify,
    ended: Cell<bool>,
}

impl Credit {
    fn give(&self, bytes: u32) {
        self.bytes.set(self.bytes.get().saturating_add(bytes));
        self.given.notify_waiters();
    }

    fn end(&self) {
        self.ended.set(true);
        self.given.notify_waiters();
    }

    /// Up to `want` bytes of credit, once there is any. `None` once the shell has ended.
    async fn take(&self, want: usize) -> Option<usize> {
        loop {
            if self.ended.get() {
                return None;
            }
            let have = self.bytes.get() as usize;
            if have > 0 {
                let take = have.min(want);
                self.bytes.set((have - take) as u32);
                return Some(take);
            }
            let given = self.given.notified();
            if self.bytes.get() > 0 || self.ended.get() {
                continue;
            }
            given.await;
        }
    }
}

/// One open shell.
struct Shell {
    input: mpsc::UnboundedSender<Option<Vec<u8>>>,
    /// Input taken and not yet written: never more than the input window.
    unwritten: Rc<Cell<u32>>,
    credit: Rc<Credit>,
    terminal: Option<OwnedFd>,
    /// Dropping it ends the shell.
    _stop: oneshot::Sender<()>,
}

impl Drop for Shell {
    fn drop(&mut self) {
        self.credit.end();
    }
}

#[derive(Default)]
struct Shells {
    open: HashMap<(SessionId, u32), Shell>,
    next: u32,
}

/// The shell service. Add it to the comm core.
pub struct ShellService {
    keeper: Rc<Keeper>,
    comm: WeakComm,
    shells: Rc<RefCell<Shells>>,
}

impl std::fmt::Debug for ShellService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ShellService")
    }
}

impl ShellService {
    /// Shells started by `keeper`, carried by `comm`.
    #[must_use]
    pub fn new(comm: &Comm, keeper: Rc<Keeper>) -> Self {
        Self {
            keeper,
            comm: comm.downgrade(),
            shells: Rc::default(),
        }
    }

    /// How many shells are open.
    #[must_use]
    pub fn open(&self) -> usize {
        self.shells.borrow().open.len()
    }

    /// A handle that says how many shells are open, for tests and status.
    pub fn counter(&self) -> impl Fn() -> usize + 'static {
        let shells = self.shells.clone();
        move || shells.borrow().open.len()
    }

    fn open_shell(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let open: ShellOpen<'_> = request.get().body()?;
        let session = cx.caller().id;
        if self
            .shells
            .borrow()
            .open
            .keys()
            .filter(|(of, _)| *of == session)
            .count()
            >= MAX_SHELLS
        {
            return Err(Error::Busy);
        }
        let caller = cx.caller();
        let caller_uid = match caller.principal {
            Principal::Local { uid, .. } => Some(uid),
            _ => None,
        };
        let from = format!("{} ({})", caller.name, caller.principal);
        let max_frame = caller.max_frame as usize;
        let admitted = self
            .keeper
            .admit(&request, open.account, caller_uid, &from)
            .ok_or(Error::Denied)?;
        let terminal = (!open.term.is_empty()).then(|| Terminal {
            term: open.term.to_owned(),
            cols: open.cols,
            rows: open.rows,
        });
        if terminal
            .as_ref()
            .is_some_and(|terminal| !terminal.is_named())
        {
            return Err(Error::Malformed);
        }
        let wanted = if open.command.is_empty() {
            Wanted::Shell
        } else {
            Wanted::Command(open.command.0.to_vec())
        };
        let started = self
            .keeper
            .start(&admitted, wanted, terminal.as_ref(), None)
            .map_err(|error| {
                warn!("shell: {from}: it could not be started: {error}");
                Error::Unavailable
            })?;
        let window = match open.window {
            0 => OUTPUT_WINDOW,
            asked => asked.min(MAX_OUTPUT_WINDOW),
        };
        let number = {
            let mut shells = self.shells.borrow_mut();
            shells.next = shells.next.wrapping_add(1).max(1);
            shells.next
        };
        let shell = carry(
            started,
            Carry {
                comm: self.comm.clone(),
                shells: self.shells.clone(),
                session,
                number,
                window,
                chunk: CHUNK.min(max_frame.saturating_sub(64)).max(1),
            },
        )
        .map_err(|error| {
            warn!("shell: {from}: {error}");
            Error::Unavailable
        })?;
        self.shells
            .borrow_mut()
            .open
            .insert((session, number), shell);
        info!("shell: {from} has shell {number} as {}", open.account);
        cx.reply(&ShellOpened {
            request_seq: cx.request_seq(),
            shell: number,
            window: INPUT_WINDOW,
        })
    }

    /// The caller's own shell `number`, or `NotFound`.
    fn with_shell<R>(
        &self,
        session: SessionId,
        number: u32,
        f: impl FnOnce(&mut Shell) -> R,
    ) -> Result<R, Error> {
        let mut shells = self.shells.borrow_mut();
        let shell = shells
            .open
            .get_mut(&(session, number))
            .ok_or(Error::NotFound)?;
        Ok(f(shell))
    }
}

impl Service for ShellService {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::SHELL_OPEN,
            kind::SHELL_DATA,
            kind::SHELL_CREDIT,
            kind::SHELL_RESIZE,
            kind::SHELL_EOF,
            kind::SHELL_CLOSE,
        ]
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        if frame.kind() == kind::SHELL_OPEN {
            let open = ShellOpen::decode(frame.payload())?;
            return Ok((What::Shell(open.account), POLICY));
        }
        // A shell already open is the caller's own, found by its session and number.
        Ok((What::Open("shell"), OWN))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let session = cx.caller().id;
        let incoming = request.get();
        match incoming.kind() {
            kind::SHELL_OPEN => self.open_shell(request, cx),
            kind::SHELL_DATA => {
                let data: ShellData<'_> = incoming.body()?;
                let over = self.with_shell(session, data.shell, |shell| {
                    let unwritten = shell.unwritten.get().saturating_add(data.data.len() as u32);
                    if unwritten > INPUT_WINDOW {
                        return true;
                    }
                    shell.unwritten.set(unwritten);
                    let _ = shell.input.send(Some(data.data.0.to_vec()));
                    false
                })?;
                if over {
                    // More than it was given room for: the caller does not keep to credit.
                    warn!(
                        "shell: session {session:?} sent past its window: shell {} ended",
                        data.shell
                    );
                    self.shells.borrow_mut().open.remove(&(session, data.shell));
                    return Err(Error::Malformed);
                }
                Ok(())
            }
            kind::SHELL_CREDIT => {
                let credit: ShellCredit = incoming.body()?;
                self.with_shell(session, credit.shell, |shell| {
                    shell.credit.give(credit.bytes)
                })
            }
            kind::SHELL_RESIZE => {
                let resize: ShellResize = incoming.body()?;
                self.with_shell(session, resize.shell, |shell| {
                    if let Some(terminal) = &shell.terminal {
                        let _ = rustix::termios::tcsetwinsize(
                            terminal,
                            winsize(resize.cols, resize.rows),
                        );
                    }
                })
            }
            kind::SHELL_EOF => {
                let eof: ShellEof = incoming.body()?;
                self.with_shell(session, eof.shell, |shell| {
                    let _ = shell.input.send(None);
                })
            }
            kind::SHELL_CLOSE => {
                let close: ShellClose = incoming.body()?;
                self.shells
                    .borrow_mut()
                    .open
                    .remove(&(session, close.shell))
                    .map(drop)
                    .ok_or(Error::NotFound)
            }
            _ => Err(Error::Unsupported),
        }
    }

    fn closed(&mut self, session: SessionId, _sessions: &mut Sessions) {
        self.shells
            .borrow_mut()
            .open
            .retain(|(of, _), _| *of != session);
    }
}

/// What answers when shells are off (`shell = off`): every open is refused `unsupported` at
/// once, so a tool is told rather than left to wait.
#[derive(Debug, Default)]
pub struct ShellOff;

impl Service for ShellOff {
    fn kinds(&self) -> &'static [u8] {
        &[kind::SHELL_OPEN]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        Err(Error::Unsupported)
    }

    fn handle(&mut self, _request: Checked<Incoming<'_>>, _cx: &mut Cx<'_>) -> Result<(), Error> {
        Err(Error::Unsupported)
    }
}

fn winsize(cols: u32, rows: u32) -> rustix::termios::Winsize {
    rustix::termios::Winsize {
        ws_row: rows.min(u32::from(u16::MAX)) as u16,
        ws_col: cols.min(u32::from(u16::MAX)) as u16,
        ws_xpixel: 0,
        ws_ypixel: 0,
    }
}

/// Where a shell's bytes go.
#[derive(Clone)]
struct Carry {
    comm: WeakComm,
    shells: Rc<RefCell<Shells>>,
    session: SessionId,
    number: u32,
    window: u32,
    chunk: usize,
}

impl Carry {
    /// Sends `body` to the shell's session. False if it is gone.
    fn send<B: orion_wire::Body>(&self, body: &B) -> bool {
        self.comm.upgrade().is_some_and(|comm| {
            comm.with(|sessions, _| sessions.send(self.session, body, Delivery::Must))
                .is_ok()
        })
    }
}

fn file(fd: OwnedFd) -> tokio::fs::File {
    tokio::fs::File::from_std(std::fs::File::from(fd))
}

/// Starts carrying `started`: its output to the caller under credit, the caller's input to it,
/// and its end.
fn carry(started: Started, to: Carry) -> std::io::Result<Shell> {
    let credit = Rc::new(Credit::default());
    credit.give(to.window);
    let unwritten = Rc::new(Cell::new(0_u32));
    let (input, from_caller) = mpsc::unbounded_channel();
    let (stop, stopped) = oneshot::channel();
    let mut pumps = Vec::new();
    let terminal = match started.io {
        Io::Terminal(ours) => {
            pumps.push(tokio::task::spawn_local(pump(
                file(ours.try_clone()?),
                to.clone(),
                credit.clone(),
                0,
            )));
            tokio::task::spawn_local(feed(
                file(ours.try_clone()?),
                from_caller,
                to.clone(),
                unwritten.clone(),
                false,
            ));
            Some(ours)
        }
        Io::Pipes {
            input: stdin,
            output,
            errors,
        } => {
            pumps.push(tokio::task::spawn_local(pump(
                file(output),
                to.clone(),
                credit.clone(),
                0,
            )));
            pumps.push(tokio::task::spawn_local(pump(
                file(errors),
                to.clone(),
                credit.clone(),
                1,
            )));
            tokio::task::spawn_local(feed(
                file(stdin),
                from_caller,
                to.clone(),
                unwritten.clone(),
                true,
            ));
            None
        }
    };
    let status = std::os::unix::net::UnixStream::from(started.status);
    status.set_nonblocking(true)?;
    let mut status = tokio::net::UnixStream::from_std(status)?;
    tokio::task::spawn_local(async move {
        let mut code = [0_u8; 4];
        let ended = tokio::select! {
            ended = status.read_exact(&mut code) => ended.is_ok(),
            // Closed by the caller, or its session ended: letting go of `status` ends it.
            _ = stopped => return,
        };
        // The last of the output first, for as long as the caller gives credit for it.
        for mut pump in pumps {
            if tokio::time::timeout(DRAIN, &mut pump).await.is_err() {
                pump.abort();
            }
        }
        let code = if ended { u32::from_le_bytes(code) } else { 255 };
        to.send(&ShellExit {
            shell: to.number,
            code,
        });
        to.shells.borrow_mut().open.remove(&(to.session, to.number));
    });
    Ok(Shell {
        input,
        unwritten,
        credit,
        terminal,
        _stop: stop,
    })
}

/// Sends what `from` gives to the caller as `stream`, as credit allows.
async fn pump(mut from: impl AsyncRead + Unpin, to: Carry, credit: Rc<Credit>, stream: u8) {
    let mut bytes = vec![0_u8; to.chunk];
    while let Some(room) = credit.take(to.chunk).await {
        // A terminal whose other end has closed reads as an error, not as an end: both end it.
        let count = match from.read(&mut bytes[..room]).await {
            Ok(count @ 1..) => count,
            _ => break,
        };
        // Credit not used goes back.
        credit.give((room - count) as u32);
        let sent = to.send(&ShellData {
            shell: to.number,
            stream,
            data: Bytes(&bytes[..count]),
        });
        if !sent {
            break;
        }
    }
}

/// Writes what the caller sends to `to`, and gives credit for each piece written. With
/// `ends`, the caller's end of input is the program's.
async fn feed(
    mut to: tokio::fs::File,
    mut input: mpsc::UnboundedReceiver<Option<Vec<u8>>>,
    carry: Carry,
    unwritten: Rc<Cell<u32>>,
    ends: bool,
) {
    while let Some(input) = input.recv().await {
        match input {
            Some(bytes) => {
                if to.write_all(&bytes).await.is_err() || to.flush().await.is_err() {
                    break;
                }
                let len = bytes.len() as u32;
                unwritten.set(unwritten.get().saturating_sub(len));
                carry.send(&ShellCredit {
                    shell: carry.number,
                    bytes: len,
                });
            }
            None if ends => break,
            // On a terminal, the end of input is a byte the caller types (^D).
            None => {}
        }
    }
    let _ = to.shutdown().await;
}
