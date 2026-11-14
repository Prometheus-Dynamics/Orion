//! A shell on the board, over the session: [`Orion::shell`](crate::Orion::shell).
//!
//! The board starts the account's shell (or a command) as an SSH login would, and carries its
//! output here and this program's input there, each under credit: output is given credit as
//! [`Shell::next`] hands it over, and [`Shell::write`] waits for credit before it sends. So a
//! shell that prints more than is read waits on the board, and the session's other calls and
//! topics go on beside it.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_connect::{Client, ClientError, Cx, Feature, Handler, Incoming};
use orion_msg_session::{
    ShellClose, ShellCredit, ShellData, ShellEof, ShellExit, ShellOpen, ShellOpened, ShellResize,
};
use orion_wire::{Bytes, kind};

/// How much output the board may send ahead of what [`Shell::next`] has handed over.
pub const WINDOW: u32 = 64 * 1024;

/// A terminal for the shell: its kind, as `TERM` names it, and its size.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Terminal {
    /// Its kind, `xterm` for instance.
    pub term: String,
    /// Width in characters.
    pub cols: u32,
    /// Height.
    pub rows: u32,
}

/// What a shell sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// Output: everything, on a terminal.
    Output(Vec<u8>),
    /// Errors, apart from the output when there is no terminal.
    Errors(Vec<u8>),
    /// It ended, with this exit code (255 for none). Nothing comes after.
    Exit(u32),
}

#[derive(Default)]
struct One {
    events: VecDeque<Event>,
    /// Bytes of input the board will still take.
    credit: u32,
}

#[derive(Default)]
pub(crate) struct State {
    opened: Vec<(u16, u32, u32)>,
    shells: HashMap<u32, One>,
}

impl Handler for State {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::SHELL_OPENED,
            kind::SHELL_DATA,
            kind::SHELL_CREDIT,
            kind::SHELL_EXIT,
        ]
    }

    fn handle(&mut self, frame: &Incoming<'_>, _cx: &mut Cx<'_>) -> Result<(), ClientError> {
        match frame.kind() {
            kind::SHELL_OPENED => {
                let opened: ShellOpened = frame.body()?;
                self.opened
                    .push((opened.request_seq, opened.shell, opened.window));
            }
            kind::SHELL_DATA => {
                let data: ShellData<'_> = frame.body()?;
                if let Some(one) = self.shells.get_mut(&data.shell) {
                    let bytes = data.data.0.to_vec();
                    one.events.push_back(if data.stream == 1 {
                        Event::Errors(bytes)
                    } else {
                        Event::Output(bytes)
                    });
                }
            }
            kind::SHELL_CREDIT => {
                let credit: ShellCredit = frame.body()?;
                if let Some(one) = self.shells.get_mut(&credit.shell) {
                    one.credit = one.credit.saturating_add(credit.bytes);
                }
            }
            _ => {
                let exit: ShellExit = frame.body()?;
                if let Some(one) = self.shells.get_mut(&exit.shell) {
                    one.events.push_back(Event::Exit(exit.code));
                }
            }
        }
        Ok(())
    }
}

/// Opens a shell as `account`: its login shell, or `command` for that shell to run, on a
/// terminal if one is given.
pub(crate) fn open(
    client: &Client,
    state: &Rc<RefCell<State>>,
    account: &str,
    command: Option<&[u8]>,
    terminal: Option<&Terminal>,
) -> Result<Shell, ClientError> {
    // An older node would not answer at all.
    client.needs(Feature::Shell)?;
    let seq = client.send(&ShellOpen {
        account,
        command: Bytes(command.unwrap_or_default()),
        term: terminal.map_or("", |terminal| terminal.term.as_str()),
        cols: terminal.map_or(0, |terminal| terminal.cols),
        rows: terminal.map_or(0, |terminal| terminal.rows),
        window: WINDOW,
    })?;
    client.answered(seq, || {
        state
            .borrow()
            .opened
            .iter()
            .any(|(asked, _, _)| *asked == seq)
    })?;
    let mut state_now = state.borrow_mut();
    let at = state_now
        .opened
        .iter()
        .position(|(asked, _, _)| *asked == seq)
        .ok_or(ClientError::Lost)?;
    let (_, number, window) = state_now.opened.remove(at);
    state_now.shells.insert(
        number,
        One {
            events: VecDeque::new(),
            credit: window,
        },
    );
    drop(state_now);
    let max_frame = client.session(|session| session.welcomed().max_frame as usize);
    Ok(Shell {
        client: client.clone(),
        state: state.clone(),
        number,
        chunk: max_frame.saturating_sub(64).clamp(1, 16 * 1024),
        ended: false,
    })
}

/// A shell on the board. Dropping it ends the shell, if it has not ended by itself.
pub struct Shell {
    client: Client,
    state: Rc<RefCell<State>>,
    number: u32,
    chunk: usize,
    ended: bool,
}

impl std::fmt::Debug for Shell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Shell({})", self.number)
    }
}

impl Shell {
    /// The next thing the shell sent, waiting up to `wait` for it. `None` if nothing came.
    /// Output handed over here is credit for the board to send more.
    ///
    /// # Errors
    ///
    /// The session broke.
    pub fn next(&mut self, wait: Duration) -> Result<Option<Event>, ClientError> {
        let deadline = Instant::now() + wait;
        loop {
            let event = self
                .state
                .borrow_mut()
                .shells
                .get_mut(&self.number)
                .and_then(|one| one.events.pop_front());
            if let Some(event) = event {
                match &event {
                    Event::Output(bytes) | Event::Errors(bytes) => {
                        self.client.send(&ShellCredit {
                            shell: self.number,
                            bytes: bytes.len() as u32,
                        })?;
                    }
                    Event::Exit(_) => {
                        self.ended = true;
                        self.state.borrow_mut().shells.remove(&self.number);
                    }
                }
                return Ok(Some(event));
            }
            if self.ended {
                return Ok(None);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(None);
            }
            self.client.pump(left.min(Duration::from_millis(200)))?;
        }
    }

    /// Sends `bytes` as input, waiting for the board to take what came before when it has no
    /// room. Output that arrives meanwhile waits for [`Shell::next`].
    ///
    /// # Errors
    ///
    /// The shell has ended, or the session broke, or the board took nothing for as long as a
    /// session may go unheard.
    pub fn write(&mut self, mut bytes: &[u8]) -> Result<(), ClientError> {
        let timeout = self.client.session(|session| session.timeout());
        let mut waited_since = Instant::now();
        while !bytes.is_empty() {
            if self.ended {
                return Err(ClientError::Protocol("the shell has ended"));
            }
            let credit = self
                .state
                .borrow()
                .shells
                .get(&self.number)
                .map_or(0, |one| one.credit) as usize;
            if credit == 0 {
                if waited_since.elapsed() > timeout {
                    return Err(ClientError::Io(std::io::ErrorKind::TimedOut.into()));
                }
                self.client.pump(Duration::from_millis(200))?;
                continue;
            }
            let take = bytes.len().min(credit).min(self.chunk);
            self.client.send(&ShellData {
                shell: self.number,
                stream: 0,
                data: Bytes(&bytes[..take]),
            })?;
            if let Some(one) = self.state.borrow_mut().shells.get_mut(&self.number) {
                one.credit -= take as u32;
            }
            bytes = &bytes[take..];
            waited_since = Instant::now();
        }
        Ok(())
    }

    /// No more input. Without a terminal, the program's input ends; on one, type `^D`.
    ///
    /// # Errors
    ///
    /// The session broke.
    pub fn end_input(&mut self) -> Result<(), ClientError> {
        self.client.send(&ShellEof { shell: self.number })?;
        Ok(())
    }

    /// The terminal is now `cols` by `rows`.
    ///
    /// # Errors
    ///
    /// The session broke.
    pub fn resize(&mut self, cols: u32, rows: u32) -> Result<(), ClientError> {
        self.client.send(&ShellResize {
            shell: self.number,
            cols,
            rows,
        })?;
        Ok(())
    }
}

impl Drop for Shell {
    fn drop(&mut self) {
        if !self.ended {
            let _ = self.client.send(&ShellClose { shell: self.number });
            self.state.borrow_mut().shells.remove(&self.number);
        }
    }
}
