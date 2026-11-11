//! The keeper's side of one connection: it answers what the child asks, and only what a
//! child in that state may ask.

use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use orion_ssh::{Admitted, Io, Keeper};
use tracing::debug;

use crate::protocol::{Answer, Ask, recv, send, terminal};

/// What one question is answered with.
fn answer(
    keeper: &Keeper,
    admitted: &mut Option<Admitted>,
    peer: Option<SocketAddr>,
    ask: Ask,
) -> (Answer, Vec<OwnedFd>) {
    let yes_no = |yes: bool| (if yes { Answer::Yes } else { Answer::No }, Vec::new());
    match ask {
        Ask::Offered(key) => yes_no(keeper.offered(key.as_ref())),
        // One login for one connection: a child cannot change who it is.
        Ask::Login(..) if admitted.is_some() => yes_no(false),
        Ask::Login(name, key) => {
            *admitted = keeper.login(&name, key.as_ref(), peer);
            yes_no(admitted.is_some())
        }
        Ask::Start(want, on) => {
            let Some(admitted) = admitted.as_ref() else {
                return (Answer::Failed("not logged in".to_owned()), Vec::new());
            };
            match keeper.start(admitted, want.into(), terminal(on).as_ref(), peer) {
                Ok(started) => match started.io {
                    Io::Terminal(ours) => (Answer::Terminal, vec![ours, started.status]),
                    Io::Pipes {
                        input,
                        output,
                        errors,
                    } => (Answer::Pipes, vec![input, output, errors, started.status]),
                },
                Err(error) => (Answer::Failed(error.to_string()), Vec::new()),
            }
        }
        Ask::Connect(host, port) => {
            let Some(admitted) = admitted.as_ref() else {
                return (Answer::Failed("not logged in".to_owned()), Vec::new());
            };
            match keeper.connect(admitted, &host, port) {
                Ok(stream) => (Answer::Connected, vec![OwnedFd::from(stream)]),
                Err(why) => (Answer::Failed(why), Vec::new()),
            }
        }
    }
}

/// Answers the child on `socket` until it is gone, it says something that is not a
/// question, or it has not logged in within `login`. `logged_in` is called when it has.
pub fn serve(
    keeper: &Keeper,
    socket: &UnixStream,
    peer: Option<SocketAddr>,
    login: Duration,
    logged_in: impl FnOnce(),
) {
    let deadline = Instant::now() + login;
    let mut admitted = None;
    let mut logged_in = Some(logged_in);
    loop {
        // The keeper does not take the child's word for the time a login has.
        let left = if admitted.is_none() {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                debug!("ssh: a child's client did not log in in time");
                return;
            }
            Some(left)
        } else {
            None
        };
        if socket.set_read_timeout(left).is_err() {
            return;
        }
        let ask = match recv::<Ask>(socket) {
            Ok(Some((ask, _))) => ask,
            Ok(None) => return,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(error) => {
                debug!("ssh: a child said something that is not a question: {error}");
                return;
            }
        };
        let (answer, fds) = answer(keeper, &mut admitted, peer, ask);
        if admitted.is_some()
            && let Some(tell) = logged_in.take()
        {
            tell();
        }
        if send(socket, &answer, &fds).is_err() {
            return;
        }
    }
}
