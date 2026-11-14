//! The shell service with a real helper and a real shell: output and errors, the exit code,
//! credit both ways, a terminal and its size, and who is refused.

use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_check::Security;
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_msg_session::{
    Hello, Ping, Pong, Refusal, Roles, ShellClose, ShellCredit, ShellData, ShellEof, ShellExit,
    ShellOpen, ShellOpened, ShellResize, Welcome,
};
use orion_server::mem::{self, MemConnection, MemConnector};
use orion_server::{Comm, Error, FdSet, Limits, Principal};
use orion_shell::ShellService;
use orion_ssh::{Helper, Keeper, SshConfig, users};
use orion_types::KeyId;
use orion_wire::{Body, Bytes, Decode, encode_body, frame, kind};

const WAIT: Duration = Duration::from_secs(10);

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

/// The account the tests run as.
fn me() -> users::User {
    users::lookup_uid(Path::new("/etc"), rustix::process::getuid().as_raw())
        .expect("the user running the tests has an account")
}

fn caller() -> Principal {
    let me = me();
    Principal::Local {
        uid: me.uid,
        gid: me.gid,
    }
}

struct Board {
    connector: MemConnector,
    open: Box<dyn Fn() -> usize>,
}

fn board(security: Security) -> Board {
    let keys = SharedKeys::new(Keys::open(KeysConfig::in_memory("lab"), security).expect("keys"));
    let comm = Comm::new(
        "board".parse().expect("name"),
        security,
        Some(Rc::new(keys.clone())),
        Limits::default(),
    );
    let config = SshConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        interface: None,
        host_key: std::env::temp_dir().join("orion-shell-unused-host-key"),
        helper: Helper {
            program: env!("CARGO_BIN_EXE_orion-shell-helper").into(),
            args: Vec::new(),
        },
        subsystems: Vec::new(),
        forward: false,
        etc: "/etc".into(),
        recovery: false,
        max_connections: 4,
        max_unauthenticated: 4,
        login: Duration::from_secs(60),
    };
    let keeper = Keeper::new(config, security, keys, "127.0.0.1:0".parse().expect("addr"));
    let service = ShellService::new(&comm, Rc::new(keeper));
    let open = service.counter();
    comm.add_service(service);
    let (carrier, connector) = mem::carrier(Roles::CLIENT, 256 * 1024);
    tokio::task::spawn_local(async move { comm.serve(carrier).await });
    Board {
        connector,
        open: Box::new(open),
    }
}

/// What the caller heard, by kind.
#[derive(Debug, PartialEq, Eq)]
enum Heard {
    Opened(u32, u32),
    Data(u32, u8, Vec<u8>),
    Credit(u32, u32),
    Exit(u32, u32),
    Refused(Option<Error>),
    Pong(u64),
}

struct Client {
    connection: MemConnection,
    buf: Vec<u8>,
    seq: u16,
}

impl Client {
    async fn open(connection: MemConnection) -> Self {
        let mut client = Self {
            connection,
            buf: vec![0; 64 * 1024],
            seq: 0,
        };
        client
            .send(&Hello {
                name: "tool",
                roles: Roles::CLIENT,
                max_frame: 32 * 1024,
                revision: 1,
                level: 0,
            })
            .await;
        let (kind, payload) = client.recv().await.expect("an answer");
        assert_eq!(kind, kind::WELCOME);
        let _ = Welcome::decode(&payload).expect("welcome");
        client
    }

    async fn send<B: Body>(&mut self, body: &B) {
        self.seq += 1;
        let mut out = vec![0_u8; 40 * 1024];
        let len = encode_body(body, self.seq, &mut out).expect("encode");
        self.connection.send(&out[..len], &[]).await.expect("send");
    }

    async fn recv(&mut self) -> Option<(u8, Vec<u8>)> {
        let len = self
            .connection
            .recv(&mut self.buf, &mut FdSet::default())
            .await
            .ok()??;
        let view = frame::decode(&self.buf[..len]).expect("frame");
        Some((view.kind(), view.payload().to_vec()))
    }

    /// The next thing heard that is not a ping, or `None` if nothing comes within `wait`.
    async fn hear(&mut self, wait: Duration) -> Option<Heard> {
        let heard = tokio::time::timeout(wait, async {
            loop {
                let (kind, payload) = self.recv().await.expect("the session is open");
                let heard = match kind {
                    kind::SHELL_OPENED => {
                        let opened = ShellOpened::decode(&payload).expect("opened");
                        Heard::Opened(opened.shell, opened.window)
                    }
                    kind::SHELL_DATA => {
                        let data = ShellData::decode(&payload).expect("data");
                        Heard::Data(data.shell, data.stream, data.data.0.to_vec())
                    }
                    kind::SHELL_CREDIT => {
                        let credit = ShellCredit::decode(&payload).expect("credit");
                        Heard::Credit(credit.shell, credit.bytes)
                    }
                    kind::SHELL_EXIT => {
                        let exit = ShellExit::decode(&payload).expect("exit");
                        Heard::Exit(exit.shell, exit.code)
                    }
                    kind::REFUSAL => {
                        Heard::Refused(Refusal::decode(&payload).expect("refusal").error())
                    }
                    kind::PONG => Heard::Pong(Pong::decode(&payload).expect("pong").now_ms),
                    _ => continue,
                };
                return heard;
            }
        })
        .await;
        heard.ok()
    }

    async fn open_shell(&mut self, command: &str, term: &str, window: u32) -> Heard {
        let account = me().name;
        self.send(&ShellOpen {
            account: &account,
            command: Bytes(command.as_bytes()),
            term,
            cols: 80,
            rows: 24,
            window,
        })
        .await;
        self.hear(WAIT).await.expect("an answer")
    }

    /// Everything the shell sends until it exits, giving credit for each piece: output,
    /// errors, the exit code.
    async fn until_exit(&mut self, shell: u32) -> (Vec<u8>, Vec<u8>, u32) {
        let (mut out, mut errors) = (Vec::new(), Vec::new());
        loop {
            match self.hear(WAIT).await.expect("the shell goes on") {
                Heard::Data(of, stream, bytes) => {
                    assert_eq!(of, shell);
                    self.send(&ShellCredit {
                        shell,
                        bytes: bytes.len() as u32,
                    })
                    .await;
                    if stream == 0 { &mut out } else { &mut errors }.extend(bytes);
                }
                Heard::Exit(of, code) => {
                    assert_eq!(of, shell);
                    return (out, errors, code);
                }
                Heard::Credit(..) => {}
                other => panic!("unexpected: {other:?}"),
            }
        }
    }
}

#[test]
fn a_command_runs_as_the_account_with_its_output_errors_and_exit_code() {
    run(async {
        let board = board(Security::On);
        let mut client = Client::open(board.connector.connect(caller())).await;
        let Heard::Opened(shell, window) = client
            .open_shell("id -u; printf oops >&2; exit 3", "", 0)
            .await
        else {
            panic!("not opened");
        };
        assert_eq!(window, orion_shell::INPUT_WINDOW);
        let (out, errors, code) = client.until_exit(shell).await;
        assert_eq!(String::from_utf8_lossy(&out).trim(), me().uid.to_string());
        assert_eq!(errors, b"oops");
        assert_eq!(code, 3);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!((board.open)(), 0, "an ended shell is gone");
    });
}

#[test]
fn output_waits_for_credit_and_the_session_goes_on_beside_it() {
    run(async {
        let board = board(Security::On);
        let mut client = Client::open(board.connector.connect(caller())).await;
        let Heard::Opened(shell, _) = client
            .open_shell("head -c 1048576 /dev/zero", "", 4096)
            .await
        else {
            panic!("not opened");
        };
        // No credit given: the window and not a byte more.
        let mut got = 0;
        while let Some(heard) = client.hear(Duration::from_millis(300)).await {
            match heard {
                Heard::Data(_, 0, bytes) => got += bytes.len(),
                other => panic!("unexpected: {other:?}"),
            }
        }
        assert_eq!(got, 4096);
        // The session is not held up by the shell waiting.
        client.send(&Ping { now_ms: 42 }).await;
        assert_eq!(client.hear(WAIT).await, Some(Heard::Pong(42)));
        // Credit for what was taken: the rest comes, and the shell ends.
        client.send(&ShellCredit { shell, bytes: 4096 }).await;
        let (out, _, code) = client.until_exit(shell).await;
        assert_eq!(got + out.len(), 1024 * 1024);
        assert_eq!(code, 0);
    });
}

#[test]
fn input_is_taken_under_credit_and_its_end_ends_a_program_without_a_terminal() {
    run(async {
        let board = board(Security::On);
        let mut client = Client::open(board.connector.connect(caller())).await;
        let Heard::Opened(shell, window) = client.open_shell("cat", "", 0).await else {
            panic!("not opened");
        };
        client
            .send(&ShellData {
                shell,
                stream: 0,
                data: Bytes(b"hello\n"),
            })
            .await;
        client.send(&ShellEof { shell }).await;
        let mut credit = 0;
        let mut out = Vec::new();
        let code = loop {
            match client.hear(WAIT).await.expect("goes on") {
                Heard::Credit(_, bytes) => credit += bytes,
                Heard::Data(_, 0, bytes) => out.extend(bytes),
                Heard::Exit(_, code) => break code,
                other => panic!("unexpected: {other:?}"),
            }
        };
        assert_eq!((out.as_slice(), credit, code), (&b"hello\n"[..], 6, 0));

        // More than the window, unwritten: the shell is ended and the caller told.
        let Heard::Opened(shell, _) = client.open_shell("sleep 30", "", 0).await else {
            panic!("not opened");
        };
        let big = vec![b'x'; 16 * 1024];
        let mut refused = None;
        for _ in 0..=(window as usize / big.len()) + 1 {
            client
                .send(&ShellData {
                    shell,
                    stream: 0,
                    data: Bytes(&big),
                })
                .await;
        }
        let began = Instant::now();
        while began.elapsed() < WAIT {
            if let Some(Heard::Refused(error)) = client.hear(Duration::from_millis(200)).await {
                refused = Some(error);
                break;
            }
        }
        assert_eq!(refused, Some(Some(Error::Malformed)));
        assert_eq!((board.open)(), 0);
    });
}

#[test]
fn a_terminal_has_the_size_asked_for_and_is_told_when_it_changes() {
    run(async {
        let board = board(Security::On);
        let mut client = Client::open(board.connector.connect(caller())).await;
        let Heard::Opened(shell, _) = client
            .open_shell("stty size; read line; stty size", "xterm", 0)
            .await
        else {
            panic!("not opened");
        };
        let mut out = Vec::new();
        while !String::from_utf8_lossy(&out).contains("24 80") {
            if let Heard::Data(_, _, bytes) = client.hear(WAIT).await.expect("output") {
                out.extend(bytes);
            }
        }
        client
            .send(&ShellResize {
                shell,
                cols: 120,
                rows: 40,
            })
            .await;
        client
            .send(&ShellData {
                shell,
                stream: 0,
                data: Bytes(b"\n"),
            })
            .await;
        let (rest, _, code) = client.until_exit(shell).await;
        assert!(
            String::from_utf8_lossy(&rest).contains("40 120"),
            "{rest:?}"
        );
        assert_eq!(code, 0);
    });
}

#[test]
fn nobody_becomes_someone_else_and_a_closed_or_left_shell_ends() {
    run(async {
        let board = board(Security::On);
        let mut client = Client::open(board.connector.connect(caller())).await;
        // Someone on the board, not root, asking for another account.
        if !rustix::process::getuid().is_root() {
            client
                .send(&ShellOpen {
                    account: "root",
                    command: Bytes(b"id"),
                    term: "",
                    cols: 80,
                    rows: 24,
                    window: 0,
                })
                .await;
            assert_eq!(
                client.hear(WAIT).await,
                Some(Heard::Refused(Some(Error::Denied)))
            );
        }
        // An operator granted nothing.
        let mut operator =
            Client::open(board.connector.connect(Principal::Operator(KeyId([3; 32])))).await;
        assert_eq!(
            operator.open_shell("id", "", 0).await,
            Heard::Refused(Some(Error::Denied))
        );
        // Another session's shell is not found from here.
        let Heard::Opened(shell, _) = client.open_shell("sleep 30", "", 0).await else {
            panic!("not opened");
        };
        operator.send(&ShellClose { shell }).await;
        assert_eq!(
            operator.hear(WAIT).await,
            Some(Heard::Refused(Some(Error::NotFound)))
        );
        assert_eq!((board.open)(), 1);
        // Closed by its own session: gone.
        client.send(&ShellClose { shell }).await;
        let began = Instant::now();
        while (board.open)() != 0 {
            assert!(began.elapsed() < WAIT, "the closed shell is still open");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Left open when the session ends: gone with it.
        let Heard::Opened(_, _) = client.open_shell("sleep 30", "", 0).await else {
            panic!("not opened");
        };
        drop(client);
        let began = Instant::now();
        while (board.open)() != 0 {
            assert!(began.elapsed() < WAIT, "a shell outlived its session");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
}
