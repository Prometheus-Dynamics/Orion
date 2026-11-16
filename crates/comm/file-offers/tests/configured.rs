//! Prefixes the settings name: the comm core serves each while no program offers it, a program
//! that offers it takes it over for as long as its session lives, and a file made later (a
//! command's output) is waited for.

#![cfg(target_os = "linux")]

use std::cell::Cell;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc;
use std::time::Duration;

use orion_connect::{Address, Client, Options, Session};
use orion_file_offers::{FileOffersService, OwnFiles, OwnName, Reading, Ready};
use orion_files::{FileState, Files, OfferedBy};
use orion_server::{Comm, Limits, Security, Sessions};
use orion_unix::UnixCarrier;

const WAIT: Duration = Duration::from_secs(10);

/// `sys/`: `now`; `later`, made 300 ms after it is asked for; `fails`, which is not made.
struct Configured {
    made: Rc<Cell<u32>>,
}

impl OwnFiles for Configured {
    fn prefix(&self) -> &str {
        "sys/"
    }

    fn names(&self) -> Vec<OwnName> {
        ["sys/now", "sys/later", "sys/fails"]
            .into_iter()
            .map(|name| OwnName {
                name: name.into(),
                max_bytes: 100,
                user_data: false,
            })
            .collect()
    }

    fn read(&mut self, name: &str, _: &Sessions, ready: Ready) -> Reading {
        match name {
            "sys/now" => Reading::Now(b"configured".to_vec()),
            "sys/later" | "sys/fails" => {
                self.made.set(self.made.get() + 1);
                let made = self.made.get();
                let fails = name == "sys/fails";
                tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    ready.done(if fails {
                        Err("it failed".into())
                    } else {
                        Ok(format!("made later, {made}").into_bytes())
                    });
                });
                Reading::Later
            }
            _ => Reading::Not(format!("{name}: no such file")),
        }
    }
}

fn comm_core(name: &str) -> (PathBuf, tokio::sync::oneshot::Sender<()>) {
    let dir = std::env::temp_dir().join(format!("orion-conf-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let socket = dir.join("orion.sock");
    let (ready, started) = mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let path = socket.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let comm = Comm::new(
                "board".parse().expect("name"),
                Security::Off,
                None,
                Limits::default(),
            );
            let files = Configured {
                made: Rc::default(),
            };
            comm.add_service(FileOffersService::standard().with_configured(&comm, files));
            let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
            tokio::task::spawn_local(async move { comm.serve(unix).await });
            ready.send(()).expect("ready");
            let _ = stopped.await;
        });
    });
    started.recv().expect("the comm core starts");
    (socket, stop)
}

fn files(socket: &Path, name: &str) -> Files {
    let session = Session::connect(&Address::Local(socket.to_owned()), name, Options::default())
        .expect("connect");
    Files::new(&Client::new(session))
}

/// The file, or why it was not taken.
fn fetch(files: &Files, name: &str) -> Result<String, String> {
    let mut into = Cursor::new(Vec::new());
    let transfer = files.fetch(name, &mut into, 0, |_, _| {}).expect(name);
    match transfer.state {
        FileState::Committed => Ok(String::from_utf8(into.into_inner()).expect("text")),
        _ => Err(transfer.detail),
    }
}

/// A program offering `sys/` that gives "program" for any name, until it is told to go.
fn program(socket: &Path) -> (mpsc::Sender<()>, std::thread::JoinHandle<()>) {
    let socket = socket.to_owned();
    let (offered_tx, offered) = mpsc::channel();
    let (go, gone) = mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        let files = files(&socket, "system-keeper");
        let mut offer = files
            .offer("sys/")
            .expect("a program may offer a configured prefix");
        offered_tx.send(()).expect("offered");
        loop {
            if gone.try_recv().is_ok() {
                return;
            }
            if let Some(asked) = offer.next(Duration::from_millis(20)).expect("next") {
                asked
                    .give(Cursor::new(b"program".to_vec()))
                    .expect("give")
                    .finish()
                    .expect("given");
            }
        }
    });
    offered.recv().expect("offered");
    (go, handle)
}

#[test]
fn a_file_made_later_is_waited_for_by_every_fetch_of_it() {
    let (socket, _stop) = comm_core("later");
    let fetches: Vec<_> = (0..2)
        .map(|_| {
            let socket = socket.clone();
            std::thread::spawn(move || fetch(&files(&socket, "collector"), "sys/later"))
        })
        .collect();
    for fetched in fetches {
        assert_eq!(fetched.join().expect("fetch"), Ok("made later, 1".into()));
    }
    // Made once, for both: a second making would be what is held now.
    std::thread::sleep(Duration::from_millis(400));
    let again = fetch(&files(&socket, "collector"), "sys/later");
    assert_eq!(again, Ok("made later, 1".into()));
}

#[test]
fn a_file_not_made_is_rejected_saying_why() {
    let (socket, _stop) = comm_core("fails");
    let client = files(&socket, "collector");
    assert_eq!(fetch(&client, "sys/fails"), Err("it failed".into()));
}

#[test]
fn a_program_takes_a_configured_prefix_over_while_its_session_lives() {
    let (socket, _stop) = comm_core("over");
    let client = files(&socket, "collector");
    assert_eq!(fetch(&client, "sys/now"), Ok("configured".into()));
    let (offers, _) = client.offers().expect("offers");
    assert_eq!(
        (offers[0].prefix.as_str(), offers[0].by),
        ("sys/", OfferedBy::Configured)
    );

    let (go, keeper) = program(&socket);
    assert_eq!(fetch(&client, "sys/now"), Ok("program".into()));
    let (offers, clashes) = client.offers().expect("offers");
    assert_eq!(clashes, 0, "taking a configured prefix over is no clash");
    assert_eq!(offers.len(), 1, "{offers:?}");
    assert_eq!(
        (offers[0].by, offers[0].name.as_str()),
        (OfferedBy::Program, "system-keeper")
    );

    go.send(()).expect("go");
    keeper.join().expect("program");
    let began = std::time::Instant::now();
    loop {
        let (offers, _) = client.offers().expect("offers");
        if offers[0].by == OfferedBy::Configured {
            break;
        }
        assert!(began.elapsed() < WAIT, "the settings never served it again");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(fetch(&client, "sys/now"), Ok("configured".into()));
}

#[test]
fn a_fetch_under_way_keeps_the_side_it_began_with() {
    let (socket, _stop) = comm_core("keeps");
    let socket_for = socket.clone();
    let waiting = std::thread::spawn(move || fetch(&files(&socket_for, "collector"), "sys/later"));
    // Asked for, and being made, when the program takes the prefix over.
    std::thread::sleep(Duration::from_millis(100));
    let (go, keeper) = program(&socket);
    assert_eq!(waiting.join().expect("fetch"), Ok("made later, 1".into()));
    go.send(()).expect("go");
    keeper.join().expect("program");
}
