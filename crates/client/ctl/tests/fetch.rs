//! Files from a board, through the command line: fetched, and what a fetch says while the
//! program giving it is quiet.

#![cfg(target_os = "linux")]

use std::time::Duration;

use orion::{Address, Options, Orion, Roles};
use orion_stubs::Board;

use crate::verbs::{connect, ctl};

#[test]
fn file_offers_and_fetch() {
    let board = Board::start("ctl-fetch");
    assert_eq!(
        ctl(&board, "file offers"),
        (0, "nothing is offered\n".into(), String::new())
    );

    let kernel: Vec<u8> = (0..200_000_u32).map(|index| (index % 241) as u8).collect();
    let served = kernel.clone();
    let socket = board.socket().to_owned();
    let (ready, offered) = std::sync::mpsc::channel();
    let program = std::thread::spawn(move || {
        let orion = connect(&socket, "logger", Roles::CLIENT);
        let mut offer = orion.files().offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let mut asked_from = Vec::new();
        for _ in 0..2 {
            let asked = offer
                .next(Duration::from_secs(10))
                .expect("next")
                .expect("asked");
            asked_from.push(asked.from());
            let gift = asked
                .give(std::io::Cursor::new(served.clone()))
                .expect("give");
            let _ = gift.finish().expect("over");
        }
        asked_from
    });
    offered.recv().expect("offered");

    let (code, out, err) = ctl(&board, "file offers");
    assert_eq!((code, err.as_str()), (0, ""));
    assert!(out.starts_with("logs/  logger (local:"), "{out}");

    // Half of it is here already: the rest is fetched, and the whole checked.
    let path = board.dir().join("kernel.log");
    std::fs::write(&path, &kernel[..50_000]).expect("write");
    let (code, out, err) = ctl(
        &board,
        &format!("file fetch logs/kernel {}", path.display()),
    );
    assert_eq!(
        (code, out, err),
        (
            0,
            format!("logs/kernel is in {}, 200000 bytes\n", path.display()),
            String::new()
        )
    );
    assert_eq!(std::fs::read(&path).expect("read"), kernel);

    let other = board.dir().join("other.log");
    let (code, out, _) = ctl(
        &board,
        &format!("file fetch logs/kernel {} --bound 1000", other.display()),
    );
    assert_eq!(
        (code, out.as_str()),
        (
            1,
            "logs/kernel was not fetched: the file is 200000 bytes, more than its bound of 1000\n"
        )
    );
    // Only the rest was asked for.
    assert_eq!(program.join().expect("program"), [50_000, 0]);
}

#[test]
fn file_fetch_says_a_stall_waits_it_out_and_gives_up_only_when_asked() {
    let board = Board::start("ctl-stall");
    let size = 4 << 20;
    let socket = board.socket().to_owned();
    let (ready, offered) = std::sync::mpsc::channel();
    // How long the program is quiet in each fetch, once the client has some of the file.
    let pauses = [Duration::from_millis(1500), Duration::from_millis(5600)];
    let program = std::thread::spawn(move || {
        // The program's own wait for the client is longer than its quiet: what is tested is
        // the fetch's patience, not the program's.
        let options = Options {
            roles: Roles::CLIENT,
            timeout: Duration::from_secs(30),
            ..Options::default()
        };
        let orion =
            Orion::connect_with(&Address::Local(socket), "logger", options).expect("connect");
        let mut offer = orion.files().offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let mut ended = Vec::new();
        for pause in pauses {
            let asked = offer
                .next(Duration::from_secs(10))
                .expect("next")
                .expect("asked");
            let bytes: Vec<u8> = (0..size).map(|at| (at % 251) as u8).collect();
            let mut gift = asked.give(std::io::Cursor::new(bytes)).expect("give");
            loop {
                if let orion::files::Giving::Progress { received, .. } =
                    gift.step(Duration::from_millis(5)).expect("step")
                    && received > 0
                {
                    break;
                }
            }
            std::thread::sleep(pause);
            ended.push(gift.finish().expect("over"));
        }
        ended
    });
    offered.recv().expect("offered");

    // With a limit, the quiet ends the fetch, saying the limit was set.
    let path = board.dir().join("big.log");
    let (code, out, err) = ctl(
        &board,
        &format!("file fetch logs/big {} --give-up 300", path.display()),
    );
    assert_eq!(
        (code, out.as_str(), err.as_str()),
        (
            1,
            "logs/big was not fetched: gave up: no progress for 300ms, the limit set for this fetch\n",
            ""
        )
    );

    // Without one, which is the default, it says the program is quiet, and waits it out.
    let (code, out, err) = ctl(&board, &format!("file fetch logs/big {}", path.display()));
    assert_eq!((code, err.as_str()), (0, ""), "{out}");
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 2, "{out}");
    assert!(
        lines[0].starts_with("logs/big: no progress for 5 s, ")
            && lines[0].ends_with(&format!(" of {size} bytes here; still waiting")),
        "{out}"
    );
    assert_eq!(
        lines[1],
        format!("logs/big is in {}, {size} bytes", path.display())
    );

    let ended = program.join().expect("program");
    assert!(
        matches!(&ended[0], orion::files::Giving::Done { taken: false, detail } if detail.starts_with("gave up")),
        "{ended:?}"
    );
    assert!(
        matches!(&ended[1], orion::files::Giving::Done { taken: true, .. }),
        "{ended:?}"
    );
}
