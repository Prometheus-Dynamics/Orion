//! Why the comm core ends a session, and a program on the board that is quiet but not gone.

use crate::common;

use common::*;

#[test]
fn every_session_the_comm_core_ends_is_kept_with_who_it_was_and_why() {
    run(async {
        let limits = Limits {
            outbox_bytes: 8 * 1024,
            ..Limits::default()
        };
        let board = board(Security::Off, limits);

        // One its own program closes is not a drop.
        drop(Client::open(&board, USER).await);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(whys(&board), []);

        // No hello.
        let mut silent = Client::new(&board, USER);
        assert!(silent.is_closed().await);
        // Quiet, with both numbers.
        let mut quiet = Client::open(&board, USER).await;
        assert!(quiet.is_closed().await);
        // A second hello.
        let mut twice = Client::open(&board, ROOT).await;
        let _: Option<Welcome> = twice.hello(Roles::CLIENT).await;
        assert!(twice.is_closed().await);
        // A hello of another wire revision.
        let mut other = Client::new(&board, USER);
        let _: Option<Reject> = other.hello_at(Roles::CLIENT, 99).await;
        assert!(other.is_closed().await);
        // A request that makes a service panic.
        let mut poison = Client::open(&board, USER).await;
        poison.ask(kind::FILE_BEGIN, 1, &[]).await;
        assert!(poison.is_closed().await);

        assert_eq!(
            whys(&board),
            [
                Dropped::NoHello { limit_ms: 5000 },
                Dropped::Quiet {
                    quiet_ms: 6000,
                    limit_ms: 6000
                },
                Dropped::OutOfTurn { kind: kind::HELLO },
                Dropped::Rejected(RejectReason::RevisionMismatch),
                Dropped::Panicked,
            ]
        );
        let drops = board.comm.drops();
        assert_eq!(
            (drops[1].name.as_str(), drops[1].principal.as_str()),
            ("test", "local:1000")
        );
        assert_eq!(drops[2].principal, "local:0");
        // The words carry the numbers.
        assert_eq!(
            drops[1].why.to_string(),
            "quiet for 6000 ms, and the idle time is 6000 ms: is its program still pumping \
             the session?"
        );
    });
}

#[test]
fn a_session_that_does_not_read_is_dropped_with_what_did_not_fit() {
    run(async {
        let limits = Limits {
            outbox_bytes: 8 * 1024,
            ..Limits::default()
        };
        let board = board(Security::Off, limits);
        let mut deaf = Client::open(&board, USER).await;
        // It asks and asks, and never reads an answer.
        let mut number = 0;
        while deaf
            .connection
            .send(&ask_frame(kind::GET, number), &[])
            .await
            .is_ok()
            && whys(&board).is_empty()
        {
            number += 1;
            if number % 64 == 0 {
                tokio::task::yield_now().await;
            }
        }
        let whys = whys(&board);
        assert!(
            matches!(
                whys.as_slice(),
                [Dropped::OutboxFull { kind: kind::PONG, limit: 8192, waiting, frame }]
                    if *waiting + *frame > 8192 && *waiting <= 8192
            ),
            "{whys:?}"
        );
    });
}

#[test]
fn a_program_on_the_board_is_not_dropped_for_being_quiet_and_is_seen_to_be() {
    run(async {
        let board = board(Security::Off, Limits::default());
        let mut local = Client::open_local(&board, USER).await;
        let quiet_since = |board: &Board| {
            board.comm.with(|sessions, _| {
                sessions
                    .iter()
                    .map(|info| info.quiet_since)
                    .collect::<Vec<_>>()
            })
        };
        // Within the idle time: not quiet.
        tokio::time::sleep(Duration::from_millis(5999)).await;
        assert_eq!(quiet_since(&board), [None]);
        // Far past it: still there, seen as quiet since it was last heard, and not a drop.
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert_eq!(quiet_since(&board), [Some(0)]);
        assert_eq!((whys(&board), board.comm.stats().dropped), (vec![], 0));
        // Every service is told once, with how long it had been quiet: the idle time.
        let told = |board: &Board| board.seen.borrow().quiet.clone();
        let session = told(&board).first().expect("told").0;
        assert_eq!(told(&board), [(session, 6000)]);
        // It speaks: answered, no longer quiet, and the services are told so.
        local.send(&Ping { now_ms: 5 }).await;
        assert_eq!(local.recv::<Pong>().await.expect("pong").now_ms, 5);
        assert_eq!(quiet_since(&board), [None]);
        assert_eq!(told(&board), [(session, 6000), (session, 0)]);
        // Quiet again, it is seen again, from when it last spoke.
        tokio::time::sleep(Duration::from_millis(6001)).await;
        assert!(matches!(quiet_since(&board).as_slice(), [Some(since)] if *since > 3_600_000));
        // Its end is closed: gone at once, by its own doing.
        drop(local);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(board.comm.with(|sessions, _| sessions.len()), 0);
        assert_eq!(whys(&board), []);

        // One that never says hello is dropped all the same.
        let mut silent = Client::new_local(&board, USER);
        assert!(silent.is_closed().await);
        assert_eq!(whys(&board), [Dropped::NoHello { limit_ms: 5000 }]);
    });
}

#[test]
fn a_session_whose_writes_stall_or_fail_is_dropped_saying_which_and_on_what() {
    run(async {
        // Room in its outbox, so it is the writes that wait, not the outbox that fills.
        let limits = Limits {
            outbox_bytes: 1 << 20,
            ..Limits::default()
        };
        let board = board(Security::Off, limits);

        // It asks, and never reads: more answers than the connection holds (16 KiB), far
        // fewer than the outbox does. Once the connection is full, a write waits out the
        // deadline.
        let mut deaf = Client::open(&board, USER).await;
        for number in 0..4000 {
            deaf.connection
                .send(&ask_frame(kind::GET, number), &[])
                .await
                .expect("the board reads");
            if number % 64 == 0 {
                tokio::task::yield_now().await;
            }
        }
        while whys(&board).is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let whys_now = whys(&board);
        assert!(
            matches!(
                whys_now.as_slice(),
                [Dropped::WriteStalled { kind: kind::PONG, limit_ms: 5000, waiting }]
                    if *waiting > 0
            ),
            "{whys_now:?}"
        );
        assert!(
            whys_now[0]
                .to_string()
                .starts_with("it did not take a frame of kind 0x05 within 5000 ms, with "),
            "{}",
            whys_now[0]
        );

        // Its reading end is gone while it still asks: the answer cannot be written.
        let gone = Client::open(&board, USER).await;
        let (read, mut write) = orion_server::Connection::split(gone.connection);
        use orion_server::FrameWrite as _;
        drop(read);
        write
            .send(&ask_frame(kind::GET, 1), &[])
            .await
            .expect("the board's end still reads");
        while whys(&board).len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let whys_now = whys(&board);
        assert_eq!(
            whys_now[1],
            Dropped::WriteFailed {
                kind: kind::PONG,
                error: std::io::ErrorKind::BrokenPipe
            }
        );
        assert_eq!(
            whys_now[1].to_string(),
            "its connection failed while it was sent a frame of kind 0x05: broken pipe"
        );
    });
}

#[test]
fn what_a_program_said_before_it_closed_is_handled_though_a_write_to_it_failed_first() {
    use orion_server::{Connection, FrameWrite};
    run(async {
        let board = board(Security::Off, Limits::default());
        for round in 0..8_u64 {
            let client = Client::open(&board, USER).await;
            let ids: Vec<SessionId> = board
                .comm
                .with(|sessions, _| sessions.iter().map(|info| info.id).collect());
            let [id] = ids[..] else {
                panic!("one session open, not {ids:?}");
            };
            // Its program closes its end for reading, then says one last thing and goes. The
            // comm core has something for it already, so its write finds the program gone
            // before its reader reaches the last word: the order a client that gives up and
            // exits meets while a program is still sending to it.
            let (read, mut write) = Connection::split(client.connection);
            drop(read);
            board
                .comm
                .with(|sessions, _| sessions.send(id, &Pong { now_ms: 1 }, Delivery::Must))
                .expect("queued");
            let before = board.seen.borrow().callers.len();
            write
                .send(&ask_frame(kind::GET, round), &[])
                .await
                .expect("sent");
            drop(write);
            tokio::time::sleep(Duration::from_millis(1)).await;
            assert_eq!(
                board.seen.borrow().callers.len(),
                before + 1,
                "round {round}: what it said before it went was not handled"
            );
        }
        // Each is still said to have gone at the comm core's write to it.
        let whys = whys(&board);
        assert_eq!(whys.len(), 8, "{whys:?}");
        assert!(
            whys.iter().all(|why| matches!(
                why,
                Dropped::WriteFailed {
                    kind: kind::PONG,
                    error: std::io::ErrorKind::BrokenPipe
                }
            )),
            "{whys:?}"
        );
    });
}

#[test]
fn a_frame_for_a_program_that_closed_is_taken_until_what_it_said_last_is_handled() {
    use orion_server::{Connection, FrameWrite};
    run(async {
        let board = board(Security::Off, Limits::default());
        let mut watcher = Client::open(&board, USER).await;
        watcher.ask(kind::WATCH, 0, &[]).await;
        let mut other = Client::open(&board, USER).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        let id = board.seen.borrow().watcher.expect("watching");

        // The watcher closes its end for reading, a write to it finds that, another session
        // sends it something, and only then is the watcher's last word read: the order of a
        // client that gives up and exits while the program giving it a file sends a chunk.
        // What it is sent meanwhile is taken, as by a socket a moment before its peer closed,
        // so nobody acts on it being gone before its last word is heard.
        let (read, mut write) = Connection::split(watcher.connection);
        drop(read);
        board
            .comm
            .with(|sessions, _| sessions.send(id, &Pong { now_ms: 1 }, Delivery::Must))
            .expect("queued");
        other.ask(kind::FILE_CHUNK, 0, &[]).await;
        write
            .send(&ask_frame(kind::GET, 9), &[])
            .await
            .expect("sent");
        drop(write);
        tokio::time::sleep(Duration::from_millis(1)).await;
        let seen = board.seen.borrow();
        assert_eq!(seen.to_watcher, [true]);
        assert_eq!(seen.callers.len(), 1, "its last word was handled");
        assert_eq!(seen.closed, [id]);
    });
}
