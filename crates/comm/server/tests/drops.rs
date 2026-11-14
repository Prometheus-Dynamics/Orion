//! Why the comm core ends a session, and a program on the board that is quiet but not gone.

mod common;

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
        // It speaks: answered, and no longer quiet.
        local.send(&Ping { now_ms: 5 }).await;
        assert_eq!(local.recv::<Pong>().await.expect("pong").now_ms, 5);
        assert_eq!(quiet_since(&board), [None]);
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
