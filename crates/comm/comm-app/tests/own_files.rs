//! The comm core's own files, `orion/`: fetched as any offered file is, running and in
//! recovery, with the values of settings that may be secret left out.

use std::io::Cursor;
use std::path::Path;
use std::time::Duration;

use orion_comm_app::{CommCore, CommSettings, RecoverWith, recover};
use orion_config::Source;
use orion_connect::{Address, Client, Options, Session};
use orion_files::index::Index;
use orion_files::{FileState, Files, OfferedBy};
use orion_scratch::Scratch;

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

fn temp(name: &str) -> Scratch {
    Scratch::new(&format!("own-{name}"))
}

/// Settings on a board whose security record says off, with a key whose value is a secret.
fn settings(dir: &Path) -> CommSettings {
    settings_with_window(dir, 4096)
}

fn settings_with_window(dir: &Path, window: usize) -> CommSettings {
    let view = dir.join("security");
    std::fs::write(&view, "off\ngeneration 1\n").expect("security record");
    let text = format!(
        "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n\
         ssh.host_key = {0}/very-secret-key\nlog.window_bytes = {window}\n",
        dir.display()
    );
    let mut source = Source::parse(&text, Vec::new()).expect("source");
    CommSettings::from_source_with_view(&mut source, &view).expect("settings")
}

/// Fetches each of `names` from the comm core on `socket`, on a thread that may block, with
/// what it offers.
async fn fetch(
    socket: &Path,
    names: &'static [&'static str],
) -> (Vec<String>, Vec<orion_files::Offered>) {
    let socket = socket.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut session = None;
        for _ in 0..200 {
            match Session::connect(
                &Address::Local(socket.clone()),
                "collector",
                Options::default(),
            ) {
                Ok(opened) => {
                    session = Some(opened);
                    break;
                }
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
        let files = Files::new(&Client::new(session.expect("connect")));
        let fetched = names
            .iter()
            .map(|name| {
                let mut into = Cursor::new(Vec::new());
                let transfer = files.fetch(name, &mut into, 0, |_, _| {}).expect(name);
                assert_eq!(
                    transfer.state,
                    FileState::Committed,
                    "{name}: {}",
                    transfer.detail
                );
                String::from_utf8(into.into_inner()).expect("text")
            })
            .collect();
        (fetched, files.offers().expect("offers").0)
    })
    .await
    .expect("fetched")
}

/// With `mdns = false` the board is not announced and hears nobody: there is no
/// `orion/boards`.
#[cfg(feature = "announce")]
#[test]
fn a_board_that_is_not_announced_has_no_file_of_boards_heard() {
    run(async {
        let dir = temp("no-boards");
        let view = dir.join("security");
        std::fs::write(&view, "off\ngeneration 1\n").expect("security record");
        let text = format!(
            "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\nmdns = false\n",
            dir.display()
        );
        let mut source = Source::parse(&text, Vec::new()).expect("source");
        let settings = CommSettings::from_source_with_view(&mut source, &view).expect("settings");
        let core = CommCore::start(settings).await.expect("start");
        let (fetched, _) = fetch(core.socket(), &["orion/index"]).await;
        let index = Index::parse(&fetched[0]).expect("an index");
        assert!(
            index.entries.iter().all(|e| e.name != "orion/boards"),
            "{index:?}"
        );
        assert!(index.entries.iter().any(|e| e.name == "orion/state"));
    });
}

#[test]
fn a_running_comm_core_offers_its_own_files_with_secrets_left_out() {
    run(async {
        let dir = temp("running");
        orion_log::install();
        let core = CommCore::start(settings(&dir)).await.expect("start");
        let names = &[
            "orion/index",
            "orion/settings",
            "orion/state",
            "orion/sessions",
            "orion/version",
            "orion/log",
        ];
        let (fetched, offers) = fetch(core.socket(), names).await;
        let index = Index::parse(&fetched[0]).expect("an index");
        let listed: Vec<&str> = index
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        let mut own = vec![
            "orion/log",
            "orion/settings",
            "orion/last-good.carriers",
            "orion/state",
            "orion/sessions",
            "orion/version",
        ];
        if cfg!(feature = "announce") {
            // The board would be announced (`mdns`), with a way in to announce: here there
            // is none, and the file says why it lists nobody.
            own.push("orion/boards");
            let (boards, _) = fetch(core.socket(), &["orion/boards"]).await;
            assert_eq!(
                boards[0],
                "# nothing is announced here, so nothing is heard\n"
            );
        }
        assert_eq!(listed, own);
        assert_eq!(index.bound("orion/log"), Some(4096), "log.window_bytes");
        assert_eq!(
            index.total,
            index
                .entries
                .iter()
                .map(|entry| entry.max_bytes)
                .sum::<u64>()
        );

        let said = &fetched[1];
        assert!(said.contains("node.name = board    # file\n"), "{said}");
        assert!(
            said.contains("ssh.host_key = (left out)    # file\n"),
            "{said}"
        );
        assert!(!said.contains("very-secret-key"), "{said}");
        assert_eq!(fetched[2], "running\npanics contained: 0\n");
        assert!(fetched[3].contains(" collector local:"), "{}", fetched[3]);
        assert!(
            fetched[4].contains(&format!(
                "\ninterface_level {}\n",
                orion_wire::INTERFACE_LEVEL
            )),
            "{}",
            fetched[4]
        );
        assert!(fetched[4].contains("\nkinds_sha256 "), "{}", fetched[4]);
        // Other tests in this process log too, so which lines are newest is not this test's
        // to say: the lines themselves are checked through a large window, below.
        assert!(fetched[5].len() <= 4096);
        assert!(fetched[5].is_empty() || fetched[5].ends_with('\n'));

        assert_eq!(offers[0].prefix, "orion/");
        assert_eq!(offers[0].by, OfferedBy::CommCore);
        drop(core);
    });
}

#[test]
fn two_comm_cores_in_one_process_each_keep_their_own_log_window() {
    run(async {
        orion_log::install();
        let (small_dir, large_dir) = (temp("small-window"), temp("large-window"));
        let small = CommCore::start(settings_with_window(&small_dir, 4096))
            .await
            .expect("start the small one");
        let large = CommCore::start(settings_with_window(&large_dir, 262_144))
            .await
            .expect("start the large one");
        // More than the small window holds, all of it within the large one.
        for n in 0..200 {
            tracing::info!("line {n} of a log longer than four kilobytes");
        }
        let names = &["orion/index", "orion/log"];
        let (from_small, _) = fetch(small.socket(), names).await;
        let (from_large, _) = fetch(large.socket(), names).await;
        let bound = |index: &str| Index::parse(index).expect("an index").bound("orion/log");
        assert_eq!(bound(&from_small[0]), Some(4096));
        assert_eq!(bound(&from_large[0]), Some(262_144));
        // The large window keeps the lines long, and the small one still shows only its own,
        // whole lines. Other tests in this process log too, so the lines are looked for in the
        // large one, which their few lines cannot push these out of.
        assert!(from_small[1].len() <= 4096, "{}", from_small[1].len());
        assert!(from_small[1].ends_with('\n'), "{}", from_small[1]);
        assert!(from_large[1].len() > 4096, "{}", from_large[1].len());
        for n in [0, 199] {
            let line = format!("line {n} of a log longer than four kilobytes");
            assert!(from_large[1].contains(&line), "{line}");
        }
        drop((small, large));
    });
}

#[test]
fn recovery_offers_the_comm_core_s_own_files_and_says_why() {
    run(async {
        let dir = temp("recovery");
        let settings = settings(&dir);
        let socket = settings.config.socket.clone();
        let with = RecoverWith {
            listen: vec!["127.0.0.1:0".parse().expect("addr")],
            keys_file: dir.join("recovery_keys"),
            helper: None,
            retry: Duration::from_millis(100),
            last_good: dir.join("no-record"),
            security_view: dir.join("security"),
            #[cfg(feature = "login-helper")]
            login: Default::default(),
        };
        std::fs::create_dir_all(dir.join("state")).expect("state");
        std::fs::write(
            dir.join("state/last-good.carriers"),
            "socket = /run/x.sock\n",
        )
        .expect("record");
        let recovery = recover(Some(&settings), "the broker would not start".into(), with);
        let (fetched, offers) = fetch(
            &socket,
            &["orion/state", "orion/settings", "orion/last-good.carriers"],
        )
        .await;
        assert_eq!(
            fetched[0],
            "recovery\nwhy: the broker would not start\npanics contained: 0\n"
        );
        assert!(
            fetched[1].contains("ssh.host_key = (left out)"),
            "{}",
            fetched[1]
        );
        assert_eq!(fetched[2], "socket = /run/x.sock\n");
        assert_eq!(offers[0].by, OfferedBy::CommCore);
        drop(recovery);
    });
}

/// A cluster name no announcement can carry: `orion/state` says `_orion._tcp` is not
/// announced, and why, naming the setting. (No way in is open: nothing is announced here.)
#[cfg(feature = "announce")]
#[test]
fn a_cluster_too_long_to_announce_is_said_in_the_state() {
    run(async {
        let dir = temp("long-cluster");
        let view = dir.join("security");
        std::fs::write(&view, "off\ngeneration 1\n").expect("security record");
        let text = format!(
            "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\ncluster = {1}\n",
            dir.display(),
            "c".repeat(253)
        );
        let mut source = Source::parse(&text, Vec::new()).expect("source");
        let settings = CommSettings::from_source_with_view(&mut source, &view).expect("settings");
        let core = CommCore::start(settings).await.expect("start");
        let (fetched, _) = fetch(core.socket(), &["orion/state"]).await;
        assert_eq!(
            fetched[0],
            "running\nmdns: setting `cluster` is 253 bytes, and an announcement carries 252 at \
             most: `_orion._tcp` is not announced; the board's name and SSH still are\n\
             panics contained: 0\n"
        );
        // A responder would have claimed its name by now: none was started.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(core.mdns_name(), None, "nothing to announce");
    });
}

/// Ways in on the loopback address alone: nothing to announce, so nothing is.
#[cfg(feature = "announce")]
#[test]
fn ways_in_on_loopback_alone_are_not_announced() {
    run(async {
        let dir = temp("loopback-only");
        let view = dir.join("security");
        std::fs::write(&view, "off\ngeneration 1\n").expect("security record");
        let text = format!(
            "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n\
             peer.listen = 127.0.0.1:0\nssh.listen = 127.0.0.1:0\nssh.sandbox = false\n\
             ssh.host_key = {0}/host_key\n",
            dir.display()
        );
        let mut source = Source::parse(&text, Vec::new()).expect("source");
        let settings = CommSettings::from_source_with_view(&mut source, &view).expect("settings");
        let core = CommCore::start(settings).await.expect("start");
        // A responder would have claimed its name by now: none was started.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(core.mdns_name(), None);
        assert!(!core.summary().contains("mdns"), "{}", core.summary());
    });
}

/// Measuring the time: `orion/state` says the clock is not set, which sources were tried and
/// what the last try found.
#[cfg(feature = "time")]
#[test]
fn the_state_says_whether_the_clock_is_set_and_what_was_tried() {
    run(async {
        let dir = temp("clock");
        let view = dir.join("security");
        std::fs::write(&view, "off\ngeneration 1\n").expect("security record");
        let text = format!(
            "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n\
             time.sync = report\ntime.servers = operator\ntime.synced_file = off\n",
            dir.display()
        );
        let mut source = Source::parse(&text, Vec::new()).expect("source");
        let settings = CommSettings::from_source_with_view(&mut source, &view).expect("settings");
        let core = CommCore::start(settings).await.expect("start");
        let (fetched, _) = fetch(core.socket(), &["orion/state"]).await;
        let said = &fetched[0];
        assert!(said.starts_with("running\n"), "{said}");
        let clock = said.lines().nth(1).unwrap_or_default();
        assert!(
            clock
                .starts_with("clock: not set (time.sync = report; sources tried: operator; last: "),
            "{said}"
        );
    });
}

/// Settings that were given and that nothing reads: the board says them in `orion/state` and
/// in its hello, where a person looking at the board sees them.
#[test]
fn settings_nothing_reads_are_said_in_the_state_and_the_hello() {
    run(async {
        let dir = temp("unread");
        // A key of the file and a variable of the environment that nothing reads, as whoever
        // starts the comm core finds them.
        let view = dir.join("security");
        std::fs::write(&view, "off\ngeneration 1\n").expect("security record");
        let text = format!(
            "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\ntopcis.max = 5\n",
            dir.display()
        );
        let variables = [("ORION_TYPO".to_owned(), "1".to_owned())];
        let mut source = Source::parse(&text, variables).expect("source");
        let settings = CommSettings::from_source_with_view(&mut source, &view).expect("settings");
        let settings = settings.told_unread(&source.warnings());
        let core = CommCore::start(settings).await.expect("start");
        let (fetched, _) = fetch(core.socket(), &["orion/state"]).await;
        assert_eq!(
            fetched[0],
            "running\nsettings not read: topcis.max, ORION_TYPO\npanics contained: 0\n"
        );
        let socket = core.socket().to_owned();
        let said = tokio::task::spawn_blocking(move || {
            let orion = orion::Orion::connect(&Address::Local(socket), "tool").expect("connect");
            orion
                .node()
                .expect("node")
                .settings_unread()
                .map(str::to_owned)
        })
        .await
        .expect("asked");
        assert_eq!(said.as_deref(), Some("topcis.max, ORION_TYPO"));
        drop(core);

        // With none, neither says anything of it.
        let dir = temp("all-read");
        let core = CommCore::start(self::settings(&dir)).await.expect("start");
        let (fetched, _) = fetch(core.socket(), &["orion/state"]).await;
        assert_eq!(fetched[0], "running\npanics contained: 0\n");
        let socket = core.socket().to_owned();
        let said = tokio::task::spawn_blocking(move || {
            let orion = orion::Orion::connect(&Address::Local(socket), "tool").expect("connect");
            orion
                .node()
                .expect("node")
                .settings_unread()
                .map(str::to_owned)
        })
        .await
        .expect("asked");
        assert_eq!(said, None);
    });
}
