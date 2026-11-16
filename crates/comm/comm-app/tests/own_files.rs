//! The comm core's own files, `orion/`: fetched as any offered file is, running and in
//! recovery, with the values of settings that may be secret left out.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::Duration;

use orion_comm_app::{CommCore, CommSettings, RecoverWith, recover};
use orion_config::Source;
use orion_connect::{Address, Client, Options, Session};
use orion_files::index::Index;
use orion_files::{FileState, Files, OfferedBy};

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-own-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

/// Settings on a board whose security record says off, with a key whose value is a secret.
fn settings(dir: &Path) -> CommSettings {
    let view = dir.join("security");
    std::fs::write(&view, "off\ngeneration 1\n").expect("security record");
    let text = format!(
        "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n\
         ssh.host_key = {0}/very-secret-key\nlog.window_bytes = 4096\n",
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

#[test]
fn a_running_comm_core_offers_its_own_files_with_secrets_left_out() {
    run(async {
        let dir = temp("running");
        orion_log::install();
        let core = CommCore::start(settings(&dir)).await.expect("start");
        tracing::info!("a line to find in orion/log");
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
        assert_eq!(
            listed,
            [
                "orion/log",
                "orion/settings",
                "orion/last-good.carriers",
                "orion/state",
                "orion/sessions",
                "orion/version"
            ]
        );
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
        assert_eq!(fetched[2], "running\n");
        assert!(fetched[3].contains(" collector local:"), "{}", fetched[3]);
        assert!(
            fetched[4].contains("\ninterface_level 4\n"),
            "{}",
            fetched[4]
        );
        assert!(fetched[4].contains("\nkinds_sha256 "), "{}", fetched[4]);
        assert!(fetched[5].len() <= 4096);
        assert!(
            fetched[5].contains("a line to find in orion/log"),
            "{}",
            fetched[5]
        );

        assert_eq!(offers[0].prefix, "orion/");
        assert_eq!(offers[0].by, OfferedBy::CommCore);
        drop(core);
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
        assert_eq!(fetched[0], "recovery\nwhy: the broker would not start\n");
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
