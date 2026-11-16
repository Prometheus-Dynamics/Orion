//! Files the settings name (`offers.file`): read as they are or made by a command, served in
//! running and in recovery, and only from a file no one but its owner may write.

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
    let dir = std::env::temp_dir().join(format!("orion-configured-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

/// Settings naming `dir/offers.conf`, whose owner is this test's user, with `mode`.
fn settings(dir: &Path, mode: u32) -> CommSettings {
    std::fs::write(dir.join("host.txt"), "board-1\n").expect("host");
    let offers = dir.join("offers.conf");
    std::fs::write(
        &offers,
        format!(
            "# what this board hands over\n\
             sys/ host = path {}/host.txt bound 100\n\
             sys/ echo = command /usr/bin/echo made by echo bound 100 timeout 5000\n\
             sys/ yes = command /usr/bin/yes bound 1000 timeout 5000 user-data\n\
             sys/ slow = command /usr/bin/sleep 5 bound 10 timeout 200\n\
             sys/ fails = command /usr/bin/false bound 10 timeout 5000\n",
            dir.display()
        ),
    )
    .expect("offers");
    std::fs::set_permissions(&offers, std::os::unix::fs::PermissionsExt::from_mode(mode))
        .expect("mode");
    let view = dir.join("security");
    std::fs::write(&view, "off\ngeneration 1\n").expect("security record");
    let text = format!(
        "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n\
         offers.file = {0}/offers.conf\n",
        dir.display()
    );
    let mut source = Source::parse(&text, Vec::new()).expect("source");
    let mut settings = CommSettings::from_source_with_view(&mut source, &view).expect("settings");
    settings.offers_owner = rustix::process::getuid().as_raw();
    settings
}

/// Each of `names` from the comm core on `socket`, or why it was not taken, and what is
/// offered.
async fn fetch(
    socket: &Path,
    names: &'static [&'static str],
) -> (Vec<Result<String, String>>, Vec<orion_files::Offered>) {
    let socket = socket.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut session = None;
        for _ in 0..200 {
            if let Ok(opened) = Session::connect(
                &Address::Local(socket.clone()),
                "collector",
                Options::default(),
            ) {
                session = Some(opened);
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let files = Files::new(&Client::new(session.expect("connect")));
        let fetched = names
            .iter()
            .map(|name| {
                let mut into = Cursor::new(Vec::new());
                let transfer = files.fetch(name, &mut into, 0, |_, _| {}).expect(name);
                match transfer.state {
                    FileState::Committed => Ok(String::from_utf8(into.into_inner()).expect("text")),
                    _ => Err(transfer.detail),
                }
            })
            .collect();
        (fetched, files.offers().expect("offers").0)
    })
    .await
    .expect("fetched")
}

#[test]
fn a_running_comm_core_serves_the_files_its_settings_name() {
    run(async {
        let dir = temp("running");
        let core = CommCore::start(settings(&dir, 0o644)).await.expect("start");
        let names = &[
            "sys/index",
            "sys/host",
            "sys/echo",
            "sys/yes",
            "sys/slow",
            "sys/fails",
        ];
        let (fetched, offers) = fetch(core.socket(), names).await;
        let index = Index::parse(fetched[0].as_ref().expect("index")).expect("an index");
        assert_eq!(index.total, 100 + 100 + 1000 + 10 + 10);
        assert_eq!(index.bound("sys/yes"), Some(1000));
        assert!(index.entries[2].user_data && !index.entries[0].user_data);
        assert_eq!(fetched[1], Ok("board-1\n".into()));
        assert_eq!(fetched[2], Ok("made by echo\n".into()));
        assert_eq!(fetched[3], Ok("y\n".repeat(500)), "ended at its bound");
        assert_eq!(
            fetched[4],
            Err("sys/slow: /usr/bin/sleep did not finish in 200 ms".into())
        );
        let fails = fetched[5].as_ref().expect_err("false fails");
        assert!(
            fails.starts_with("sys/fails: /usr/bin/false exit status: 1"),
            "{fails}"
        );
        assert!(
            offers
                .iter()
                .any(|offer| offer.prefix == "sys/" && offer.by == OfferedBy::Configured),
            "{offers:?}"
        );
        drop(core);
    });
}

#[test]
fn a_file_of_offers_others_may_write_stops_the_start_and_recovery_still_serves_its_own() {
    run(async {
        let dir = temp("writable");
        let error = CommCore::start(settings(&dir, 0o664))
            .await
            .expect_err("not served");
        let said = error.to_string();
        assert!(
            said.contains("offers.file") && said.contains("may be written by others"),
            "{said}"
        );

        // In recovery the same file is passed over, and the comm core's own files served.
        let settings = settings(&dir, 0o664);
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
        let recovery = recover(Some(&settings), said, with);
        let (fetched, offers) = fetch(&socket, &["orion/state"]).await;
        assert!(
            fetched[0]
                .as_ref()
                .expect("state")
                .starts_with("recovery\n")
        );
        assert!(
            offers.iter().all(|offer| offer.prefix != "sys/"),
            "{offers:?}"
        );
        drop(recovery);
    });
}

#[test]
fn recovery_serves_the_files_its_settings_name() {
    run(async {
        let dir = temp("recovery");
        let settings = settings(&dir, 0o644);
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
        let recovery = recover(Some(&settings), "the broker would not start".into(), with);
        let (fetched, _) = fetch(&socket, &["sys/host", "sys/echo"]).await;
        assert_eq!(
            fetched,
            [Ok("board-1\n".into()), Ok("made by echo\n".into())]
        );
        drop(recovery);
    });
}
