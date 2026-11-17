//! Orion's precheck of the security switch: what it has against turning it on or off, asked
//! of a running comm core and of recovery.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use orion::node::SecurityTo;
use orion_comm_app::{CommCore, CommSettings, RecoverWith, recover};
use orion_config::Source;

/// A recovery key, as the recovery keys file names one.
const RECOVERY_KEY: &str =
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJdPzuf2vXKst+3E6o/FQhon0GGi/cASkI4hy6RM0TME precheck\n";

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-precheck-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("keys")).expect("dir");
    dir
}

/// Settings on a board whose record says off, with `more` added.
fn settings(dir: &Path, more: &str) -> CommSettings {
    let view = dir.join("security");
    std::fs::write(&view, "off\ngeneration 1\n").expect("security record");
    let text = format!(
        "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\nkeys.dir = {0}/keys\n\
         recovery.keys = {0}/recovery_keys\n{more}",
        dir.display()
    );
    let mut source = Source::parse(&text, Vec::new()).expect("source");
    CommSettings::from_source_with_view(&mut source, &view).expect("settings")
}

/// What the comm core on `socket` has against turning the switch on, and off.
async fn precheck(socket: &Path) -> (Vec<String>, Vec<String>) {
    let address: orion::Address = socket.display().to_string().parse().expect("address");
    tokio::task::spawn_blocking(move || {
        let began = Instant::now();
        let orion = loop {
            match orion::Orion::connect(&address, "bridge") {
                Ok(orion) => break orion,
                Err(error) => assert!(began.elapsed() < Duration::from_secs(10), "{error}"),
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let on = orion.security_precheck(SecurityTo::On).expect("on");
        let off = orion.security_precheck(SecurityTo::Off).expect("off");
        (on, off)
    })
    .await
    .expect("asked")
}

fn no_way_in(lines: &[String], dir: &Path) {
    assert_eq!(lines.len(), 1, "{lines:?}");
    let line = &lines[0];
    assert!(
        line.starts_with("missing: orion: nobody but root"),
        "{line}"
    );
    let file = dir.join("recovery_keys");
    assert!(line.contains(&file.display().to_string()), "{line}");
}

#[test]
fn turning_it_on_needs_a_way_in_other_than_root_and_turning_it_off_needs_nothing() {
    run(async {
        // Nobody but root.
        let dir = temp("none");
        let core = CommCore::start(settings(&dir, "")).await.expect("start");
        let (on, off) = precheck(core.socket()).await;
        no_way_in(&on, &dir);
        assert!(off.is_empty(), "{off:?}");
        // A key in the recovery keys file: read when asked, so it counts at once.
        std::fs::write(dir.join("recovery_keys"), RECOVERY_KEY).expect("keys");
        assert!(precheck(core.socket()).await.0.is_empty());
        // A line that is not a key lets nobody in, so it is not one.
        std::fs::write(dir.join("recovery_keys"), "ssh-ed25519 nonsense\n").expect("keys");
        no_way_in(&precheck(core.socket()).await.0, &dir);
        drop(core);

        // An enrolled operator.
        let dir = temp("operator");
        std::fs::write(
            dir.join("keys/operator.seed"),
            format!("{}\n", "ab".repeat(32)),
        )
        .expect("seed");
        let core = CommCore::start(settings(&dir, "")).await.expect("start");
        assert_eq!(precheck(core.socket()).await, (Vec::new(), Vec::new()));
        drop(core);

        // An enrollment key, which lets a newcomer enroll itself.
        let dir = temp("enrollment");
        let core = CommCore::start(settings(&dir, "enrollment.key = shared\n"))
            .await
            .expect("start");
        assert_eq!(precheck(core.socket()).await, (Vec::new(), Vec::new()));
        drop(core);
    });
}

#[test]
fn recovery_answers_from_its_recovery_keys_alone() {
    run(async {
        // An enrollment key does not count: recovery does not open the trust store, and lets
        // in who the recovery keys file says.
        let dir = temp("recovery");
        let settings = settings(&dir, "enrollment.key = shared\n");
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
        let recovery = recover(Some(&settings), "a test".into(), with);
        let (on, off) = precheck(&socket).await;
        no_way_in(&on, &dir);
        assert!(off.is_empty(), "{off:?}");
        std::fs::write(dir.join("recovery_keys"), RECOVERY_KEY).expect("keys");
        assert!(precheck(&socket).await.0.is_empty());
        drop(recovery);
    });
}
