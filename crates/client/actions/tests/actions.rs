//! An action registered by another process runs through the check.
//!
//! This test is the comm core, with node logic's own action list behind the door (the stub's
//! router), and the caller. It starts the same test binary again as the program that owns the
//! action.

#![cfg(target_os = "linux")]

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use orion_actions::{
    ActionCall, ActionDecl, Actions, Admits, Outcome, Policy, Request, Target, Update,
};
use orion_connect::{Address, Client, ClientError, Options, Session};
use orion_server::{Comm, Error, Limits, Security};
use orion_stubs::router::Router;
use orion_unix::UnixCarrier;

const ROLE: &str = "ORION_TEST_ROLE";
const SOCKET: &str = "ORION_TEST_SOCKET";

fn actions(socket: &Path, name: &str) -> Actions {
    let session = Session::connect(&Address::Local(socket.to_owned()), name, Options::default())
        .expect("connect");
    Actions::new(&Client::new(session))
}

/// The process that owns the actions: registers them and runs what it is handed.
#[test]
fn owner_process() {
    if std::env::var(ROLE).as_deref() != Ok("owner") {
        return;
    }
    let socket = PathBuf::from(std::env::var(SOCKET).expect("socket"));
    let side = actions(&socket, "lights");
    let node = Target::Node("board".parse().expect("name"));
    let mut runs = side
        .register(
            &ActionDecl {
                name: "lights.fade",
                target: node.clone(),
                description: "Fades the lights to a level over a second",
            },
            Policy::new(Admits::LOCAL),
        )
        .expect("register");
    // An action only root may run.
    side.register(
        &ActionDecl {
            name: "lights.reset",
            target: node,
            description: "Puts the lights back as shipped",
        },
        Policy::new(Admits::ROOT),
    )
    .expect("register");

    let started = Instant::now();
    let mut cancelled: Option<u64> = None;
    let mut slow: Option<(u64, Instant)> = None;
    while started.elapsed() < Duration::from_secs(20) {
        if let Some((call, since)) = slow
            && since.elapsed() > Duration::from_millis(400)
        {
            runs.finish(call, Outcome::Succeeded, b"detached done", "")
                .expect("finish");
            slow = None;
        }
        match runs.wait(Duration::from_millis(20)).expect("wait") {
            Some(Request::Run {
                call,
                name,
                args,
                detached,
                ..
            }) => {
                assert_ne!(
                    name, "lights.reset",
                    "a run the check refused reached the action"
                );
                // Told whether the run outlives its caller: only the one started detached.
                assert_eq!(detached, args == b"slowly", "{args:?}");
                match args.as_slice() {
                    b"stop" => return,
                    b"forever" => {
                        runs.progress(call, 1, "fading, slowly", &[])
                            .expect("progress");
                        cancelled = Some(call);
                    }
                    b"slowly" => {
                        runs.progress(call, 50, "half way", &[]).expect("progress");
                        slow = Some((call, Instant::now()));
                    }
                    b"badly" => runs
                        .finish_with_code(call, Outcome::Failed, 11, &[], "the bulb is gone")
                        .expect("finish"),
                    level => {
                        for percent in [25, 50, 75] {
                            runs.progress(call, percent, "fading", level)
                                .expect("progress");
                        }
                        let mut output = b"faded to ".to_vec();
                        output.extend_from_slice(level);
                        runs.finish(call, Outcome::Succeeded, &output, "")
                            .expect("finish");
                    }
                }
            }
            Some(Request::Cancel { call, .. }) => {
                assert_eq!(
                    cancelled,
                    Some(call),
                    "asked to stop a run that is not running"
                );
                runs.finish(call, Outcome::Cancelled, &[], "stopped half way")
                    .expect("finish");
            }
            Some(Request::CallerGone { .. }) | None => {}
        }
    }
    panic!("never told to stop");
}

#[test]
fn an_action_registered_from_another_process_runs_through_the_check() {
    if std::env::var(ROLE).is_ok() {
        return;
    }
    let dir = std::env::temp_dir().join(format!("orion-actions-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let socket = dir.join("orion.sock");

    let (ready, started) = mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let path = socket.clone();
    let comm_core = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            // Security on: the check decides, as on a real board.
            let comm = Comm::new(
                "board".parse().expect("name"),
                Security::On,
                None,
                Limits::default(),
            );
            let router = Router::new("board".parse().expect("name"));
            comm.add_service(router.clone());
            let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
            let stats = comm.clone();
            tokio::task::spawn_local(async move { comm.serve(unix).await });
            ready.send(()).expect("ready");
            let _ = stopped.await;
            // Every run that reached the list.
            let reached: Vec<String> = router
                .take_events()
                .into_iter()
                .map(|event| event.name.to_string())
                .collect();
            (reached, stats.stats().refused)
        })
    });
    started.recv().expect("the comm core starts");

    let mut owner = Command::new(std::env::current_exe().expect("this test"))
        .args(["--exact", "owner_process", "--nocapture"])
        .env(ROLE, "owner")
        .env(SOCKET, &socket)
        .spawn()
        .expect("the owner starts");

    let side = actions(&socket, "caller");
    let node = Target::Node("board".parse().expect("name"));
    let call = |args: &'static [u8], detached| ActionCall {
        name: "lights.fade",
        target: node.clone(),
        args,
        // The node keeps no detached run without an end to it.
        deadline_ms: if detached { 60_000 } else { 0 },
        detached,
    };
    // Until the other process has registered it, there is no such action.
    let waiting = Instant::now();
    let mut run = loop {
        let mut run = side.run(&call(b"40", false)).expect("run");
        match run.wait(Duration::from_millis(100)) {
            Err(ClientError::Refused {
                error: Error::NotFound,
                ..
            }) => {
                assert!(
                    waiting.elapsed() < Duration::from_secs(20),
                    "the action was never registered"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {}
            Ok(Some(update)) => break (run, Some(update)),
            Err(other) => panic!("{other}"),
        }
    };

    // The run is followed: how it is doing, then how it ended, with what it returned.
    let mut percents = Vec::new();
    let mut update = run.1.take();
    let finished = loop {
        match update
            .take()
            .or_else(|| run.0.wait(Duration::from_secs(5)).expect("wait"))
        {
            Some(Update::Doing(doing)) => {
                assert_eq!(
                    (doing.message.as_str(), doing.data.as_slice()),
                    ("fading", b"40".as_slice())
                );
                percents.push(doing.percent);
            }
            Some(Update::Finished(finished)) => break finished,
            None => panic!("the run never ended"),
        }
    };
    assert!(
        percents.windows(2).all(|pair| pair[0] < pair[1]) && !percents.is_empty(),
        "{percents:?}"
    );
    assert_eq!(
        (finished.outcome, finished.output.as_slice()),
        (Outcome::Succeeded, b"faded to 40".as_slice())
    );

    // An action that fails says so, with why.
    let failed = side
        .run(&call(b"badly", false))
        .expect("run")
        .finish(Duration::from_secs(5))
        .expect("ended");
    assert_eq!(
        (failed.outcome, failed.code, failed.detail.as_str()),
        (Outcome::Failed, 11, "the bulb is gone")
    );

    // A run can be stopped.
    let mut long = side.run(&call(b"forever", false)).expect("run");
    assert!(matches!(
        long.wait(Duration::from_secs(5)).expect("wait"),
        Some(Update::Doing(_))
    ));
    long.cancel().expect("cancel");
    let stopped_run = long.finish(Duration::from_secs(5)).expect("ended");
    assert_eq!(
        (stopped_run.outcome, stopped_run.detail.as_str()),
        (Outcome::Cancelled, "stopped half way")
    );

    // A detached run outlives its caller, and another session comes back to it.
    let leaving = actions(&socket, "leaving");
    let mut detached = leaving.run(&call(b"slowly", true)).expect("run");
    assert!(matches!(
        detached.wait(Duration::from_secs(5)).expect("wait"),
        Some(Update::Doing(_))
    ));
    let number = detached.call();
    drop(detached);
    drop(leaving);
    let back = actions(&socket, "back");
    let mut again = back.attach(number).expect("attach");
    let ended = again.finish(Duration::from_secs(5)).expect("ended");
    assert_eq!(
        (ended.outcome, ended.output.as_slice()),
        (Outcome::Succeeded, b"detached done".as_slice())
    );

    // The check: an action registered for root only is refused to anyone else, before it
    // reaches the program that owns it.
    let is_root = std::fs::metadata("/proc/self")
        .map(|meta| std::os::unix::fs::MetadataExt::uid(&meta) == 0)
        .unwrap_or(false);
    let mut reset = side
        .run(&ActionCall {
            name: "lights.reset",
            target: node.clone(),
            args: b"",
            deadline_ms: 0,
            detached: false,
        })
        .expect("run");
    let refused = reset.wait(Duration::from_secs(2));
    if is_root {
        eprintln!("running as root: the refusal of a root-only action is not tested here");
    } else {
        assert!(
            matches!(
                refused,
                Err(ClientError::Refused {
                    error: Error::Denied,
                    ..
                })
            ),
            "{refused:?}"
        );
    }
    // An action nobody registered.
    let mut missing = side
        .run(&ActionCall {
            name: "lights.dance",
            ..call(b"", false)
        })
        .expect("run");
    assert!(matches!(
        missing.wait(Duration::from_secs(2)),
        Err(ClientError::Refused {
            error: Error::NotFound,
            ..
        })
    ));
    // A name that is taken.
    let taken = side.register(
        &ActionDecl {
            name: "lights.fade",
            target: node.clone(),
            description: "mine now",
        },
        Policy::new(Admits::ANYONE),
    );
    assert!(
        matches!(
            taken,
            Err(ClientError::Refused {
                error: Error::AlreadyExists,
                ..
            })
        ),
        "{taken:?}"
    );

    // Done: the owner stops, and everything it was handed went through the check first.
    let _ = side.run(&call(b"stop", false)).expect("run");
    let status = owner.wait().expect("the owner ends");
    assert!(
        status.success(),
        "the owner failed: {:?} (signal {:?})",
        status.code(),
        status.signal()
    );
    let _ = stop.send(());
    let (reached, refused) = comm_core.join().expect("the comm core stops");
    assert!(
        reached.iter().all(|name| name == "lights.fade"),
        "{reached:?}"
    );
    if !is_root {
        assert!(refused >= 2, "{refused} refusals");
    }
    let _ = std::fs::remove_dir_all(dir);
}
