//! The time sync against real sockets on loopback and a clock that records what it is asked.

use std::cell::RefCell;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use tokio::net::UdpSocket;

use crate::config::{TimeConfig, TimeMode};
use crate::discipline::{Clock, implausible};
use crate::operator::OperatorTime;
use crate::sntp::{self, ServedClock};
use crate::sync::{FIRST_POLL, SAVED_TIME_FILE, ServedState, Syncer, best_sample};

const S: i64 = 1_000_000_000;

/// Records what the sync asks of the clock instead of changing it.
#[derive(Default)]
pub(crate) struct FakeClock {
    pub(crate) steps: RefCell<Vec<i64>>,
    pub(crate) slews: RefCell<Vec<i64>>,
}

impl Clock for FakeClock {
    fn step(&self, offset_ns: i64) -> std::io::Result<()> {
        self.steps.borrow_mut().push(offset_ns);
        Ok(())
    }

    fn slew(&self, offset_ns: i64, _max_error_ns: i64) -> std::io::Result<()> {
        self.slews.borrow_mut().push(offset_ns);
        Ok(())
    }
}

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-time-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

fn config(mode: TimeMode, servers: Vec<String>) -> TimeConfig {
    TimeConfig {
        mode,
        servers,
        synced_file: None,
        ..TimeConfig::default()
    }
}

fn syncer(
    config: TimeConfig,
    clock: &Rc<FakeClock>,
    saved: Option<PathBuf>,
    timesyncd: bool,
) -> Syncer {
    Syncer::new(
        config,
        clock.clone(),
        ServedState::default(),
        saved,
        timesyncd,
    )
}

async fn poll(syncer: &mut Syncer) {
    let result = best_sample(&syncer.config.servers, &OperatorTime::default(), None, None).await;
    syncer.apply(result);
}

/// An Orion node serving a clock `ahead_ns` off, as the network's reference.
async fn peer_source(ahead_ns: i64) -> SocketAddr {
    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    let addr = socket.local_addr().expect("addr");
    tokio::task::spawn_local(sntp::serve(
        socket,
        || ServedClock::Authority,
        move || sntp::NtpTime::from_unix_ns(sntp::unix_now_ns() + i128::from(ahead_ns)),
    ));
    addr
}

#[test]
fn a_board_behind_its_peer_steps_after_two_agreeing_samples() {
    run(async {
        // A board with no clock battery: about 1.9 h behind the node that serves its clock.
        let behind = 6_840 * S;
        let source = peer_source(behind).await.to_string();
        let clock = Rc::new(FakeClock::default());
        let dir = temp("step");
        let saved = dir.join(SAVED_TIME_FILE);
        let mut syncer = syncer(
            config(TimeMode::Discipline, vec![source.clone()]),
            &clock,
            Some(saved.clone()),
            false,
        );
        poll(&mut syncer).await;
        assert_eq!(syncer.status.last_action, "waiting for a second sample");
        assert!(clock.steps.borrow().is_empty());
        assert_eq!(syncer.poll, FIRST_POLL);
        poll(&mut syncer).await;
        let steps = clock.steps.borrow().clone();
        assert_eq!(steps.len(), 1, "{:?}", syncer.status);
        // Loopback answers in well under 100 ms even on a loaded machine.
        assert!(
            (steps[0] - behind).abs() < S / 10,
            "stepped {} ns",
            steps[0]
        );
        assert!(syncer.status.synced);
        assert_eq!(syncer.status.stratum, Some(10));
        assert_eq!(syncer.status.source.as_deref(), Some(source.as_str()));
        assert!(saved.exists(), "the synchronized time is saved");
        assert!(syncer.poll >= Duration::from_secs(64));
        let _ = std::fs::remove_dir_all(dir);
    });
}

#[test]
fn report_mode_measures_and_never_touches_the_clock() {
    run(async {
        let source = peer_source(3 * S).await.to_string();
        let clock = Rc::new(FakeClock::default());
        let mut syncer = syncer(config(TimeMode::Report, vec![source]), &clock, None, false);
        for _ in 0..2 {
            poll(&mut syncer).await;
        }
        assert_eq!(syncer.status.last_action, "would step");
        assert!(!syncer.status.synced);
        assert!(clock.steps.borrow().is_empty() && clock.slews.borrow().is_empty());
        let offset = syncer.status.offset_ns.expect("offset");
        assert!((offset - 3 * S).abs() < S / 10, "{offset}");
    });
}

#[test]
fn discipline_only_reports_while_timesyncd_runs() {
    let clock = Rc::new(FakeClock::default());
    let syncer = syncer(
        config(TimeMode::Discipline, vec!["127.0.0.1:9".to_owned()]),
        &clock,
        None,
        true,
    );
    assert!(!syncer.disciplining);
    assert_eq!(syncer.status.mode, "report");
}

#[test]
fn a_clock_behind_the_saved_time_moves_forward_to_it() {
    let dir = temp("saved");
    let saved = dir.join(SAVED_TIME_FILE);
    let ahead_ms = 3_600_000_i64;
    let now_ms = (sntp::unix_now_ns() / 1_000_000) as i64;
    std::fs::write(&saved, (now_ms + ahead_ms).to_string()).expect("write");
    let clock = Rc::new(FakeClock::default());
    let mut syncer = syncer(
        config(TimeMode::Discipline, vec!["127.0.0.1:9".to_owned()]),
        &clock,
        Some(saved.clone()),
        false,
    );
    syncer.restore_saved_time();
    let steps = clock.steps.borrow().clone();
    assert_eq!(steps.len(), 1);
    assert!((steps[0] / 1_000_000 - ahead_ms).abs() < 5_000, "{steps:?}");
    // A clock ahead of the saved time is left alone.
    std::fs::write(&saved, (now_ms - ahead_ms).to_string()).expect("write");
    syncer.restore_saved_time();
    assert_eq!(clock.steps.borrow().len(), 1);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn an_unanswered_server_is_an_error_and_the_poll_backs_off() {
    run(async {
        // Nothing listens on the discard port of loopback.
        let clock = Rc::new(FakeClock::default());
        let mut syncer = syncer(
            config(TimeMode::Report, vec!["127.0.0.1:9".to_owned()]),
            &clock,
            None,
            false,
        );
        poll(&mut syncer).await;
        assert!(syncer.status.error.is_some());
        assert!(syncer.poll >= FIRST_POLL && syncer.poll <= Duration::from_secs(64));
    });
}

#[test]
fn a_tcp_source_answers_like_the_udp_one() {
    run(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::task::spawn_local(sntp::serve_tcp(
            listener,
            || ServedClock::Synced(2),
            move || sntp::NtpTime::from_unix_ns(sntp::unix_now_ns() + i128::from(5 * S)),
        ));
        let (source, sample) = best_sample(
            &[format!("tcp://{addr}")],
            &OperatorTime::default(),
            None,
            None,
        )
        .await
        .expect("sample");
        assert_eq!(source, format!("tcp://{addr}"));
        assert!((sample.offset_ns - 5 * S).abs() < S / 10, "{sample:?}");
        assert_eq!(sample.stratum, 2);
    });
}

const NOW_MS: i64 = 1_800_000_000_000;
const DAY: i64 = 86_400_000;

#[test]
fn a_source_before_the_floor_is_refused_from_every_source() {
    assert!(implausible(1_000, None, false).is_some());
    assert!(implausible(1_000, None, true).is_some());
    // A clock on a 2025 date.
    assert!(implausible(1_750_000_000_000, Some(NOW_MS), false).is_some());
    assert_eq!(implausible(NOW_MS, None, false), None);
}

#[test]
fn only_a_fleet_peer_is_held_to_the_saved_time() {
    let saved = Some(NOW_MS);
    assert!(implausible(NOW_MS - 2 * DAY, saved, true).is_some());
    assert!(implausible(NOW_MS + 401 * DAY, saved, true).is_some());
    // A peer within bounds, the RTC a little fast or the board off for months, is normal.
    assert_eq!(implausible(NOW_MS + 90 * DAY, saved, true), None);
    assert_eq!(implausible(NOW_MS - 3_600_000, saved, true), None);
    // Internet NTP or the managing computer is believed however long the board was off.
    assert_eq!(implausible(NOW_MS + 500 * DAY, saved, false), None);
    assert_eq!(implausible(NOW_MS - 2 * DAY, saved, false), None);
}

#[test]
fn a_wrong_peer_is_passed_over_for_the_next_source() {
    run(async {
        // The first peer is about a month behind the last synchronized time.
        let wrong = peer_source(-30 * 24 * 3600 * S).await;
        let right = peer_source(0).await;
        let saved = (sntp::unix_now_ns() / 1_000_000) as i64;
        let servers = vec![format!("peer://{wrong}"), right.to_string()];
        let (source, _) = best_sample(&servers, &OperatorTime::default(), None, Some(saved))
            .await
            .expect("the second source answers");
        assert_eq!(source, right.to_string());
    });
}

#[test]
fn the_synced_file_follows_the_sync_state() {
    let dir = temp("synced");
    let flag = dir.join("time-synced");
    let mut config = config(TimeMode::Discipline, Vec::new());
    config.synced_file = Some(flag.clone());
    let clock = Rc::new(FakeClock::default());
    let mut syncer = syncer(config, &clock, None, false);
    let sample = |offset_ns| sntp::Sample {
        offset_ns,
        delay_ns: 1_000_000,
        stratum: 2,
    };
    syncer.apply(Err("no server answered".to_owned()));
    assert!(!flag.exists(), "not synced yet");
    syncer.apply(Ok(("a".to_owned(), sample(1_000_000))));
    assert!(flag.exists(), "synced");
    assert_eq!(*clock.slews.borrow(), [1_000_000]);
    syncer.set_synced_file(false);
    assert!(!flag.exists(), "sync lost");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_clock_this_board_synchronized_is_served_one_stratum_down() {
    let served = ServedState::default();
    assert_eq!(served.served(true), ServedClock::Authority);
    let clock = Rc::new(FakeClock::default());
    let mut syncer = Syncer::new(
        config(TimeMode::Discipline, Vec::new()),
        clock,
        served.clone(),
        None,
        false,
    );
    syncer.apply(Ok((
        "a".to_owned(),
        sntp::Sample {
            offset_ns: 1_000,
            delay_ns: 1_000,
            stratum: 2,
        },
    )));
    assert_eq!(served.served(false), ServedClock::Synced(3));
    assert_eq!(served.served(true), ServedClock::Synced(3));
}
