use super::*;
use std::sync::Mutex;

const S: i64 = 1_000_000_000;

/// Records what the sync asks of the clock instead of changing it.
#[derive(Default)]
struct FakeClock {
    steps: Mutex<Vec<i64>>,
    slews: Mutex<Vec<i64>>,
}

impl Clock for FakeClock {
    fn step(&self, offset_ns: i64) -> std::io::Result<()> {
        self.steps.lock().expect("steps").push(offset_ns);
        Ok(())
    }

    fn slew(&self, offset_ns: i64, _max_error_ns: i64) -> std::io::Result<()> {
        self.slews.lock().expect("slews").push(offset_ns);
        Ok(())
    }
}

fn config(mode: TimeSyncMode, servers: Vec<String>) -> TimeSyncConfig {
    TimeSyncConfig {
        mode,
        servers,
        serve: None,
        serve_authority: false,
        min_poll: Duration::from_secs(64),
        max_poll: Duration::from_secs(1024),
    }
}

/// An Orion node serving a clock `ahead_ns` off, as the network's reference.
async fn peer_source(ahead_ns: i64) -> SocketAddr {
    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    let addr = socket.local_addr().expect("addr");
    tokio::spawn(sntp::serve(
        socket,
        || ServedClock::Authority,
        move || sntp::NtpTime::from_unix_ns(sntp::unix_now_ns() + i128::from(ahead_ns)),
    ));
    addr
}

#[tokio::test]
async fn a_board_behind_its_peer_steps_after_two_agreeing_samples() {
    // The test board: about 1.9 h behind the controller, which serves its clock.
    let behind = 6_840 * S;
    let source = peer_source(behind).await.to_string();
    let clock = Arc::new(FakeClock::default());
    let dir = crate::test_temp::unique("time-sync", "");
    std::fs::create_dir_all(&dir).expect("dir");
    let saved = dir.join(SAVED_TIME_FILE);
    let mut syncer = Syncer::new(
        config(TimeSyncMode::Discipline, vec![source.clone()]),
        clock.clone(),
        ServedState::default(),
        Some(saved.clone()),
        false,
    );
    syncer.apply(best_sample(&syncer.config.servers).await);
    assert_eq!(syncer.status.last_action, "waiting for a second sample");
    assert!(clock.steps.lock().expect("steps").is_empty());
    assert_eq!(syncer.poll, FIRST_POLL);
    syncer.apply(best_sample(&syncer.config.servers).await);
    let steps = clock.steps.lock().expect("steps").clone();
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
}

#[tokio::test]
async fn report_mode_measures_and_never_touches_the_clock() {
    let source = peer_source(3 * S).await.to_string();
    let clock = Arc::new(FakeClock::default());
    let mut syncer = Syncer::new(
        config(TimeSyncMode::Report, vec![source]),
        clock.clone(),
        ServedState::default(),
        None,
        false,
    );
    for _ in 0..2 {
        syncer.apply(best_sample(&syncer.config.servers).await);
    }
    assert_eq!(syncer.status.last_action, "would step");
    assert!(!syncer.status.synced);
    assert!(clock.steps.lock().expect("steps").is_empty());
    let offset = syncer.status.offset_ns.expect("offset");
    assert!((offset - 3 * S).abs() < S / 10, "{offset}");
}

#[tokio::test]
async fn discipline_only_reports_while_timesyncd_runs() {
    let syncer = Syncer::new(
        config(TimeSyncMode::Discipline, vec!["127.0.0.1:9".to_owned()]),
        Arc::new(FakeClock::default()),
        ServedState::default(),
        None,
        true,
    );
    assert!(!syncer.disciplining);
    assert_eq!(syncer.status.mode, "report");
}

#[test]
fn a_clock_behind_the_saved_time_moves_forward_to_it() {
    let dir = crate::test_temp::unique("time-sync-saved", "");
    std::fs::create_dir_all(&dir).expect("dir");
    let saved = dir.join(SAVED_TIME_FILE);
    let ahead_ms = 3_600_000i64;
    let now_ms = (sntp::unix_now_ns() / 1_000_000) as i64;
    std::fs::write(&saved, (now_ms + ahead_ms).to_string()).expect("write");
    let clock = Arc::new(FakeClock::default());
    let mut syncer = Syncer::new(
        config(TimeSyncMode::Discipline, vec!["127.0.0.1:9".to_owned()]),
        clock.clone(),
        ServedState::default(),
        Some(saved.clone()),
        false,
    );
    syncer.restore_saved_time();
    let steps = clock.steps.lock().expect("steps").clone();
    assert_eq!(steps.len(), 1);
    assert!((steps[0] / 1_000_000 - ahead_ms).abs() < 5_000, "{steps:?}");
    // A clock ahead of the saved time is left alone.
    std::fs::write(&saved, (now_ms - ahead_ms).to_string()).expect("write");
    syncer.restore_saved_time();
    assert_eq!(clock.steps.lock().expect("steps").len(), 1);
}

#[tokio::test]
async fn an_unanswered_server_is_an_error_and_the_poll_backs_off() {
    // Nothing listens on the discard port of loopback.
    let mut syncer = Syncer::new(
        config(TimeSyncMode::Report, vec!["127.0.0.1:9".to_owned()]),
        Arc::new(FakeClock::default()),
        ServedState::default(),
        None,
        false,
    );
    syncer.apply(best_sample(&syncer.config.servers).await);
    assert!(syncer.status.error.is_some());
    assert!(syncer.poll >= FIRST_POLL && syncer.poll <= Duration::from_secs(64));
}

#[tokio::test]
async fn a_tcp_source_answers_like_the_udp_one() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(sntp::serve_tcp(
        listener,
        || ServedClock::Synced(2),
        move || sntp::NtpTime::from_unix_ns(sntp::unix_now_ns() + i128::from(5 * S)),
    ));
    let (source, sample) = best_sample(&[format!("tcp://{addr}")])
        .await
        .expect("sample");
    assert_eq!(source, format!("tcp://{addr}"));
    assert!((sample.offset_ns - 5 * S).abs() < S / 10, "{sample:?}");
    assert_eq!(sample.stratum, 2);
}
