//! A lease with no end, held by a program that has gone quiet, and another that wants it.

mod common;

use common::*;
use orion_resources::QUIET_MS;

const DETECTOR: u32 = 30;
const RECORDER: u32 = 40;

fn lease(holder: &str, expires_ms: u64) -> Vec<u8> {
    let mut value = vec![0_u8; 128];
    let len = encode_fields(
        &LeaseRecord {
            resource: name("cam.front"),
            holder,
            expires_ms,
        },
        &mut value,
    )
    .expect("encode");
    value.truncate(len);
    value
}

/// The camera, held by the detector's session with no end.
fn held() -> Bench {
    let mut bench = Bench::with_camera();
    let value = lease("workload:detector", 0);
    bench
        .write(
            DETECTOR,
            &entry(RecordClass::Lease, "cam.front", &value, false),
        )
        .expect("bind");
    bench
}

fn claim(bench: &mut Bench, session: u32, holder: &str) -> Result<(), Error> {
    let value = lease(holder, 0);
    bench.write(
        session,
        &entry(RecordClass::Lease, "cam.front", &value, false),
    )
}

fn holder_now(bench: &Bench) -> Option<String> {
    holder(&bench.store, "cam.front", NOW)
}

/// The detector went quiet at `at`, and it is now `now`.
fn quiet_since(bench: &mut Bench, at: u64, now: u64) {
    bench.now = now;
    bench.resources.quiet(DETECTOR, now - at, now);
}

#[test]
fn a_quiet_holder_loses_its_lease_to_a_claimant_and_is_told_by_whom() {
    let mut bench = held();
    quiet_since(&mut bench, 1_000, 1_000 + QUIET_MS);
    claim(&mut bench, RECORDER, "workload:recorder").expect("taken from a quiet holder");
    assert_eq!(holder_now(&bench).as_deref(), Some("workload:recorder"));

    // The detector speaks again: whatever it asks under the lease is refused, with who took it.
    bench.resources.quiet(DETECTOR, 0, bench.now + 5);
    let words = "lease lost while quiet: taken by workload:recorder";
    assert_eq!(
        claim(&mut bench, DETECTOR, "workload:detector"),
        Err(Error::LeaseLost)
    );
    assert_eq!(bench.said.as_deref(), Some(words));
    // Freeing the lease it lost would free the new holder's.
    assert_eq!(
        bench.write(DETECTOR, &entry(RecordClass::Lease, "cam.front", &[], true)),
        Err(Error::LeaseLost)
    );
    assert_eq!(holder_now(&bench).as_deref(), Some("workload:recorder"));
    // Running the resource's actions, and not anything else.
    assert_eq!(
        bench
            .resources
            .lost_action(&bench.store, DETECTOR, "cam.front.focus", NOW)
            .as_deref(),
        Some(words)
    );
    assert_eq!(
        bench
            .resources
            .lost_action(&bench.store, DETECTOR, "fan.speed", NOW),
        None
    );
    assert_eq!(
        bench
            .resources
            .lost_action(&bench.store, RECORDER, "cam.front.focus", NOW),
        None
    );

    // Once the new holder lets go, the old one may claim it like anyone.
    bench
        .write(RECORDER, &entry(RecordClass::Lease, "cam.front", &[], true))
        .expect("release");
    claim(&mut bench, DETECTOR, "workload:detector").expect("free again");
}

#[test]
fn a_holder_that_answers_or_has_not_been_quiet_long_keeps_its_lease_and_the_claimant_is_told_who() {
    let mut bench = held();
    bench.now = 50_000;
    assert_eq!(
        claim(&mut bench, RECORDER, "workload:recorder"),
        Err(Error::Busy)
    );
    let said = bench.said.take().expect("words");
    assert!(
        said.starts_with(&format!(
            "cam.front is held by workload:detector since {NOW} ms"
        )),
        "{said}"
    );
    assert!(said.ends_with("its holder is answering"), "{said}");

    // Quiet, but not for long enough.
    quiet_since(&mut bench, 1_000, 1_000 + QUIET_MS - 1);
    assert_eq!(
        claim(&mut bench, RECORDER, "workload:recorder"),
        Err(Error::Busy)
    );
    let said = bench.said.take().expect("words");
    assert!(
        said.contains(&format!("quiet {} ms", QUIET_MS - 1))
            && said.ends_with(&format!("only after {QUIET_MS} ms quiet")),
        "{said}"
    );

    // Heard again before anyone claimed: quiet starts over.
    quiet_since(&mut bench, 1_000, 1_000 + QUIET_MS * 5);
    bench.resources.quiet(DETECTOR, 0, bench.now);
    assert_eq!(
        claim(&mut bench, RECORDER, "workload:recorder"),
        Err(Error::Busy)
    );
    assert_eq!(holder_now(&bench).as_deref(), Some("workload:detector"));
}

#[test]
fn a_run_of_the_resources_action_the_holder_asked_for_is_not_quiet() {
    let mut bench = held();
    quiet_since(&mut bench, 1_000, 1_000 + QUIET_MS * 3);
    bench.running.push((DETECTOR, "cam.front.focus".to_owned()));
    assert_eq!(
        claim(&mut bench, RECORDER, "workload:recorder"),
        Err(Error::Busy)
    );
    let said = bench.said.take().expect("words");
    assert!(said.ends_with("a run of its action is under way"), "{said}");

    // A run of something else, or one another session asked for, does not count.
    bench.running = vec![
        (DETECTOR, "fan.speed".to_owned()),
        (RECORDER, "cam.front.focus".to_owned()),
    ];
    claim(&mut bench, RECORDER, "workload:recorder").expect("taken");
}

#[test]
fn without_a_claimant_nothing_is_lost_however_long_the_holder_is_quiet() {
    let mut bench = held();
    quiet_since(&mut bench, 1_000, 1_000 + QUIET_MS * 1_000);
    assert_eq!(holder_now(&bench).as_deref(), Some("workload:detector"));
    // It still renews what it holds.
    claim(&mut bench, DETECTOR, "workload:detector").expect("its own");
    assert_eq!(
        bench
            .resources
            .lost_action(&bench.store, DETECTOR, "cam.front.focus", NOW),
        None
    );
}

#[test]
fn a_lease_that_ends_on_its_own_is_not_contested() {
    let mut bench = Bench::with_camera();
    let value = lease("workload:detector", NOW + 60_000);
    bench
        .write(
            DETECTOR,
            &entry(RecordClass::Lease, "cam.front", &value, false),
        )
        .expect("bind");
    quiet_since(&mut bench, 1_000, 1_000 + QUIET_MS * 3);
    assert_eq!(
        claim(&mut bench, RECORDER, "workload:recorder"),
        Err(Error::Busy)
    );
    let said = bench.said.take().expect("words");
    assert!(said.ends_with("its lease ends on its own"), "{said}");
}

#[test]
fn a_session_that_is_gone_or_forgotten_holds_nothing_to_contest_and_has_lost_nothing() {
    let mut bench = held();
    quiet_since(&mut bench, 1_000, 1_000 + QUIET_MS);
    claim(&mut bench, RECORDER, "workload:recorder").expect("taken");
    // The old holder's session ends: what it lost is forgotten with it.
    bench.resources.gone(&mut bench.store, DETECTOR, NOW);
    assert_eq!(
        bench
            .resources
            .lost_action(&bench.store, DETECTOR, "cam.front.focus", NOW),
        None
    );

    // The new holder's session is forgotten by a restart: its lease stands, and is no longer
    // any session's to be quiet in.
    bench.resources.forget_sessions();
    bench.resources.quiet(RECORDER, QUIET_MS * 2, bench.now);
    assert_eq!(
        claim(&mut bench, DETECTOR, "workload:detector"),
        Err(Error::Busy)
    );
    let said = bench.said.take().expect("words");
    assert!(said.ends_with("its holder is not a session here"), "{said}");
}
