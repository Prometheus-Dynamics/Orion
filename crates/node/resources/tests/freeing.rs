//! Who may free a lease: the session that wrote it, a caller whose principal is its holder, or
//! root. Anyone else is told who holds it.

use crate::common;

use common::*;

const DETECTOR: u32 = 30;
const RECORDER: u32 = 40;

fn lease(holder: &str, expires_ms: u64) -> Vec<u8> {
    let mut value = vec![0_u8; 128];
    let len = encode_fields(
        &LeaseRecord {
            resource: name("cam.front"),
            holder,
            expires_ms,
            label: "",
        },
        &mut value,
    )
    .expect("encode");
    value.truncate(len);
    value
}

fn caller(session: u32, principal: &str, root: bool) -> Caller<'_> {
    Caller {
        session,
        principal,
        root,
    }
}

/// The camera, held by `holder` with a lease `session` wrote.
fn held_by(session: u32, holder: &str, expires_ms: u64) -> Bench {
    let mut bench = Bench::with_camera();
    let value = lease(holder, expires_ms);
    bench
        .write(
            session,
            &entry(RecordClass::Lease, "cam.front", &value, false),
        )
        .expect("bind");
    bench
}

fn free(bench: &mut Bench, by: &Caller<'_>) -> Result<(), Error> {
    bench.write_as(by, &entry(RecordClass::Lease, "cam.front", &[], true))
}

fn holder_now(bench: &Bench) -> Option<String> {
    holder(&bench.store, "cam.front", NOW)
}

#[test]
fn another_session_may_not_free_a_lease_and_is_told_who_holds_it() {
    let mut bench = held_by(DETECTOR, "workload:detector", 0);
    assert_eq!(
        free(&mut bench, &caller(RECORDER, "local:1000", false)),
        Err(Error::Denied)
    );
    assert_eq!(
        bench.said.as_deref(),
        Some("cam.front is held by workload:detector: only its holder or root frees it")
    );
    assert_eq!(holder_now(&bench).as_deref(), Some("workload:detector"));

    // The session that wrote it frees it.
    free(&mut bench, &caller(DETECTOR, "local:1000", false)).expect("its own");
    assert_eq!(holder_now(&bench), None);
}

#[test]
fn the_holder_by_principal_and_root_may_free_it_from_any_session() {
    // Held in the name of user 1001, written from one session; freed from another of the same
    // user.
    let mut bench = held_by(DETECTOR, "local:1001", 0);
    assert_eq!(
        free(&mut bench, &caller(RECORDER, "local:1002", false)),
        Err(Error::Denied)
    );
    free(&mut bench, &caller(RECORDER, "local:1001", false)).expect("its holder");
    assert_eq!(holder_now(&bench), None);

    // Root frees anyone's.
    let mut bench = held_by(DETECTOR, "workload:detector", 0);
    free(&mut bench, &caller(RECORDER, "local:0", true)).expect("root");
    assert_eq!(holder_now(&bench), None);
}

#[test]
fn a_lease_that_ended_or_is_not_there_holds_nothing_and_anyone_clears_it() {
    let mut bench = held_by(DETECTOR, "workload:detector", NOW - 1);
    free(&mut bench, &caller(RECORDER, "local:1000", false)).expect("ended");
    let mut bench = Bench::with_camera();
    free(&mut bench, &caller(RECORDER, "local:1000", false)).expect("not there");
}

#[test]
fn a_holder_whose_session_is_gone_frees_after_writing_it_again() {
    let mut bench = held_by(DETECTOR, "workload:detector", 0);
    bench.resources.gone(&mut bench.store, DETECTOR, NOW);
    // The program, back on a new session, is not the writer until it writes the lease again.
    let back = caller(RECORDER, "local:1000", false);
    assert_eq!(free(&mut bench, &back), Err(Error::Denied));
    let value = lease("workload:detector", 0);
    bench
        .write_as(
            &back,
            &entry(RecordClass::Lease, "cam.front", &value, false),
        )
        .expect("renewed");
    free(&mut bench, &back).expect("its own again");
    assert_eq!(holder_now(&bench), None);
}
