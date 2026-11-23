//! A write that says which version it is based on: made if the record is still that one,
//! refused as stale if another wrote it since, with nothing written. Through the real comm
//! core, node logic and client.

use std::time::{Duration, Instant};

use orion::records::{RecordClass, RecordEntry};
use orion::types::HlcTimestamp;
use orion::workloads::{RestartPolicy, Workload};
use orion::{ClientError, Orion};
use orion_test_board::TestBoard;

fn pipeline<'a>(spec: &'a [u8]) -> Workload<'a> {
    Workload {
        name: "pipeline.front",
        kind: "graph",
        spec,
        node: None,
        restart: RestartPolicy::Never,
    }
}

/// What the last write of the workload was refused with. No executor runs it here, so the
/// board says nothing of where it is, and a write that was made is seen in its record.
fn refused(orion: &Orion) -> ClientError {
    orion
        .workloads()
        .wait("pipeline.front", Duration::from_secs(10), |_| true)
        .expect_err("refused")
}

/// The stamp of the workload's record, once it holds `spec`; what a write was based on is of
/// that write, and not kept in the record.
fn written(orion: &Orion, spec: &[u8]) -> HlcTimestamp {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let records = orion
            .records()
            .get(RecordClass::Workload, "pipeline.front")
            .expect("get");
        if let Some(record) = records.first() {
            let put: orion_msg_workloads::WorkloadPut<'_> =
                orion_wire::Decode::decode(&record.value).expect("a workload");
            assert_eq!(put.based_on, None);
            if put.spec.0 == spec {
                return record.stamp;
            }
        }
        assert!(Instant::now() < deadline, "never written: {records:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The spec the board holds for the workload, or none.
fn held(orion: &Orion) -> Option<Vec<u8>> {
    let records = orion
        .records()
        .get(RecordClass::Workload, "pipeline.front")
        .expect("get");
    let record = records.first()?;
    let put: orion_msg_workloads::WorkloadPut<'_> =
        orion_wire::Decode::decode(&record.value).expect("a workload");
    Some(put.spec.0.to_vec())
}

#[test]
fn two_editors_of_one_workload_cannot_overwrite_each_other_unseen() {
    let board = TestBoard::start("based-workload");
    // Two sessions, as two browser tabs on one pipeline.
    let (first, second) = (
        board.connect("first").expect("connect"),
        board.connect("second").expect("connect"),
    );

    // Both make it, saying there is none yet: one does, and the other is told it is there.
    first
        .workloads()
        .put_based_on(&pipeline(b"one"), &[], HlcTimestamp::ZERO)
        .expect("sent");
    let made = written(&first, b"one");
    assert_ne!(made, HlcTimestamp::ZERO);
    second
        .workloads()
        .put_based_on(&pipeline(b"two"), &[], HlcTimestamp::ZERO)
        .expect("sent");
    let said = refused(&second);
    assert!(said.is_stale(), "{said}");
    assert!(said.to_string().contains("is there already"), "{said}");
    assert_eq!(held(&first).as_deref(), Some(&b"one"[..]));

    // Both read it and edit. The first to write does; the second is told, and writes nothing.
    first
        .workloads()
        .put_based_on(&pipeline(b"first's edit"), &[], made)
        .expect("sent");
    let edited = written(&first, b"first's edit");
    assert!(edited > made);
    second
        .workloads()
        .put_based_on(&pipeline(b"second's edit"), &[], made)
        .expect("sent");
    let said = refused(&second);
    assert!(said.is_stale() && !said.is_busy() && !said.is_held());
    assert!(
        said.to_string().contains("pipeline.front was written at "),
        "{said}"
    );
    assert_eq!(held(&first).as_deref(), Some(&b"first's edit"[..]));

    // The second reads it again and decides to write over it: now it is based on what is there.
    second
        .workloads()
        .put_based_on(&pipeline(b"second's edit"), &[], edited)
        .expect("sent");
    let again = written(&second, b"second's edit");

    // A remove based on what it was before is refused too, and one based on what it is, made.
    first
        .workloads()
        .remove_based_on("pipeline.front", false, edited)
        .expect("sent");
    assert!(refused(&first).is_stale());
    assert_eq!(held(&first).as_deref(), Some(&b"second's edit"[..]));
    first
        .workloads()
        .remove_based_on("pipeline.front", false, again)
        .expect("sent");
    let deadline = Instant::now() + Duration::from_secs(10);
    while held(&first).is_some() {
        assert!(Instant::now() < deadline, "never removed");
        std::thread::sleep(Duration::from_millis(10));
    }

    // A write that says nothing of what it is based on is made whatever is there, as ever.
    second
        .workloads()
        .put(&pipeline(b"whatever"))
        .expect("sent");
    written(&second, b"whatever");
}

#[test]
fn a_record_written_with_the_stamp_it_was_read_with_is_refused_if_it_was_written_since() {
    let board = TestBoard::start("based-record");
    let orion = board.connect("writer").expect("connect");
    let value = |label: &str| {
        let record = orion_msg_records::ResourceRecord {
            name: "lamp".parse().expect("a name"),
            kind: label,
            node: "board".parse().expect("a name"),
            state: orion_msg_records::ResourceState::Ready,
            attributes: orion_wire::Bytes(&[]),
        };
        let mut value = vec![0_u8; 256];
        let len = orion_wire::body::encode_fields(&record, &mut value).expect("encode");
        value.truncate(len);
        value
    };
    let put = |value: &[u8], stamp| {
        orion.records().put(&[RecordEntry {
            class: RecordClass::Resource,
            key: "lamp",
            stamp,
            deleted: false,
            value: orion::records::Bytes(value),
        }])
    };
    let kind_now = || {
        let records = orion
            .records()
            .get(RecordClass::Resource, "lamp")
            .expect("get");
        let record: orion_msg_records::ResourceRecord<'_> =
            orion_wire::Decode::decode(&records[0].value).expect("a resource");
        record.kind.to_owned()
    };

    // Written with no stamp: whatever is there. The answer has the stamp it was given.
    let first = put(&value("one"), HlcTimestamp::ZERO).expect("written")[0].stamp;
    // Based on that stamp: written, and the answer has the new one.
    let second = put(&value("two"), first).expect("written")[0].stamp;
    assert!(second > first);
    // Based on the old stamp again: refused, in words, and nothing written.
    let refused = put(&value("three"), first).expect_err("written since");
    assert!(refused.is_stale(), "{refused}");
    assert!(
        refused.to_string().contains("lamp was written at "),
        "{refused}"
    );
    assert_eq!(kind_now(), "two");
    // Based on what is there now: written.
    put(&value("three"), second).expect("written");
    assert_eq!(kind_now(), "three");
}
