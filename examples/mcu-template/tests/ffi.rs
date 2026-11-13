//! The C API over a UART, against a host: the calls a C firmware makes, in its order.

#![cfg(all(feature = "ffi-uart", not(feature = "ffi-can")))]

use std::ffi::CStr;

use orion_mcu_template::ffi::*;
use orion_msg_actions::{ActionResult, Cancel, Claim, Outcome, Progress, Register, Target};
use orion_msg_topics::Sample;
use orion_wire::{Bytes, kind};

mod common;
use common::{Peer, StreamReader, stream};

const NAME: &CStr = c"c-board";

/// One pass of a C main loop's I/O: what the device wrote goes to the host, and what the
/// host answers comes back.
fn exchange(peer: &mut Peer, reader: &mut StreamReader) {
    let mut out = [0_u8; 64];
    loop {
        // SAFETY: `out` is valid for its length.
        let n = unsafe { orion_tx(out.as_mut_ptr(), out.len()) };
        if n == 0 {
            break;
        }
        for sent in reader.read(&out[..n]) {
            for frame in peer.answer(sent) {
                feed(&frame);
            }
        }
    }
}

/// Main-loop passes up to `until`.
fn run(peer: &mut Peer, reader: &mut StreamReader, now: &mut u32, until: u32, events: &mut u32) {
    while *now < until {
        *now += 1;
        *events |= orion_poll(*now);
        exchange(peer, reader);
    }
}

fn feed(frame: &[u8]) {
    let bytes = stream(frame);
    // SAFETY: `bytes` is valid for its length.
    unsafe { orion_rx(bytes.as_ptr(), bytes.len()) };
}

#[test]
fn a_c_firmware_declares_publishes_and_answers_runs() {
    assert_eq!(orion_api_version(), ORION_MCU_API_VERSION);
    assert_eq!(orion_api_version() >> 16, 1);
    assert_eq!(orion_interface_revision(), 1);
    // Nothing before orion_init.
    assert_eq!(orion_poll(0), 0);
    assert_eq!(orion_publish(0, 0, 1.0), ORION_ERR_STATE);

    // SAFETY: the strings are literals, NUL-terminated and never changed.
    unsafe {
        assert!(!orion_init(c"".as_ptr()), "an empty name");
        assert!(orion_init(NAME.as_ptr()));
        assert_eq!(
            orion_topic(c"c-board.yaw".as_ptr(), 9, std::ptr::null()),
            ORION_ERR_ARG
        );
        assert_eq!(
            orion_topic(c"c-board.yaw".as_ptr(), ORION_FIELD_FLOAT, c"deg".as_ptr()),
            0
        );
        assert_eq!(
            orion_topic(c"c-board.ok".as_ptr(), ORION_FIELD_BOOL, std::ptr::null()),
            1
        );
        assert_eq!(
            orion_action(c"zero".as_ptr(), c"Sets yaw to zero".as_ptr(), 0),
            ORION_ERR_ARG
        );
        assert_eq!(
            orion_action(
                c"zero".as_ptr(),
                c"Sets yaw to zero".as_ptr(),
                ORION_ADMITS_LOCAL | ORION_ADMITS_OPERATOR,
            ),
            0
        );
    }

    let (mut peer, mut reader) = (Peer::default(), StreamReader::default());
    let mut events = 0;
    let mut now = 0;
    run(&mut peer, &mut reader, &mut now, 100, &mut events);
    // The tables are fixed once polled.
    // SAFETY: as above.
    unsafe {
        assert_eq!(
            orion_topic(c"late".as_ptr(), ORION_FIELD_INT, std::ptr::null()),
            ORION_ERR_STATE
        );
    }
    assert_eq!(peer.name, "c-board");
    assert_eq!(
        peer.topics,
        [
            ("c-board.yaw".to_owned(), 101),
            ("c-board.ok".to_owned(), 102)
        ]
    );
    assert_eq!(peer.actions, 1);
    assert_eq!(orion_state(), 2);
    assert_ne!(events & ORION_EVENT_CONNECTED, 0);
    assert_ne!(events & ORION_EVENT_DEMAND, 0);
    assert_eq!(orion_subscribers(0), 1);

    // Publishes.
    assert_eq!(orion_publish(0, 7_000, 12.5), 0);
    assert_eq!(
        orion_publish(1, 7_000, 1.0),
        ORION_ERR_BUSY,
        "one frame at a time"
    );
    exchange(&mut peer, &mut reader);
    assert_eq!(orion_publish_bool(1, 8_000, true), 0);
    exchange(&mut peer, &mut reader);
    assert_eq!(orion_publish(5, 0, 0.0), ORION_ERR_CLOSED);
    let samples = peer.take(kind::TOPIC_SAMPLE);
    let first: Sample = samples[0].body();
    assert_eq!(
        (first.handle, first.payload.0),
        (101, &12.5_f64.to_le_bytes()[..])
    );
    let second: Sample = samples[1].body();
    assert_eq!((second.handle, second.payload.0), (102, &[1][..]));

    // A run, with its arguments, progress and result.
    let claim = peer.raw(&Claim {
        call: 41,
        name: "zero".parse().expect("name"),
        target: Target::Resource("c-board".parse().expect("name")),
        args: Bytes(b"now"),
        deadline_ms: 0,
        caller: "",
    });
    feed(&claim);
    // A second while the first waits is turned away at once.
    let second = peer.raw(&Claim {
        call: 42,
        name: "zero".parse().expect("name"),
        target: Target::Resource("c-board".parse().expect("name")),
        args: Bytes(&[]),
        deadline_ms: 0,
        caller: "",
    });
    feed(&second);
    exchange(&mut peer, &mut reader);
    assert_ne!(orion_poll(now) & ORION_EVENT_RUN, 0);
    let (mut call, mut len) = (0_u64, 0_usize);
    let mut args = [0_u8; 2];
    // SAFETY: every pointer is valid for what it is given.
    let action = unsafe { orion_run(&mut call, args.as_mut_ptr(), args.len(), &mut len) };
    assert_eq!(
        (action, call, len, &args),
        (0, 41, 3, b"no"),
        "cut to the room given"
    );
    // SAFETY: as above.
    assert_eq!(
        unsafe { orion_run(&mut call, args.as_mut_ptr(), 2, &mut len) },
        -1
    );
    // SAFETY: a literal.
    assert_eq!(unsafe { orion_progress(41, 50, c"halfway".as_ptr()) }, 0);
    exchange(&mut peer, &mut reader);
    // SAFETY: the output is valid for its length; the detail is null.
    assert_eq!(
        unsafe {
            orion_finish(
                41,
                ORION_OUTCOME_SUCCEEDED,
                b"ok".as_ptr(),
                2,
                std::ptr::null(),
            )
        },
        0
    );
    exchange(&mut peer, &mut reader);
    let progress = peer.take(kind::ACTION_PROGRESS);
    let progress: Progress = progress[0].body();
    assert_eq!(
        (progress.call, progress.percent, progress.message),
        (41, 50, "halfway")
    );
    let results = peer.take(kind::ACTION_RESULT);
    let results: Vec<(u64, Outcome, Vec<u8>)> = results
        .iter()
        .map(|sent| {
            let result: ActionResult = sent.body();
            (result.call, result.outcome, result.output.0.to_vec())
        })
        .collect();
    assert_eq!(
        results,
        [
            (42, Outcome::Rejected, Vec::new()),
            (41, Outcome::Succeeded, b"ok".to_vec())
        ]
    );

    // A cancel names the run.
    let cancel = peer.raw(&Cancel { call: 77 });
    feed(&cancel);
    assert_ne!(orion_poll(now) & ORION_EVENT_CANCEL, 0);
    assert_eq!(orion_cancelled(), 77);
    assert_eq!(orion_cancelled(), 0);

    // Starting over says hello again, with the new tables.
    // SAFETY: literals.
    unsafe {
        assert!(orion_init(NAME.as_ptr()));
        assert_eq!(
            orion_action(c"reset".as_ptr(), std::ptr::null(), ORION_ADMITS_LOCAL),
            0
        );
    }
    let mut events = 0;
    let until = now + 100;
    run(&mut peer, &mut reader, &mut now, until, &mut events);
    assert_ne!(events & ORION_EVENT_CONNECTED, 0);
    let registers: Vec<String> = peer
        .take(kind::ACTION_REGISTER)
        .iter()
        .map(|sent| {
            let register: Register = sent.body();
            register.name.as_str().to_owned()
        })
        .collect();
    assert!(registers.is_empty(), "answered, not kept: {registers:?}");
    assert_eq!(peer.actions, 2);
}
