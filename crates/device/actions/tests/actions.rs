//! Device actions against a host played by the test, which reads and writes every body with
//! the general codec.

use orion_device_actions::{
    ActionSpec, Actions, Admits, Heard, Outcome, Policy, RETRY_MS, Request, SendError, Target,
};
use orion_device_core::Link;
use orion_device_host::{Drive, Rig, Sent};
use orion_msg_actions::{ActionResult, Cancel, Claim, Progress, Register, Registered};
use orion_msg_session::Refusal;
use orion_wire::{Bytes, FrameView};

#[global_allocator]
static GLOBAL: orion_measure::CountingAlloc = orion_measure::CountingAlloc::system();

const TX: usize = 256;
type ActionRig = Rig<Actions<2>, 256, TX>;

static SPECS: [ActionSpec; 2] = [
    ActionSpec::on_resource("zero", "imu", "Sets yaw to zero"),
    ActionSpec {
        name: "reboot",
        target: Target::Node("board"),
        policy: Policy::new(Admits::LOCAL),
        description: "",
    },
];

fn poll(actions: &mut Actions<2>, link: &mut Link<TX>, now: u32) {
    actions.poll(link, now);
}

fn handle(actions: &mut Actions<2>, _: &mut Link<TX>, frame: &FrameView<'_>) -> String {
    format!("{:?}", actions.handle(frame))
}

fn rig() -> ActionRig {
    Rig::new(Actions::new(&SPECS), Drive { poll, handle })
}

fn registered(rig: &mut ActionRig, register: &Sent) {
    rig.send(&Registered {
        request_seq: register.seq(),
    });
}

/// A device whose two actions are registered.
fn ready() -> ActionRig {
    let mut rig = rig();
    let sent = rig.drain();
    assert_eq!(sent.len(), 2);
    registered(&mut rig, &sent[0]);
    registered(&mut rig, &sent[1]);
    rig
}

fn claim(call: u64, name: &str, target: orion_msg_actions::Target, args: &[u8]) -> Vec<u8> {
    let claim = Claim {
        call,
        name: name.parse().expect("a name"),
        target,
        args: Bytes(args),
        deadline_ms: 2_000,
        caller: "operator:9f3c",
    };
    orion_device_host::Host::default().frame(&claim)
}

fn on_imu() -> orion_msg_actions::Target {
    orion_msg_actions::Target::Resource("imu".parse().expect("a name"))
}

#[test]
fn a_session_registers_every_action_as_the_host_reads_it() {
    let mut rig = rig();
    let sent = rig.drain();
    assert_eq!(sent.len(), 2);
    let zero: Register<'_> = sent[0].body();
    assert_eq!(zero.name.as_str(), "zero");
    assert_eq!(zero.target, on_imu());
    assert_eq!(zero.policy, Policy::new(Admits::OPERATOR));
    assert_eq!(zero.description, "Sets yaw to zero");
    let reboot: Register<'_> = sent[1].body();
    assert_eq!(
        reboot.target,
        orion_msg_actions::Target::Node("board".parse().expect("a name"))
    );
    assert_eq!(reboot.policy, Policy::new(Admits::LOCAL));

    registered(&mut rig, &sent[0]);
    assert_eq!(rig.heard, ["Registered { action: 0 }"]);
    assert_eq!(rig.layer.state(0), Some(Request::Answered));
    assert!(matches!(rig.layer.state(1), Some(Request::Asked { .. })));
}

#[test]
fn a_run_is_handed_over_and_its_result_reaches_the_host() {
    let mut rig = ready();
    rig.feed(&claim(41, "zero", on_imu(), b"now"));
    let want = Heard::Run {
        action: 0,
        call: 41,
        args: b"now",
        deadline_ms: 2_000,
    };
    assert_eq!(rig.last_heard(), Some(format!("{want:?}").as_str()));

    let link = &mut rig.device.link;
    rig.layer.progress(link, 41, 50, "halfway").expect("taken");
    let sent = rig.drain();
    let progress: Progress<'_> = sent[0].body();
    assert_eq!(
        (progress.call, progress.percent, progress.message),
        (41, 50, "halfway")
    );
    assert_eq!(progress.data, Bytes(b""));

    let link = &mut rig.device.link;
    rig.layer
        .finish(link, 41, Outcome::Succeeded, &[7, 8], "")
        .expect("taken");
    let sent = rig.drain();
    let result: ActionResult<'_> = sent[0].body();
    assert_eq!(result.call, 41);
    assert_eq!(result.outcome, Outcome::Succeeded);
    assert_eq!(result.output, Bytes(&[7, 8]));

    let link = &mut rig.device.link;
    rig.layer
        .finish(link, 42, Outcome::Failed, &[], "no sensor")
        .expect("taken");
    let sent = rig.drain();
    let result: ActionResult<'_> = sent[0].body();
    assert_eq!(
        (result.outcome, result.detail),
        (Outcome::Failed, "no sensor")
    );
}

#[test]
fn a_run_of_something_else_is_not_this_devices() {
    let mut rig = ready();
    // Another action, the same action on another target, and a target of the other kind
    // with the same name.
    rig.feed(&claim(1, "spin", on_imu(), b""));
    rig.feed(&claim(
        2,
        "zero",
        orion_msg_actions::Target::Resource("gyro".parse().expect("a name")),
        b"",
    ));
    rig.feed(&claim(
        3,
        "zero",
        orion_msg_actions::Target::Node("imu".parse().expect("a name")),
        b"",
    ));
    assert_eq!(rig.heard[2..], ["Other", "Other", "Other"]);
}

#[test]
fn a_run_before_registration_is_not_taken() {
    let mut rig = rig();
    rig.drain();
    rig.feed(&claim(1, "zero", on_imu(), b""));
    assert_eq!(rig.last_heard(), Some("Other"));
}

#[test]
fn a_cancel_is_passed_on() {
    let mut rig = ready();
    rig.send(&Cancel { call: 9 });
    assert_eq!(rig.last_heard(), Some("Cancel { call: 9 }"));
}

#[test]
fn a_result_the_link_has_no_room_for_is_sent_again_later() {
    let mut rig = ready();
    let link = &mut rig.device.link;
    rig.layer.progress(link, 1, 255, "").expect("taken");
    assert_eq!(
        rig.layer.finish(link, 1, Outcome::Succeeded, &[], ""),
        Err(SendError::Busy)
    );
    rig.drain();
    let link = &mut rig.device.link;
    rig.layer
        .finish(link, 1, Outcome::Succeeded, &[], "")
        .expect("taken");
    assert_eq!(rig.drain().len(), 1);
}

#[test]
fn registration_is_said_again_until_answered_and_refusal_is_kept() {
    let mut rig = rig();
    let first = rig.drain();
    rig.send(&Refusal {
        request_seq: first[1].seq(),
        code: 3,
        detail: "",
    });
    assert_eq!(rig.last_heard(), Some("Refused { action: 1, code: 3 }"));
    assert!(rig.run(RETRY_MS - 1).is_empty());
    let again = rig.run(1);
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].body::<Register<'_>>().name.as_str(), "zero");
    registered(&mut rig, &again[0]);
    assert!(rig.run(5 * RETRY_MS).is_empty());
    // A new session registers both again.
    rig.quiet = true;
    rig.run(400);
    rig.quiet = false;
    rig.connect();
    assert_eq!(rig.drain().len(), 2);
}

#[test]
fn answering_runs_allocates_nothing() {
    let mut rig = ready();
    let claim = claim(5, "zero", on_imu(), b"x");
    let before = orion_measure::thread_counts().allocs;
    for _ in 0..100 {
        let layer = &mut rig.layer;
        let mut call = None;
        rig.device.receive(&claim, |_, frame| {
            if let Heard::Run { call: c, .. } = layer.handle(frame) {
                call = Some(c);
            }
        });
        let call = call.expect("a run");
        layer
            .finish(&mut rig.device.link, call, Outcome::Succeeded, b"ok", "")
            .expect("taken");
        let mut out = [0_u8; 64];
        while rig.device.transmit(&mut out) != 0 {}
    }
    assert_eq!(orion_measure::thread_counts().allocs, before);
}
