//! Samples of the actions' kinds.

use orion_check::{Admits, Policy};
use orion_msg_actions::{
    ActionResult, Attach, CallerGone, Cancel, CancelReason, Claim, Outcome, Progress, Register,
    Registered, Run, Started, Target,
};
use orion_wire::{Bytes, body::encode_fields};

use super::{Sample, action, decode_like, kind_of, node, target};

pub(super) fn samples(out: &mut Vec<Sample>) {
    let policy = Policy::new(Admits::LOCAL.and(Admits::OPERATOR));
    sample!(
        out,
        Register {
            name: action(),
            target: Target::Node(node("left")),
            policy,
            description: "restart it",
            keep_ms: 10_000
        }
    );
    sample!(
        out,
        Run {
            call: 7,
            name: action(),
            target: target(),
            args: Bytes(b"a"),
            deadline_ms: 5000,
            detached: true,
            on_behalf: 4
        }
    );
    sample!(
        out,
        Claim {
            call: 7,
            name: action(),
            target: target(),
            args: Bytes(b"a"),
            deadline_ms: 5000,
            caller: "local:1000",
            session: 3,
            detached: true,
            kept: true
        }
    );
    sample!(
        out,
        Progress {
            call: 7,
            percent: 40,
            message: "halfway",
            data: Bytes(b"d"),
            instance: 0x55,
            run: 6,
        }
    );
    sample!(
        out,
        ActionResult {
            call: 7,
            outcome: Outcome::Succeeded,
            output: Bytes(b"o"),
            detail: "",
            code: 11,
            instance: 0x55,
            run: 6,
            action: "pump.start",
        }
    );
    sample!(
        out,
        Started {
            call: 7,
            instance: 0x55,
            run: 6,
            action: "pump.start",
        }
    );
    sample!(
        out,
        Cancel {
            call: 7,
            reason: CancelReason::CallerGone,
            instance: 0x55,
            run: 6,
        }
    );
    sample!(
        out,
        Attach {
            call: 7,
            instance: 0x55,
            run: 6,
        }
    );
    sample!(out, Registered { request_seq: 9 });
    sample!(out, CallerGone { session: 3 });
}
