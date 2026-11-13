//! Action bodies. Orion ships almost no actions. Any program that connects can add its own, and
//! registering one needs a policy, so it can't skip the check.

#![no_std]

use orion_check::Policy;
use orion_types::{ActionName, NodeName, ResourceName};
use orion_wire::{Bytes, body, kind, open_enum};
use serde::{Deserialize, Serialize};

/// What an action runs against.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Target {
    /// A node.
    Node(NodeName),
    /// A resource.
    Resource(ResourceName),
}

open_enum! {
    /// How an action ended.
    pub enum Outcome: u8 {
        /// It worked.
        Succeeded = 1,
        /// It ran and failed.
        Failed = 2,
        /// It was refused without running.
        Rejected = 3,
        /// It ran out of time.
        TimedOut = 4,
        /// It was cancelled.
        Cancelled = 5,
    }
}

body! {
    /// Adds an action. The policy is required.
    pub struct Register<'a> = kind::ACTION_REGISTER {
        /// Its name.
        pub name: ActionName,
        /// What it applies to.
        pub target: Target,
        /// Who may run it.
        pub policy: Policy,
        /// A description for people.
        pub description: &'a str,
    }
}

body! {
    /// Runs an action. Answered with [`Progress`] and an [`ActionResult`].
    pub struct Run<'a> = kind::ACTION_RUN {
        /// Chosen by the caller, unique on its connection.
        pub call: u64,
        /// The action.
        pub name: ActionName,
        /// What to run it against.
        pub target: Target,
        /// Its arguments, which only the action reads.
        pub args: Bytes<'a>,
        /// When to give up. Zero for no deadline.
        pub deadline_ms: u64,
        /// Whether it carries on if the caller goes away.
        pub detached: bool,
    }
}

body! {
    /// Hands a run to the program that registered the action.
    pub struct Claim<'a> = kind::ACTION_CLAIM {
        /// The run.
        pub call: u64,
        /// The action.
        pub name: ActionName,
        /// What to run it against.
        pub target: Target,
        /// Its arguments.
        pub args: Bytes<'a>,
        /// When to give up. Zero for no deadline.
        pub deadline_ms: u64,
        ;
        /// Who asked for the run, as the audit log names them: `local:<uid>`,
        /// `operator:<key>`, `peer:<node>`, `device:<name>` or `anonymous`. Empty from a host
        /// that does not say.
        pub caller: &'a str,
    }
}

body! {
    /// How a long action is doing.
    pub struct Progress<'a> = kind::ACTION_PROGRESS {
        /// The run.
        pub call: u64,
        /// Percent done, or 255 if unknown.
        pub percent: u8,
        /// What it's doing now.
        pub message: &'a str,
        /// Whatever else the action reports.
        pub data: Bytes<'a>,
    }
}

body! {
    /// How an action ended.
    pub struct ActionResult<'a> = kind::ACTION_RESULT {
        /// The run.
        pub call: u64,
        /// How it ended.
        pub outcome: Outcome,
        /// What it returned.
        pub output: Bytes<'a>,
        /// A description for people if it didn't succeed.
        pub detail: &'a str,
    }
}

body! {
    /// Stops a run.
    pub struct Cancel = kind::ACTION_CANCEL {
        /// The run.
        pub call: u64,
    }
}

body! {
    /// Reattaches to a detached run. The latest progress or the result follows.
    pub struct Attach = kind::ACTION_ATTACH {
        /// The run.
        pub call: u64,
    }
}

body! {
    /// Answers a [`Register`]: the action is on the list, and its runs will come as [`Claim`].
    pub struct Registered = kind::ACTION_REGISTERED {
        /// The `seq` of the [`Register`].
        pub request_seq: u16,
    }
}
