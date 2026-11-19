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

/// The [`ActionResult::code`] of a run that its program stopped because node logic told it its
/// deadline passed ([`Outcome::TimedOut`]): `ETIMEDOUT`'s number. A run ended any other way by
/// node logic (its program gone, [`Outcome::Indeterminate`]) has code 0; a program's own codes
/// are its own.
pub const DEADLINE_CODE: u32 = 110;

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
        /// Node logic told it to stop (its deadline passed, or node logic was stopping), and
        /// its program did not say how it ended in time: it may have finished, failed or
        /// stopped. Not to be read as done, nor as not done. Never `Unknown`: an older peer
        /// reads this number as `Unknown(6)`, which is not a success either.
        Indeterminate = 6,
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
        ;
        /// For a run another node carries here: that node's token for the session that asked
        /// it, so each session behind one peer link is a caller of its own here. Zero for the
        /// asker's own session, and read as zero from anyone but a peer.
        pub on_behalf: u64,
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
        /// The session that asked, as an opaque token: never the same for two sessions while
        /// node logic runs, also when one identity connects twice. [`CallerGone`] names it
        /// when that session ends. Zero from a host that does not say.
        pub session: u64,
        /// The run goes on without its caller, until it ends or its deadline passes: a
        /// [`CallerGone`] for its session does not end it. False from a host that does not
        /// say.
        pub detached: bool,
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
        ;
        /// To a caller: the run's [`RunHandle`], its node-logic instance. Zero from a node
        /// that does not say, and from a program.
        pub instance: u64,
        /// To a caller: the run's [`RunHandle`], its number in that instance.
        pub run: u64,
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
        ;
        /// The action's own status code, for a caller to branch on: what it means is the
        /// action's to say (a service's `BUSY`, say). Zero: none given. The words stay in
        /// `detail`.
        pub code: u32,
        /// To a caller: the run's [`RunHandle`], its node-logic instance. Zero from a node
        /// that does not say, and from a program.
        pub instance: u64,
        /// To a caller: the run's [`RunHandle`], its number in that instance.
        pub run: u64,
        /// To a caller: the action the run was started as. Empty from a node that does not
        /// say.
        pub action: &'a str,
    }
}

body! {
    /// Tells a caller a run it started, or came back to, is under way here, and what to call
    /// it from now on: its [`RunHandle`], which no other run is ever given.
    pub struct Started<'a> = kind::ACTION_STARTED {
        /// The caller's number for the run, as it started it or came back to it.
        pub call: u64,
        /// The run's node-logic instance.
        pub instance: u64,
        /// The run's number in that instance.
        pub run: u64,
        /// The action it was started as.
        pub action: &'a str,
    }
}

/// A run, as the node names it: no other run is ever given the same handle. `instance` is
/// drawn at random each time node logic starts, and `run` counts up within it, so a handle
/// from before a restart names nothing afterwards, never another run.
///
/// Its encodings are stable: [`RunHandle::to_bytes`] (sixteen bytes, each number
/// little-endian) and the text form `<instance>-<run>`, each sixteen lowercase hex digits. A
/// client may keep either and give it back after any reconnect or restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RunHandle {
    /// The node-logic instance that started the run. Never zero.
    pub instance: u64,
    /// The run's number in that instance. Never zero.
    pub run: u64,
}

impl RunHandle {
    /// A handle from what a node said, if it said one.
    #[must_use]
    pub const fn said(instance: u64, run: u64) -> Option<Self> {
        if instance == 0 || run == 0 {
            None
        } else {
            Some(Self { instance, run })
        }
    }

    /// The sixteen bytes it is kept as.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&self.instance.to_le_bytes());
        bytes[8..].copy_from_slice(&self.run.to_le_bytes());
        bytes
    }

    /// The handle kept as `bytes`, if they are one.
    #[must_use]
    pub fn from_bytes(bytes: &[u8; 16]) -> Option<Self> {
        let mut instance = [0; 8];
        let mut run = [0; 8];
        instance.copy_from_slice(&bytes[..8]);
        run.copy_from_slice(&bytes[8..]);
        Self::said(u64::from_le_bytes(instance), u64::from_le_bytes(run))
    }
}

impl core::fmt::Display for RunHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:016x}-{:016x}", self.instance, self.run)
    }
}

impl core::str::FromStr for RunHandle {
    type Err = &'static str;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        const NOT: &str = "a run handle is <instance>-<run>, each sixteen hex digits";
        let (instance, run) = text.split_once('-').ok_or(NOT)?;
        if instance.len() != 16 || run.len() != 16 {
            return Err(NOT);
        }
        let instance = u64::from_str_radix(instance, 16).map_err(|_| NOT)?;
        let run = u64::from_str_radix(run, 16).map_err(|_| NOT)?;
        Self::said(instance, run).ok_or(NOT)
    }
}

open_enum! {
    /// Why a run is stopped. Set by whoever passes the [`Cancel`] to the program that runs it,
    /// never taken from the caller as it is.
    #[derive(Default)]
    pub enum CancelReason: u8 {
        /// Not said: a host that does not say.
        #[default]
        Unsaid = 0,
        /// Its caller asked.
        Asked = 1,
        /// Its caller's session ended before the run did.
        CallerGone = 2,
        /// Its deadline passed.
        Deadline = 3,
        /// Node logic is stopping.
        Stopping = 4,
    }
}

body! {
    /// Stops a run.
    pub struct Cancel = kind::ACTION_CANCEL {
        /// The run.
        pub call: u64,
        ;
        /// Why. A caller's own is not kept: node logic says [`CancelReason::Asked`] for it,
        /// and passes on what another node's node logic said for a run it carried.
        pub reason: CancelReason,
        /// From a caller: the run's [`RunHandle`], its instance. With it, the run that handle
        /// names and no other; zero, a run this same session started under `call`.
        pub instance: u64,
        /// From a caller: the run's [`RunHandle`], its number.
        pub run: u64,
    }
}

body! {
    /// Reattaches to a detached run. The latest progress or the result follows.
    pub struct Attach = kind::ACTION_ATTACH {
        /// The run, by the number its starter gave it. Without a handle, it is found by this
        /// only if it is the one run of the starter's with that number; with one, it is what
        /// this session will call the run.
        pub call: u64,
        ;
        /// The run's [`RunHandle`], its instance: the run that handle names and no other. Zero
        /// when not said.
        pub instance: u64,
        /// The run's [`RunHandle`], its number.
        pub run: u64,
    }
}

body! {
    /// Answers a [`Register`]: the action is on the list, and its runs will come as [`Claim`].
    pub struct Registered = kind::ACTION_REGISTERED {
        /// The `seq` of the [`Register`].
        pub request_seq: u16,
    }
}

body! {
    /// The session named by `session` in a [`Claim`] ended, for whatever cause. Sent once to
    /// each program that was given a run from it, whether its runs are still going or not. A
    /// detached run goes on; what a program holds for that caller is its own business.
    ///
    /// Also sent by a peer's node logic to this node, naming its own token for a session it
    /// carried runs for with [`Run::on_behalf`]: that session ended there, so it has here.
    pub struct CallerGone = kind::ACTION_CALLER_GONE {
        /// The session's token.
        pub session: u64,
    }
}
