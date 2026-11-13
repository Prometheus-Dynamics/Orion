//! Actions on a microcontroller.
//!
//! A firmware describes its actions once, as constants, and [`Actions`] registers each when
//! a session starts, says it again if the answer does not come, and from then on hands the
//! firmware every run the host claims for it. The firmware answers a run with
//! [`Actions::progress`] and [`Actions::finish`], when it likes: a run is a number and its
//! arguments, and nothing about it is kept here.
//!
//! The link takes one frame at a time. A result it has no room for comes back as
//! [`SendError::Busy`] and the firmware sends it again on a later pass; a run whose result
//! never comes is ended by its deadline on the host.
//!
//! ```
//! use orion_device_actions::{ActionSpec, Actions, Heard, Outcome};
//! use orion_device_core::{DeviceConfig, StreamDevice};
//!
//! static ACTIONS: [ActionSpec; 1] = [ActionSpec::on_resource("zero", "imu", "Sets yaw to zero")];
//!
//! let mut device = StreamDevice::<128, 128>::new(DeviceConfig::new("imu-board"));
//! let mut actions = Actions::new(&ACTIONS);
//! let mut ran = None;
//! # let bytes: &[u8] = &[];
//! device.receive(bytes, |_, frame| {
//!     if let Heard::Run { call, .. } = actions.handle(frame) {
//!         ran = Some(call);
//!     }
//! });
//! device.link.poll(0);
//! actions.poll(&mut device.link, 0);
//! if let Some(call) = ran {
//!     let _ = actions.finish(&mut device.link, call, Outcome::Succeeded, &[], "");
//! }
//! ```

#![cfg_attr(not(test), no_std)]

pub use orion_check::{Admits, Policy};
pub use orion_device_core::{RETRY_MS, Request, SendError};
pub use orion_msg_actions::Outcome;

use orion_device_core::codec::{Reader, Writer};
use orion_device_core::{Link, Requests};
use orion_wire::{FrameView, kind};

/// What an action runs against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// The node the device is connected to.
    Node(&'static str),
    /// A resource, such as the device itself.
    Resource(&'static str),
}

/// One action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActionSpec {
    /// Its name: letters, digits, `-`, `_` and `.`, up to 63 bytes.
    pub name: &'static str,
    /// What it runs against.
    pub target: Target,
    /// Who may run it.
    pub policy: Policy,
    /// A description for people.
    pub description: &'static str,
}

impl ActionSpec {
    /// An action on resource `resource` that operators may run once granted it.
    #[must_use]
    pub const fn on_resource(
        name: &'static str,
        resource: &'static str,
        description: &'static str,
    ) -> Self {
        Self {
            name,
            target: Target::Resource(resource),
            policy: Policy::new(Admits::OPERATOR),
            description,
        }
    }

    /// Writes the registration's payload and says how long it is.
    fn write_register(&self, out: &mut [u8]) -> Option<usize> {
        let mut w = Writer::new(out);
        w.bytes(self.name.as_bytes());
        // An enum: the variant's number, then what it holds.
        match self.target {
            Target::Node(node) => w.varint(0).bytes(node.as_bytes()),
            Target::Resource(resource) => w.varint(1).bytes(resource.as_bytes()),
        };
        w.u8(self.policy.to_bits())
            .bytes(self.description.as_bytes())
            .finish()
    }
}

/// What a frame from the host meant to the actions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Heard<'a> {
    /// Not about an action of this device: for another layer.
    Other,
    /// The host listed action `action`.
    Registered {
        /// Its index in the specs.
        action: usize,
    },
    /// The host refused action `action`.
    Refused {
        /// Its index in the specs.
        action: usize,
        /// The error's code.
        code: u16,
    },
    /// Run action `action`. Answer with [`Actions::finish`] and the same `call`.
    Run {
        /// Its index in the specs.
        action: usize,
        /// The run's number.
        call: u64,
        /// Its arguments, which only the action reads.
        args: &'a [u8],
        /// When the host gives up on it, in milliseconds from when it was asked. Zero for
        /// never.
        deadline_ms: u64,
    },
    /// Stop run `call`. The host ends it as cancelled; a result sent after is dropped.
    Cancel {
        /// The run.
        call: u64,
        /// Why, as the wire numbers it: 1 asked, 2 its caller gone, 3 its deadline passed,
        /// 4 node logic stopping, 0 not said.
        reason: u8,
    },
}

/// The actions of a device, `N` of them.
#[derive(Debug)]
pub struct Actions<const N: usize> {
    specs: &'static [ActionSpec],
    registered: Requests<N>,
}

impl<const N: usize> Actions<N> {
    /// Actions that are not registered yet.
    #[must_use]
    pub const fn new(specs: &'static [ActionSpec; N]) -> Self {
        Self::from_slice(specs)
    }

    /// Actions from a table filled in at run time, as a C API does: up to `N` of them, and
    /// any after the `N`th are left out.
    #[must_use]
    pub const fn from_slice(specs: &'static [ActionSpec]) -> Self {
        Self {
            specs,
            registered: Requests::new(),
        }
    }

    /// Where action `action` stands in this session: [`Request::Answered`] once the host
    /// has listed it.
    #[must_use]
    pub fn state(&self, action: usize) -> Option<Request> {
        self.registered.get(action)
    }

    /// Registers what is not registered in this session, and again what was not answered
    /// in [`RETRY_MS`], one frame a call while the link has room. A new session starts every
    /// action over. Call it after [`Link::poll`], with the same counter.
    pub fn poll<const TX: usize>(&mut self, link: &mut Link<TX>, now_ms: u32) {
        let specs = self.specs;
        self.registered
            .poll(link, now_ms, kind::ACTION_REGISTER, |action, out| {
                specs.get(action)?.write_register(out)
            });
    }

    /// Reads a frame the session did not take: the answer to a registration, a refusal of
    /// one, a run or a cancel.
    pub fn handle<'a>(&mut self, frame: &FrameView<'a>) -> Heard<'a> {
        let mut r = Reader::new(frame.payload());
        match frame.kind() {
            kind::ACTION_REGISTERED => match r.u16().and_then(|seq| self.registered.answer(seq)) {
                Some(action) => Heard::Registered { action },
                None => Heard::Other,
            },
            kind::REFUSAL => {
                let (Some(seq), Some(code)) = (r.u16(), r.u16()) else {
                    return Heard::Other;
                };
                match self.registered.refuse(seq, code) {
                    Some(action) => Heard::Refused { action, code },
                    None => Heard::Other,
                }
            }
            kind::ACTION_CLAIM => {
                let (Some(call), Some(name)) = (r.varint(), r.bytes()) else {
                    return Heard::Other;
                };
                let (Some(variant), Some(target)) = (r.varint(), r.bytes()) else {
                    return Heard::Other;
                };
                let (Some(args), Some(deadline_ms)) = (r.bytes(), r.varint()) else {
                    return Heard::Other;
                };
                let registered = &self.registered;
                let found = self.specs.iter().enumerate().position(|(action, spec)| {
                    registered.get(action) == Some(Request::Answered)
                        && spec.name.as_bytes() == name
                        && spec.target.is(variant, target)
                });
                match found {
                    Some(action) => Heard::Run {
                        action,
                        call,
                        args,
                        deadline_ms,
                    },
                    None => Heard::Other,
                }
            }
            kind::ACTION_CANCEL => match r.varint() {
                Some(call) => Heard::Cancel {
                    call,
                    reason: r.u8().unwrap_or(0),
                },
                None => Heard::Other,
            },
            _ => Heard::Other,
        }
    }

    /// Says how run `call` is doing: `percent` done, 255 if unknown, and what it is doing.
    ///
    /// # Errors
    ///
    /// [`SendError`]. Nothing was sent.
    pub fn progress<const TX: usize>(
        &mut self,
        link: &mut Link<TX>,
        call: u64,
        percent: u8,
        message: &str,
    ) -> Result<u16, SendError> {
        link.send_with(kind::ACTION_PROGRESS, |out| {
            Writer::new(out)
                .varint(call)
                .u8(percent)
                .bytes(message.as_bytes())
                .varint(0)
                .finish()
        })
    }

    /// Ends run `call` with `outcome`, what it returned, and a description for people if it
    /// did not succeed.
    ///
    /// # Errors
    ///
    /// [`SendError`]. Nothing was sent: send it again when the link has room.
    pub fn finish<const TX: usize>(
        &mut self,
        link: &mut Link<TX>,
        call: u64,
        outcome: Outcome,
        output: &[u8],
        detail: &str,
    ) -> Result<u16, SendError> {
        link.send_with(kind::ACTION_RESULT, |out| {
            Writer::new(out)
                .varint(call)
                .u8(outcome.code())
                .bytes(output)
                .bytes(detail.as_bytes())
                .finish()
        })
    }
}

impl Target {
    /// Whether it is the target that arrived as `variant` and `name`.
    fn is(self, variant: u64, name: &[u8]) -> bool {
        match self {
            Self::Node(node) => variant == 0 && node.as_bytes() == name,
            Self::Resource(resource) => variant == 1 && resource.as_bytes() == name,
        }
    }
}
