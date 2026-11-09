//! The one check.
//!
//! Every request of every kind passes through [`Check::decide`], and a handler can only be called
//! with the [`Checked`] it returns. `Checked<T>` has no public constructor, so skipping the check,
//! or forgetting it, doesn't compile. An action can only be registered with a [`Policy`], and
//! `Policy` has no default, so forgetting one doesn't compile either.
//!
//! There is one switch, [`Security`]. Off, the check admits everything without looking at the
//! policy. Built without feature `enforce`, it isn't there at all and `Checked<T>` is just a `T`.

#![no_std]

mod check;
mod policy;
mod principal;

pub use check::{Check, Checked, Grants, Refused, Request, Security, What};
pub use policy::{Admits, Policy, Registered};
pub use principal::{Principal, PrincipalClass};
