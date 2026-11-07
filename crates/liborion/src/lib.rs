//! liborion: the C library. Everything is in `orion-capi`; this crate only links it as
//! `liborion.a` and `liborion.so.<major>` (docs/capi.md).

#![cfg(unix)]

pub use orion_capi::*;
