//! Recorded bytes for every kind in the table, so any change to a body fails a test, and the
//! decoders the fuzz targets drive.

pub mod decoders;
mod samples;

pub use samples::{Sample, all};
