//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

#[path = "actions.rs"]
mod actions;
#[path = "files.rs"]
mod files;
#[path = "forward.rs"]
mod forward;
#[path = "leases.rs"]
mod leases;
#[path = "node.rs"]
mod node;
#[path = "placement.rs"]
mod placement;
#[path = "pull.rs"]
mod pull;
#[path = "resources.rs"]
mod resources;
#[path = "restoring.rs"]
mod restoring;
#[path = "sink.rs"]
mod sink;
#[path = "stops.rs"]
mod stops;
#[path = "sync.rs"]
mod sync;
#[path = "workloads.rs"]
mod workloads;
