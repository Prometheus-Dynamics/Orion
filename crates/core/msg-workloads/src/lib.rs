//! Workload and executor bodies. Orion is the authority on what should run. An executor starts
//! what it's told and reports what is true, and keeps no journal of its own.

#![no_std]

use orion_types::{FileName, NodeName, WorkloadKind, WorkloadName};
use orion_wire::{Bytes, List, body, kind, open_enum};

open_enum! {
    /// Where a workload is.
    pub enum WorkloadState: u8 {
        /// Waiting to be placed or started.
        Pending = 1,
        /// An executor is starting it.
        Starting = 2,
        /// Running.
        Running = 3,
        /// Asked to stop, not stopped yet.
        Stopping = 4,
        /// Stopped, and its record can go.
        Gone = 5,
        /// It failed.
        Failed = 6,
        /// Its executor or node went away, so nobody knows.
        Lost = 7,
    }
}

open_enum! {
    /// What to do when a workload ends.
    pub enum RestartPolicy: u8 {
        /// Always start it again.
        Always = 1,
        /// Start it again only if it failed.
        OnFailure = 2,
        /// Leave it.
        Never = 3,
    }
}

body! {
    /// A file a workload needs before it starts.
    pub struct NeededFile {
        /// Where it should be.
        pub name: FileName,
        /// Its digest.
        pub digest: [u8; 32],
    }
}

body! {
    /// One `key=value` a node must have for a workload to run there.
    pub struct RequiredLabel<'a> {
        /// The key.
        pub key: &'a str,
        /// The value.
        pub value: &'a str,
    }
}

body! {
    /// A workload kind an executor can run.
    pub struct OfferedKind {
        /// The kind.
        pub kind: WorkloadKind,
    }
}

body! {
    /// What an executor has right now.
    pub struct InventoryItem<'a> {
        /// The workload.
        pub name: WorkloadName,
        /// Where it is.
        pub state: WorkloadState,
        /// A description for people.
        pub detail: &'a str,
    }
}

body! {
    /// Says a workload should exist.
    pub struct WorkloadPut<'a> = kind::WORKLOAD_PUT {
        /// Its name.
        pub name: WorkloadName,
        /// Its kind, which picks the executor.
        pub kind: WorkloadKind,
        /// What to run, which only the executor reads.
        pub spec: Bytes<'a>,
        /// Files it needs.
        pub files: List<'a, NeededFile>,
        /// A specific node to run on, if it must be that one.
        pub node: Option<NodeName>,
        /// Labels a node must have.
        pub labels: List<'a, RequiredLabel<'a>>,
        /// CPUs it may use, as a bitmask with bit 0 the first CPU. Empty for any.
        pub cpus: Bytes<'a>,
        /// What to do when it ends.
        pub restart: RestartPolicy,
    }
}

body! {
    /// Says a workload should stop existing. The record stays until the run is really gone,
    /// unless `force` is set.
    pub struct WorkloadRemove = kind::WORKLOAD_REMOVE {
        /// The workload.
        pub name: WorkloadName,
        /// Drop the record even if the run can't be confirmed gone.
        pub force: bool,
    }
}

body! {
    /// Where a workload is, as Orion tracks it.
    pub struct WorkloadStatus<'a> = kind::WORKLOAD_STATUS {
        /// The workload.
        pub name: WorkloadName,
        /// Where it is.
        pub state: WorkloadState,
        /// The node it's on, if it has one.
        pub node: Option<NodeName>,
        /// A description for people.
        pub detail: &'a str,
    }
}

body! {
    /// An executor says what it can run.
    pub struct ExecutorOffer<'a> = kind::EXECUTOR_OFFER {
        /// The kinds.
        pub kinds: List<'a, OfferedKind>,
    }
}

body! {
    /// Tells an executor to start a workload.
    pub struct ExecutorStart<'a> = kind::EXECUTOR_START {
        /// The workload.
        pub name: WorkloadName,
        /// Its kind.
        pub kind: WorkloadKind,
        /// What to run.
        pub spec: Bytes<'a>,
        /// CPUs it may use. Empty for any.
        pub cpus: Bytes<'a>,
    }
}

body! {
    /// Asks an executor to stop a workload.
    pub struct ExecutorStop = kind::EXECUTOR_STOP {
        /// The workload.
        pub name: WorkloadName,
    }
}

body! {
    /// Tells an executor to end a workload now.
    pub struct ExecutorKill = kind::EXECUTOR_KILL {
        /// The workload.
        pub name: WorkloadName,
    }
}

body! {
    /// An executor says where one workload is.
    pub struct ExecutorReport<'a> = kind::EXECUTOR_REPORT {
        /// The workload.
        pub name: WorkloadName,
        /// Where it is.
        pub state: WorkloadState,
        /// A description for people.
        pub detail: &'a str,
    }
}

body! {
    /// An executor says everything it has, so Orion can adopt runs by name after a restart.
    pub struct ExecutorInventory<'a> = kind::EXECUTOR_INVENTORY {
        /// What it has.
        pub workloads: List<'a, InventoryItem<'a>>,
    }
}

body! {
    /// Orion asks an executor to say everything it has again, with [`ExecutorInventory`]. It
    /// does when it has just started and found the executor already connected: whatever the
    /// executor reported before was said to an Orion that is gone.
    pub struct ExecutorAsk = kind::EXECUTOR_ASK {
        /// The kinds Orion takes this executor to run, as many as it offered.
        pub kinds: u8,
    }
}
