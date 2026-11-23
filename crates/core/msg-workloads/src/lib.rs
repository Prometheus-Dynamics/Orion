//! Workload and executor bodies. Orion is the authority on what should run. An executor starts
//! what it's told and reports what is true, and keeps no journal of its own.

#![no_std]

use orion_types::{FileName, HlcTimestamp, NodeName, WorkloadKind, WorkloadName};
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
        ;
        /// The stamp of the workload's record that this is based on, if the writer says: the
        /// write is made only if the record still has it, and refused as stale otherwise.
        /// Zero says there is no record yet: make it, never replace one. `None` writes
        /// whatever is there. Level 7; not kept in the record.
        pub based_on: Option<HlcTimestamp>,
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
        ;
        /// As [`WorkloadPut::based_on`]: removed only if its record still has this stamp.
        pub based_on: Option<HlcTimestamp>,
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
        ;
        /// The stamp of the workload's record as the node holds it, zero if it holds none:
        /// what a later write is based on. Level 7.
        pub stamp: HlcTimestamp,
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

#[cfg(test)]
mod levels {
    //! What was appended at level 7, between a side that has it and one that does not.

    extern crate std;

    use std::vec;

    use orion_wire::Decode;
    use orion_wire::body::encode_fields;

    use super::*;

    body! {
        /// `WorkloadPut` as level 6 had it.
        pub struct PutAt6<'a> {
            pub name: WorkloadName,
            pub kind: WorkloadKind,
            pub spec: Bytes<'a>,
            pub files: List<'a, NeededFile>,
            pub node: Option<NodeName>,
            pub labels: List<'a, RequiredLabel<'a>>,
            pub cpus: Bytes<'a>,
            pub restart: RestartPolicy,
        }
    }

    body! {
        /// `WorkloadRemove` as level 6 had it.
        pub struct RemoveAt6 {
            pub name: WorkloadName,
            pub force: bool,
        }
    }

    body! {
        /// `WorkloadStatus` as level 6 had it.
        pub struct StatusAt6<'a> {
            pub name: WorkloadName,
            pub state: WorkloadState,
            pub node: Option<NodeName>,
            pub detail: &'a str,
        }
    }

    fn name() -> WorkloadName {
        "vision".parse().expect("a name")
    }

    #[test]
    fn a_level_6_side_and_a_level_7_side_read_each_others_workload_messages() {
        let mut buf = vec![0_u8; 256];
        let stamp = HlcTimestamp::new(5_000, 1, 9);

        // A level-6 client's put and remove, read by a level-7 node: based on nothing.
        let old = PutAt6 {
            name: name(),
            kind: "graph".parse().expect("a kind"),
            spec: Bytes(b"spec"),
            files: List::new(&[]),
            node: None,
            labels: List::new(&[]),
            cpus: Bytes(&[]),
            restart: RestartPolicy::Always,
        };
        let len = encode_fields(&old, &mut buf).expect("encode");
        let put = WorkloadPut::decode(&buf[..len]).expect("a level-7 node reads it");
        assert_eq!((put.spec.0, put.based_on), (&b"spec"[..], None));
        let len = encode_fields(
            &RemoveAt6 {
                name: name(),
                force: true,
            },
            &mut buf,
        )
        .expect("encode");
        let remove = WorkloadRemove::decode(&buf[..len]).expect("a level-7 node reads it");
        assert_eq!((remove.force, remove.based_on), (true, None));

        // A level-7 node's status, read by a level-6 client: all it knew, the stamp left.
        let status = WorkloadStatus {
            name: name(),
            state: WorkloadState::Running,
            node: None,
            detail: "ok",
            stamp,
        };
        let len = encode_fields(&status, &mut buf).expect("encode");
        let read = StatusAt6::decode(&buf[..len]).expect("a level-6 client reads it");
        assert_eq!((read.state, read.detail), (WorkloadState::Running, "ok"));

        // A level-7 client's put and remove that are based on nothing, read by a level-6
        // node: what it always read. (One based on a stamp is never sent to it.)
        let new = WorkloadPut {
            name: name(),
            kind: "graph".parse().expect("a kind"),
            spec: Bytes(b"spec"),
            files: List::new(&[]),
            node: None,
            labels: List::new(&[]),
            cpus: Bytes(&[]),
            restart: RestartPolicy::Always,
            based_on: None,
        };
        let len = encode_fields(&new, &mut buf).expect("encode");
        assert_eq!(
            PutAt6::decode(&buf[..len]).expect("a level-6 node reads it"),
            old
        );
        let len = encode_fields(
            &WorkloadRemove {
                name: name(),
                force: false,
                based_on: None,
            },
            &mut buf,
        )
        .expect("encode");
        let read = RemoveAt6::decode(&buf[..len]).expect("a level-6 node reads it");
        assert!(!read.force);

        // A level-6 node's status, read by a level-7 client: no stamp, so zero.
        let len = encode_fields(
            &StatusAt6 {
                name: name(),
                state: WorkloadState::Pending,
                node: None,
                detail: "",
            },
            &mut buf,
        )
        .expect("encode");
        let read = WorkloadStatus::decode(&buf[..len]).expect("a level-7 client reads it");
        assert_eq!(read.stamp, HlcTimestamp::ZERO);
    }
}
