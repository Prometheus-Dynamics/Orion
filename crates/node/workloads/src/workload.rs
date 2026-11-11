//! What should run, as it is kept.

use orion_msg_workloads::{RestartPolicy, WorkloadPut};
use orion_types::{Error, FileName, NodeName, WorkloadKind, WorkloadName};

/// A workload: what the wire's `WorkloadPut` says, owned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Workload {
    /// Its name.
    pub name: WorkloadName,
    /// Its kind, which picks the executor.
    pub kind: WorkloadKind,
    /// What to run, which only the executor reads.
    pub spec: Vec<u8>,
    /// The files it needs before it starts: where, and the digest.
    pub files: Vec<(FileName, [u8; 32])>,
    /// A specific node to run on, if it must be that one.
    pub node: Option<NodeName>,
    /// Labels a node must have.
    pub labels: Vec<(String, String)>,
    /// CPUs it may use, as a bitmask with bit 0 the first CPU. Empty for any.
    pub cpus: Vec<u8>,
    /// What to do when it ends.
    pub restart: RestartPolicy,
}

impl Workload {
    /// A workload of `kind` with `spec`, restarted always, with nothing else asked.
    #[must_use]
    pub fn new(name: WorkloadName, kind: WorkloadKind, spec: &[u8]) -> Self {
        Self {
            name,
            kind,
            spec: spec.to_vec(),
            files: Vec::new(),
            node: None,
            labels: Vec::new(),
            cpus: Vec::new(),
            restart: RestartPolicy::Always,
        }
    }

    /// The workload a `WorkloadPut` asks for.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] if one of its lists does not decode.
    pub fn from_put(put: &WorkloadPut<'_>) -> Result<Self, Error> {
        let mut files = Vec::new();
        for file in put.files.iter() {
            let file = file?;
            files.push((file.name, file.digest));
        }
        let mut labels = Vec::new();
        for label in put.labels.iter() {
            let label = label?;
            labels.push((label.key.to_owned(), label.value.to_owned()));
        }
        Ok(Self {
            name: put.name.clone(),
            kind: put.kind.clone(),
            spec: put.spec.0.to_vec(),
            files,
            node: put.node.clone(),
            labels,
            cpus: put.cpus.0.to_vec(),
            restart: put.restart,
        })
    }
}
