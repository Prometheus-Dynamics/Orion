//! A workload tracker: what should run, handed to an executor that offered the kind, and
//! where each run is as the executor reports it.

use orion_msg_workloads::{
    ExecutorInventory, ExecutorKill, ExecutorOffer, ExecutorReport, ExecutorStart, ExecutorStop,
    WorkloadPut, WorkloadRemove, WorkloadState, WorkloadStatus,
};
use orion_server::{
    Admits, Checked, Cx, Delivery, Error, Incoming, Policy, Service, SessionId, Sessions, What,
};
use orion_types::{NodeName, WorkloadName};
use orion_wire::{Bytes, FrameView, kind};

#[derive(Debug)]
struct Tracked {
    name: WorkloadName,
    state: WorkloadState,
    detail: String,
    /// Who asked about it, and is told when it changes.
    asked: Vec<SessionId>,
    executor: Option<SessionId>,
}

/// The tracker.
#[derive(Debug)]
pub struct Tracker {
    node: NodeName,
    executors: Vec<(SessionId, Vec<String>)>,
    workloads: Vec<Tracked>,
}

impl Tracker {
    /// A tracker on `node`.
    #[must_use]
    pub const fn new(node: NodeName) -> Self {
        Self {
            node,
            executors: Vec::new(),
            workloads: Vec::new(),
        }
    }

    fn tell(&self, sessions: &mut Sessions, tracked: &Tracked) {
        let status = WorkloadStatus {
            name: tracked.name.clone(),
            state: tracked.state,
            node: Some(self.node.clone()),
            detail: &tracked.detail,
        };
        for session in &tracked.asked {
            let _ = sessions.send(*session, &status, Delivery::Must);
        }
    }

    fn set(
        &mut self,
        sessions: &mut Sessions,
        name: &WorkloadName,
        state: WorkloadState,
        detail: &str,
    ) {
        if let Some(at) = self
            .workloads
            .iter()
            .position(|tracked| tracked.name == *name)
        {
            self.workloads[at].state = state;
            self.workloads[at].detail = detail.to_owned();
            self.tell(sessions, &self.workloads[at]);
            if state == WorkloadState::Gone {
                self.workloads.remove(at);
            }
        }
    }
}

impl Service for Tracker {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::WORKLOAD_PUT,
            kind::WORKLOAD_REMOVE,
            kind::EXECUTOR_OFFER,
            kind::EXECUTOR_REPORT,
            kind::EXECUTOR_INVENTORY,
        ]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        Ok((
            What::Write("workloads"),
            Policy::new(Admits::ANYONE).without_grant(),
        ))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let incoming = request.get();
        let from = cx.from();
        match incoming.kind() {
            kind::WORKLOAD_PUT => {
                let put: WorkloadPut<'_> = incoming.body()?;
                let executor = self
                    .executors
                    .iter()
                    .find(|(_, kinds)| kinds.iter().any(|kind| kind == put.kind.as_str()))
                    .map(|(session, _)| *session);
                self.workloads.retain(|tracked| tracked.name != put.name);
                self.workloads.push(Tracked {
                    name: put.name.clone(),
                    state: WorkloadState::Pending,
                    detail: if executor.is_some() {
                        String::new()
                    } else {
                        "no executor runs this kind".into()
                    },
                    asked: vec![from],
                    executor,
                });
                let sessions = cx.sessions();
                if let Some(tracked) = self.workloads.last() {
                    self.tell(sessions, tracked);
                }
                if let Some(executor) = executor {
                    sessions.send(
                        executor,
                        &ExecutorStart {
                            name: put.name.clone(),
                            kind: put.kind.clone(),
                            spec: put.spec,
                            cpus: Bytes(put.cpus.0),
                        },
                        Delivery::Must,
                    )?;
                }
                Ok(())
            }
            kind::WORKLOAD_REMOVE => {
                let remove: WorkloadRemove = incoming.body()?;
                let tracked = self
                    .workloads
                    .iter_mut()
                    .find(|tracked| tracked.name == remove.name)
                    .ok_or(Error::NotFound)?;
                if !tracked.asked.contains(&from) {
                    tracked.asked.push(from);
                }
                let executor = tracked.executor;
                let sessions = cx.sessions();
                match executor {
                    Some(executor) if remove.force => {
                        sessions.send(
                            executor,
                            &ExecutorKill {
                                name: remove.name.clone(),
                            },
                            Delivery::Must,
                        )?;
                        // Forced: the record goes whether or not the run is confirmed gone.
                        self.set(
                            sessions,
                            &remove.name,
                            WorkloadState::Gone,
                            "removed by force",
                        );
                    }
                    Some(executor) => {
                        sessions.send(
                            executor,
                            &ExecutorStop {
                                name: remove.name.clone(),
                            },
                            Delivery::Must,
                        )?;
                        self.set(sessions, &remove.name, WorkloadState::Stopping, "");
                    }
                    None => self.set(sessions, &remove.name, WorkloadState::Gone, ""),
                }
                Ok(())
            }
            kind::EXECUTOR_OFFER => {
                let offer: ExecutorOffer<'_> = incoming.body()?;
                self.executors.retain(|(session, _)| *session != from);
                self.executors.push((
                    from,
                    offer
                        .kinds
                        .iter()
                        .flatten()
                        .map(|offered| offered.kind.to_string())
                        .collect(),
                ));
                Ok(())
            }
            kind::EXECUTOR_INVENTORY => {
                // Runs the executor already has are taken back by name.
                let inventory: ExecutorInventory<'_> = incoming.body()?;
                let sessions = cx.sessions();
                for item in inventory.workloads.iter().flatten() {
                    if !self
                        .workloads
                        .iter()
                        .any(|tracked| tracked.name == item.name)
                    {
                        self.workloads.push(Tracked {
                            name: item.name.clone(),
                            state: item.state,
                            detail: String::new(),
                            asked: Vec::new(),
                            executor: Some(from),
                        });
                    }
                    self.set(sessions, &item.name, item.state, "");
                }
                Ok(())
            }
            _ => {
                let report: ExecutorReport<'_> = incoming.body()?;
                self.set(cx.sessions(), &report.name, report.state, report.detail);
                Ok(())
            }
        }
    }

    fn closed(&mut self, session: SessionId, sessions: &mut Sessions) {
        self.executors.retain(|(executor, _)| *executor != session);
        let lost: Vec<WorkloadName> = self
            .workloads
            .iter_mut()
            .filter(|tracked| tracked.executor == Some(session))
            .map(|tracked| {
                tracked.executor = None;
                tracked.name.clone()
            })
            .collect();
        for tracked in &mut self.workloads {
            tracked.asked.retain(|asked| *asked != session);
        }
        for name in lost {
            self.set(
                sessions,
                &name,
                WorkloadState::Lost,
                "its executor went away",
            );
        }
    }
}
