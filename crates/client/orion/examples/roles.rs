//! Every role a program can have, through the `orion` crate alone.
//!
//! Three programs talk to one comm core here, each with one [`Orion`] handle:
//!
//! - a **provider** speaks for a fan: it says the fan exists, publishes its speed as a topic
//!   and registers an action that sets it;
//! - an **executor** runs workloads of the kind `script`;
//! - a **client**, which knows nothing of fans or scripts, finds the fan, reads it, drives
//!   it, asks for a workload and watches it run.
//!
//! `cargo run -p orion --example roles --features full` starts a comm core for them to talk
//! to and prints what happened.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use orion::actions::{ActionCall, Admits, Outcome, Policy, Request, Target};
use orion::executor::{ExecError, Executor, WorkloadKind, WorkloadName, WorkloadState};
use orion::provider::ResourceState;
use orion::topics::{FieldId, FrameTime};
use orion::workloads::{RestartPolicy, Workload};
use orion::{Address, Error, Options, Orion, Roles, fields};

/// An executor whose workloads are lines in a map.
struct Scripts {
    kinds: Vec<WorkloadKind>,
    running: BTreeMap<String, WorkloadState>,
}

impl Executor for Scripts {
    fn kinds(&self) -> &[WorkloadKind] {
        &self.kinds
    }

    fn start(&mut self, name: &WorkloadName, spec: &[u8]) -> Result<(), ExecError> {
        if spec.is_empty() {
            return Err(ExecError("an empty script".into()));
        }
        self.running
            .insert(name.to_string(), WorkloadState::Running);
        Ok(())
    }

    fn stop(&mut self, name: &WorkloadName) {
        self.running.remove(name.as_str());
    }

    fn kill(&mut self, name: &WorkloadName) {
        self.running.remove(name.as_str());
    }

    fn inventory(&self) -> Vec<(WorkloadName, WorkloadState)> {
        self.running
            .iter()
            .filter_map(|(name, state)| Some((name.parse().ok()?, *state)))
            .collect()
    }
}

fn connect(socket: &Path, name: &str, roles: Roles) -> Result<Orion, Error> {
    let options = Options {
        roles,
        ..Options::default()
    };
    Orion::connect_with(&Address::Local(socket.to_owned()), name, options)
}

/// Runs the three programs against the comm core at `socket` and returns what happened.
///
/// # Errors
///
/// Whatever the client fails with.
pub fn run(socket: &Path) -> Result<Vec<String>, Error> {
    let mut story = Vec::new();

    // The provider.
    let driver = connect(socket, "fan-driver", Roles::CLIENT.and(Roles::PROVIDER))?;
    let mut describing = driver.provider().resource("fan.left", "fan")?;
    let mut speed = describing.topic("speed", &fields![rpm: Float "rpm"], 16, 8)?;
    describing.action(
        "set",
        "Sets the fan's speed, in rpm",
        Policy::new(Admits::LOCAL),
    )?;
    let mut fan = describing.publish(ResourceState::Ready)?;
    speed.publish(
        FieldId(0),
        FrameTime::from_nanos(1),
        &900.0_f64.to_le_bytes(),
    )?;

    // The executor.
    let runner = connect(socket, "script-runner", Roles::EXECUTOR)?;
    let mut scripts = Scripts {
        kinds: vec![
            "script"
                .parse()
                .map_err(|_| orion::ClientError::Protocol("a kind"))?,
        ],
        running: BTreeMap::new(),
    };
    let mut serving = runner.executor();
    serving.start(&scripts)?;

    // The client: no code for fans, none for scripts.
    let dashboard = connect(socket, "dashboard", Roles::CLIENT)?;
    let found = dashboard.resources("")?;
    for resource in &found {
        story.push(format!(
            "found {} ({}), {:?}",
            resource.name, resource.kind, resource.state
        ));
    }
    let Some(resource) = found.first() else {
        return Ok(story);
    };
    let mut reading = dashboard.subscribe(&resource.topics[0])?;
    reading.wait(Duration::from_secs(2))?;
    if let Some(sample) = reading.latest(FieldId(0)) {
        let rpm = f64::from_le_bytes(sample.payload.try_into().unwrap_or_default());
        story.push(format!("{} reads {rpm}", resource.topics[0]));
    }

    let mut set = dashboard.run(&ActionCall {
        name: &resource.actions[0],
        target: Target::Resource(
            resource
                .name
                .parse()
                .map_err(|_| orion::ClientError::Protocol("a name"))?,
        ),
        args: &1500.0_f64.to_le_bytes(),
        deadline_ms: 0,
        detached: false,
    })?;
    // The provider's turn: it is handed the run, does it, and says so.
    if let Some(requests) = fan.requests()
        && let Some(Request::Run { call, args, .. }) = requests.wait(Duration::from_secs(2))?
    {
        let rpm = f64::from_le_bytes(args.as_slice().try_into().unwrap_or_default());
        speed.publish(FieldId(0), FrameTime::from_nanos(2), &rpm.to_le_bytes())?;
        requests.finish(call, Outcome::Succeeded, &[], "")?;
    }
    let ended = set.finish(Duration::from_secs(2))?;
    story.push(format!("{} ended {:?}", resource.actions[0], ended.outcome));
    reading.wait(Duration::from_secs(2))?;
    while !matches!(reading.next(), orion::topics::Next::Empty) {}
    if let Some(sample) = reading.latest(FieldId(0)) {
        let rpm = f64::from_le_bytes(sample.payload.try_into().unwrap_or_default());
        story.push(format!("{} reads {rpm}", resource.topics[0]));
    }

    // A workload: asked for by the client, run by the executor, tracked by Orion.
    dashboard.workloads().put(&Workload {
        name: "nightly",
        kind: "script",
        spec: b"tidy logs",
        node: None,
        restart: RestartPolicy::Never,
    })?;
    for _ in 0..200 {
        serving.turn(&mut scripts, Duration::from_millis(5))?;
        if let Some(tracked) = dashboard.workloads().status("nightly")?
            && tracked.state == WorkloadState::Running
        {
            story.push(format!(
                "workload {} is {:?} on {}",
                tracked.name,
                tracked.state,
                tracked.node.unwrap_or_default()
            ));
            break;
        }
    }
    dashboard.workloads().remove("nightly", false)?;
    for _ in 0..200 {
        serving.turn(&mut scripts, Duration::from_millis(5))?;
        if dashboard
            .workloads()
            .status("nightly")?
            .is_some_and(|tracked| tracked.state == WorkloadState::Gone)
        {
            story.push("workload nightly is Gone".to_owned());
            break;
        }
    }
    Ok(story)
}

#[allow(dead_code)]
fn main() -> Result<(), Error> {
    let board = orion_stubs::Board::start("roles-example");
    for line in run(board.socket())? {
        println!("{line}");
    }
    Ok(())
}
