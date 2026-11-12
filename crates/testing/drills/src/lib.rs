//! Failure drills.
//!
//! The design's table says what keeps working when each part of a board dies, and how it
//! comes back. The drills hold every row to it with real processes: the comm core and node
//! logic as `orion` runs them, and beside them the programs a board has, each its own
//! process. A drill starts them, kills one with `SIGKILL` while the others run, watches the
//! others, and starts it again. Then the misbehaving neighbours: a client, a peer, a provider
//! and a link that do what they should not, and that the rest of the board must not notice.
//!
//! The roles are here, run by the `orion-drill` binary. Each prints what it sees, one line
//! at a time:
//!
//! - `publisher`: a count on `drill.count`, one a millisecond. `published N`.
//! - `subscriber`: reads `drill.count`, or the topic it is given, and checks it has every
//!   value in order. `seen N`, and `gap A B` for a value that did not follow the one before.
//!   Given `stall` it subscribes and never reads again.
//! - `camera`: a 4 KiB frame on `drill.frames` every 10 ms, each filled with one byte from
//!   its number. `camera N`.
//! - `viewer`: reads `drill.frames` and checks each frame is whole. `frame N`, and `torn N`.
//! - `executor`: the sample executor, offering `process`.
//! - `provider`: the actions `drill.echo` (answers with its arguments), `drill.stuck` (never
//!   answers) and `drill.crash` (the provider exits while running it), and the resource
//!   `drill.thing`, `Ready` while it runs.
//! - `throttle LISTEN TARGET RATE`: not a program of the board's but a slow link to it, for a
//!   page watching from far away (`testing/board-runs/I7.md`).
//!
//! Every role that loses the comm core keeps doing its work and connects again, one try at a
//! time between pieces of work, and says `reconnected` when it is back.

#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use orion::actions::{ActionDecl, Admits, Outcome, Policy, Request, Target};
use orion::buffers::{HoldLimits, NextBatch, Pool};
use orion::provider::ResourceState;
use orion::topics::{FieldId, Next, SampleTime, TopicDecl};
use orion::{Address, Error, Options, Orion, Roles, fields};

/// Why a role stopped.
type Failed = Box<dyn std::error::Error>;

mod throttle;
use orion_sample_executor::{Sample, run};

/// The topic of counts.
pub const COUNT: &str = "drill.count";
/// The topic of frames.
pub const FRAMES: &str = "drill.frames";
/// The size of a frame.
pub const FRAME_SIZE: usize = 4096;
/// The resource the provider speaks for.
pub const THING: &str = "drill.thing";

/// Runs the role `args` names, or `None` if they do not name one.
#[must_use]
pub fn role(args: &[String]) -> Option<ExitCode> {
    let (name, socket) = match args {
        [name, socket, ..] => (name.as_str(), PathBuf::from(socket)),
        _ => return None,
    };
    let result = match name {
        "publisher" => publisher(&socket),
        "subscriber" => {
            let stall = args.iter().any(|arg| arg == "stall");
            let topic = args.get(2).filter(|arg| *arg != "stall");
            subscriber(&socket, topic.map_or(COUNT, String::as_str), stall)
        }
        "camera" => camera(&socket),
        "viewer" => viewer(&socket),
        "executor" => executor(&socket),
        "provider" => provider(&socket),
        "throttle" => match (args.get(2), args.get(3).and_then(|rate| rate.parse().ok())) {
            (Some(target), Some(rate)) => throttle::throttle(&args[1], target, rate),
            _ => Err("usage: throttle <listen> <target> <bytes a second>".into()),
        },
        _ => return None,
    };
    Some(match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            println!("failed {error}");
            ExitCode::FAILURE
        }
    })
}

fn connect(socket: &Path, name: &str, roles: Roles) -> Result<Orion, Error> {
    // A role may start before the comm core has its socket up.
    let started = Instant::now();
    loop {
        match Orion::connect_with(
            &Address::Local(socket.to_owned()),
            name,
            Options {
                roles,
                ..Options::default()
            },
        ) {
            Ok(orion) => return Ok(orion),
            Err(error) if started.elapsed() > Duration::from_secs(10) => return Err(error),
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// Keeps a session between pieces of work: takes in what came, and once it is lost, tries
/// once to connect again each time it is asked.
struct Keep {
    lost: bool,
    last: Instant,
}

impl Keep {
    fn new() -> Self {
        Self {
            lost: false,
            last: Instant::now(),
        }
    }

    /// Every 20 ms at most.
    fn tend(&mut self, orion: &Orion) {
        if self.last.elapsed() < Duration::from_millis(20) {
            return;
        }
        self.last = Instant::now();
        if !self.lost
            && let Err(error) = orion.pump(Duration::ZERO)
        {
            println!("lost {error}");
            self.lost = true;
        }
        if self.lost && orion.reconnect(|_| false).is_ok() {
            println!("reconnected");
            self.lost = false;
        }
    }
}

fn publisher(socket: &Path) -> Result<(), Failed> {
    let orion = connect(socket, "drill-publisher", Roles::CLIENT)?;
    let mut count = orion.declare(&TopicDecl {
        name: COUNT,
        fields: &fields![count: Float],
        history: 4096,
        expiry_ms: 0,
        max_payload: 8,
    })?;
    let mut keep = Keep::new();
    for value in 1..=u64::MAX {
        let published = count.publish(
            FieldId(0),
            SampleTime::from_nanos(value),
            &(value as f64).to_le_bytes(),
        );
        if let Err(error) = published {
            println!("unpublished {value} {error}");
        }
        if value % 100 == 0 {
            println!("published {value}");
        }
        keep.tend(&orion);
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

fn subscriber(socket: &Path, topic: &str, stall: bool) -> Result<(), Failed> {
    let orion = connect(socket, "drill-subscriber", Roles::CLIENT)?;
    let mut reading = orion.subscribe(topic)?;
    if stall {
        println!("stalled");
        loop {
            std::thread::sleep(Duration::from_secs(3600));
        }
    }
    let (mut keep, mut last, mut said) = (Keep::new(), 0_u64, Instant::now());
    loop {
        loop {
            match reading.next() {
                Next::Sample(sample) => {
                    let value = sample
                        .payload
                        .try_into()
                        .map(f64::from_le_bytes)
                        .unwrap_or(-1.0) as u64;
                    if last != 0 && value != last + 1 {
                        println!("gap {last} {value}");
                    }
                    last = value;
                }
                Next::Skipped(lost) => println!("skipped {lost}"),
                Next::Empty => break,
            }
        }
        if said.elapsed() >= Duration::from_millis(50) {
            println!("seen {last}");
            said = Instant::now();
        }
        keep.tend(&orion);
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn camera(socket: &Path) -> Result<(), Failed> {
    let orion = connect(socket, "drill-camera", Roles::CLIENT)?;
    let pool = Pool::in_memory(1, 6, FRAME_SIZE)?;
    let mut frames = orion.declare_buffers(FRAMES, 8, 16, pool)?;
    let mut keep = Keep::new();
    for number in 1..=u64::MAX {
        let _ = frames.poll();
        match frames.take() {
            Ok(buffer) => {
                if let Some(bytes) = frames.buffer_mut(buffer) {
                    bytes.fill(fill(number));
                }
                if let Err(error) = frames.publish(&[buffer], SampleTime::from_nanos(number), &[]) {
                    println!("unpublished {number} {error}");
                }
            }
            Err(_) => println!("dropped {number}"),
        }
        if number % 10 == 0 {
            println!("camera {number}");
        }
        keep.tend(&orion);
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

/// The byte frame `number` is filled with.
#[must_use]
pub const fn fill(number: u64) -> u8 {
    (number % 251) as u8
}

fn viewer(socket: &Path) -> Result<(), Failed> {
    let orion = connect(socket, "drill-viewer", Roles::CLIENT)?;
    let limits = HoldLimits {
        max_held: 2,
        max_hold_ms: 0,
    };
    let mut frames = orion.subscribe_buffers(FRAMES, &[], limits)?;
    let (mut keep, mut last, mut said) = (Keep::new(), 0_u64, Instant::now());
    loop {
        loop {
            match frames.next() {
                NextBatch::Batch(frame) => {
                    let number = frame.time.as_nanos();
                    let whole = frame.held.bytes(0).is_some_and(|bytes| {
                        bytes.len() == FRAME_SIZE && bytes.iter().all(|&b| b == fill(number))
                    });
                    if !whole {
                        println!("torn {number}");
                    }
                    last = number;
                }
                NextBatch::Empty => break,
                NextBatch::CutOff => {
                    println!("cut off");
                    break;
                }
                _ => {}
            }
        }
        if said.elapsed() >= Duration::from_millis(50) {
            println!("frame {last}");
            said = Instant::now();
        }
        keep.tend(&orion);
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn executor(socket: &Path) -> Result<(), Failed> {
    let kind = "process".parse().map_err(|_| "a kind")?;
    let mut sample = Sample::new(vec![kind]);
    println!("executor");
    Ok(run(socket, &mut sample, || true)?)
}

fn provider(socket: &Path) -> Result<(), Failed> {
    let orion = connect(socket, "drill-provider", Roles::CLIENT.and(Roles::PROVIDER))?;
    let node = orion.node()?.name;
    let target = Target::Node(node.parse().map_err(|_| "a node name")?);
    let mut registrations = Vec::new();
    for name in ["drill.echo", "drill.stuck", "drill.crash"] {
        let decl = ActionDecl {
            name,
            target: target.clone(),
            description: "a drill",
        };
        registrations.push(orion.register_action(&decl, Policy::new(Admits::LOCAL))?);
    }
    // A resource it speaks for, kept while it runs.
    let _thing = orion
        .provider()
        .resource(THING, "drill")?
        .publish(ResourceState::Ready)?;
    println!("registered");
    let mut keep = Keep::new();
    loop {
        for registration in &mut registrations {
            while let Ok(Some(request)) = registration.poll() {
                let Request::Run {
                    call, name, args, ..
                } = request
                else {
                    continue;
                };
                println!("run {name}");
                match name.as_str() {
                    "drill.echo" => {
                        let _ = registration.finish(call, Outcome::Succeeded, &args, "");
                    }
                    "drill.crash" => std::process::exit(3),
                    _ => {}
                }
            }
        }
        keep.tend(&orion);
        std::thread::sleep(Duration::from_millis(5));
    }
}
