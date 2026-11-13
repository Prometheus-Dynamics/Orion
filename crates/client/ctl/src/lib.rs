//! The Orion command line.
//!
//! One tool for a person at a keyboard, on the one client: what it can do to a board on the
//! bench it can do to one across the network. Its verbs follow the six things Orion is made
//! of:
//!
//! ```text
//! orion [--address A] [--name N] [--timeout MS] <concept> <verb> ...
//!
//! node      show                      who the board is, checked against its signature
//! node      peers                     the nodes it knows
//! node      time                      how its clock compares with this computer's
//! node      sync                      where its sync with each other node is
//! node      forgive PEER              lets a stale peer sync again
//! resource  list [PREFIX]             the things that exist
//! resource  show NAME                 one of them: its topics, its actions, its labels
//! topic     read TOPIC                the newest value of each field
//! topic     watch TOPIC [--count N] [--for MS]
//! topic     publish TOPIC NAME:TYPE=VALUE... [--hold MS]
//! action    run NAME [--node N | --resource R] [--args TEXT]
//!                    [--deadline MS [--detached]] [--for MS]
//! action    attach CALL [--for MS]
//! workload  put NAME --kind K [--spec TEXT | --spec-file PATH] [--node N]
//!                    [--restart always|on-failure|never] [--for MS]
//! workload  remove NAME [--force] [--for MS]
//! file      push DESTINATION PATH
//! file      pull DESTINATION URL --size BYTES --sha256 HEX [--for MS]
//! ```
//!
//! [`run`] is the whole tool as a function: arguments in, text out, an exit code back. That
//! is what lets it be a mode of the one binary, and what its tests call.

mod args;
mod operator;
mod value;

use std::io::Write;
use std::time::{Duration, Instant};

use orion::actions::{ActionCall, Outcome, Target, Update};
use orion::files::{FileState, Pull};
use orion::records::RecordClass;
use orion::topics::{FieldId, Next, SampleTime, Status, Subscriber, TopicDecl};
use orion::workloads::{RestartPolicy, Workload, WorkloadState};
use orion::{Address, Error, Options, Orion};
use orion_msg_records::NodeRecord;
use orion_msg_topics::{Declare, Field, FieldType};
use orion_wire::Decode;

use args::{Args, Usage};

/// The lines printed for `help`, and after a command that was not understood.
pub const USAGE: &str = "\
usage: orion [--address A] [--name N] [--timeout MS] [--key FILE] <concept> <verb> ...

  node      show | peers | time | sync | forgive PEER
  resource  list [PREFIX] | show NAME
  topic     read TOPIC | watch TOPIC [--count N] [--for MS]
            publish TOPIC NAME:TYPE=VALUE... [--hold MS]
  action    run NAME [--node N | --resource R] [--args TEXT]
            [--deadline MS [--detached]] [--for MS]
            attach CALL [--for MS]
  workload  put NAME --kind K [--spec TEXT | --spec-file PATH] [--node N]
            [--restart always|on-failure|never] [--for MS]
            remove NAME [--force] [--for MS]
  file      push DESTINATION PATH
            pull DESTINATION URL --size BYTES --sha256 HEX [--for MS]
  operator  ask --as NAME [--enrollment-key-file PATH]   (needs --key: enrolls that key)
            enroll KEY --as NAME                          (root on the board, any key)

An address is a socket path, orion+tcp://host[:port] or serial:<path>[@baud].
Without one, the board's own socket. --key FILE is this operator's key, made there if it is
not: a network session is signed with it.";

/// Why a command did not do what was asked.
pub(crate) enum Failed {
    /// The command line cannot be understood.
    Usage(String),
    /// The client failed, or the board said no.
    Client(Error),
    /// It was done, and the answer is that it did not work. Already printed.
    Unsuccessful,
}

impl From<Usage> for Failed {
    fn from(usage: Usage) -> Self {
        Self::Usage(usage.0)
    }
}

impl From<Error> for Failed {
    fn from(error: Error) -> Self {
        Self::Client(error)
    }
}

impl From<orion::ClientError> for Failed {
    fn from(error: orion::ClientError) -> Self {
        Self::Client(error.into())
    }
}

impl From<orion::topics::TopicError> for Failed {
    fn from(error: orion::topics::TopicError) -> Self {
        Self::Client(error.into())
    }
}

impl From<std::io::Error> for Failed {
    fn from(error: std::io::Error) -> Self {
        Self::Client(Error::Client(error.into()))
    }
}

pub(crate) type Done = Result<(), Failed>;

/// Runs one command. `args` are the words after the program's name. What a person is to read
/// goes to `out`, what went wrong to `err`. Returns the exit code: 0 done, 1 not done, 2 not
/// understood.
pub fn run(args: &[String], out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    match command(args, out) {
        Ok(()) => 0,
        Err(Failed::Unsuccessful) => 1,
        Err(Failed::Client(error)) => {
            let _ = writeln!(err, "orion: {error}");
            1
        }
        Err(Failed::Usage(why)) => {
            let _ = writeln!(err, "orion: {why}\n\n{USAGE}");
            2
        }
    }
}

fn command(words: &[String], out: &mut dyn Write) -> Done {
    let mut args = Args::new(words);
    let address = match args.option("--address")? {
        Some(text) => text.parse::<Address>().map_err(Failed::Usage)?,
        None => Address::default(),
    };
    let name = args
        .option("--name")?
        .unwrap_or_else(|| "orion-ctl".to_owned());
    let timeout = Duration::from_millis(args.number("--timeout")?.unwrap_or(5000));
    let identity = match args.option("--key")? {
        Some(path) => Some(orion::Identity::load_or_create(std::path::Path::new(
            &path,
        ))?),
        None => None,
    };
    let concept = args.word("a concept: node, resource, topic, action, workload or file")?;
    if concept == "help" {
        writeln!(out, "{USAGE}")?;
        return Ok(());
    }
    let verb = args.word("a verb")?;
    if concept == "operator" {
        let own = identity.as_ref().map(orion::Identity::public);
        let check = operator::understood(&verb, own)?;
        let options = Options {
            timeout,
            identity,
            ..Options::default()
        };
        let orion = Orion::connect_with(&address, &name, options)?;
        return operator::run(&orion, check, &mut args, out);
    }
    let run: fn(&Orion, &mut Args, &mut dyn Write) -> Done = match (concept.as_str(), verb.as_str())
    {
        ("node", "show") => node_show,
        ("node", "peers") => node_peers,
        ("node", "time") => node_time,
        ("node", "sync") => node_sync,
        ("node", "forgive") => node_forgive,
        ("resource", "list") => resource_list,
        ("resource", "show") => resource_show,
        ("topic", "read") => topic_read,
        ("topic", "watch") => topic_watch,
        ("topic", "publish") => topic_publish,
        ("action", "run") => action_run,
        ("action", "attach") => action_attach,
        ("workload", "put") => workload_put,
        ("workload", "remove") => workload_remove,
        ("file", "push") => file_push,
        ("file", "pull") => file_pull,
        _ => return Err(Failed::Usage(format!("there is no `{concept} {verb}`"))),
    };
    // Understood before anything is asked of a board: a mistake costs no connection.
    let options = Options {
        timeout,
        identity,
        ..Options::default()
    };
    let orion = Orion::connect_with(&address, &name, options)?;
    run(&orion, &mut args, out)
}

fn wait_option(args: &mut Args, default_ms: u64) -> Result<Duration, Usage> {
    Ok(Duration::from_millis(
        args.number("--for")?.unwrap_or(default_ms),
    ))
}

fn node_show(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    args.done()?;
    let node = orion.node()?;
    writeln!(out, "node        {}", node.name)?;
    writeln!(out, "cluster     {}", node.cluster)?;
    writeln!(out, "key         {}", node.fingerprint)?;
    writeln!(out, "revision    {}", node.revision)?;
    for fact in &node.facts {
        let ty = FieldType::from_code(fact.ty);
        writeln!(out, "{:<11} {}", fact.name, value::show(ty, &fact.value))?;
    }
    Ok(())
}

fn node_peers(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    args.done()?;
    for record in orion.records().get(RecordClass::Node, "")? {
        let Ok(node) = NodeRecord::decode(&record.value) else {
            writeln!(out, "{}  (a record this build cannot read)", record.key)?;
            continue;
        };
        let labels: Vec<String> = node
            .labels
            .iter()
            .flatten()
            .map(|label| format!("{}={}", label.key, label.value))
            .collect();
        writeln!(out, "{}  {}  {}", node.name, node.address, labels.join(" ")).map(|()| ())?;
    }
    Ok(())
}

fn node_time(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    args.done()?;
    let clock = orion.clock()?;
    let (offset_ms, delay_ms) = (clock.offset_ns as f64 / 1e6, clock.delay_ns as f64 / 1e6);
    let which = if offset_ms >= 0.0 {
        "ahead of"
    } else {
        "behind"
    };
    writeln!(
        out,
        "the board's clock is {:.3} ms {which} this computer's (round trip {delay_ms:.3} ms)",
        offset_ms.abs()
    )?;
    Ok(())
}

fn node_sync(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    args.done()?;
    sync_peers(&orion.sync_peers()?, out)
}

fn node_forgive(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let peer = args.word("the peer to forgive")?;
    args.done()?;
    let peers = orion.forgive(&peer)?;
    writeln!(
        out,
        "{peer} is forgiven: what was deleted while it was away may come back. It has to forgive this node too."
    )?;
    sync_peers(&peers, out)
}

fn sync_peers(peers: &[orion::node::SyncPeer], out: &mut dyn Write) -> Done {
    for peer in peers {
        let agreed = peer.agreed_ms.map_or_else(
            || "never agreed".to_owned(),
            |at| format!("agreed at {at} ms"),
        );
        writeln!(out, "{}  {:?}  {agreed}", peer.node, peer.state)?;
    }
    Ok(())
}

fn resource_list(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let prefix = args.maybe_word().unwrap_or_default();
    args.done()?;
    for found in orion.resources(&prefix)? {
        writeln!(
            out,
            "{}  {}  {:?}  on {}",
            found.name, found.kind, found.state, found.node
        )?;
    }
    Ok(())
}

fn resource_show(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let name = args.word("a resource's name")?;
    args.done()?;
    let Some(found) = orion
        .resources(&name)?
        .into_iter()
        .find(|found| found.name == name)
    else {
        writeln!(out, "there is no resource {name}")?;
        return Err(Failed::Unsuccessful);
    };
    writeln!(
        out,
        "{}  {}  {:?}  on {}",
        found.name, found.kind, found.state, found.node
    )?;
    for (key, value) in &found.labels {
        writeln!(out, "  {key} = {value}")?;
    }
    for topic in &found.topics {
        writeln!(out, "  topic   {topic}")?;
    }
    for action in &found.actions {
        writeln!(out, "  action  {action}")?;
    }
    Ok(())
}

/// A topic's fields, from its declaration: name, type and unit.
fn fields_of(subscriber: &Subscriber) -> Vec<(String, FieldType, String)> {
    subscriber.with_declaration(|declaration| {
        declaration
            .and_then(|declaration| Declare::decode(declaration).ok())
            .map(|declare| {
                declare
                    .fields
                    .iter()
                    .flatten()
                    .map(|field| (field.name.to_owned(), field.ty, field.unit.to_owned()))
                    .collect()
            })
            .unwrap_or_default()
    })
}

fn line(name: &str, ty: FieldType, unit: &str, payload: &[u8]) -> String {
    format!("{name} = {} {unit}", value::show(ty, payload))
        .trim_end()
        .to_owned()
}

/// Subscribes, and waits for the topic to exist.
fn subscribe(orion: &Orion, topic: &str, wait: Duration) -> Result<Subscriber, Failed> {
    let mut subscriber = orion.subscribe(topic)?;
    let deadline = Instant::now() + wait;
    while subscriber.status() == Status::Waiting && Instant::now() < deadline {
        subscriber.wait(Duration::from_millis(20))?;
    }
    Ok(subscriber)
}

fn topic_read(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let topic = args.word("a topic")?;
    args.done()?;
    let mut subscriber = subscribe(orion, &topic, Duration::from_millis(500))?;
    if subscriber.status() == Status::Waiting {
        writeln!(out, "nobody has declared {topic}")?;
        return Err(Failed::Unsuccessful);
    }
    for (index, (name, ty, unit)) in fields_of(&subscriber).iter().enumerate() {
        match subscriber.latest(FieldId(index as u16)) {
            Some(sample) => writeln!(out, "{}", line(name, *ty, unit, sample.payload))?,
            None => writeln!(out, "{name} has no value yet")?,
        }
    }
    // A topic with no publisher is answered with its ring and then a word that the publisher
    // is gone: take in what has already come before saying which it is.
    subscriber.wait(Duration::ZERO)?;
    if subscriber.status() == Status::PublisherGone {
        writeln!(out, "(its publisher is gone; these are the last values)")?;
    }
    Ok(())
}

fn topic_watch(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let topic = args.word("a topic")?;
    let count = args.number("--count")?.unwrap_or(u64::MAX);
    let wait = wait_option(args, 10_000)?;
    args.done()?;
    let mut subscriber = orion.subscribe(&topic)?;
    let deadline = Instant::now() + wait;
    let (mut seen, mut fields) = (0_u64, Vec::new());
    while seen < count {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        subscriber.wait(left)?;
        loop {
            if fields.is_empty() {
                fields = fields_of(&subscriber);
            }
            match subscriber.next() {
                Next::Empty => break,
                Next::Skipped(lost) => {
                    writeln!(out, "({lost} samples were lost: this reader was too slow)")?
                }
                Next::Sample(sample) => {
                    let named = fields.get(usize::from(sample.field.0));
                    let text = match named {
                        Some((name, ty, unit)) => line(name, *ty, unit, sample.payload),
                        None => {
                            format!("field {} = {}", sample.field.0, value::hex(sample.payload))
                        }
                    };
                    writeln!(out, "{} {text}", sample.time.as_nanos())?;
                    seen += 1;
                    if seen >= count {
                        break;
                    }
                }
            }
        }
    }
    if seen == 0 {
        writeln!(out, "nothing was published to {topic}")?;
        return Err(Failed::Unsuccessful);
    }
    Ok(())
}

fn topic_publish(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let topic = args.word("a topic")?;
    let hold = Duration::from_millis(args.number("--hold")?.unwrap_or(0));
    let given = args.rest();
    if given.is_empty() {
        return Err(Failed::Usage("missing fields: name:type=value".into()));
    }
    let parsed: Vec<(String, FieldType, Vec<u8>)> = given
        .iter()
        .map(|text| value::field(text))
        .collect::<Result<_, _>>()
        .map_err(Failed::Usage)?;
    let fields: Vec<Field<'_>> = parsed
        .iter()
        .map(|(name, ty, _)| Field {
            name,
            ty: *ty,
            unit: "",
            type_name: "",
        })
        .collect();
    let largest = parsed
        .iter()
        .map(|(.., bytes)| bytes.len())
        .max()
        .unwrap_or(8)
        .max(8);
    let mut publisher = orion.declare(&TopicDecl {
        name: &topic,
        fields: &fields,
        history: 16,
        expiry_ms: 0,
        max_payload: largest as u32,
        on_subscribe: Default::default(),
    })?;
    let time = SampleTime::now();
    for (index, (.., bytes)) in parsed.iter().enumerate() {
        publisher.publish(FieldId(index as u16), time, bytes)?;
    }
    writeln!(
        out,
        "published {} to {topic}",
        if parsed.len() == 1 {
            "1 field".to_owned()
        } else {
            format!("{} fields", parsed.len())
        }
    )?;
    // A topic lives while someone publishes or reads it. Holding gives a reader time to come.
    let until = Instant::now() + hold;
    while Instant::now() < until {
        orion.pump(
            until
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(50)),
        )?;
    }
    Ok(())
}

/// Follows a run to its end, printing what it says, for up to `wait`.
fn follow(mut run: orion::actions::Run, wait: Duration, out: &mut dyn Write) -> Done {
    let deadline = Instant::now() + wait;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match run.wait(left)? {
            Some(Update::Doing(doing)) => {
                let percent = if doing.percent == 255 {
                    String::new()
                } else {
                    format!("{}% ", doing.percent)
                };
                writeln!(out, "{percent}{}", doing.message)?;
            }
            Some(Update::Finished(finished)) => {
                let output = String::from_utf8_lossy(&finished.output);
                let said = [output.as_ref(), finished.detail.as_str()]
                    .iter()
                    .filter(|text| !text.is_empty())
                    .copied()
                    .collect::<Vec<_>>()
                    .join(": ");
                writeln!(
                    out,
                    "{:?}{}{said}",
                    finished.outcome,
                    if said.is_empty() { "" } else { ": " }
                )?;
                return if finished.outcome == Outcome::Succeeded {
                    Ok(())
                } else {
                    Err(Failed::Unsuccessful)
                };
            }
            None => {
                writeln!(
                    out,
                    "still running as call {}; `action attach {}` comes back to it",
                    run.call(),
                    run.call()
                )?;
                return Err(Failed::Unsuccessful);
            }
        }
    }
}

fn action_run(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let name = args.word("an action's name")?;
    let on_node = args.option("--node")?;
    let on_resource = args.option("--resource")?;
    let text = args.option("--args")?.unwrap_or_default();
    let detached = args.flag("--detached");
    let deadline_ms = args.number("--deadline")?.unwrap_or(0);
    let wait = wait_option(args, 30_000)?;
    args.done()?;
    if detached && deadline_ms == 0 {
        // The board would refuse it: a run that outlives its caller must end by itself.
        return Err(Failed::Usage("--detached needs --deadline MS".into()));
    }
    let not_a_name = |what: &str| Failed::Usage(format!("{what:?} is not a name"));
    let target = match (on_node, on_resource) {
        (Some(_), Some(_)) => {
            return Err(Failed::Usage(
                "--node and --resource are one or the other".into(),
            ));
        }
        (Some(node), None) => Target::Node(node.parse().map_err(|_| not_a_name(&node))?),
        (None, Some(resource)) => {
            Target::Resource(resource.parse().map_err(|_| not_a_name(&resource))?)
        }
        // Asked of nothing in particular: the board being spoken to.
        (None, None) => Target::Node(orion.welcomed().node),
    };
    let run = orion.run(&ActionCall {
        name: &name,
        target,
        args: text.as_bytes(),
        deadline_ms,
        detached,
    })?;
    if detached {
        writeln!(out, "call {}", run.call())?;
    }
    follow(run, wait, out)
}

fn action_attach(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let call = args.word("a call's number")?;
    let call: u64 = call
        .parse()
        .map_err(|_| Failed::Usage(format!("{call:?} is not a call's number")))?;
    let wait = wait_option(args, 30_000)?;
    args.done()?;
    follow(orion.attach(call)?, wait, out)
}

/// Prints where a workload is as it changes, until `done` says it is where it was wanted or
/// `wait` has passed.
fn track(
    orion: &Orion,
    name: &str,
    wait: Duration,
    out: &mut dyn Write,
    done: impl Fn(WorkloadState) -> bool,
) -> Done {
    let deadline = Instant::now() + wait;
    let mut last = None;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let next = orion
            .workloads()
            .wait(name, left, |tracked| Some(tracked.state) != last)?;
        let Some(tracked) = next else {
            writeln!(out, "{name} did not get there in time")?;
            return Err(Failed::Unsuccessful);
        };
        let place = tracked
            .node
            .as_deref()
            .map(|node| format!(" on {node}"))
            .unwrap_or_default();
        let why = if tracked.detail.is_empty() {
            String::new()
        } else {
            format!(": {}", tracked.detail)
        };
        writeln!(out, "{name} is {:?}{place}{why}", tracked.state)?;
        if done(tracked.state) {
            return Ok(());
        }
        if matches!(tracked.state, WorkloadState::Failed | WorkloadState::Lost) {
            return Err(Failed::Unsuccessful);
        }
        last = Some(tracked.state);
    }
}

fn workload_put(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let name = args.word("a workload's name")?;
    let kind = args
        .option("--kind")?
        .ok_or_else(|| Failed::Usage("missing --kind".into()))?;
    let spec = match (args.option("--spec")?, args.option("--spec-file")?) {
        (Some(_), Some(_)) => {
            return Err(Failed::Usage(
                "--spec and --spec-file are one or the other".into(),
            ));
        }
        (Some(text), None) => text.into_bytes(),
        (None, Some(path)) => {
            std::fs::read(&path).map_err(|error| Failed::Usage(format!("{path}: {error}")))?
        }
        (None, None) => Vec::new(),
    };
    let node = args.option("--node")?;
    let restart = match args.option("--restart")?.as_deref() {
        None | Some("on-failure") => RestartPolicy::OnFailure,
        Some("always") => RestartPolicy::Always,
        Some("never") => RestartPolicy::Never,
        Some(other) => {
            return Err(Failed::Usage(format!(
                "--restart is always, on-failure or never, not {other:?}"
            )));
        }
    };
    let wait = wait_option(args, 10_000)?;
    args.done()?;
    orion.workloads().put(&Workload {
        name: &name,
        kind: &kind,
        spec: &spec,
        node: node.as_deref(),
        restart,
    })?;
    track(orion, &name, wait, out, |state| {
        state == WorkloadState::Running
    })
}

fn workload_remove(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let name = args.word("a workload's name")?;
    let force = args.flag("--force");
    let wait = wait_option(args, 10_000)?;
    args.done()?;
    orion.workloads().remove(&name, force)?;
    track(orion, &name, wait, out, |state| {
        state == WorkloadState::Gone
    })
}

fn file_push(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let destination = args.word("a destination")?;
    let path = args.word("a file")?;
    args.done()?;
    let mut file =
        std::fs::File::open(&path).map_err(|error| Failed::Usage(format!("{path}: {error}")))?;
    let ended = orion.files().push(&destination, &mut file, |_, _| {})?;
    finished_transfer(&destination, &ended, out)
}

fn finished_transfer(
    destination: &str,
    ended: &orion::files::Transfer,
    out: &mut dyn Write,
) -> Done {
    if ended.state == FileState::Committed {
        writeln!(out, "{destination} is in place, {} bytes", ended.received)?;
        Ok(())
    } else {
        writeln!(out, "{destination} was rejected: {}", ended.detail)?;
        Err(Failed::Unsuccessful)
    }
}

fn file_pull(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let destination = args.word("a destination")?;
    let url = args.word("a URL")?;
    let size = args
        .number("--size")?
        .ok_or_else(|| Failed::Usage("missing --size".into()))?;
    let sum = args
        .option("--sha256")?
        .ok_or_else(|| Failed::Usage("missing --sha256".into()))?;
    let digest: [u8; 32] = value::unhex(&sum)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| Failed::Usage("--sha256 is 64 hex digits".into()))?;
    let wait = wait_option(args, 600_000)?;
    args.done()?;
    // The same file to the same place is the same transfer, so asking again follows it.
    let transfer = u64::from_le_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ]);
    let ended = orion.files().pull(
        &Pull {
            transfer,
            url: &url,
            destination: &destination,
            size,
            digest,
        },
        wait,
        |_, _| {},
    )?;
    finished_transfer(&destination, &ended, out)
}
