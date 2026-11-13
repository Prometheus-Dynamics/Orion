//! A provider's device appears, is read and is driven by a program with no code for it.

#![cfg(target_os = "linux")]

use std::time::{Duration, Instant};

use orion_actions::{ActionCall, Actions, Admits, Outcome, Policy, Request, Target};
use orion_connect::{Address, Client, Options, Roles, Session};
use orion_msg_topics::{Declare, Field, FieldType};
use orion_provider::{Found, Provider, ResourceState, resources};
use orion_records::Records;
use orion_stubs::Board;
use orion_topics::{Publisher, Status, Topics};
use orion_wire::Decode;
use shm_ring::{FieldId, SampleTime};

struct Side {
    records: Records,
    topics: Topics,
    actions: Actions,
    client: Client,
}

fn side(board: &Board, name: &str, roles: Roles) -> Side {
    let options = Options {
        roles,
        ..Options::default()
    };
    let session = Session::connect(&Address::Local(board.socket().to_owned()), name, options)
        .expect("connect");
    let client = Client::new(session);
    Side {
        records: Records::new(&client),
        topics: Topics::new(&client),
        actions: Actions::new(&client),
        client,
    }
}

/// The reading side. It knows the six concepts and nothing about fans: everything it shows
/// comes from what the provider wrote down.
fn show(reader: &Side, found: &Found) -> Vec<String> {
    let mut lines = vec![format!(
        "{} ({}) on {}: {:?}",
        found.name, found.kind, found.node, found.state
    )];
    for (key, value) in &found.labels {
        lines.push(format!("  {key} = {value}"));
    }
    for topic in &found.topics {
        let mut subscriber = reader.topics.subscribe(topic, &[]).expect("subscribe");
        let deadline = Instant::now() + Duration::from_secs(2);
        while subscriber.latest(FieldId(0)).is_none() && Instant::now() < deadline {
            let _ = subscriber.wait(Duration::from_millis(20)).expect("wait");
            while !matches!(subscriber.next(), shm_ring::Next::Empty) {}
        }
        // The declaration says what the fields are; the reader decides nothing.
        let fields: Vec<(String, FieldType, String)> = subscriber.with_declaration(|declaration| {
            let declare = Declare::decode(declaration.expect("declared")).expect("declaration");
            declare
                .fields
                .iter()
                .flatten()
                .map(|field| (field.name.to_owned(), field.ty, field.unit.to_owned()))
                .collect()
        });
        for (index, (name, ty, unit)) in fields.iter().enumerate() {
            let value = match subscriber.latest(FieldId(index as u16)) {
                None => "-".to_owned(),
                Some(sample) => match ty {
                    FieldType::Bool => (sample.payload.first() == Some(&1)).to_string(),
                    FieldType::Int => {
                        i64::from_le_bytes(sample.payload.try_into().expect("int")).to_string()
                    }
                    FieldType::Float => {
                        f64::from_le_bytes(sample.payload.try_into().expect("float")).to_string()
                    }
                    _ => String::from_utf8_lossy(sample.payload).into_owned(),
                },
            };
            lines.push(
                format!("  {topic}.{name} = {value} {unit}")
                    .trim_end()
                    .to_owned(),
            );
        }
    }
    for action in &found.actions {
        lines.push(format!("  can: {action}"));
    }
    lines
}

/// The provider's own code: the only place that knows what a fan is.
struct Fan {
    speed: Publisher,
    rpm: f64,
    ticks: u64,
}

impl Fan {
    fn publish(&mut self) {
        self.ticks += 1;
        let time = SampleTime::from_nanos(self.ticks);
        self.speed
            .publish(FieldId(0), time, &self.rpm.to_le_bytes())
            .expect("publish");
        self.speed
            .publish(FieldId(1), time, &[u8::from(self.rpm > 0.0)])
            .expect("publish");
    }
}

#[test]
fn a_providers_device_appears_with_no_device_code_on_the_reading_side() {
    let board = Board::start("provider");
    let owner = side(&board, "fan-driver", Roles::CLIENT.and(Roles::PROVIDER));
    let reader = side(&board, "dashboard", Roles::CLIENT);
    assert!(resources(&reader.records, "").expect("list").is_empty());

    // The provider says the fan exists, with what it reports and what it can be told to do.
    let provider = Provider::new(&owner.client, &owner.records, &owner.topics, &owner.actions);
    let mut describing = provider
        .resource("fan.left", "fan")
        .expect("name")
        .label("mounted", "left intake");
    let speed = describing
        .topic(
            "speed",
            &[
                Field {
                    name: "rpm",
                    ty: FieldType::Float,
                    unit: "rpm",
                    type_name: "",
                },
                Field {
                    name: "spinning",
                    ty: FieldType::Bool,
                    unit: "",
                    type_name: "",
                },
            ],
            8,
            8,
        )
        .expect("topic");
    describing
        .action(
            "set",
            "Sets the fan's speed, in rpm",
            Policy::new(Admits::LOCAL),
        )
        .expect("action");
    let mut resource = describing.publish(ResourceState::Ready).expect("publish");
    let mut fan = Fan {
        speed,
        rpm: 1200.0,
        ticks: 0,
    };
    fan.publish();

    // The reading side finds it and shows it, from the record and the declaration alone.
    let found = resources(&reader.records, "").expect("list");
    assert_eq!(found.len(), 1);
    assert_eq!(
        show(&reader, &found[0]),
        [
            "fan.left (fan) on board: Ready",
            "  mounted = left intake",
            "  fan.left.speed.rpm = 1200 rpm",
            "  fan.left.speed.spinning = true",
            "  can: fan.left.set",
        ]
    );

    // And drives it: the action's name came from the record, the argument is the caller's.
    let action = &found[0].actions[0];
    let mut run = reader
        .actions
        .run(&ActionCall {
            name: action,
            target: Target::Resource(found[0].name.parse().expect("name")),
            args: &0.0_f64.to_le_bytes(),
            deadline_ms: 0,
            detached: false,
        })
        .expect("run");
    // The provider's loop: take what is asked, do it, say so.
    let requests = resource.requests().expect("the fan has an action");
    let Some(Request::Run {
        call,
        name,
        args,
        target,
        ..
    }) = requests.wait(Duration::from_secs(2)).expect("wait")
    else {
        panic!("the run never reached the provider");
    };
    assert_eq!(
        (name.as_str(), target),
        (
            "fan.left.set",
            Target::Resource("fan.left".parse().expect("name"))
        )
    );
    fan.rpm = f64::from_le_bytes(args.as_slice().try_into().expect("a speed"));
    fan.publish();
    requests
        .finish(call, Outcome::Succeeded, &[], "")
        .expect("finish");
    assert_eq!(
        run.finish(Duration::from_secs(2)).expect("ended").outcome,
        Outcome::Succeeded
    );
    let lines = show(&reader, &found[0]);
    assert_eq!(
        &lines[2..4],
        [
            "  fan.left.speed.rpm = 0 rpm",
            "  fan.left.speed.spinning = false"
        ]
    );

    // Its state is the provider's to say, and anyone watching sees it change.
    resource.set_state(ResourceState::Failed).expect("state");
    assert_eq!(
        resources(&reader.records, "fan.").expect("list")[0].state,
        ResourceState::Failed
    );
    // A second resource, and a look for one kind of name.
    let mut other = provider.resource("camera.front", "camera").expect("name");
    other
        .topic(
            "exposure",
            &[Field {
                name: "ms",
                ty: FieldType::Int,
                unit: "ms",
                type_name: "",
            }],
            2,
            8,
        )
        .expect("topic");
    let _camera = other.publish(ResourceState::Pending).expect("publish");
    let names = |prefix: &str| -> Vec<String> {
        resources(&reader.records, prefix)
            .expect("list")
            .into_iter()
            .map(|found| found.name)
            .collect()
    };
    assert_eq!(names(""), ["camera.front", "fan.left"]);
    assert_eq!(names("camera."), ["camera.front"]);

    // Gone is gone, and so are its topics: their readers hear the publisher is gone, and the
    // provider's publisher of them can publish no more.
    let mut speed = reader
        .topics
        .subscribe("fan.left.speed", &[])
        .expect("subscribe");
    resource.withdraw().expect("withdraw");
    assert_eq!(names(""), ["camera.front"]);
    let deadline = Instant::now() + Duration::from_secs(5);
    while speed.status() != Status::PublisherGone {
        assert!(Instant::now() < deadline, "the reader never heard");
        speed.wait(Duration::from_millis(20)).expect("wait");
    }
    assert!(fan.speed.withdrawn());
    assert!(provider.resource("not a name", "fan").is_err());
}
