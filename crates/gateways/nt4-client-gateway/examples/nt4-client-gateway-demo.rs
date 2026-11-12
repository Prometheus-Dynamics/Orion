//! A comm core that joins an NT4 server as a client, a program that publishes a tank's
//! level and a program that prints what the server says of the robot. For `interop/run.sh`
//! and for a run against a real robot controller:
//!
//! ```text
//! nt4-client-gateway-demo <directory> <server host[:port]> [<NT4 name to publish> [<type@name to accept>]]
//! ```
//!
//! `tank.level:litres` is published on the server as `/Tank/level` (double), or under the
//! name given; the server's `/Robot/enabled` (boolean), or the topic given, is published
//! here as `robot.state:value`.

use std::time::Duration;

use orion::topics::{FieldId, SampleTime, TopicDecl};
use orion::{Address, Orion, fields};
use orion_comm_app::{CommCore, CommSettings};
use orion_config::Source;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(dir), Some(server)) = (args.next(), args.next()) else {
        return Err(
            "usage: nt4-client-gateway-demo <directory> <server host[:port]> \
                    [<NT4 name to publish> [<type@name to accept>]]"
                .into(),
        );
    };
    let publish = args.next().unwrap_or_else(|| "/Tank/level".to_owned());
    let accept = args
        .next()
        .unwrap_or_else(|| "boolean@/Robot/enabled".to_owned());
    let kind = accept.split('@').next().unwrap_or_default().to_owned();
    let text = format!(
        "node.name = board\nsocket = {dir}/orion.sock\nstate.dir = {dir}/state\n\
         nt4.server = {server}\nnt4.name = orion-demo\n\
         nt4.publish = tank.level:litres=double@{publish}\n\
         nt4.accept = robot.state:value={accept}\n"
    );
    let settings = CommSettings::from_source(&mut Source::parse(&text, Vec::new())?)?;
    let (ready, started) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let core = CommCore::start(settings)
                .await
                .expect("the comm core starts");
            ready.send(core.socket().to_owned()).expect("ready");
            let mut joined = false;
            loop {
                let now = core.nt4_client().is_some_and(|gateway| gateway.connected());
                if now != joined {
                    joined = now;
                    println!(
                        "{}",
                        if joined {
                            "joined the server"
                        } else {
                            "lost the server"
                        }
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    let socket = started.recv()?;

    let orion = Orion::connect(&Address::Local(socket), "tank")?;
    let mut level = orion.declare(&TopicDecl {
        name: "tank.level",
        fields: &fields![litres: Float "l"],
        history: 8,
        expiry_ms: 0,
        max_payload: 8,
    })?;
    let mut robot = orion.subscribe("robot.state")?;
    let (mut step, mut last): (u64, Option<Vec<u8>>) = (0, None);
    loop {
        step += 1;
        let litres = step as f64 * 0.5;
        level.publish(
            FieldId(0),
            SampleTime::from_nanos(step),
            &litres.to_le_bytes(),
        )?;
        if let Some(sample) = robot.latest(FieldId(0))
            && last.as_deref() != Some(sample.payload)
        {
            let shown = match (kind.as_str(), sample.payload) {
                ("boolean", [flag]) => (*flag == 1).to_string(),
                ("double" | "float", bytes) => <[u8; 8]>::try_from(bytes)
                    .map(|bytes| f64::from_le_bytes(bytes).to_string())
                    .unwrap_or_default(),
                ("int", bytes) => <[u8; 8]>::try_from(bytes)
                    .map(|bytes| i64::from_le_bytes(bytes).to_string())
                    .unwrap_or_default(),
                ("string", bytes) => String::from_utf8_lossy(bytes).into_owned(),
                (_, bytes) => format!("{} bytes", bytes.len()),
            };
            println!("robot.state value = {shown}");
            last = Some(sample.payload.to_vec());
        }
        orion.pump(Duration::from_millis(20))?;
    }
}
