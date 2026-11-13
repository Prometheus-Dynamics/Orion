//! A first program: it publishes a value on a topic, reads it back as another program would,
//! and asks the board who it is.
//!
//! With a comm core running (`orion comm FILE`, docs/getting-started.md):
//!
//! ```text
//! ORION_SOCKET=/tmp/orion/orion.sock cargo run -p orion --example first_program
//! ```

use std::path::PathBuf;
use std::time::Duration;

use orion::topics::{FieldId, Next, SampleTime, TopicDecl};
use orion::{Address, Error, Orion, fields};

fn main() -> Result<(), Error> {
    let socket = std::env::var_os("ORION_SOCKET")
        .map_or_else(|| PathBuf::from("/run/orion/orion.sock"), PathBuf::from);
    let address = Address::Local(socket);

    // A sensor declares its topic: a name, its fields, how many samples are kept.
    let sensor = Orion::connect(&address, "tank-sensor")?;
    let mut level = sensor.declare(&TopicDecl {
        name: "tank.level",
        fields: &fields![litres: Float "l"],
        history: 64,
        expiry_ms: 0,
        max_payload: 8,
        on_subscribe: Default::default(),
    })?;

    // A dashboard subscribes. It reads the sensor's ring directly: nothing is copied.
    let dashboard = Orion::connect(&address, "dashboard")?;
    let mut reading = dashboard.subscribe("tank.level")?;

    for litres in [40.0_f64, 41.5, 43.0] {
        level.publish(FieldId(0), SampleTime::from_nanos(0), &litres.to_le_bytes())?;
    }
    reading.wait(Duration::from_secs(2))?;
    while let Next::Sample(sample) = reading.next() {
        let litres = f64::from_le_bytes(sample.payload.try_into().unwrap_or_default());
        println!("tank.level: {litres} l");
    }

    let board = dashboard.node()?;
    println!("the board is {}", board.name);
    Ok(())
}
