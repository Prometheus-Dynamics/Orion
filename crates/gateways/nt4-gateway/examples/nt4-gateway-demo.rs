//! A comm core with the NT4 gateway, a program that publishes a tank's level and a program
//! that prints what NT4 clients write back. For `interop/run.sh`, and to try an NT4 client
//! against by hand:
//!
//! ```text
//! nt4-gateway-demo <directory> <NT4 port>
//! ```
//!
//! `tank.level:litres` is the NT4 topic `/Tank/level` (double); the NT4 topic
//! `/Panel/setpoint` (double) is published here as `panel.setpoint:value`.

use std::time::Duration;

use orion::topics::{FieldId, FrameTime, Next, TopicDecl};
use orion::{Address, Orion, fields};
use orion_comm_app::{CommCore, CommSettings};
use orion_config::Source;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(dir), Some(port)) = (args.next(), args.next()) else {
        return Err("usage: nt4-gateway-demo <directory> <NT4 port>".into());
    };
    let port: u16 = port.parse()?;
    let text = format!(
        "node.name = board\nsocket = {dir}/orion.sock\nstate.dir = {dir}/state\n\
         nt4.listen = 127.0.0.1:{port}\n\
         nt4.publish = tank.level:litres=double@/Tank/level\n\
         nt4.accept = panel.setpoint:value=double@/Panel/setpoint\n"
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
            std::future::pending::<()>().await;
        });
    });
    let socket = started.recv()?;
    println!("NT4 gateway on 127.0.0.1:{port}");

    let orion = Orion::connect(&Address::Local(socket), "tank")?;
    let mut level = orion.declare(&TopicDecl {
        name: "tank.level",
        fields: &fields![litres: Float "l"],
        history: 8,
        expiry_ms: 0,
        max_payload: 8,
    })?;
    let mut panel = orion.subscribe("panel.setpoint")?;
    let mut step = 0_u64;
    loop {
        step += 1;
        let litres = step as f64 * 0.5;
        level.publish(
            FieldId(0),
            FrameTime::from_nanos(step),
            &litres.to_le_bytes(),
        )?;
        while let Next::Sample(sample) = panel.next() {
            if let Ok(bytes) = <[u8; 8]>::try_from(sample.payload) {
                println!("panel.setpoint value = {}", f64::from_le_bytes(bytes));
            }
        }
        orion.pump(Duration::from_millis(20))?;
    }
}
