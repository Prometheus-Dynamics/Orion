//! Connects to an NT4 server, subscribes to a prefix (everything by default), and prints every
//! announce, value and connection event.
//!
//! ```text
//! cargo run -p orion-nt4 --example nt4-dump -- --host 192.0.2.2 --prefix /CameraPublisher/
//! ```

use std::error::Error;

use orion_nt4::{Client, ClientConfig, ClientEvent, SubscribeOptions, TopicEvent};

/// What was asked for: `--host`, `--port`, `--prefix` (the topic-name prefix to subscribe
/// to) and `--name` (the client's name: the WebSocket path is `/nt/<name>`).
struct Args {
    host: String,
    port: u16,
    prefix: String,
    name: String,
}

fn args() -> Result<Args, Box<dyn Error>> {
    let mut args = Args {
        host: "127.0.0.1".into(),
        port: 5810,
        prefix: "/".into(),
        name: "nt4-dump".into(),
    };
    let mut given = std::env::args().skip(1);
    while let Some(flag) = given.next() {
        let value = given
            .next()
            .ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--host" => args.host = value,
            "--port" => args.port = value.parse()?,
            "--prefix" => args.prefix = value,
            "--name" => args.name = value,
            other => return Err(format!("unknown option {other}").into()),
        }
    }
    Ok(args)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = args()?;
    let mut config = ClientConfig::new(args.host, args.name);
    config.port = args.port;
    let mut client = Client::start(config);
    let mut topics = client.handle().subscribe(
        &[args.prefix.as_str()],
        SubscribeOptions {
            prefix: true,
            ..Default::default()
        },
    )?;
    loop {
        tokio::select! {
            event = client.next_event() => match event {
                Some(ClientEvent::Connected) => println!("connected"),
                Some(ClientEvent::Disconnected { reason }) => println!("disconnected: {reason}"),
                Some(ClientEvent::TimeSync { offset_us, rtt_us }) => {
                    println!("time sync: offset {offset_us} us, rtt {rtt_us} us");
                }
                Some(ClientEvent::ProtocolWarning(message)) => println!("warning: {message}"),
                None => break,
            },
            event = topics.next() => match event {
                Some(TopicEvent::Announced(info)) => {
                    println!("announce {} [{}] id={}", info.name, info.type_name, info.id);
                }
                Some(TopicEvent::Unannounced { name, .. }) => println!("unannounce {name}"),
                Some(TopicEvent::PropertiesChanged { name, update }) => {
                    println!("properties {name} {update:?}");
                }
                Some(TopicEvent::Value { name, timestamp_us, value, .. }) => {
                    println!("{name} @{timestamp_us} = {value:?}");
                }
                None => break,
            },
        }
    }
    Ok(())
}
