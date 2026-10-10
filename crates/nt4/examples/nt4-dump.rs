//! Connects to an NT4 server, subscribes to a prefix (everything by default), and prints every
//! announce, value and connection event.
//!
//! ```text
//! cargo run -p orion-nt4 --example nt4-dump -- --host 10.0.0.2 --prefix /CameraPublisher/
//! ```

use std::error::Error;

use clap::Parser;
use orion_nt4::{Client, ClientConfig, ClientEvent, SubscribeOptions, TopicEvent};

#[derive(Parser)]
struct Args {
    /// Server host name or address.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Server port.
    #[arg(long, default_value_t = 5810)]
    port: u16,
    /// Topic-name prefix to subscribe to.
    #[arg(long, default_value = "/")]
    prefix: String,
    /// Client name (WebSocket path /nt/<name>).
    #[arg(long, default_value = "nt4-dump")]
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
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
