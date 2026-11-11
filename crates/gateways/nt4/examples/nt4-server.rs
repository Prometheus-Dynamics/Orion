//! A NetworkTables 4 server with topics you can edit, for testing cameras and robot code without
//! a robot. Not a simulator: it only holds the values you give it.
//!
//! ```text
//! cargo run -p orion-nt4 --example nt4-server -- \
//!     --topic /CameraPublisher/exposure=double:20 --topic /CameraPublisher/name=string:cam0
//! ```
//!
//! Then type `NAME VALUE` on stdin (for example `/CameraPublisher/exposure 33`); the value is
//! parsed with the topic's own type. Values are relayed to every subscribed client.

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

use orion_nt4::{Properties, Server, ServerConfig, ServerEvent, Value};
use tokio::io::{AsyncBufReadExt, BufReader};

/// What was asked for: `--bind` (the listen address), `--persist` (a JSON file to keep
/// `persistent` topics in) and any number of `--topic NAME=TYPE:VALUE` (types: boolean,
/// double, int, float, string).
struct Args {
    bind: SocketAddr,
    persist: Option<PathBuf>,
    topics: Vec<String>,
}

fn args() -> Result<Args, Box<dyn Error>> {
    let mut args = Args {
        bind: SocketAddr::from(([0, 0, 0, 0], 5810)),
        persist: None,
        topics: Vec::new(),
    };
    let mut given = std::env::args().skip(1);
    while let Some(flag) = given.next() {
        let value = given
            .next()
            .ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--bind" => args.bind = value.parse()?,
            "--persist" => args.persist = Some(value.into()),
            "--topic" => args.topics.push(value),
            other => return Err(format!("unknown option {other}").into()),
        }
    }
    Ok(args)
}

fn parse_value(type_name: &str, text: &str) -> Result<Value, Box<dyn Error>> {
    Ok(match type_name {
        "boolean" => Value::Boolean(text.parse()?),
        "double" => Value::Double(text.parse()?),
        "int" => Value::Int(text.parse()?),
        "float" => Value::Float(text.parse()?),
        "string" => Value::String(text.to_owned()),
        other => return Err(format!("the editor only takes scalar types, not {other}").into()),
    })
}

fn parse_topic(spec: &str) -> Result<(String, String, Value), Box<dyn Error>> {
    let (name, typed) = spec.split_once('=').ok_or("expected NAME=TYPE:VALUE")?;
    let (type_name, text) = typed.split_once(':').ok_or("expected NAME=TYPE:VALUE")?;
    let value = parse_value(type_name, text)?;
    Ok((name.to_owned(), type_name.to_owned(), value))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = args()?;
    let server = Server::start(ServerConfig {
        bind: args.bind,
        persist_path: args.persist,
        ..ServerConfig::default()
    })
    .await?;
    let host = server.handle();
    println!("NT4 server on {}", server.local_addr());

    for spec in &args.topics {
        let (name, type_name, value) = parse_topic(spec)?;
        host.publish(&name, &type_name, Properties::new())?;
        host.set_value(&name, value)?;
    }

    let mut events = host.events();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            match event {
                ServerEvent::ClientConnected { name, .. } => println!("client connected: {name}"),
                ServerEvent::ClientDisconnected { name, .. } => println!("client left: {name}"),
                ServerEvent::ValueChanged {
                    name,
                    value,
                    client: Some(_),
                    ..
                } => {
                    println!("{name} <- {value:?} (from a client)");
                }
                ServerEvent::ProtocolWarning { message, .. } => println!("warning: {message}"),
                _ => {}
            }
        }
    });

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let mut parts = line.splitn(2, ' ');
        let (Some(name), Some(text)) = (parts.next(), parts.next()) else {
            println!("usage: NAME VALUE");
            continue;
        };
        let Some(topic) = host.topic(name) else {
            println!("no topic {name}");
            continue;
        };
        match parse_value(&topic.type_name, text.trim()) {
            Ok(value) => match host.set_value(name, value) {
                Ok(()) => println!("{name} = {text}"),
                Err(e) => println!("{e}"),
            },
            Err(e) => println!("{e}"),
        }
    }
    Ok(())
}
