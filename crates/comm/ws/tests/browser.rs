//! A real browser: a page opens a WebSocket to the comm core, says hello, asks who the board
//! is and reads the answer. If no browser is installed the test says so and passes: the
//! protocol itself is tested in `ws.rs` without one.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::rc::Rc;
use std::sync::mpsc;
use std::time::Duration;

use orion_broker::{Broker, BrokerConfig};
use orion_hello::Hello as HelloService;
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_msg_session::{Hello, Identify};
use orion_server::{Comm, Limits, Roles, Security};
use orion_wire::{Body, Bytes, INTERFACE_REVISION, encode_body, kind};
use orion_ws::{WsCarrier, WsListener, WsOptions};

fn encode<B: Body>(body: &B, seq: u16) -> Vec<u8> {
    let mut out = vec![0_u8; 1024];
    let len = encode_body(body, seq, &mut out).expect("encode");
    out.truncate(len);
    out
}

fn js_bytes(bytes: &[u8]) -> String {
    let items: Vec<String> = bytes.iter().map(u8::to_string).collect();
    format!("new Uint8Array([{}])", items.join(","))
}

/// The page: it sends the two frames it is given and reports the kinds it was answered
/// with to the server it came from.
fn page(ws: SocketAddr) -> String {
    let hello = encode(
        &Hello {
            name: "page",
            roles: Roles::CLIENT,
            max_frame: 64 * 1024,
            revision: INTERFACE_REVISION,
        },
        1,
    );
    let identify = encode(
        &Identify {
            nonce: Bytes(b"from a browser"),
        },
        2,
    );
    format!(
        r#"<!doctype html><meta charset="utf-8"><title>orion</title><script>
const hello = {hello};
const identify = {identify};
const got = [];
const done = (what) => fetch("/done?" + what).catch(() => {{}});
const ws = new WebSocket("ws://{ws}/orion", "orion.v1");
ws.binaryType = "arraybuffer";
ws.onopen = () => ws.send(hello);
ws.onerror = () => done("error=socket");
ws.onmessage = (event) => {{
  if (typeof event.data === "string") {{ done("error=text"); return; }}
  const bytes = new Uint8Array(event.data);
  const kind = bytes[1];
  if (kind === {ping} || kind === {pong}) return;
  got.push(kind);
  if (kind === {welcome}) ws.send(identify);
  if (kind === {identity}) {{
    const text = new TextDecoder().decode(bytes);
    done("kinds=" + got.join(".") + "&protocol=" + ws.protocol
      + "&board=" + (text.includes("board") ? "yes" : "no")).then(() => ws.close());
  }}
}};
</script>"#,
        hello = js_bytes(&hello),
        identify = js_bytes(&identify),
        ping = kind::PING,
        pong = kind::PONG,
        welcome = kind::WELCOME,
        identity = kind::IDENTITY,
    )
}

/// Serves the page, and takes its report.
fn serve(listener: TcpListener, page: String, report: mpsc::Sender<String>) {
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let mut first = String::new();
            let _ = reader.read_line(&mut first);
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap_or(0) != 0 && line != "\r\n" {
                line.clear();
            }
            let path = first.split(' ').nth(1).unwrap_or("/").to_owned();
            let body = if let Some(query) = path.strip_prefix("/done?") {
                let _ = report.send(query.to_owned());
                String::from("ok")
            } else if path == "/" {
                page.clone()
            } else {
                String::new()
            };
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
}

/// A headless browser on `url`, if one is installed.
fn browse(url: &str, profile: &PathBuf) -> Option<Child> {
    let firefox = |program: &str| {
        Command::new(program)
            .args(["--headless", "--no-remote", "--new-instance", "--profile"])
            .arg(profile)
            .arg(url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()
    };
    let chrome = |program: &str| {
        Command::new(program)
            .args([
                "--headless=new",
                "--disable-gpu",
                "--no-first-run",
                "--no-sandbox",
            ])
            .arg(format!("--user-data-dir={}", profile.display()))
            .arg(url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()
    };
    firefox("firefox")
        .or_else(|| chrome("chromium"))
        .or_else(|| chrome("chromium-browser"))
        .or_else(|| chrome("google-chrome"))
}

#[test]
fn a_browser_connects_and_exchanges_frames() {
    // The page's server first: its address is the page's origin, which the comm core is
    // told is the one page that may connect.
    let http = TcpListener::bind("127.0.0.1:0").expect("listen");
    let origin = format!("http://{}", http.local_addr().expect("addr"));

    let (ready, started) = mpsc::channel();
    let allowed = origin.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let keys = SharedKeys::new(
                Keys::open(KeysConfig::in_memory("lab"), Security::Off).expect("keys"),
            );
            let node: orion_types::NodeName = "board".parse().expect("name");
            let comm = Comm::new(
                node.clone(),
                Security::Off,
                Some(Rc::new(keys.clone())),
                Limits::default(),
            );
            let broker = Broker::new(node.clone(), BrokerConfig::default());
            comm.add_service(HelloService::new(node, keys, Some(broker.topics())));
            comm.add_service(broker);
            let options = WsOptions {
                origins: vec![allowed],
                ..WsOptions::default()
            };
            let mut listener = WsListener::bind("127.0.0.1:0".parse().expect("addr"), options)
                .await
                .expect("listen");
            ready
                .send(listener.local_addr().expect("addr"))
                .expect("ready");
            let carrier = WsCarrier::on(&mut listener);
            tokio::task::spawn_local(listener.run());
            comm.serve(carrier).await;
        });
    });
    let ws = started.recv().expect("the comm core starts");

    let (report, reported) = mpsc::channel();
    serve(http, page(ws), report);

    let profile = std::env::temp_dir().join(format!("orion-ws-browser-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&profile);
    std::fs::create_dir_all(&profile).expect("profile");
    let Some(mut browser) = browse(&format!("{origin}/"), &profile) else {
        println!("no browser is installed: the browser test was not run");
        return;
    };
    let result = reported.recv_timeout(Duration::from_secs(90));
    let _ = browser.kill();
    let _ = browser.wait();
    let _ = std::fs::remove_dir_all(&profile);
    let result = result.expect("the page reported within ninety seconds");
    println!("the page reported: {result}");
    // Welcomed, then answered who the board is, over the subprotocol it asked for.
    assert_eq!(
        result,
        format!(
            "kinds={}.{}&protocol=orion.v1&board=yes",
            kind::WELCOME,
            kind::IDENTITY
        )
    );
}
