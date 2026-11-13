//! The web client in a headless browser against a comm core: it subscribes to what a program
//! on the board publishes, publishes what a program on the board reads, and runs an action a
//! program on the board does. If no browser is installed the test says so and passes.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use orion::actions::{ActionDecl, Admits, Outcome, Policy, Request, Target};
use orion::topics::{FieldId, Next, SampleTime, TopicDecl};
use orion::{Address, Orion, fields};
use orion_stubs::Board;
use orion_web_tests::browser::browse;
use sha2::{Digest, Sha256};

fn client_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../web/client")
}

/// The page: everything the client does, once, and a report of what came of it.
fn page(ws: std::net::SocketAddr) -> String {
    format!(
        r#"<!doctype html><meta charset="utf-8"><title>orion web client</title>
<script type="module">
import {{ Orion, FieldType, Status, Outcome, Refused, crc32c }} from "/orion.js";
const report = (what) => fetch("/done?" + what).catch(() => {{}});
const until = async (done) => {{
  for (let tries = 0; tries < 2000 && !done(); tries++) await new Promise((next) => setTimeout(next, 5));
  if (!done()) throw new Error("it never happened");
}};
try {{
  // The checksum, against the standard's own example.
  const check = crc32c(new TextEncoder().encode("123456789")).toString(16);

  const orion = await Orion.connect("ws://{ws}/orion", "page");

  // Subscribe: what a program on the board publishes arrives here, with its declaration.
  const seen = [];
  const level = await orion.subscribe("tank.level", (sample) => {{
    const litres = new DataView(sample.payload.buffer, sample.payload.byteOffset).getFloat64(0, true);
    seen.push([sample.field, sample.time, litres]);
  }});
  await until(() => seen.length >= 3 && level.declaration !== null);
  const field = level.declaration.fields[0];
  const declared = [level.declaration.name, field.name, field.type, field.unit, level.declaration.history].join(":");
  // In order, each a step after the one before, with the time it was published under.
  const first = seen[0];
  const steady = seen.slice(0, 3).every(([which, time, litres], index) =>
    which === 0 && time === first[1] + BigInt(index) && litres === first[2] + 0.5 * index);

  // Publish: a program on the board reads what this page publishes.
  const clicks = await orion.declare({{
    name: "page.clicks",
    fields: [{{ name: "count", type: FieldType.Int }}],
    history: 16,
    maxPayload: 8,
  }});
  await until(() => clicks.subscribers >= 1);
  for (let count = 1; count <= 3; count++) {{
    const payload = new Uint8Array(8);
    new DataView(payload.buffer).setBigInt64(0, BigInt(count * 100), true);
    clicks.publish(0, BigInt(count), payload, count === 1);
  }}

  // Run an action: a program on the board does it, and says how it went.
  const progress = [];
  const result = await orion.run(
    {{ name: "pump.start", args: new TextEncoder().encode("gently") }},
    (step) => progress.push(step.percent + ":" + step.message),
  );
  const output = new TextDecoder().decode(result.output);

  // One that is not there is refused, with the board's code for "not found".
  let refused = "ran";
  try {{
    await orion.run({{ name: "no.such.action" }});
  }} catch (error) {{
    refused = (error instanceof Refused ? "Refused" : "other") + ":" + error.code;
  }}

  // One on another board's topic that nobody carries: refused or left waiting, not broken.
  await report([
    "check=" + check,
    "node=" + orion.node,
    "declared=" + declared,
    "status=" + (level.status === Status.Live ? "live" : level.status),
    "steady=" + steady,
    "request=" + new TextDecoder().decode(clicks.request),
    "outcome=" + (result.outcome === Outcome.Succeeded ? "succeeded" : result.outcome),
    "output=" + output,
    "progress=" + progress.join("|"),
    "refused=" + refused,
  ].join("&"));
  level.close();
  orion.close();
}} catch (error) {{
  report("error=" + encodeURIComponent(String(error && error.stack ? error.stack : error)));
}}
</script>"#
    )
}

/// Serves the page and the client it imports, and takes the page's report.
fn serve(listener: TcpListener, page: String, client: String, report: mpsc::Sender<String>) {
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
            let (kind, body) = if let Some(query) = path.strip_prefix("/done?") {
                let _ = report.send(query.to_owned());
                ("text/plain", String::from("ok"))
            } else if path == "/orion.js" {
                ("text/javascript", client.clone())
            } else if path == "/" {
                ("text/html", page.clone())
            } else {
                ("text/plain", String::new())
            };
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
}

/// A program on the board, in a thread, until dropped.
struct Program {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Program {
    fn start(body: impl FnOnce(&AtomicBool) + Send + 'static) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        Self {
            stop,
            thread: Some(std::thread::spawn(move || body(&stopping))),
        }
    }
}

impl Drop for Program {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[test]
fn what_was_built_is_what_the_source_says() {
    // `dist/` is checked in so that a page can use the client without a build. It must be
    // the build of the source beside it: `web/client/build.sh` makes it so.
    let dir = client_dir();
    let source = std::fs::read(dir.join("src/orion.ts")).expect("the source");
    let digest: String = Sha256::digest(&source)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let built_from = std::fs::read_to_string(dir.join("dist/source.sha256")).expect("the note");
    assert_eq!(
        built_from.trim(),
        digest,
        "web/client/src/orion.ts changed since dist/ was built: run web/client/build.sh"
    );
    let built = std::fs::read_to_string(dir.join("dist/orion.js")).expect("the build");
    assert!(built.contains("export class Orion"));
    // No framework, small.
    assert!(built.len() < 32 * 1024, "{} bytes", built.len());
    assert!(
        !built.contains("require(") && !built.contains("from \""),
        "it imports nothing"
    );
}

#[test]
fn the_web_client_subscribes_publishes_and_runs_actions_in_a_browser() {
    let board = Board::start("web-client");
    let socket = board.socket().to_owned();

    // A sensor on the board: a level that goes up half a litre every twenty milliseconds.
    let sensor_socket = socket.clone();
    let _sensor = Program::start(move |stop| {
        let orion = Orion::connect(&Address::Local(sensor_socket), "sensor").expect("connect");
        let mut level = orion
            .declare(&TopicDecl {
                name: "tank.level",
                fields: &fields![litres: Float "l"],
                history: 64,
                expiry_ms: 0,
                max_payload: 8,
                on_subscribe: Default::default(),
            })
            .expect("declare");
        let mut step = 0_u64;
        while !stop.load(Ordering::Relaxed) {
            step += 1;
            level
                .publish(
                    FieldId(0),
                    SampleTime::from_nanos(step),
                    &(step as f64 * 0.5).to_le_bytes(),
                )
                .expect("publish");
            // Not a sleep: a program that goes quiet on its session is taken for dead, and
            // a browser can take longer than that to start.
            orion.pump(Duration::from_millis(20)).expect("pump");
        }
    });

    // The program that has the pump's action.
    let pump_socket = socket.clone();
    let (ready, is_ready) = mpsc::channel();
    let _pump = Program::start(move |stop| {
        let orion = Orion::connect(&Address::Local(pump_socket), "pump-driver").expect("connect");
        let mut start = orion
            .register_action(
                &ActionDecl {
                    name: "pump.start",
                    target: Target::Node("board".parse().expect("name")),
                    description: "Starts the pump",
                },
                Policy::new(Admits::ANYONE).without_grant(),
            )
            .expect("register");
        ready.send(()).expect("ready");
        while !stop.load(Ordering::Relaxed) {
            if let Ok(Some(Request::Run { call, args, .. })) = start.wait(Duration::from_millis(20))
            {
                start.progress(call, 50, "priming", b"").expect("progress");
                let mut output = b"started ".to_vec();
                output.extend_from_slice(&args);
                start
                    .finish(call, Outcome::Succeeded, &output, "")
                    .expect("finish");
            }
        }
    });
    is_ready
        .recv_timeout(Duration::from_secs(10))
        .expect("the action is registered");

    // A dashboard on the board that reads what the page publishes. It asks before the page
    // has declared anything, and says what it would like.
    let dashboard = Orion::connect(&Address::Local(socket), "dashboard").expect("connect");
    let mut clicks = dashboard
        .subscribe_asking("page.clicks", b"every click")
        .expect("subscribe");

    let http = TcpListener::bind("127.0.0.1:0").expect("listen");
    let url = format!("http://{}/", http.local_addr().expect("addr"));
    let client = std::fs::read_to_string(client_dir().join("dist/orion.js")).expect("the client");
    let (report, reported) = mpsc::channel();
    serve(http, page(board.websocket()), client, report);

    let profile = std::env::temp_dir().join(format!("orion-web-client-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&profile);
    std::fs::create_dir_all(&profile).expect("profile");
    let Some(mut browser) = browse(&url, &profile) else {
        println!("no browser is installed: the web client was not run");
        return;
    };

    // What the page publishes, read on the board while the page is still at work.
    let mut read = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    while read.len() < 3 && Instant::now() < deadline {
        clicks.wait(Duration::from_millis(50)).expect("wait");
        while let Next::Sample(sample) = clicks.next() {
            read.push((
                sample.time.as_nanos(),
                i64::from_le_bytes(sample.payload.try_into().expect("eight bytes")),
                sample.flags.0,
            ));
        }
    }
    let result = reported.recv_timeout(Duration::from_secs(90));
    let _ = browser.kill();
    let _ = browser.wait();
    let _ = std::fs::remove_dir_all(&profile);
    let result = result.expect("the page reported within ninety seconds");
    println!("the page reported: {result}");

    assert_eq!(
        result,
        "check=e3069283&node=board&declared=tank.level:litres:3:l:64&status=live&steady=true\
         &request=every%20click&outcome=succeeded&output=started%20gently&progress=50:priming\
         &refused=Refused:8"
            .replace("%20", " ")
            .replace(' ', "%20"),
        "{result}"
    );
    // The page's three samples, as the board read them: times, values, and the first one
    // marked as one the others depend on.
    assert_eq!(read, [(1, 100, 1), (2, 200, 0), (3, 300, 0)]);
}
