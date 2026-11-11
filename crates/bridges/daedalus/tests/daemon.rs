//! The plugin in `daedalusd`: a graph reads a topic, writes one, and takes frames from a
//! buffer topic, with a comm core on the far side.
//!
//! `DAEDALUSD` names the daemon to run (`scripts/daedalus-bridge.sh` builds it from the same
//! Daedalus commit as the plugin). Without it the test says so and passes.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use daedalus_client::{Client, Endpoint, Options};
use daedalus_graph::{
    EdgeDoc, EdgePolicyDoc, GraphBody, GraphDoc, NodeDoc, NodeKindDoc, PortRefDoc,
};
use daedalus_types::Scalar;
use daedalus_wire::service::{StartConfig, TypedValue};
use daedalus_wire::{WireType, WireValue};
use orion::buffers::Pool;
use orion::topics::{FieldId, FrameTime, TopicDecl};
use orion::{Address, Orion, fields};
use orion_comm_app::{CommCore, CommSettings};
use tokio::sync::oneshot;

const WAIT: Duration = Duration::from_secs(60);

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-daedalus-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

/// The plugin library cargo built for this test: next to it, or one directory up.
fn plugin() -> PathBuf {
    let exe = std::env::current_exe().expect("exe");
    let name = format!(
        "{}orion_daedalus{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let deps = exe.parent().expect("deps");
    [deps, deps.parent().expect("profile")]
        .into_iter()
        .map(|dir| dir.join(&name))
        .find(|path| path.exists())
        .expect("the plugin library is built")
}

/// A comm core in a thread of its own, until dropped.
struct Comm {
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Comm {
    fn start(dir: &Path) -> Self {
        let text = format!(
            "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n",
            dir.display()
        );
        let mut source = orion_config::Source::parse(&text, Vec::new()).expect("source");
        let settings = CommSettings::from_source(&mut source).expect("settings");
        let (stop, stopped) = oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let core = CommCore::start(settings)
                    .await
                    .expect("the comm core starts");
                let _ = stopped.await;
                drop(core);
            });
        });
        Self {
            stop: Some(stop),
            thread: Some(thread),
        }
    }
}

impl Drop for Comm {
    fn drop(&mut self) {
        if let (Some(stop), Some(thread)) = (self.stop.take(), self.thread.take()) {
            let _ = stop.send(());
            let _ = thread.join();
        }
    }
}

/// `daedalusd` with this plugin in its plugin directory, until dropped.
struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn node(id: &str, label: &str) -> NodeDoc {
    let mut node = NodeDoc::new(id);
    node.label = Some(label.to_owned());
    node
}

fn edge(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_owned(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_owned(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

/// The host's ports around the three nodes: the host feeds a topic's name to read it, a name
/// and a number to write one, and takes `value`, `published` and `frame`.
fn graph() -> String {
    let mut host = NodeDoc::new("host");
    host.kind = NodeKindDoc::HostBridge;
    host.label = Some("io".to_owned());
    for (port, ty) in [
        ("read", "string"),
        ("write", "string"),
        ("number", "f64"),
        ("frames", "string"),
        ("value", "f64"),
        ("published", "u64"),
        ("frame", "bytes"),
    ] {
        host.port_types.insert(port.to_owned(), ty.to_owned());
    }
    let body = GraphBody {
        nodes: vec![
            host,
            node("orion.topic.read", "read"),
            node("orion.topic.write", "write"),
            node("orion.frame.read", "frame"),
        ],
        edges: vec![
            edge(0, "read", 1, "topic"),
            edge(1, "value", 0, "value"),
            edge(0, "write", 2, "topic"),
            edge(0, "number", 2, "value"),
            edge(2, "published", 0, "published"),
            edge(0, "frames", 3, "topic"),
            edge(3, "frame", 0, "frame"),
        ],
        groups: Vec::new(),
    };
    GraphDoc::new(body).to_json().expect("graph")
}

fn typed(scalar: Scalar, value: WireValue) -> TypedValue {
    TypedValue::new(WireType::Scalar(scalar), value)
}

#[test]
fn a_graph_in_daedalusd_reads_and_writes_topics_and_takes_frames() {
    let Some(daedalusd) = std::env::var_os("DAEDALUSD") else {
        println!("DAEDALUSD is not set: the plugin was not run in a daemon");
        return;
    };
    let dir = temp("daemon");
    let comm = Comm::start(&dir);
    let socket = dir.join("orion.sock");
    until("the comm core's socket", || socket.exists());
    let address: Address = socket.to_str().expect("path").parse().expect("address");
    let orion = Orion::connect(&address, "test").expect("connect");

    // What the graph reads: field 1 of `imu`.
    let mut imu = orion
        .declare(&TopicDecl {
            name: "imu",
            fields: &fields![roll: Float "deg", yaw: Float "deg"],
            history: 16,
            expiry_ms: 0,
            max_payload: 8,
        })
        .expect("declare imu");
    imu.publish(
        FieldId(1),
        FrameTime::from_nanos(1),
        &42.5_f64.to_le_bytes(),
    )
    .expect("publish");

    // The frames: one buffer of a known pattern.
    let mut camera = orion
        .declare_buffers("camera", 4, 16, Pool::in_memory(1, 4, 4096).expect("pool"))
        .expect("declare camera");

    // What the graph writes.
    let mut echo = orion.subscribe("echo").expect("subscribe echo");

    let plugins = dir.join("plugins");
    std::fs::create_dir_all(&plugins).expect("plugins");
    std::fs::copy(plugin(), plugins.join(plugin().file_name().expect("name"))).expect("copy");
    let config = dir.join("daedalusd.toml");
    let control = dir.join("d.sock");
    std::fs::write(
        &config,
        format!(
            "listen = [\"unix:{}\"]\nplugin_dir = \"{}\"\n",
            control.display(),
            plugins.display()
        ),
    )
    .expect("config");
    let _daemon = Daemon(
        Command::new(daedalusd)
            .arg("--config")
            .arg(&config)
            .env("ORION_ADDRESS", &socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("daedalusd starts"),
    );
    let mut client = None;
    until("daedalusd to listen", || {
        client = Client::connect(Endpoint::unix(&control), Options::default()).ok();
        client.is_some()
    });
    let client = client.expect("client");
    let diagnostics = client.put_graph("bridge", &graph()).expect("put graph");
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let run = client
        .start("bridge", StartConfig::default())
        .expect("start");
    let text = |text: &str| typed(Scalar::String, WireValue::String(text.to_owned()));

    // Read: the newest value of `imu.yaw`, once the subscription has its ring.
    let mut read = None;
    until("the graph to read imu", || {
        client.feed(run, "read", text("imu:1")).expect("feed");
        client.step(run, 1).expect("step");
        read = client.take(run, "value").expect("take");
        read.is_some()
    });
    assert_eq!(read.map(|v| v.value), Some(WireValue::F64(42.5)));

    // Write: what the graph is fed comes out on `echo`.
    for (n, value) in [1.25_f64, 2.5, 5.0].into_iter().enumerate() {
        client.feed(run, "write", text("echo")).expect("feed");
        client
            .feed(run, "number", typed(Scalar::Float, WireValue::F64(value)))
            .expect("feed");
        client.step(run, 1).expect("step");
        let published = client
            .take(run, "published")
            .expect("take")
            .map(|v| v.value);
        assert_eq!(published, Some(WireValue::U64(n as u64 + 1)));
    }
    // The topic was declared by the first write, so the subscriber's ring came after it: the
    // newest value is what it is sure to see.
    let mut newest = None;
    until("echo to carry the values", || {
        let _ = orion.pump(Duration::ZERO);
        newest = echo
            .latest(FieldId(0))
            .map(|sample| f64::from_le_bytes(sample.payload.try_into().expect("8 bytes")));
        newest == Some(5.0)
    });

    // Frames: the bytes the publisher filled, as Daedalus sees them.
    let mut frame = None;
    let mut sent = 0_u8;
    until("the graph to take a frame", || {
        let _ = orion.pump(Duration::ZERO);
        camera.poll().expect("poll");
        if let Ok(buffer) = camera.take() {
            sent = sent.wrapping_add(1);
            camera.buffer_mut(buffer).expect("memory").fill(sent);
            camera
                .publish(&[buffer], FrameTime::from_nanos(u64::from(sent)), &[])
                .expect("publish frame");
        }
        client.feed(run, "frames", text("camera")).expect("feed");
        client.step(run, 1).expect("step");
        frame = client.take(run, "frame").expect("take");
        frame.is_some()
    });
    let Some(WireValue::Bytes(bytes)) = frame.map(|v| v.value) else {
        panic!("a frame is bytes");
    };
    assert_eq!(bytes.len(), 4096);
    assert!(
        bytes.iter().all(|b| *b == bytes[0] && *b != 0),
        "one whole frame"
    );

    drop(client);
    drop(comm);
    let _ = std::fs::remove_dir_all(&dir);
}
