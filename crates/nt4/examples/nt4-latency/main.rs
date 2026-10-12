//! Measures a vision camera's end-to-end latency and timestamp accuracy, with no robot.
//!
//! The tool is the NT4 server. A PhotonVision camera connects to it, syncs its clock to the tool's,
//! and publishes results under `/photonvision/<camera>/`. A page on this machine shows an AprilTag
//! that the tool switches on and off at logged times. Each flip is matched to the first camera
//! result that shows it. See the README section "Measuring camera latency".
//!
//! ```text
//! cargo run -p orion-nt4 --example nt4-latency -- --camera cam0 --csv flips.csv
//! ```

mod analysis;
mod clock;
mod display;
mod tag;
mod tag36h11;

use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use analysis::{
    Analysis, Flip, HasTargetSample, MatchConfig, ResultSample, analyze, decode_metadata,
    format_summary, write_csv,
};
use clap::Parser;
use clock::Clock;
use display::{Display, PageEvent};
use orion_nt4::{Server, ServerConfig, ServerEvent, Value, now_micros};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::oneshot;
use tokio::time::{Instant, MissedTickBehavior};

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Parser)]
struct Args {
    /// PhotonVision camera name. The tool reads /photonvision/<camera>/hasTarget and rawBytes.
    #[arg(long)]
    camera: String,
    /// NT4 listen address. Point PhotonVision's NT server address at this host.
    #[arg(long, default_value = "0.0.0.0:5810")]
    nt_bind: SocketAddr,
    /// Port of the display page, served on 127.0.0.1.
    #[arg(long, default_value_t = 8080)]
    web_port: u16,
    /// AprilTag tag36h11 id to show.
    #[arg(long, default_value_t = 0)]
    tag_id: u32,
    /// Display delay in ms: from the page's paint to the light leaving the screen. Measure it once
    /// (see the README). Until then c is only relative.
    #[arg(long, default_value_t = 0.0)]
    display_delay_ms: f64,
    /// Number of flips.
    #[arg(long, default_value_t = 60)]
    count: u32,
    /// Shortest gap between flips, in ms.
    #[arg(long, default_value_t = 300)]
    min_interval_ms: u64,
    /// Longest gap between flips, in ms. Gaps are random in between, so they do not alias with the
    /// camera's frame rate.
    #[arg(long, default_value_t = 700)]
    max_interval_ms: u64,
    /// Per-flip CSV output.
    #[arg(long)]
    csv: Option<PathBuf>,
    /// Seconds to wait for the page and for PhotonVision's clock sync before giving up.
    #[arg(long, default_value_t = 120)]
    wait_s: u64,
}

/// What `run` needs. The CLI fills it in; the integration test fills it in directly.
pub struct RunConfig {
    pub camera: String,
    pub nt_bind: SocketAddr,
    pub web_bind: SocketAddr,
    pub tag_id: u32,
    pub display_delay_us: i64,
    pub count: u32,
    pub interval_ms: (u64, u64),
    /// How long to wait for the page and the clock sync.
    pub wait: Duration,
    /// How long to keep listening for camera results after the last flip.
    pub drain: Duration,
}

/// Where the servers ended up listening (useful with port 0).
pub struct Ready {
    pub nt_addr: SocketAddr,
    pub web_addr: SocketAddr,
}

#[derive(Clone, Copy)]
enum Phase {
    /// Waiting for the page and at least three clock samples.
    Waiting {
        since: Instant,
    },
    Running {
        next_flip: Instant,
    },
    /// All flips sent; listening for the last camera results.
    Draining {
        until: Instant,
    },
}

/// Runs one measurement and returns the analysis. Sends the bound addresses on `ready` once the
/// servers are up.
pub async fn run(
    config: RunConfig,
    ready: Option<oneshot::Sender<Ready>>,
) -> Result<Analysis, BoxError> {
    if config.count == 0 || config.interval_ms.0 == 0 || config.interval_ms.0 > config.interval_ms.1
    {
        return Err("need --count >= 1 and 0 < --min-interval-ms <= --max-interval-ms".into());
    }
    let server = Server::start(ServerConfig {
        bind: config.nt_bind,
        persist_path: None,
    })
    .await?;
    let mut nt_events = server.handle().events();
    let html = tag::page_html(config.tag_id)?;
    let mut display = Display::start(config.web_bind, html).await?;
    println!(
        "NT4 server on {} (point PhotonVision's NT server address at this host)",
        server.local_addr()
    );
    println!(
        "display: http://{}/ (fullscreen it, then aim the camera at it)",
        display.local_addr
    );
    if let Some(tx) = ready {
        let _ = tx.send(Ready {
            nt_addr: server.local_addr(),
            web_addr: display.local_addr,
        });
    }

    let has_topic = format!("/photonvision/{}/hasTarget", config.camera);
    let raw_topic = format!("/photonvision/{}/rawBytes", config.camera);
    let mut clock = Clock::default();
    let mut rng = Rng::from_clock();
    let mut flips: Vec<Flip> = Vec::new();
    let mut has: Vec<HasTargetSample> = Vec::new();
    let mut results: Vec<ResultSample> = Vec::new();
    let mut connected = false;
    let mut phase = Phase::Waiting {
        since: Instant::now(),
    };
    let mut on = false;
    let mut ping = tokio::time::interval(Duration::from_millis(200));
    ping.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        let timer = match phase {
            Phase::Running { next_flip } => Some(next_flip),
            Phase::Draining { until } => Some(until),
            Phase::Waiting { .. } => None,
        };
        tokio::select! {
            _ = ping.tick() => display.ping(now_micros()),
            _ = sleep_until_opt(timer), if timer.is_some() => match phase {
                Phase::Running { .. } => {
                    let seq = flips.len() as u32;
                    on = !on;
                    let cmd_us = now_micros();
                    display.flip(seq, on);
                    flips.push(Flip { seq, on, cmd_us, painted_us: cmd_us, painted_confirmed: false });
                    phase = if seq + 1 >= config.count {
                        Phase::Draining { until: Instant::now() + config.drain }
                    } else {
                        let gap = rng.range(config.interval_ms.0, config.interval_ms.1);
                        Phase::Running { next_flip: Instant::now() + Duration::from_millis(gap) }
                    };
                }
                _ => break,
            },
            event = display.events.recv() => match event {
                Some(PageEvent::Connected) => {
                    connected = true;
                    println!("page connected");
                }
                Some(PageEvent::Disconnected) => {
                    connected = false;
                    println!("page disconnected (reload it; flips already sent stay logged)");
                }
                Some(PageEvent::Pong { sent_us, received_us, page_us }) => {
                    clock.add_pong(sent_us, received_us, page_us);
                }
                Some(PageEvent::Painted { seq, page_us }) => {
                    if let (Some(flip), Some(tool_us)) = (flips.get_mut(seq as usize), clock.page_to_tool(page_us)) {
                        flip.painted_us = tool_us;
                        flip.painted_confirmed = true;
                    }
                }
                None => return Err("display stopped".into()),
            },
            event = nt_events.recv() => match event {
                Ok(ServerEvent::ValueChanged { name, value, timestamp_us, client: Some(_) }) => {
                    if name == has_topic {
                        if let Some(has_target) = value.as_bool() {
                            has.push(HasTargetSample { arrival_us: timestamp_us, has_target });
                        }
                    } else if let Value::Raw(bytes) = &value
                        && name == raw_topic
                        && let Some(meta) = decode_metadata(bytes)
                    {
                        results.push(ResultSample { arrival_us: timestamp_us, meta });
                    }
                }
                Ok(_) => {}
                Err(RecvError::Lagged(missed)) => eprintln!("warning: {missed} NT events were dropped"),
                Err(RecvError::Closed) => return Err("NT server stopped".into()),
            },
        }

        if let Phase::Waiting { since } = phase {
            if connected && clock.sample_count() >= 3 {
                println!(
                    "clock synced ({} pings, best round trip {} us); flipping {} times",
                    clock.sample_count(),
                    clock.best_rtt_us().unwrap_or_default(),
                    config.count
                );
                phase = Phase::Running {
                    next_flip: Instant::now() + Duration::from_secs(1),
                };
            } else if since.elapsed() > config.wait {
                return Err(format!(
                    "no page and clock sync within {} s (page connected: {connected}, pings answered: {})",
                    config.wait.as_secs(),
                    clock.sample_count()
                )
                .into());
            }
        }
    }

    Ok(analyze(
        &flips,
        &has,
        &results,
        &MatchConfig::new(config.display_delay_us),
    ))
}

async fn sleep_until_opt(at: Option<Instant>) {
    if let Some(at) = at {
        tokio::time::sleep_until(at).await;
    }
}

/// xorshift64: flip gaps only need to be unpredictable enough to avoid aliasing.
struct Rng(u64);

impl Rng {
    fn from_clock() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos() as u64);
        Self(nanos | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Uniform in `lo..=hi`.
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next_u64() % (hi - lo + 1)
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let config = RunConfig {
        camera: args.camera,
        nt_bind: args.nt_bind,
        web_bind: SocketAddr::from(([127, 0, 0, 1], args.web_port)),
        tag_id: args.tag_id,
        display_delay_us: (args.display_delay_ms * 1000.0).round() as i64,
        count: args.count,
        interval_ms: (args.min_interval_ms, args.max_interval_ms),
        wait: Duration::from_secs(args.wait_s),
        drain: Duration::from_millis(1500),
    };
    let display_delay_us = config.display_delay_us;
    let analysis = run(config, None).await?;
    print!("{}", format_summary(&analysis));
    if display_delay_us == 0 {
        println!(
            "note: display delay is 0, so c is relative. Measure it once (README: Measuring camera latency)."
        );
    }
    if let Some(path) = args.csv {
        let mut out = BufWriter::new(File::create(&path)?);
        write_csv(&mut out, &analysis.rows)?;
        out.flush()?;
        println!("wrote {}", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use futures_util::{SinkExt, StreamExt};
    use orion_nt4::{Client, ClientConfig};
    use serde_json::json;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::Message;

    /// The page's clock runs this far ahead of the tool's.
    const PAGE_SKEW_US: i64 = 1_234_567;
    /// Paint to light leaving the screen, in the simulation.
    const DISPLAY_LAG_US: i64 = 20_000;
    /// Capture to publish, in the fake camera.
    const PUBLISH_LAG_US: u64 = 25_000;
    const FRAME: Duration = Duration::from_micros(33_333);

    /// (paint time on the tool clock, tag shown)
    type Screen = Arc<Mutex<Vec<(i64, bool)>>>;

    #[tokio::test]
    async fn fake_camera_and_page_measure_the_simulated_latency() {
        let config = RunConfig {
            camera: "cam0".into(),
            nt_bind: "127.0.0.1:0".parse().unwrap(),
            web_bind: "127.0.0.1:0".parse().unwrap(),
            tag_id: 0,
            display_delay_us: DISPLAY_LAG_US,
            count: 10,
            interval_ms: (60, 120),
            wait: Duration::from_secs(10),
            drain: Duration::from_millis(800),
        };
        let (tx, rx) = oneshot::channel();
        let tool = tokio::spawn(run(config, Some(tx)));
        let ready = rx.await.unwrap();
        let screen: Screen = Arc::default();
        let camera = tokio::spawn(fake_camera(ready.nt_addr, Arc::clone(&screen)));
        let page = tokio::spawn(fake_page(ready.web_addr, Arc::clone(&screen)));

        let analysis = tool.await.unwrap().unwrap();
        camera.abort();
        page.abort();

        let c = analysis.counts.clone();
        assert_eq!((c.flips, c.matched, c.dropped), (10, 10, 0), "{c:?}");
        for row in &analysis.rows {
            let m = row.matched.expect("every flip matched");
            // b: the camera publishes 25 ms after capture; NT adds about a millisecond.
            assert!(
                (24_000..=40_000).contains(&m.reported_latency_us),
                "b {m:?}"
            );
            // a: display lag (20 ms), plus the capture phase (0 to one frame), plus publish lag.
            assert!((40_000..=95_000).contains(&m.true_latency_us), "a {m:?}");
            // c: capture - paint - display lag is the capture phase, from 0 to one frame.
            let frame_us = FRAME.as_micros() as i64;
            assert!(
                (-10_000..=frame_us + 10_000).contains(&m.timestamp_error_us),
                "c {m:?}"
            );
        }
    }

    /// The camera: one result per frame, showing the screen as it was one display lag earlier.
    async fn fake_camera(nt_addr: SocketAddr, screen: Screen) {
        let mut config = ClientConfig::new("127.0.0.1", "fake-photon");
        config.port = nt_addr.port();
        let client = Client::start(config);
        let handle = client.handle();
        let has = handle
            .publish("/photonvision/cam0/hasTarget", "boolean")
            .unwrap();
        let raw = handle
            .publish("/photonvision/cam0/rawBytes", "raw")
            .unwrap();
        let mut seq = 0i64;
        loop {
            tokio::time::sleep(FRAME).await;
            let capture = now_micros();
            let on = visible_state(&screen, capture - DISPLAY_LAG_US);
            seq += 1;
            let (has, raw) = (has.clone(), raw.clone());
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_micros(PUBLISH_LAG_US)).await;
                let publish = now_micros();
                let _ = has.set(Value::Boolean(on));
                let _ = raw.set(Value::Raw(metadata_bytes(seq, capture, publish)));
            });
        }
    }

    /// The page: answers pings with its skewed clock, and records each paint.
    async fn fake_page(web_addr: SocketAddr, screen: Screen) {
        let (ws, _) = connect_async(format!("ws://{web_addr}/ws")).await.unwrap();
        let (mut sink, mut source) = ws.split();
        while let Some(Ok(Message::Text(text))) = source.next().await {
            let message: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
            let reply = match message["t"].as_str() {
                Some("ping") => json!({
                    "t": "pong",
                    "st": message["st"],
                    "pc": now_micros() + PAGE_SKEW_US,
                }),
                Some("flip") => {
                    let painted = now_micros();
                    screen
                        .lock()
                        .unwrap()
                        .push((painted, message["on"].as_bool().unwrap()));
                    json!({
                        "t": "painted",
                        "seq": message["seq"],
                        "pc": painted + PAGE_SKEW_US,
                    })
                }
                _ => continue,
            };
            sink.send(Message::Text(reply.to_string().into()))
                .await
                .unwrap();
        }
    }

    fn visible_state(screen: &Screen, at_us: i64) -> bool {
        screen
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(painted, _)| *painted <= at_us)
            .map(|(_, on)| *on)
            .unwrap_or(false)
    }

    fn metadata_bytes(seq: i64, capture: i64, publish: i64) -> Vec<u8> {
        [seq, capture, publish, 0]
            .into_iter()
            .flat_map(i64::to_le_bytes)
            .collect()
    }
}
