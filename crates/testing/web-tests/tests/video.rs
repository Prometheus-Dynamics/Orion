//! The video example in a headless browser, on plain `http://`, over a link slower than the
//! video: JPEG pictures are shown and H.264 plays, and what the link has no room for is
//! dropped by the board, not queued. If no browser is installed the test says so and passes.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use orion::{Address, Orion};
use orion_scratch::Scratch;
use orion_stubs::Board;
use orion_web_tests::browser::{browse, outside_address, serve, web_dir};
use orion_web_tests::video::{Clip, H264, JPEG, fragments, publish};
use socket2::{Domain, Socket, Type};

/// Bytes a second the link carries towards the page.
const LINK: usize = 48 * 1024;
/// Samples a second on each topic: about three times what the link carries.
const RATE: u32 = 30;

/// What the link saw of one connection through it: bytes carried each way, when the last of
/// them passed (seconds after the link was made), and how each way ended.
#[derive(Default)]
struct Carried {
    connected: f64,
    to_page: usize,
    last_to_page: f64,
    to_page_ended: Option<String>,
    to_board: usize,
    last_to_board: f64,
    to_board_ended: Option<String>,
}

/// Every connection through the link, for a failure to say where the stream stopped.
type Seen = Arc<std::sync::Mutex<Vec<Carried>>>;

fn describe(seen: &Seen) -> String {
    let seen = seen.lock().expect("the link's record");
    if seen.is_empty() {
        return "the link: nothing connected".to_owned();
    }
    seen.iter()
        .enumerate()
        .map(|(n, c)| {
            format!(
                "the link, connection {n} (at {:.1} s): towards the page {} bytes, the last at \
                 {:.1} s, {}; towards the board {} bytes, the last at {:.1} s, {}",
                c.connected,
                c.to_page,
                c.last_to_page,
                c.to_page_ended.as_deref().unwrap_or("still open"),
                c.to_board,
                c.last_to_board,
                c.to_board_ended.as_deref().unwrap_or("still open"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// A link to `board` that carries `LINK` bytes a second towards whoever connects to it, and
/// holds little in between. What it carries is written to the returned record.
fn throttled(listener: TcpListener, board: SocketAddr) -> Seen {
    let seen: Seen = Arc::default();
    let record = seen.clone();
    let made = Instant::now();
    std::thread::spawn(move || {
        for page in listener.incoming().flatten() {
            // Small buffers on both sides of the link: a slow link is not a deep one.
            let Ok(socket) = Socket::new(Domain::IPV4, Type::STREAM, None) else {
                continue;
            };
            let _ = socket.set_recv_buffer_size(8 * 1024);
            if socket.connect(&board.into()).is_err() {
                continue;
            }
            let _ = Socket::from(page.try_clone().expect("clone")).set_send_buffer_size(8 * 1024);
            let upstream: TcpStream = socket.into();
            let index = {
                let mut seen = record.lock().expect("the link's record");
                seen.push(Carried {
                    connected: made.elapsed().as_secs_f64(),
                    ..Carried::default()
                });
                seen.len() - 1
            };
            let (mut from_page, mut to_board) = (
                page.try_clone().expect("clone"),
                upstream.try_clone().expect("clone"),
            );
            let towards_board = record.clone();
            std::thread::spawn(move || {
                let mut bytes = [0_u8; 1024];
                let ended = loop {
                    match from_page.read(&mut bytes) {
                        Ok(0) => break "the page closed".to_owned(),
                        Ok(count) => {
                            if let Err(error) = to_board.write_all(&bytes[..count]) {
                                break format!("writing to the board failed: {error}");
                            }
                            let mut seen = towards_board.lock().expect("the link's record");
                            seen[index].to_board += count;
                            seen[index].last_to_board = made.elapsed().as_secs_f64();
                        }
                        Err(error) => break format!("reading from the page failed: {error}"),
                    }
                };
                towards_board.lock().expect("the link's record")[index].to_board_ended =
                    Some(format!("{ended} at {:.1} s", made.elapsed().as_secs_f64()));
                let _ = to_board.shutdown(std::net::Shutdown::Both);
            });
            let towards_page = record.clone();
            let (mut from_board, mut to_page) = (upstream, page);
            std::thread::spawn(move || {
                let mut bytes = [0_u8; 1024];
                let began = Instant::now();
                let mut carried = 0_usize;
                let ended = loop {
                    match from_board.read(&mut bytes) {
                        Ok(0) => break "the board closed".to_owned(),
                        Ok(count) => {
                            if let Err(error) = to_page.write_all(&bytes[..count]) {
                                break format!("writing to the page failed: {error}");
                            }
                            carried += count;
                            {
                                let mut seen = towards_page.lock().expect("the link's record");
                                seen[index].to_page += count;
                                seen[index].last_to_page = made.elapsed().as_secs_f64();
                            }
                            let due = Duration::from_secs_f64(carried as f64 / LINK as f64);
                            std::thread::sleep(due.saturating_sub(began.elapsed()));
                        }
                        Err(error) => break format!("reading from the board failed: {error}"),
                    }
                };
                towards_page.lock().expect("the link's record")[index].to_page_ended =
                    Some(format!("{ended} at {:.1} s", made.elapsed().as_secs_f64()));
                let _ = to_page.shutdown(std::net::Shutdown::Both);
            });
        }
    });
    seen
}

#[test]
fn the_clip_is_cut_at_its_fragments_and_key_frames_carry_the_header() {
    let clip = Clip::load(&web_dir().join("video-example/media")).expect("the media");
    assert_eq!(clip.jpegs.len(), 16);
    assert!(
        clip.jpegs
            .iter()
            .all(|jpeg| jpeg.starts_with(&[0xff, 0xd8]))
    );
    // Sixteen pictures in groups of eight: one fragment each, two of them key frames.
    let keys: Vec<bool> = clip.fragments.iter().map(|fragment| fragment.key).collect();
    assert_eq!(keys.len(), 16);
    assert_eq!(
        keys.iter()
            .enumerate()
            .filter(|(_, key)| **key)
            .map(|(at, _)| at)
            .collect::<Vec<_>>(),
        [0, 8]
    );
    for fragment in &clip.fragments {
        // A key frame starts with the header; the rest are a `moof` and its `mdat` alone.
        let starts = &fragment.bytes[4..8];
        assert_eq!(starts, if fragment.key { b"ftyp" } else { b"moof" });
    }
    // Junk is not a clip, and is not a reason to fall over.
    assert!(fragments(b"\x00\x00\x00\x04junk and more junk").is_empty());
    assert!(fragments(&[0xff; 64]).is_empty());
}

#[test]
fn video_plays_in_a_browser_on_plain_http_over_a_throttled_link() {
    let board = Board::start("web-video");
    let clip = Clip::load(&web_dir().join("video-example/media")).expect("the media");

    // The camera: the clip in a loop, faster than the link can carry.
    let stop = Arc::new(AtomicBool::new(false));
    let (stopping, socket) = (stop.clone(), board.socket().to_owned());
    let camera = std::thread::spawn(move || {
        let orion = Orion::connect(&Address::Local(socket), "camera").expect("connect");
        publish(&orion, &clip, RATE, &stopping).expect("publish");
    });
    let result = play(board.websocket(), true);
    stop.store(true, Ordering::Relaxed);
    camera.join().expect("camera");
    if let Some((result, link)) = result {
        check(&result, &link);
    }
}

/// The same against a real board: `ORION_WEB_BOARD` is a slow link to its `ws.listen`, made on
/// the board (`orion-drill throttle <listen> <ws.listen> 49152`) so that nothing deep sits
/// between the board and the slow link, and the board publishes the clip itself
/// (`publish-video <socket> <media> 30`). Not run by default: it needs the board.
#[test]
#[ignore = "needs a board: ORION_WEB_BOARD=host:port"]
fn video_from_a_board_plays_over_a_throttled_link() {
    let board: SocketAddr = std::env::var("ORION_WEB_BOARD")
        .expect("ORION_WEB_BOARD")
        .parse()
        .expect("host:port");
    if let Some((result, link)) = play(board, false) {
        check(&result, &link);
    }
}

/// Opens the video page in a browser, to the board's WebSocket at `websocket` (over a link
/// this throttles, if `throttle`), and returns what the page reported; `None` if no browser
/// is installed.
fn play(websocket: SocketAddr, throttle: bool) -> Option<(String, String)> {
    // The page comes from an address that is not the loopback when the machine has one: a
    // browser counts a page from the loopback as secure even on `http://`.
    let host = outside_address().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
    let (link_at, seen) = if throttle {
        let link = TcpListener::bind((host, 0)).expect("listen");
        let at = link.local_addr().expect("addr");
        (at, Some(throttled(link, websocket)))
    } else {
        (websocket, None)
    };
    let http = TcpListener::bind((host, 0)).expect("listen");
    let site = http.local_addr().expect("addr");
    let (report, reported) = mpsc::channel();
    serve(http, web_dir(), report);

    let url = format!(
        "http://{site}/video-example/index.html?board=ws://{link_at}/orion&jpeg={JPEG}&h264={H264}\
         &mime=video/mp4;%20codecs=%22avc1.42D00B%22&report=/done&enough=40&losses=1"
    );
    let profile = Scratch::new("web-video");
    let Some(mut browser) = browse(&url, &profile) else {
        println!("no browser is installed: the video example was not run");
        return None;
    };
    let result = reported.recv_timeout(Duration::from_secs(120));
    assert!(browser.end(), "a process of the browser outlived it");
    let result = result.expect("the page reported within two minutes");
    println!("the page reported: {result}");
    let link = seen.map_or_else(
        || "no link of this test's own".to_owned(),
        |seen| describe(&seen),
    );
    println!("{link}");
    Some((result, link))
}

/// What the page must have seen: pictures and video shown, and less sent than published.
/// `link` is what the test's link saw, for a failure to say where the stream stopped.
fn check(result: &str, link: &str) {
    let host = outside_address().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));

    let said: HashMap<&str, &str> = result
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .collect();
    let number = |name: &str| {
        said.get(name)
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(-1.0)
    };
    assert!(!said.contains_key("error"), "{result}\n{link}");
    assert_eq!(said["protocol"], "http%3A", "{result}\n{link}");
    if !host.is_loopback() {
        // Plain HTTP and not secure: the page has no WebCodecs to fall back on.
        assert_eq!(said["secure"], "false", "{result}\n{link}");
        assert_eq!(said["codecs"], "false", "{result}\n{link}");
    }

    // JPEG: pictures shown, at their real size.
    assert!(number("jpegShown") >= 40.0, "{result}\n{link}");
    assert!(
        (number("jpegWidth") - 160.0).abs() < 0.5,
        "{result}\n{link}"
    );

    // H.264: frames decoded and the play head moving, unless this browser has no decoder.
    if said["h264"] == "plays" {
        assert!(number("h264Frames") >= 40.0, "{result}\n{link}");
        assert!(
            (number("h264Width") - 160.0).abs() < 0.5,
            "{result}\n{link}"
        );
        assert!(number("h264Time") > 1.0, "{result}\n{link}");
    } else {
        println!(
            "this browser cannot play H.264: only JPEG was shown ({})",
            said["why"]
        );
    }

    // The link carried less than was published, and the board dropped the rest: the page
    // was not sent everything, and still showed video.
    assert!(number("jpegMissed") > 0.0, "{result}\n{link}");
    if said["h264"] == "plays" {
        // After its loss the next fragment sent was a key frame, and it played on.
        assert!(number("h264Missed") > 0.0, "{result}\n{link}");
        assert!(
            number("h264Frames") >= number("h264FramesAtLoss") + 24.0,
            "{result}\n{link}"
        );
    }
}
