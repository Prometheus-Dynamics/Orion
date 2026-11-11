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
use orion_stubs::Board;
use orion_web_tests::browser::{browse, outside_address, serve, web_dir};
use orion_web_tests::video::{Clip, H264, JPEG, fragments, publish};
use socket2::{Domain, Socket, Type};

/// Bytes a second the link carries towards the page.
const LINK: usize = 48 * 1024;
/// Samples a second on each topic: about three times what the link carries.
const RATE: u32 = 30;

/// A link to `board` that carries `LINK` bytes a second towards whoever connects to it, and
/// holds little in between.
fn throttled(listener: TcpListener, board: SocketAddr) {
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
            let (mut from_page, mut to_board) = (
                page.try_clone().expect("clone"),
                upstream.try_clone().expect("clone"),
            );
            std::thread::spawn(move || {
                let _ = std::io::copy(&mut from_page, &mut to_board);
                let _ = to_board.shutdown(std::net::Shutdown::Both);
            });
            let (mut from_board, mut to_page) = (upstream, page);
            std::thread::spawn(move || {
                let mut bytes = [0_u8; 1024];
                let began = Instant::now();
                let mut carried = 0_usize;
                while let Ok(count @ 1..) = from_board.read(&mut bytes) {
                    if to_page.write_all(&bytes[..count]).is_err() {
                        break;
                    }
                    carried += count;
                    let due = Duration::from_secs_f64(carried as f64 / LINK as f64);
                    std::thread::sleep(due.saturating_sub(began.elapsed()));
                }
                let _ = to_page.shutdown(std::net::Shutdown::Both);
            });
        }
    });
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
    if let Some(result) = result {
        check(&result);
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
    if let Some(result) = play(board, false) {
        check(&result);
    }
}

/// Opens the video page in a browser, to the board's WebSocket at `websocket` (over a link
/// this throttles, if `throttle`), and returns what the page reported; `None` if no browser
/// is installed.
fn play(websocket: SocketAddr, throttle: bool) -> Option<String> {
    // The page comes from an address that is not the loopback when the machine has one: a
    // browser counts a page from the loopback as secure even on `http://`.
    let host = outside_address().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
    let link_at = if throttle {
        let link = TcpListener::bind((host, 0)).expect("listen");
        let at = link.local_addr().expect("addr");
        throttled(link, websocket);
        at
    } else {
        websocket
    };
    let http = TcpListener::bind((host, 0)).expect("listen");
    let site = http.local_addr().expect("addr");
    let (report, reported) = mpsc::channel();
    serve(http, web_dir(), report);

    let url = format!(
        "http://{site}/video-example/index.html?board=ws://{link_at}/orion&jpeg={JPEG}&h264={H264}\
         &mime=video/mp4;%20codecs=%22avc1.42D00B%22&report=/done&enough=40&losses=1"
    );
    let profile = std::env::temp_dir().join(format!("orion-web-video-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&profile);
    std::fs::create_dir_all(&profile).expect("profile");
    let Some(mut browser) = browse(&url, &profile) else {
        println!("no browser is installed: the video example was not run");
        return None;
    };
    let result = reported.recv_timeout(Duration::from_secs(120));
    let _ = browser.kill();
    let _ = browser.wait();
    let _ = std::fs::remove_dir_all(&profile);
    let result = result.expect("the page reported within two minutes");
    println!("the page reported: {result}");
    Some(result)
}

/// What the page must have seen: pictures and video shown, and less sent than published.
fn check(result: &str) {
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
    assert!(!said.contains_key("error"), "{result}");
    assert_eq!(said["protocol"], "http%3A", "{result}");
    if !host.is_loopback() {
        // Plain HTTP and not secure: the page has no WebCodecs to fall back on.
        assert_eq!(said["secure"], "false", "{result}");
        assert_eq!(said["codecs"], "false", "{result}");
    }

    // JPEG: pictures shown, at their real size.
    assert!(number("jpegShown") >= 40.0, "{result}");
    assert!((number("jpegWidth") - 160.0).abs() < 0.5, "{result}");

    // H.264: frames decoded and the play head moving, unless this browser has no decoder.
    if said["h264"] == "plays" {
        assert!(number("h264Frames") >= 40.0, "{result}");
        assert!((number("h264Width") - 160.0).abs() < 0.5, "{result}");
        assert!(number("h264Time") > 1.0, "{result}");
    } else {
        println!(
            "this browser cannot play H.264: only JPEG was shown ({})",
            said["why"]
        );
    }

    // The link carried less than was published, and the board dropped the rest: the page
    // was not sent everything, and still showed video.
    assert!(number("jpegMissed") > 0.0, "{result}");
    if said["h264"] == "plays" {
        // After its loss the next fragment sent was a key frame, and it played on.
        assert!(number("h264Missed") > 0.0, "{result}");
        assert!(
            number("h264Frames") >= number("h264FramesAtLoss") + 24.0,
            "{result}"
        );
    }
}
