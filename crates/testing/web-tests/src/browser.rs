//! A headless browser, and a small site for it to load.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;

/// Firefox's settings for a profile that leaves the GPU alone.
const SOFTWARE_ONLY: &str = "\
user_pref(\"gfx.webrender.software\", true);
user_pref(\"layers.acceleration.disabled\", true);
user_pref(\"gfx.canvas.accelerated\", false);
user_pref(\"webgl.disabled\", true);
user_pref(\"media.hardware-video-decoding.enabled\", false);
user_pref(\"media.ffmpeg.vaapi.enabled\", false);
";

/// A headless browser on `url`, if one is installed. `profile` is an empty directory for it.
/// The browser's temporary files go in `profile/tmp`, not the shared temporary directory: a
/// profile that is removed takes them with it.
#[must_use]
pub fn browse(url: &str, profile: &Path) -> Option<Child> {
    let temporary = profile.join("tmp");
    std::fs::create_dir_all(&temporary).expect("the browser's temporary directory");
    // Nothing here runs on this machine's GPU: drawing, video and GL are all in software.
    let firefox = |program: &str| {
        std::fs::write(profile.join("user.js"), SOFTWARE_ONLY).ok()?;
        Command::new(program)
            .env("LIBGL_ALWAYS_SOFTWARE", "1")
            .env("TMPDIR", &temporary)
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
            .env("LIBGL_ALWAYS_SOFTWARE", "1")
            .env("TMPDIR", &temporary)
            .args([
                "--headless=new",
                "--disable-gpu",
                "--disable-gpu-compositing",
                "--disable-accelerated-video-decode",
                "--no-first-run",
                "--no-sandbox",
                "--autoplay-policy=no-user-gesture-required",
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

/// The repository's `web` directory.
#[must_use]
pub fn web_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../web")
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("html") => "text/html",
        Some("js") => "text/javascript",
        Some("jpg") => "image/jpeg",
        Some("mp4") => "video/mp4",
        _ => "text/plain",
    }
}

/// Serves the files under `root` over plain HTTP at `listener`, in a thread, for as long as
/// the test runs. A request for `/done?<query>` is a page's report: its query is sent to
/// `report`.
pub fn serve(listener: TcpListener, root: PathBuf, report: mpsc::Sender<String>) {
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let Ok(clone) = stream.try_clone() else {
                continue;
            };
            let mut reader = BufReader::new(clone);
            let mut first = String::new();
            let _ = reader.read_line(&mut first);
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap_or(0) != 0 && line != "\r\n" {
                line.clear();
            }
            let asked = first.split(' ').nth(1).unwrap_or("/");
            let (path, query) = asked.split_once('?').unwrap_or((asked, ""));
            let (status, kind, body) = if path == "/done" {
                let _ = report.send(query.to_owned());
                ("200 OK", "text/plain", b"ok".to_vec())
            } else if path.contains("..") {
                ("404 Not Found", "text/plain", Vec::new())
            } else {
                match std::fs::read(root.join(path.trim_start_matches('/'))) {
                    Ok(body) => ("200 OK", content_type(path), body),
                    Err(_) => ("404 Not Found", "text/plain", Vec::new()),
                }
            };
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(&body);
        }
    });
}

/// An address of this machine that is not the loopback, if it has one. A page loaded from
/// the loopback counts as secure to a browser even on `http://`; one loaded from here does
/// not.
#[must_use]
pub fn outside_address() -> Option<std::net::IpAddr> {
    // Asking which address would be used to reach somewhere else sends nothing.
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect(SocketAddr::from(([192, 0, 2, 1], 9))).ok()?;
    let address = socket.local_addr().ok()?.ip();
    (!address.is_loopback() && !address.is_unspecified()).then_some(address)
}
