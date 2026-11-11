//! Fetching from a local server: whole, resumed where the server allows, started over where
//! it does not, and everything a server can get wrong.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orion_files_pull::{Fetch, MAX_PULLS, Pulls, Report, fetchable};

/// What the server is told to do.
#[derive(Clone, Copy, Default)]
struct Manner {
    /// Honour `Range`.
    ranges: bool,
    /// Close the connection after sending this many bytes of the body, on the first request.
    drop_after: Option<usize>,
    /// Answer with this status and no body.
    status: Option<u16>,
    /// Send this many bytes more than the file.
    extra: usize,
    /// Wait this long before answering.
    delay: Duration,
}

/// A small HTTP server for one file, which remembers what it was asked.
struct Server {
    address: SocketAddr,
    /// The `Range` header of each request, if it had one.
    asked: Arc<Mutex<Vec<Option<String>>>>,
}

impl Server {
    fn start(file: Vec<u8>, manner: Manner) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listen");
        let address = listener.local_addr().expect("address");
        let asked = Arc::new(Mutex::new(Vec::new()));
        let heard = asked.clone();
        std::thread::spawn(move || {
            for (number, stream) in listener.incoming().flatten().enumerate() {
                let range = serve(stream, &file, manner, number == 0);
                heard.lock().expect("lock").push(range);
            }
        });
        Self { address, asked }
    }

    fn url(&self) -> String {
        format!("http://{}/file.bin", self.address)
    }

    fn asked(&self) -> Vec<Option<String>> {
        self.asked.lock().expect("lock").clone()
    }
}

/// Answers one request. Returns its `Range` header.
fn serve(mut stream: TcpStream, file: &[u8], manner: Manner, first: bool) -> Option<String> {
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut range = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("range:") {
            range = Some(value.trim().to_owned());
        }
    }
    std::thread::sleep(manner.delay);
    if let Some(status) = manner.status {
        let _ = write!(
            stream,
            "HTTP/1.1 {status} No\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        return range;
    }
    let from = range
        .as_deref()
        .filter(|_| manner.ranges)
        .and_then(|range| range.strip_prefix("bytes="))
        .and_then(|range| range.strip_suffix('-'))
        .and_then(|from| from.parse::<usize>().ok())
        .filter(|from| *from <= file.len());
    let mut body = file[from.unwrap_or(0)..].to_vec();
    body.extend(std::iter::repeat_n(b'!', manner.extra));
    let head = match from {
        Some(from) => format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {from}-{}/{}\r\n",
            file.len() - 1,
            file.len()
        ),
        None => "HTTP/1.1 200 OK\r\n".to_owned(),
    };
    let _ = write!(
        stream,
        "{head}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let send = match manner.drop_after.filter(|_| first) {
        Some(some) => &body[..some.min(body.len())],
        None => &body[..],
    };
    let _ = stream.write_all(send);
    range
}

fn file() -> Vec<u8> {
    (0..300_000_u32)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect()
}

fn part(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-files-pull-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("directory");
    let path = dir.join(format!("{name}.part"));
    let _ = std::fs::remove_file(&path);
    path
}

/// Fetches and waits for how it ended.
fn fetch(pulls: &mut Pulls, url: &str, part: &Path, size: u64) -> Report {
    assert!(pulls.start(Fetch {
        transfer: 7,
        url: url.to_owned(),
        part: part.to_path_buf(),
        size,
    }));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        for report in pulls.poll() {
            if !matches!(report, Report::Received { .. }) {
                assert!(!pulls.active());
                return report;
            }
        }
        assert!(Instant::now() < deadline, "the fetch never ended");
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn a_file_is_fetched_whole() {
    let data = file();
    let server = Server::start(data.clone(), Manner::default());
    let part = part("whole");
    let mut pulls = Pulls::new();
    let report = fetch(&mut pulls, &server.url(), &part, data.len() as u64);
    assert_eq!(report, Report::Fetched { transfer: 7 });
    assert_eq!(std::fs::read(&part).expect("part"), data);
    assert_eq!(
        server.asked(),
        [None],
        "nothing was here: the whole file was asked for"
    );
}

#[test]
fn a_fetch_that_broke_off_goes_on_where_the_server_allows() {
    let data = file();
    let server = Server::start(
        data.clone(),
        Manner {
            ranges: true,
            drop_after: Some(100_000),
            ..Manner::default()
        },
    );
    let part = part("resume");
    let mut pulls = Pulls::new();
    // The first try: the server hangs up part way.
    let report = fetch(&mut pulls, &server.url(), &part, data.len() as u64);
    let Report::Failed { received, why, .. } = report else {
        panic!("{report:?}");
    };
    assert_eq!(received, 100_000, "{why}");
    assert_eq!(std::fs::metadata(&part).expect("part").len(), 100_000);
    // The second: only the rest is asked for, and the file is whole.
    let report = fetch(&mut pulls, &server.url(), &part, data.len() as u64);
    assert_eq!(report, Report::Fetched { transfer: 7 });
    assert_eq!(std::fs::read(&part).expect("part"), data);
    assert_eq!(server.asked(), [None, Some("bytes=100000-".to_owned())]);
}

#[test]
fn a_server_that_does_not_take_ranges_is_fetched_from_the_start() {
    let data = file();
    let server = Server::start(
        data.clone(),
        Manner {
            ranges: false,
            drop_after: Some(100_000),
            ..Manner::default()
        },
    );
    let part = part("no-ranges");
    let mut pulls = Pulls::new();
    let report = fetch(&mut pulls, &server.url(), &part, data.len() as u64);
    assert!(
        matches!(
            report,
            Report::Failed {
                received: 100_000,
                ..
            }
        ),
        "{report:?}"
    );
    // It asks for the rest, is sent everything, and takes everything.
    let report = fetch(&mut pulls, &server.url(), &part, data.len() as u64);
    assert_eq!(report, Report::Fetched { transfer: 7 });
    assert_eq!(std::fs::read(&part).expect("part"), data);
    assert_eq!(server.asked()[1], Some("bytes=100000-".to_owned()));
}

#[test]
fn a_part_that_is_already_whole_is_not_fetched_again() {
    let data = file();
    let server = Server::start(data.clone(), Manner::default());
    let part = part("already");
    std::fs::write(&part, &data).expect("part");
    let mut pulls = Pulls::new();
    let report = fetch(&mut pulls, &server.url(), &part, data.len() as u64);
    assert_eq!(report, Report::Fetched { transfer: 7 });
    assert!(server.asked().is_empty(), "the server was not asked");
    // One that is longer than the file is not that file: it is started over.
    std::fs::write(&part, vec![0_u8; data.len() + 5]).expect("part");
    let report = fetch(&mut pulls, &server.url(), &part, data.len() as u64);
    assert_eq!(report, Report::Fetched { transfer: 7 });
    assert_eq!(std::fs::read(&part).expect("part"), data);
}

#[test]
fn what_a_server_gets_wrong_is_said() {
    let data = file();
    let mut pulls = Pulls::new();

    // It has no such file.
    let server = Server::start(
        data.clone(),
        Manner {
            status: Some(404),
            ..Manner::default()
        },
    );
    let report = fetch(&mut pulls, &server.url(), &part("404"), data.len() as u64);
    let Report::Failed { why, received, .. } = report else {
        panic!("{report:?}");
    };
    assert_eq!((why.as_str(), received), ("the server answered 404", 0));

    // It sends more than the file is.
    let server = Server::start(
        data.clone(),
        Manner {
            extra: 200_000,
            ..Manner::default()
        },
    );
    let too_much = part("too-much");
    let report = fetch(&mut pulls, &server.url(), &too_much, data.len() as u64);
    let Report::Failed { why, .. } = report else {
        panic!("{report:?}");
    };
    assert_eq!(why, "the server sent more than the file's size");
    assert!(std::fs::metadata(&too_much).expect("part").len() <= data.len() as u64);

    // It sends less.
    let server = Server::start(data[..1000].to_vec(), Manner::default());
    let report = fetch(&mut pulls, &server.url(), &part("short"), data.len() as u64);
    let Report::Failed { why, received, .. } = report else {
        panic!("{report:?}");
    };
    assert_eq!(
        (why.as_str(), received),
        ("the server sent 1000 of 300000 bytes", 1000)
    );

    // There is no server.
    let nowhere = TcpListener::bind("127.0.0.1:0").expect("listen");
    let address = nowhere.local_addr().expect("address");
    drop(nowhere);
    let report = fetch(
        &mut pulls,
        &format!("http://{address}/file.bin"),
        &part("nowhere"),
        data.len() as u64,
    );
    let Report::Failed { why, .. } = report else {
        panic!("{report:?}");
    };
    assert!(why.starts_with("the server was not reached"), "{why}");
}

#[test]
fn a_fetch_says_how_far_it_is_and_only_so_many_run_at_once() {
    let data = file();
    let slow = Manner {
        delay: Duration::from_millis(400),
        ..Manner::default()
    };
    let server = Server::start(data.clone(), slow);
    let mut pulls = Pulls::new();
    for transfer in 0..MAX_PULLS as u64 {
        assert!(pulls.start(Fetch {
            transfer,
            url: server.url(),
            part: part(&format!("many-{transfer}")),
            size: data.len() as u64,
        }));
        assert!(pulls.fetching(transfer));
    }
    let one_more = Fetch {
        transfer: 99,
        url: server.url(),
        part: part("many-99"),
        size: data.len() as u64,
    };
    assert!(!pulls.start(one_more.clone()), "no room");
    let again = Fetch {
        transfer: 0,
        ..one_more.clone()
    };
    assert!(!pulls.start(again), "it is already being fetched");
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut fetched = 0;
    while fetched < MAX_PULLS {
        fetched += pulls
            .poll()
            .iter()
            .filter(|report| matches!(report, Report::Fetched { .. }))
            .count();
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(!pulls.active());
    assert!(pulls.start(one_more), "there is room again");
}

#[test]
fn only_urls_this_build_fetches_are_fetchable() {
    assert!(fetchable("http://example.com/a"));
    assert_eq!(fetchable("https://example.com/a"), cfg!(feature = "https"));
    assert!(!fetchable("ftp://example.com/a"));
    assert!(!fetchable("file:///etc/passwd"));
    assert!(!fetchable("/etc/passwd"));
}
