//! The ways in that listen on an address of their own: the WebSocket, SSH and NT4 ways in, on
//! addresses another program holds. The comm core starts and serves its socket meanwhile, and
//! each opens on its address once it is free. Settings that cannot work still fail the start.

use std::net::{SocketAddr, TcpListener as StdListener};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use orion_comm_app::{CommCore, CommSettings};
use orion_config::Source;
use orion_server::Retry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-ways-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

fn settings(dir: &Path, extra: &str) -> CommSettings {
    let text = format!(
        "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n{extra}",
        dir.display()
    );
    let mut source = Source::parse(&text, Vec::new()).expect("source");
    let mut settings = CommSettings::from_source(&mut source).expect("settings");
    assert!(source.warnings().is_empty(), "{:?}", source.warnings());
    settings.listen_retry = Retry {
        first: Duration::from_millis(50),
        most: Duration::from_millis(100),
    };
    settings
}

fn held() -> (StdListener, SocketAddr) {
    let holder = StdListener::bind("127.0.0.1:0").expect("bind");
    let addr = holder.local_addr().expect("addr");
    (holder, addr)
}

/// What `addr` first says to `ask`, once something other than the holder answers there.
async fn first_words(addr: SocketAddr, ask: &[u8]) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let said = async {
            let mut stream = TcpStream::connect(addr).await.ok()?;
            stream.write_all(ask).await.ok()?;
            let mut buf = [0_u8; 64];
            let len = stream.read(&mut buf).await.ok()?;
            (len > 0).then(|| String::from_utf8_lossy(&buf[..len]).into_owned())
        };
        if let Ok(Some(said)) = tokio::time::timeout(Duration::from_millis(500), said).await {
            return said;
        }
        assert!(Instant::now() < deadline, "nothing opened on {addr}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

const GET: &[u8] = b"GET / HTTP/1.1\r\nHost: board\r\n\r\n";

#[test]
fn ways_in_on_taken_addresses_open_once_they_are_free_and_the_rest_runs_meanwhile() {
    run(async {
        let dir = temp("taken");
        let (ws, ws_at) = held();
        let (ssh, ssh_at) = held();
        let (nt4, nt4_at) = held();
        let core = CommCore::start(settings(
            &dir,
            &format!(
                "ws.listen = {ws_at}\nssh.listen = {ssh_at}\nssh.sandbox = false\n\
                 nt4.listen = {nt4_at}\n"
            ),
        ))
        .await
        .expect("starts without the ways in that cannot open");
        // Announced at the address it was given; the others are not open.
        assert_eq!(core.ws_addr(), Some(ws_at));
        assert_eq!(core.ssh_addr(), None);
        assert!(core.nt4().is_none());
        // The socket is served meanwhile.
        std::os::unix::net::UnixStream::connect(core.socket()).expect("the socket is served");
        // Several waits go by while they are held.
        tokio::time::sleep(Duration::from_millis(300)).await;

        drop((ws, ssh, nt4));
        // Each is the way in it should be: an HTTP answer that is not an upgrade, an SSH
        // banner, and NT4's own HTTP answer.
        let said = first_words(ws_at, GET).await;
        assert!(said.starts_with("HTTP/1.1 4"), "ws: {said}");
        let said = first_words(ssh_at, b"SSH-2.0-test\r\n").await;
        assert!(said.starts_with("SSH-2.0-"), "ssh: {said}");
        let said = first_words(nt4_at, GET).await;
        assert!(said.starts_with("HTTP/1.1 "), "nt4: {said}");
        drop(core);
        let _ = std::fs::remove_dir_all(dir);
    });
}

#[test]
fn settings_that_cannot_work_still_fail_the_start() {
    run(async {
        let dir = temp("broken");
        // A mapping onto a topic the comm core publishes itself: trying again would not help.
        let error = CommCore::start(settings(
            &dir,
            "nt4.listen = 127.0.0.1:0\nnt4.accept = orion.host:x=double@/X\n",
        ))
        .await
        .expect_err("refused");
        assert!(error.to_string().starts_with("nt4.listen"), "{error}");
        let _ = std::fs::remove_dir_all(dir);
    });
}
