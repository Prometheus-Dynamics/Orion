//! What a server with a port of its own uses: an upgrade on a connection it accepted
//! itself, a choice among several subprotocols, and a read that may be dropped half way
//! through a message, as `select!` drops it.

use std::net::SocketAddr;
use std::time::Duration;

use orion_ws::{Heard, WsOptions, WsSocket, accept, connect_on};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn run(test: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(test);
}

/// A server that takes `protocols`, the one it prefers first, and hands over each
/// connection it upgrades.
async fn server(
    protocols: &'static [&'static str],
) -> (SocketAddr, tokio::sync::mpsc::Receiver<WsSocket>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
    let addr = listener.local_addr().expect("addr");
    let (hand, taken) = tokio::sync::mpsc::channel(4);
    // A socket goes to another task: it has to be `Send`.
    tokio::spawn(async move {
        loop {
            let (stream, from) = listener.accept().await.expect("accept");
            if let Ok(Some(socket)) = accept(stream, from, &WsOptions::default(), protocols).await {
                let _ = hand.send(socket).await;
            }
        }
    });
    (addr, taken)
}

/// One frame as a client sends it: masked.
fn frame(opcode: u8, last: bool, payload: &[u8]) -> Vec<u8> {
    let mask = [0x11, 0x22, 0x33, 0x44];
    let mut out = vec![if last { 0x80 } else { 0 } | opcode];
    match payload.len() {
        short @ 0..=125 => out.push(0x80 | short as u8),
        medium @ 126..=65_535 => {
            out.push(0x80 | 126);
            out.extend_from_slice(&(medium as u16).to_be_bytes());
        }
        long => {
            out.push(0x80 | 127);
            out.extend_from_slice(&(long as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(&mask);
    out.extend(
        payload
            .iter()
            .enumerate()
            .map(|(at, byte)| byte ^ mask[at % 4]),
    );
    out
}

/// A client that speaks the bytes itself, after the upgrade.
async fn raw(addr: SocketAddr, protocol: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let request = format!(
        "GET /nt/raw HTTP/1.1\r\nHost: board\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Protocol: {protocol}\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut answer = Vec::new();
    let mut byte = [0_u8; 1];
    while !answer.ends_with(b"\r\n\r\n") {
        assert_eq!(stream.read(&mut byte).await.expect("read"), 1);
        answer.push(byte[0]);
    }
    assert!(
        answer.starts_with(b"HTTP/1.1 101"),
        "{}",
        String::from_utf8_lossy(&answer)
    );
    stream
}

#[test]
fn the_server_chooses_among_what_is_offered_and_the_client_is_told() {
    run(async {
        let (addr, mut taken) = server(&["new", "old"]).await;
        // The client would rather have the old one; the server's order decides.
        let stream = TcpStream::connect(addr).await.expect("connect");
        let client = connect_on(stream, "board:5810", "/nt/panel", &["old", "new"], None)
            .await
            .expect("upgrade");
        assert_eq!(client.protocol, "new");
        let served = taken.recv().await.expect("a connection");
        assert_eq!(
            (served.protocol.as_str(), served.path.as_str()),
            ("new", "/nt/panel")
        );

        // A client that only knows the old one gets it.
        let stream = TcpStream::connect(addr).await.expect("connect");
        let client = connect_on(stream, "board", "/nt/old", &["old"], None)
            .await
            .expect("upgrade");
        assert_eq!(client.protocol, "old");
        assert_eq!(taken.recv().await.expect("a connection").protocol, "old");

        // One that offers neither is turned away, and is told.
        let stream = TcpStream::connect(addr).await.expect("connect");
        let refused = connect_on(stream, "board", "/nt/x", &["other"], None).await;
        assert!(refused.expect_err("refused").to_string().contains("404"));
    });
}

#[test]
fn a_read_that_is_dropped_half_way_carries_on_where_it_was() {
    run(async {
        let (addr, mut taken) = server(&["new"]).await;
        let mut client = raw(addr, "new").await;
        let mut served = taken.recv().await.expect("a connection");

        // A text message of 300 bytes, a ping in the middle of a binary message in three
        // frames, and a long binary message: a byte at a time, slowly.
        let text = "t".repeat(300);
        let long: Vec<u8> = (0..70_000_u32).map(|at| at as u8).collect();
        let mut bytes = frame(0x1, true, text.as_bytes());
        bytes.extend(frame(0x2, false, b"one "));
        bytes.extend(frame(0x9, true, b"are you there"));
        bytes.extend(frame(0x0, false, b"two "));
        bytes.extend(frame(0x0, true, b"three"));
        let slow = bytes.len();
        bytes.extend(frame(0x2, true, &long));
        let writer = tokio::spawn(async move {
            for (at, byte) in bytes.iter().enumerate() {
                client.write_all(&[*byte]).await.expect("write");
                if at < slow && at % 3 == 0 {
                    tokio::time::sleep(Duration::from_micros(200)).await;
                }
            }
            client
        });

        // Each read is given a fraction of a millisecond and then dropped.
        let mut heard = Vec::new();
        let mut dropped = 0;
        while heard.len() < 4 {
            match tokio::time::timeout(Duration::from_micros(300), served.read.next(1 << 20)).await
            {
                Ok(next) => {
                    let what = next.expect("read").expect("open");
                    heard.push((what, served.read.message().to_vec()));
                }
                Err(_) => dropped += 1,
            }
        }
        assert!(dropped > 20, "the read was only dropped {dropped} times");
        assert_eq!(heard[0], (Heard::Text, text.into_bytes()));
        // The ping is heard when it comes, in the middle of the message it interrupts.
        assert_eq!(heard[1].0, Heard::Alive);
        assert_eq!(heard[2], (Heard::Binary, b"one two three".to_vec()));
        assert_eq!(heard[3], (Heard::Binary, long));

        // The ping was answered, with what it carried.
        let mut client = writer.await.expect("writer");
        let mut pong = [0_u8; 15];
        client.read_exact(&mut pong).await.expect("pong");
        assert_eq!(
            (pong[0], pong[1], &pong[2..]),
            (0x8A, 13, b"are you there".as_slice())
        );

        // A message longer than is taken ends the connection before it is read.
        client
            .write_all(&frame(0x2, true, &[0; 2000])[..8])
            .await
            .expect("write");
        assert!(served.read.next(1000).await.is_err());
    });
}

#[test]
fn a_ping_is_answered_and_a_goodbye_ends_it() {
    run(async {
        let (addr, mut taken) = server(&["new"]).await;
        let stream = TcpStream::connect(addr).await.expect("connect");
        let mut client = connect_on(stream, "board", "/nt/a", &["new"], None)
            .await
            .expect("upgrade");
        let mut served = taken.recv().await.expect("a connection");

        served.write.ping().await.expect("ping");
        assert_eq!(
            client.read.next(64).await.expect("read"),
            Some(Heard::Alive)
        );
        assert_eq!(
            served.read.next(64).await.expect("read"),
            Some(Heard::Alive)
        );

        client.write.send_text("hello").await.expect("send");
        assert_eq!(served.read.next(64).await.expect("read"), Some(Heard::Text));
        assert_eq!(served.read.message(), b"hello");
        served.write.send_binary(&[1, 2, 3]).await.expect("send");
        assert_eq!(
            client.read.next(64).await.expect("read"),
            Some(Heard::Binary)
        );
        assert_eq!(client.read.message(), [1, 2, 3]);

        client.write.close().await.expect("close");
        assert_eq!(served.read.next(64).await.expect("read"), None);
        assert_eq!(client.read.next(64).await.expect("read"), None);
    });
}
