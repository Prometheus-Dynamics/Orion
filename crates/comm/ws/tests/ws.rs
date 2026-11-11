//! The WebSocket carrier: frames go both ways, the upgrade takes only what it should, and
//! the protocol's corners are handled. Then the same with a real browser.

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use orion_broker::{Broker, BrokerConfig};
use orion_hello::Hello as HelloService;
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_msg_session::{Hello, Identify, Identity, Welcome};
use orion_server::{Comm, Limits, Roles, Security};
use orion_wire::{Body, Bytes, Decode, INTERFACE_REVISION, encode_body, frame, kind};
use orion_ws::{Message, ORION, WsCarrier, WsListener, WsOptions, WsSocket, connect};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

/// A comm core that answers who it is, on a WebSocket listener that also takes a second
/// subprotocol, `echo`, whose connections are handed to the test.
async fn board(options: WsOptions) -> (SocketAddr, orion_ws::Incoming) {
    let keys =
        SharedKeys::new(Keys::open(KeysConfig::in_memory("lab"), Security::Off).expect("keys"));
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
    let mut listener = WsListener::bind("127.0.0.1:0".parse().expect("addr"), options)
        .await
        .expect("listen");
    let addr = listener.local_addr().expect("addr");
    let carrier = WsCarrier::on(&mut listener);
    let echo = listener.protocol("echo");
    tokio::task::spawn_local(listener.run());
    tokio::task::spawn_local(async move { comm.serve(carrier).await });
    (addr, echo)
}

fn encode<B: Body>(body: &B, seq: u16) -> Vec<u8> {
    let mut out = vec![0_u8; 1024];
    let len = encode_body(body, seq, &mut out).expect("encode");
    out.truncate(len);
    out
}

fn hello() -> Vec<u8> {
    encode(
        &Hello {
            name: "page",
            roles: Roles::CLIENT,
            max_frame: 64 * 1024,
            revision: INTERFACE_REVISION,
        },
        1,
    )
}

/// The next Orion frame on `socket` that is not a heartbeat: its kind and payload.
async fn next(socket: &mut WsSocket) -> (u8, Vec<u8>) {
    let mut buf = vec![0_u8; 64 * 1024];
    loop {
        let Some(Message::Binary(len)) = socket.read.recv(&mut buf).await.expect("recv") else {
            panic!("the connection ended, or sent text");
        };
        let view = frame::decode(&buf[..len]).expect("one whole frame a message");
        if !matches!(view.kind(), kind::PING | kind::PONG) {
            return (view.kind(), view.payload().to_vec());
        }
    }
}

#[test]
fn the_same_frames_go_both_ways_over_a_websocket() {
    run(async {
        let (addr, _echo) = board(WsOptions::default()).await;
        let mut socket = connect(addr, "/orion", ORION, None).await.expect("connect");
        assert_eq!(socket.protocol, ORION);
        socket.write.send_binary(&hello()).await.expect("send");
        let (kind, payload) = next(&mut socket).await;
        assert_eq!(kind, kind::WELCOME);
        let welcome = Welcome::decode(&payload).expect("welcome");
        assert_eq!(welcome.node.as_str(), "board");

        // A request and its answer, as on any carrier.
        let asked = encode(
            &Identify {
                nonce: Bytes(b"from a page"),
            },
            2,
        );
        socket.write.send_binary(&asked).await.expect("send");
        let (kind, payload) = next(&mut socket).await;
        assert_eq!(kind, kind::IDENTITY);
        let identity = Identity::decode(&payload).expect("identity");
        assert_eq!(identity.node.as_str(), "board");
        assert_eq!(identity.request_seq, 2);

        // A message in pieces is one message: the browser may split as it likes.
        let again = encode(
            &Identify {
                nonce: Bytes(b"in pieces"),
            },
            3,
        );
        let (first, rest) = again.split_at(5);
        let mut raw = Raw::open(addr, ORION, None).await;
        raw.frame(0x2, false, &hello()).await;
        raw.frame(0x0, true, &[]).await;
        raw.frame(0x2, false, first).await;
        // A ping between the pieces is answered, and does not break the message.
        raw.frame(0x9, true, b"are you there").await;
        raw.frame(0x0, true, rest).await;
        let mut kinds = Vec::new();
        for _ in 0..3 {
            let (opcode, payload) = raw.read().await;
            match opcode {
                0xA => {
                    assert_eq!(payload, b"are you there");
                    kinds.push(0xFF);
                }
                0x2 => kinds.push(frame::decode(&payload).expect("frame").kind()),
                other => panic!("opcode {other:#x}"),
            }
        }
        kinds.sort_unstable();
        assert_eq!(kinds, [kind::WELCOME, kind::IDENTITY, 0xFF]);

        // Goodbye is answered, and the session is over.
        raw.frame(0x8, true, &1000_u16.to_be_bytes()).await;
        let (opcode, payload) = raw.read().await;
        assert_eq!(
            (opcode, payload.as_slice()),
            (0x8, &1000_u16.to_be_bytes()[..])
        );
        socket.write.close().await.expect("close");
        let mut buf = [0_u8; 256];
        loop {
            match socket.read.recv(&mut buf).await {
                Ok(None) | Err(_) => break,
                Ok(Some(_)) => {}
            }
        }
    });
}

/// A WebSocket client down at the bytes, to send what a well-behaved one would not.
struct Raw(TcpStream);

impl Raw {
    async fn request(addr: SocketAddr, request: &str) -> (Self, String) {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream.write_all(request.as_bytes()).await.expect("write");
        let mut answer = Vec::new();
        let mut byte = [0_u8; 1];
        while !answer.ends_with(b"\r\n\r\n") {
            if stream.read(&mut byte).await.expect("read") == 0 {
                break;
            }
            answer.push(byte[0]);
        }
        (Self(stream), String::from_utf8_lossy(&answer).into_owned())
    }

    fn upgrade(protocol: &str, origin: Option<&str>) -> String {
        let origin = origin.map_or(String::new(), |origin| format!("Origin: {origin}\r\n"));
        format!(
            "GET /orion HTTP/1.1\r\nHost: board\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Protocol: {protocol}\r\n{origin}\r\n"
        )
    }

    async fn open(addr: SocketAddr, protocol: &str, origin: Option<&str>) -> Self {
        let (raw, answer) = Self::request(addr, &Self::upgrade(protocol, origin)).await;
        assert!(answer.starts_with("HTTP/1.1 101"), "{answer}");
        // The answer to the key in the standard's own example.
        assert!(
            answer.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"),
            "{answer}"
        );
        raw
    }

    /// Sends one frame, masked as a client's must be.
    async fn frame(&mut self, opcode: u8, last: bool, payload: &[u8]) {
        self.frame_as(opcode, last, payload, true).await;
    }

    async fn frame_as(&mut self, opcode: u8, last: bool, payload: &[u8], masked: bool) {
        let mut out = vec![if last { 0x80 } else { 0 } | opcode];
        let mask_bit = if masked { 0x80 } else { 0 };
        match payload.len() {
            small @ 0..=125 => out.push(mask_bit | small as u8),
            medium @ 126..=65_535 => {
                out.push(mask_bit | 126);
                out.extend_from_slice(&(medium as u16).to_be_bytes());
            }
            large => {
                out.push(mask_bit | 127);
                out.extend_from_slice(&(large as u64).to_be_bytes());
            }
        }
        if masked {
            let mask = [0x11, 0x22, 0x33, 0x44];
            out.extend_from_slice(&mask);
            out.extend(
                payload
                    .iter()
                    .enumerate()
                    .map(|(index, byte)| byte ^ mask[index % 4]),
            );
        } else {
            out.extend_from_slice(payload);
        }
        self.0.write_all(&out).await.expect("write");
    }

    /// The next frame from the server: its opcode and payload. A server's are not masked.
    async fn read(&mut self) -> (u8, Vec<u8>) {
        let mut head = [0_u8; 2];
        self.0.read_exact(&mut head).await.expect("a frame");
        assert_eq!(head[1] & 0x80, 0, "a server does not mask");
        let len = match head[1] & 0x7F {
            126 => u64::from(self.0.read_u16().await.expect("length")),
            127 => self.0.read_u64().await.expect("length"),
            short => u64::from(short),
        };
        let mut payload = vec![0_u8; len as usize];
        self.0.read_exact(&mut payload).await.expect("payload");
        (head[0] & 0x0F, payload)
    }

    /// Whether the server has hung up.
    async fn closed(&mut self) -> bool {
        let mut rest = Vec::new();
        matches!(
            tokio::time::timeout(Duration::from_secs(5), self.0.read_to_end(&mut rest)).await,
            Ok(Ok(_))
        )
    }
}

#[test]
fn the_upgrade_takes_a_websocket_and_nothing_else() {
    run(async {
        let options = WsOptions {
            origins: vec!["https://panel.example".to_owned()],
            handshake: Duration::from_millis(300),
            ..WsOptions::default()
        };
        let (addr, _echo) = board(options).await;

        // An ordinary page request: this is not a web server.
        let (_, answer) = Raw::request(addr, "GET / HTTP/1.1\r\nHost: board\r\n\r\n").await;
        assert!(answer.starts_with("HTTP/1.1 426"), "{answer}");
        // Not HTTP at all.
        let (_, answer) = Raw::request(addr, "hello there\r\n\r\n").await;
        assert!(answer.starts_with("HTTP/1.1 400"), "{answer}");
        // A subprotocol nobody here speaks.
        let (_, answer) = Raw::request(addr, &Raw::upgrade("gopher", None)).await;
        assert!(answer.starts_with("HTTP/1.1 404"), "{answer}");
        // A page from somewhere that may not connect.
        let (_, answer) = Raw::request(
            addr,
            &Raw::upgrade(ORION, Some("https://elsewhere.example")),
        )
        .await;
        assert!(answer.starts_with("HTTP/1.1 403"), "{answer}");
        // The page that may, and a program, which names no page.
        Raw::open(addr, ORION, Some("https://panel.example")).await;
        Raw::open(addr, ORION, None).await;
        // Of several offered, the first that is spoken here.
        let (_, answer) = Raw::request(addr, &Raw::upgrade("gopher, echo, orion.v1", None)).await;
        assert!(
            answer.contains("Sec-WebSocket-Protocol: echo\r\n"),
            "{answer}"
        );

        // A connection that asks for nothing is dropped when the handshake time is up.
        let mut silent = Raw(TcpStream::connect(addr).await.expect("connect"));
        assert!(silent.closed().await);
        // And one that never ends its request, however much it sends.
        let mut endless = TcpStream::connect(addr).await.expect("connect");
        let line = format!("X-Padding: {}\r\n", "a".repeat(1000));
        let _ = endless.write_all(b"GET /orion HTTP/1.1\r\n").await;
        for _ in 0..20 {
            if endless.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
        assert!(Raw(endless).closed().await);
    });
}

#[test]
fn what_does_not_keep_to_the_protocol_ends_the_connection() {
    run(async {
        let (addr, _echo) = board(WsOptions::default()).await;
        // A client that does not mask.
        let mut raw = Raw::open(addr, ORION, None).await;
        raw.frame_as(0x2, true, &hello(), false).await;
        assert!(raw.closed().await);
        // Text where a frame belongs.
        let mut raw = Raw::open(addr, ORION, None).await;
        raw.frame(0x1, true, b"hello").await;
        assert!(raw.closed().await);
        // A message longer than the largest frame: refused by its length, not read.
        let mut raw = Raw::open(addr, ORION, None).await;
        let mut head = vec![0x82, 0x80 | 127];
        head.extend_from_slice(&(1_u64 << 40).to_be_bytes());
        head.extend_from_slice(&[0; 4]);
        raw.0.write_all(&head).await.expect("write");
        assert!(raw.closed().await);
        // A continuation of nothing.
        let mut raw = Raw::open(addr, ORION, None).await;
        raw.frame(0x0, true, b"x").await;
        assert!(raw.closed().await);
        // And the board still takes the next one that behaves.
        let mut socket = connect(addr, "/orion", ORION, None).await.expect("connect");
        socket.write.send_binary(&hello()).await.expect("send");
        assert_eq!(next(&mut socket).await.0, kind::WELCOME);
    });
}

#[test]
fn another_subprotocol_shares_the_listener() {
    run(async {
        let (addr, mut echo) = board(WsOptions::default()).await;
        // What the NT4 gateway will do: take its own subprotocol's connections, text and
        // bytes, from the listener the comm core's frames come in on.
        tokio::task::spawn_local(async move {
            while let Some(mut socket) = echo.next().await {
                let mut buf = vec![0_u8; 70_000];
                while let Ok(Some(message)) = socket.read.recv(&mut buf).await {
                    let sent = match message {
                        Message::Text(len) => {
                            let text = std::str::from_utf8(&buf[..len]).expect("text");
                            socket.write.send_text(&text.to_uppercase()).await
                        }
                        Message::Binary(len) => socket.write.send_binary(&buf[..len]).await,
                    };
                    if sent.is_err() {
                        break;
                    }
                }
            }
        });
        let mut socket = connect(addr, "/nt/page", "echo", None)
            .await
            .expect("connect");
        assert_eq!(
            (socket.protocol.as_str(), socket.path.as_str()),
            ("echo", "/nt/page")
        );
        let mut buf = vec![0_u8; 70_000];
        socket.write.send_text("values").await.expect("send");
        assert_eq!(
            socket.read.recv(&mut buf).await.expect("recv"),
            Some(Message::Text(6))
        );
        assert_eq!(&buf[..6], b"VALUES");
        // Every length a frame can have: short, two bytes of length, eight.
        for len in [0_usize, 125, 126, 65_535, 65_536] {
            let payload: Vec<u8> = (0..len).map(|index| index as u8).collect();
            socket.write.send_binary(&payload).await.expect("send");
            assert_eq!(
                socket.read.recv(&mut buf).await.expect("recv"),
                Some(Message::Binary(len))
            );
            assert_eq!(&buf[..len], payload.as_slice());
        }
        // The comm core's own subprotocol is still there beside it.
        let mut orion = connect(addr, "/orion", ORION, None).await.expect("connect");
        orion.write.send_binary(&hello()).await.expect("send");
        assert_eq!(next(&mut orion).await.0, kind::WELCOME);
    });
}
