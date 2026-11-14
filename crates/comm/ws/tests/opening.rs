//! A WebSocket listener on an address that is taken: its protocols are said and its carrier
//! made at once, and it takes connections once the address is free.

use std::net::{SocketAddr, TcpListener as StdListener};
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_hello::Hello as HelloService;
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_msg_session::Hello;
use orion_server::{Comm, Limits, Retry, Roles, Security};
use orion_wire::{INTERFACE_REVISION, encode_body, frame, kind};
use orion_ws::{Message, ORION, WsCarrier, WsListener, WsOptions, connect};

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

const QUICK: Retry = Retry {
    first: Duration::from_millis(20),
    most: Duration::from_millis(40),
};

#[test]
fn a_listener_on_a_taken_address_opens_there_once_it_is_free() {
    run(async {
        let holder = StdListener::bind("127.0.0.1:0").expect("bind");
        let addr: SocketAddr = holder.local_addr().expect("addr");
        let error = WsListener::bind(addr, WsOptions::default())
            .await
            .expect_err("taken");
        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        assert!(orion_server::may_clear(&error));

        let keys =
            SharedKeys::new(Keys::open(KeysConfig::in_memory("lab"), Security::Off).expect("keys"));
        let node: orion_types::NodeName = "board".parse().expect("name");
        let comm = Comm::new(
            node.clone(),
            Security::Off,
            Some(Rc::new(keys.clone())),
            Limits::default(),
        );
        comm.add_service(HelloService::new(node, keys, None));

        let mut listener = WsListener::opening(addr, WsOptions::default(), QUICK);
        assert!(listener.local_addr().is_err(), "not open yet");
        // Said before it opens, as on one that is open.
        let carrier = WsCarrier::on(&mut listener);
        let running = tokio::task::spawn_local(listener.run());
        tokio::task::spawn_local(async move { comm.serve(carrier).await });
        // Several waits go by, and it is still trying: the address is the holder's.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!running.is_finished());

        drop(holder);
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut socket = loop {
            match tokio::time::timeout(
                Duration::from_millis(500),
                connect(addr, "/orion", ORION, None),
            )
            .await
            {
                Ok(Ok(socket)) => break socket,
                _ => {
                    assert!(Instant::now() < deadline, "never opened");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        let hello = Hello {
            name: "page",
            roles: Roles::CLIENT,
            max_frame: 64 * 1024,
            revision: INTERFACE_REVISION,
            level: 0,
        };
        let mut out = vec![0_u8; 1024];
        let len = encode_body(&hello, 1, &mut out).expect("encode");
        socket.write.send_binary(&out[..len]).await.expect("send");
        let mut buf = vec![0_u8; 64 * 1024];
        let Some(Message::Binary(len)) = socket.read.recv(&mut buf).await.expect("recv") else {
            panic!("no answer");
        };
        assert_eq!(
            frame::decode(&buf[..len]).expect("frame").kind(),
            kind::WELCOME
        );
    });
}
