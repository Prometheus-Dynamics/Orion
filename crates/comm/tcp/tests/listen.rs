//! The listening socket: on one interface if asked, and tried again until it opens.

use std::net::{SocketAddr, TcpListener as StdListener};
use std::time::Duration;

use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_server::Security;
use orion_tcp::{Listen, Retry, TcpCarrier, TcpOptions};

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

fn options() -> TcpOptions {
    TcpOptions {
        security: Security::Off,
        keys: SharedKeys::new(
            Keys::open(KeysConfig::in_memory("lab"), Security::Off).expect("keys"),
        ),
        max_frame: 64 * 1024,
        handshake: Duration::from_secs(5),
    }
}

const QUICK: Retry = Retry {
    first: Duration::from_millis(20),
    most: Duration::from_millis(40),
};

#[test]
fn a_taken_address_is_tried_again_until_it_is_free() {
    run(async {
        let holder = StdListener::bind("127.0.0.1:0").expect("bind");
        let addr: SocketAddr = holder.local_addr().expect("addr");
        let listen = Listen {
            addr,
            interface: None,
        };
        // Taken: refused at once, as itself.
        let error = TcpCarrier::listen(&listen, options(), QUICK).expect_err("taken");
        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);

        let opening = tokio::task::spawn_local(async move {
            TcpCarrier::listen_retrying(&listen, options(), QUICK).await
        });
        // Several waits go by, and it is still trying.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!opening.is_finished());
        // Free: it opens there.
        drop(holder);
        let carrier = tokio::time::timeout(Duration::from_secs(2), opening)
            .await
            .expect("opened")
            .expect("task");
        assert_eq!(carrier.local_addr().expect("addr"), addr);
    });
}

#[test]
fn the_way_in_listens_on_one_interface_when_told_and_none_that_is_not_there() {
    run(async {
        let on_loopback = Listen {
            addr: "127.0.0.1:0".parse().expect("addr"),
            interface: Some("lo".to_owned()),
        };
        let carrier = TcpCarrier::listen(&on_loopback, options(), QUICK).expect("on lo");
        let addr = carrier.local_addr().expect("addr");
        std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1)).expect("reached");

        // An interface that is not there is refused, as the kernel says.
        let nowhere = Listen {
            addr: "127.0.0.1:0".parse().expect("addr"),
            interface: Some("orion-none0".to_owned()),
        };
        let error = TcpCarrier::listen(&nowhere, options(), QUICK).expect_err("no such interface");
        assert_eq!(error.raw_os_error(), Some(19), "ENODEV: {error}");
        assert_eq!(nowhere.to_string(), "127.0.0.1:0 on orion-none0");

        // Kept to another interface that is up, a connection over loopback is not taken.
        if let Some(other) = up_interface() {
            let elsewhere = Listen {
                addr: "0.0.0.0:0".parse().expect("addr"),
                interface: Some(other.clone()),
            };
            let carrier = TcpCarrier::listen(&elsewhere, options(), QUICK).expect("on another");
            let port = carrier.local_addr().expect("addr").port();
            let loopback = SocketAddr::from(([127, 0, 0, 1], port));
            assert!(
                std::net::TcpStream::connect_timeout(&loopback, Duration::from_secs(1)).is_err(),
                "a connection over lo was taken by a socket kept to {other}"
            );
        }
    });
}

/// A network interface other than loopback that is up, if this machine has one.
fn up_interface() -> Option<String> {
    std::fs::read_dir("/sys/class/net")
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .find(|name| {
            name != "lo"
                && std::fs::read_to_string(format!("/sys/class/net/{name}/operstate"))
                    .is_ok_and(|state| state.trim() == "up")
        })
}
