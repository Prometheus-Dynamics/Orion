//! SSH on one network interface (`ssh.interface`), and one that is not there yet.

mod common;

use std::net::{SocketAddr, TcpStream};
use std::path::Path;

use common::{Server, temp};
use orion_check::Security;
use orion_config::Source;
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_ssh::{Helper, SshConfig};

fn keys() -> SharedKeys {
    SharedKeys::new(Keys::open(KeysConfig::in_memory("lab"), Security::Off).expect("keys"))
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

#[test]
fn an_interface_alone_turns_ssh_on_at_port_22_of_that_interface() {
    let mut source = Source::parse("ssh.interface = usb0\n", std::iter::empty()).expect("parse");
    let helper = Helper {
        program: "/bin/true".into(),
        args: Vec::new(),
    };
    let config = SshConfig::from_source(&mut source, Path::new("/tmp"), helper)
        .expect("read")
        .expect("ssh is on");
    assert_eq!(config.interface.as_deref(), Some("usb0"));
    assert_eq!(config.listen, SocketAddr::from(([0, 0, 0, 0], 22)));
}

#[test]
fn listening_on_one_interface_takes_connections_there() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let listener = orion_ssh::listen("127.0.0.1:0".parse().expect("addr"), Some("lo"))
            .expect("listens on lo");
        let at = listener.local_addr().expect("addr");
        let (connected, accepted) =
            tokio::join!(tokio::net::TcpStream::connect(at), listener.accept());
        connected.expect("connects");
        accepted.expect("accepted");
    });
}

#[test]
fn an_interface_that_is_not_there_is_waited_for_not_given_up_on() {
    let dir = temp("interface-absent");
    let port = free_port();
    let server = Server::start(&dir, Security::Off, keys(), |config| {
        config.listen = SocketAddr::from(([127, 0, 0, 1], port));
        config.interface = Some("orionnone0".to_owned());
    });
    // Started, and open nowhere: not on the address alone, which would be wider.
    assert_eq!(server.addr.port(), port);
    assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
    let error = orion_ssh::listen(server.addr, Some("orionnone0")).expect_err("not there");
    assert!(orion_ssh::absent(&error), "{error}");
    drop(server);
    let _ = std::fs::remove_dir_all(dir);
}
