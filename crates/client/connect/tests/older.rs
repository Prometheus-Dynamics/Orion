//! A node of another wire revision is turned away with both numbers, and an older node is asked
//! for nothing it would not know.

#![cfg(target_os = "linux")]

use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use orion_connect::{Address, ClientError, Feature, Identity, Options, Session};
use orion_stubs::Board;
use orion_stubs::older::OlderNode;
use orion_wire::{INTERFACE_LEVEL, INTERFACE_REVISION, kind};

struct Half(UnixStream);

impl orion_connect::LinkRead for Half {
    fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        std::io::Read::read(&mut self.0, into)
    }
}

impl orion_connect::LinkWrite for Half {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        std::io::Write::write_all(&mut self.0, bytes)
    }
}

fn over(link: UnixStream, identity: Option<Identity>) -> Result<Session, ClientError> {
    let write = Half(link.try_clone().expect("the link again"));
    let options = Options {
        identity,
        timeout: Duration::from_millis(500),
        ..Options::default()
    };
    Session::over(Half(link), write, None, "tool", options)
}

#[test]
fn a_node_of_another_wire_revision_is_turned_away_with_both_numbers() {
    // The node welcomes, at another revision: the client says no.
    let (_node, link) = OlderNode::start(INTERFACE_REVISION + 1);
    let error = over(link, None).expect_err("another revision");
    assert!(
        matches!(error, ClientError::Revision { node, client }
            if node == INTERFACE_REVISION + 1 && client == INTERFACE_REVISION),
        "{error:?}"
    );
    let said = error.to_string();
    assert!(
        said.contains(&format!("revision {}", INTERFACE_REVISION + 1)),
        "{said}"
    );

    // The node says no first, naming its own.
    let (_node, link) = OlderNode::rejecting(INTERFACE_REVISION + 7);
    let error = over(link, None).expect_err("rejected");
    assert!(
        matches!(error, ClientError::Revision { node, client }
            if node == INTERFACE_REVISION + 7 && client == INTERFACE_REVISION),
        "{error:?}"
    );
}

#[test]
fn a_node_that_names_no_level_is_at_level_0_and_this_board_at_this_builds() {
    let (node, link) = OlderNode::start(INTERFACE_REVISION);
    let session = over(link, None).expect("an older node at the same revision");
    assert_eq!(session.welcomed().level, 0);
    for feature in Feature::ALL {
        assert!(matches!(
            session.needs(feature),
            Err(ClientError::Level { node: 0, .. })
        ));
    }
    assert!(node.heard().is_empty());

    let board = Board::start("levels");
    let session = Session::connect(
        &Address::Local(board.socket().to_owned()),
        "tool",
        Options::default(),
    )
    .expect("board");
    assert_eq!(session.welcomed().level, INTERFACE_LEVEL);
    for feature in Feature::ALL {
        session.needs(feature).expect("this build's board has it");
    }
}

#[test]
fn a_signed_link_to_a_board_from_before_them_says_so_with_both_levels() {
    let (node, link) = OlderNode::start(INTERFACE_REVISION);
    let started = Instant::now();
    let error = over(link, Some(Identity::generate())).expect_err("unsigned board");
    assert!(
        matches!(
            error,
            ClientError::Level {
                feature: Feature::SignedLink,
                needs: 1,
                node: 0
            }
        ),
        "{error:?}"
    );
    // The signed hello was dropped unanswered; the plain hello that found the level was
    // welcomed; nothing else was sent.
    assert_eq!(node.heard(), [kind::LINK_SIGN_HELLO]);
    assert!(started.elapsed() < Duration::from_secs(5));
}
