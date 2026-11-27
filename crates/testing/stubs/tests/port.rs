//! A board that stops and starts again on its port must find the port free. The kernel hands a
//! port from its ephemeral range to any socket on the machine, an outgoing connection's own end
//! among them, so a board's port comes from below that range.

use orion_stubs::{Board, Security};

#[test]
fn a_boards_port_is_never_one_the_kernel_hands_out_by_itself() {
    let ephemeral: u16 = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .expect("the ephemeral range")
        .split_whitespace()
        .next()
        .and_then(|low| low.parse().ok())
        .expect("its low end");
    for name in ["port-one", "port-two", "port-three"] {
        let mut board = Board::start(name);
        let (dir, port) = (board.dir().to_owned(), board.network().port());
        assert!(
            port < ephemeral,
            "{name} listens on {port}, in the ephemeral range from {ephemeral}"
        );
        board.stop();
        let again = Board::start_in(&dir, Security::Off, Some(port));
        assert_eq!(again.network().port(), port);
        drop(again);
    }
}

/// A board stopped to be started again on its port keeps that port from every other board on
/// the machine meanwhile: another test's board choosing a port then, as boards do, could take
/// it, and the board would not start again (`listen: AddrInUse`, then `the board starts:
/// RecvError`).
#[test]
fn a_board_away_keeps_its_port_from_the_boards_that_choose_one_meanwhile() {
    let mut board = Board::start("port-away");
    let (dir, port) = (board.dir().to_owned(), board.network().port());
    board.stop();
    // Another board choosing its port while this one is away, this one's port its first choice.
    let other = orion_stubs::another_board_chooses_its_port_from(port);
    let taken = other.local_addr().expect("its address").port();
    assert_ne!(
        taken, port,
        "another board was handed the port of one that is away"
    );
    let again = Board::start_in(&dir, Security::Off, Some(port));
    assert_eq!(again.network().port(), port);
    drop((again, other));
}
