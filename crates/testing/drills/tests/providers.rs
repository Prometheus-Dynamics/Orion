//! A resource's provider is known again after node logic or the comm core restarts: a
//! provider that is still there writes its resources again, and one that is not has them
//! marked lost.

mod support;

use std::time::{Duration, Instant};

use support::Board;

const LONG: Duration = Duration::from_secs(10);
const THING: &str = "drill.thing";
/// Longer than node logic waits to save a change (`records.save_after_ms`, 10 ms here) and
/// the save itself: a kill sooner may lose the resource, which the design allows.
const SAVED: Duration = Duration::from_millis(300);

/// Waits until the resource's state, as `resource show` prints it, is `state`.
fn becomes(board: &Board, state: &str, limit: Duration) -> Duration {
    let began = Instant::now();
    let mut last = String::new();
    while began.elapsed() < limit {
        let (ok, shown) = board.ctl(&["resource", "show", THING]);
        if ok
            && shown
                .lines()
                .next()
                .is_some_and(|line| line.contains(state))
        {
            return began.elapsed();
        }
        last = shown;
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{THING} was not {state} in {limit:?}: {last}");
}

#[test]
fn a_provider_that_stayed_is_known_again_after_node_logic_restarts() {
    let board = Board::new("node-back", "");
    let _comm = board.comm();
    let node = board.node();
    let provider = board.role("provider", &[]);
    provider.wait_line("registered", LONG);
    becomes(&board, "Ready", LONG);
    std::thread::sleep(SAVED);

    node.kill();
    let _node = board.node();
    board.ctl_until(&["resource", "list"], LONG);
    // Past the time a provider has to say it again, it is still there.
    std::thread::sleep(Duration::from_secs(3));
    becomes(&board, "Ready", LONG);

    // And node logic knows who provides it: when the provider goes, it is lost.
    provider.kill();
    let lost = becomes(&board, "Lost", LONG);
    eprintln!("lost {lost:?} after its provider went");
}

#[test]
fn a_provider_that_went_while_node_logic_was_away_has_its_resource_lost() {
    let board = Board::new("node-away", "");
    let _comm = board.comm();
    let node = board.node();
    let provider = board.role("provider", &[]);
    provider.wait_line("registered", LONG);
    becomes(&board, "Ready", LONG);
    std::thread::sleep(SAVED);

    node.kill();
    provider.kill();
    let started = Instant::now();
    let _node = board.node();
    becomes(&board, "Lost", LONG);
    eprintln!("lost {:?} after node logic started", started.elapsed());
}

#[test]
fn a_provider_that_stayed_is_ready_again_after_the_comm_core_restarts() {
    let board = Board::new("comm-back", "");
    let comm = board.comm();
    let _node = board.node();
    let provider = board.role("provider", &[]);
    provider.wait_line("registered", LONG);
    becomes(&board, "Ready", LONG);

    comm.kill();
    let _comm = board.comm();
    provider.wait_line("reconnected", LONG);
    board.ctl_until(&["resource", "list"], LONG);
    std::thread::sleep(Duration::from_secs(3));
    becomes(&board, "Ready", LONG);

    provider.kill();
    becomes(&board, "Lost", LONG);
}
