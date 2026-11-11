//! The table's second row. The comm core dies: graphs and every same-board topic, frames
//! included, keep going, because those hold their rings; nothing new connects. It restarts,
//! publishers offer their rings again, and everything connects again with no backoff.

mod support;

use std::time::{Duration, Instant};

use support::{Board, clean};

const LONG: Duration = Duration::from_secs(10);

#[test]
fn the_comm_core_dies_and_the_rings_carry_on() {
    let board = Board::new("comm", "");
    let comm = board.comm();
    let _node = board.node();
    let publisher = board.role("publisher", &[]);
    let subscriber = board.role("subscriber", &[]);
    let camera = board.role("camera", &[]);
    let viewer = board.role("viewer", &[]);
    subscriber.wait_for("seen", 200, LONG);
    viewer.wait_for("frame", 10, LONG);
    board.ctl_until(&["resource", "list"], LONG);

    comm.kill();

    // With nothing between them, the publisher's values and the camera's frames still reach
    // their readers, all of them, in order and whole.
    let (seen, framed) = (subscriber.last("seen"), viewer.last("frame"));
    subscriber.wait_for("seen", seen + 500, LONG);
    viewer.wait_for("frame", framed + 30, LONG);
    // Nothing new gets in.
    let (ok, refused) = board.ctl(&["node", "show"]);
    assert!(!ok, "{refused}");

    // It starts again. Access is not backed off: the command line is in at once.
    let started = Instant::now();
    let _comm = board.comm();
    let access = board.ctl_until(&["node", "show"], LONG);
    assert!(access < Duration::from_secs(2), "in after {access:?}");
    // Everybody is back, node logic included.
    for proc in [&publisher, &subscriber, &camera, &viewer] {
        proc.wait_line("reconnected", LONG);
    }
    board.ctl_until(&["resource", "list"], LONG);
    eprintln!("back {:?} after the comm core started", started.elapsed());

    // The rings were offered again: a reader that is new to this comm core gets the same
    // publisher's values and the same camera's frames.
    let late = board.role("subscriber", &[]);
    let late_viewer = board.role("viewer", &[]);
    let now = publisher.last("published");
    late.wait_for("seen", now + 100, LONG);
    late_viewer.wait_for("frame", camera.last("camera") + 5, LONG);

    // Through all of it the first readers never missed a value or saw a torn frame.
    let (seen, framed) = (subscriber.last("seen"), viewer.last("frame"));
    subscriber.wait_for("seen", seen + 200, LONG);
    viewer.wait_for("frame", framed + 10, LONG);
    for proc in [&subscriber, &viewer, &late, &late_viewer] {
        clean(proc);
    }
}
