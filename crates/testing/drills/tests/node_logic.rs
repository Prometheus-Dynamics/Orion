//! The table's first row. Node logic dies: links, SSH, topics and gateways keep working, and
//! requests for records answer "restarting". It comes back by connecting as a client.

mod support;

use std::time::{Duration, Instant};

use support::{Board, clean};

const LONG: Duration = Duration::from_secs(10);

#[test]
fn node_logic_dies_and_nothing_else_notices() {
    let board = Board::new("node", "");
    let _comm = board.comm();
    let node = board.node();
    let provider = board.role("provider", &[]);
    let publisher = board.role("publisher", &[]);
    let subscriber = board.role("subscriber", &[]);
    let camera = board.role("camera", &[]);
    let viewer = board.role("viewer", &[]);
    subscriber.wait_for("seen", 200, LONG);
    viewer.wait_for("frame", 10, LONG);
    provider.wait_line("registered", LONG);
    board.ctl_until(&["resource", "list"], LONG);
    let (ok, echoed) = board.ctl(&["action", "run", "drill.echo", "--args", "before"]);
    assert!(ok && echoed.contains("before"), "{echoed}");

    node.kill();
    let killed = Instant::now();

    // Topics and frames go on, every value in order and every frame whole.
    let (seen, framed) = (subscriber.last("seen"), viewer.last("frame"));
    subscriber.wait_for("seen", seen + 300, LONG);
    viewer.wait_for("frame", framed + 20, LONG);
    // A new program still gets in, and a topic can still be read.
    let (ok, read) = board.ctl(&["topic", "read", "drill.count"]);
    assert!(ok, "{read}");
    // SSH still answers.
    let banner = board.ssh_banner().expect("SSH answers");
    assert!(banner.starts_with("SSH-2.0-"), "{banner}");
    // Records are node logic's: asked for now, they are refused at once as restarting.
    let asked = Instant::now();
    let (ok, refused) = board.ctl(&["resource", "list"]);
    assert!(!ok && refused.contains("unavailable"), "{refused}");
    assert!(asked.elapsed() < Duration::from_secs(1), "refused at once");
    assert!(killed.elapsed() < LONG);

    // It comes back as a client, and what it answers is answered again.
    let _node = board.node();
    let back = board.ctl_until(&["resource", "list"], LONG);
    let (ok, echoed) = board.ctl(&["action", "run", "drill.echo", "--args", "after"]);
    assert!(ok && echoed.contains("after"), "{echoed}");
    eprintln!("node logic answered again {back:?} after it was started");

    // Nobody else had to connect again.
    for proc in [&publisher, &subscriber, &camera, &viewer, &provider] {
        assert_eq!(proc.count("reconnected"), 0, "{}", proc.name);
    }
    clean(&subscriber);
    clean(&viewer);
}
