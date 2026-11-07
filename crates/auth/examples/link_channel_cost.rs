//! CPU cost of the remote link channel (`link_channel`): a full handshake (both sides), and
//! sealing and opening one record of a link frame's size. Run on the target, release build:
//!
//! ```text
//! cargo run --release -p orion-auth --features link-channel --example link_channel_cost
//! ```

use orion_auth::crypto::SigningKey;
use orion_auth::link_channel::{ClientHandshake, ServerHandshake};
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let node = SigningKey::from_bytes(&[9; 32]);
    let operator = SigningKey::from_bytes(&[7; 32]);
    let handshake = || {
        let client = ClientHandshake::new([1; 32], [2; 32]);
        let (hello, server) =
            ServerHandshake::respond(client.hello(), [3; 32], [4; 32], "node-a", &node)
                .expect("respond");
        let done = client
            .finish(&hello, None, "operator:alice", &operator)
            .expect("finish");
        let server = server.finish(&done.auth).expect("server finish");
        (done, server)
    };
    let rounds = 200;
    let started = Instant::now();
    for _ in 0..rounds {
        black_box(handshake());
    }
    println!(
        "handshake (both sides): {:.0} us",
        started.elapsed().as_secs_f64() * 1e6 / f64::from(rounds)
    );

    let (mut client, _server) = handshake();
    for size in [16usize, 64, 256, 1024, 4096] {
        let payload = vec![0x5au8; size];
        let mut wire = Vec::with_capacity(size + 64);
        let mut got = Vec::with_capacity(size);
        let rounds = 20_000u32;
        let started = Instant::now();
        for _ in 0..rounds {
            wire.clear();
            client.sealer.seal_data(black_box(&payload), &mut wire);
        }
        let seal = started.elapsed().as_secs_f64() * 1e9 / f64::from(rounds);
        // Opening needs records sealed in order, by a pair whose counters start together.
        let (mut sender, mut receiver) = handshake();
        let mut batch = Vec::new();
        for _ in 0..rounds {
            sender.sealer.seal_data(&payload, &mut batch);
        }
        let mut at = 0;
        let started = Instant::now();
        for _ in 0..rounds {
            got.clear();
            let (_, used) = receiver
                .opener
                .open_slice(&mut batch[at..], &mut got)
                .expect("open")
                .expect("whole record");
            at += used;
        }
        let open = started.elapsed().as_secs_f64() * 1e9 / f64::from(rounds);
        println!(
            "{size:>5} B frame: seal {seal:>7.0} ns, open {open:>7.0} ns, {:.0} MB/s sealed",
            size as f64 / seal * 1e3
        );
    }
}
