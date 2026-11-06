//! Heap allocations per value in steady state: a client sets a value, the server relays it, a
//! subscriber receives it. Everything runs on one current-thread runtime, so a thread-local
//! counter sees the client, the server and the WebSocket I/O.

#![cfg(all(feature = "client", feature = "server"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Duration;

use orion_nt4::{
    Client, ClientConfig, ClientEvent, Server, ServerConfig, SubscribeOptions, TopicEvent, Value,
    decode_frame, encode_frame_into,
};
use tokio::time::timeout;

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn count() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

const WAIT: Duration = Duration::from_secs(5);

async fn connect(port: u16, name: &str) -> Client {
    let mut config = ClientConfig::new("127.0.0.1", name);
    config.port = port;
    let mut client = Client::start(config);
    loop {
        match timeout(WAIT, client.next_event()).await.unwrap().unwrap() {
            ClientEvent::Connected => return client,
            ClientEvent::Disconnected { reason } => panic!("connect failed: {reason}"),
            _ => {}
        }
    }
}

#[test]
fn the_codec_encodes_into_a_reused_buffer_without_allocating() {
    let mut buf = Vec::with_capacity(64);
    encode_frame_into(&mut buf, 1, 0, &Value::Double(0.0)).unwrap();
    let before = count();
    for i in 0..1_000 {
        buf.clear();
        encode_frame_into(&mut buf, 1, i, &Value::Double(i as f64)).unwrap();
    }
    assert_eq!(count() - before, 0);
    // A scalar value decodes without allocating.
    let before = count();
    for _ in 0..1_000 {
        let frame = decode_frame(&buf).unwrap();
        assert_eq!(frame.id, 1);
    }
    let decode = (count() - before) as f64 / 1_000.0;
    println!("decode_frame: {decode} allocations per frame");
    assert_eq!(decode, 0.0, "decoding a scalar frame allocates");
}

#[test]
fn a_relayed_value_costs_a_bounded_number_of_allocations() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let server = Server::start(ServerConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            persist_path: None,
        })
        .await
        .unwrap();
        let port = server.local_addr().port();
        let publisher = connect(port, "pub").await;
        let speed = publisher.handle().publish("/app/speed", "double").unwrap();
        let subscriber = connect(port, "sub").await;
        let options = SubscribeOptions {
            prefix: true,
            ..Default::default()
        };
        let mut sub = subscriber.handle().subscribe(&["/app/"], options).unwrap();

        // Each value waits for the previous one to arrive, so nothing coalesces.
        let mut relay = async |rounds: u32, first: u32| {
            for i in first..first + rounds {
                speed.set(Value::Double(f64::from(i))).unwrap();
                loop {
                    let event = timeout(WAIT, sub.next()).await.unwrap().unwrap();
                    if matches!(event, TopicEvent::Value { value: Value::Double(v), .. } if v == f64::from(i))
                    {
                        break;
                    }
                }
            }
        };
        relay(200, 0).await;
        let before = count();
        relay(1_000, 1_000).await;
        let per_value = (count() - before) as f64 / 1_000.0;
        println!("relay: {per_value} allocations per value (client, server and subscriber)");
        // The publisher's WebSocket message, the server's one shared copy for every subscriber,
        // the server's event, and the subscriber's event (its topic name); the rest is the
        // WebSocket and runtime's occasional growth.
        assert!(per_value <= 6.0, "relay: {per_value}");
    });
}
