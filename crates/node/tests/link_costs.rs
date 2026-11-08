//! What one message costs the node on a `unix:` link: heap allocations and bytes allocated on the
//! node's threads, and bytes on the wire, for a provider's status update, the same update fanned
//! out to a watching client, and a stream batch to a subscriber; plus what the node allocates
//! while idle. Visibility for small boards first: the test prints the numbers and fails only on a
//! blow-up (ceilings far above today's), so a noisy machine does not fail it.
//!
//! The clients run on their own threads, which the counting allocator ignores; everything the
//! node does (gateway tasks, the status lane, the stream hub) runs on the runtime's threads and
//! is counted. Instructions are user-space instructions retired on the runtime's worker threads,
//! from one perf counter per thread; they are left out where perf events are not allowed
//! (`kernel.perf_event_paranoid` above 2, most containers). Run with `--nocapture` to see the
//! table.

#![cfg(all(target_os = "linux", feature = "link-gateway"))]

mod link_costs_harness;

use link_costs_harness::*;
#[test]
fn what_a_message_costs_the_node_on_a_unix_link() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let path = socket_path();
    let _ = std::fs::remove_file(&path);
    let ipc =
        std::env::temp_dir().join(format!("orion-link-costs-ipc-{}.sock", std::process::id()));
    let app = NodeApp::try_new(NodeConfig::for_local_node("node-costs").with_ipc_socket_path(ipc))
        .expect("node app");
    let link = LinkConfig::parse(&format!(
        "unix:{}?heartbeat_ms=5000&missed_heartbeats=3",
        path.display()
    ))
    .expect("link config");
    let gateway = runtime
        .block_on(async { app.start_link_gateway(vec![link]) })
        .expect("gateway");

    // A provider session with one resource.
    let socket = path.clone();
    let mut provider = client(move || {
        let mut provider = Session::connect(&socket, "cost-board", Roles::PROVIDER);
        provider.send(&Message::ProviderState(ProviderState {
            provider: ProviderRecord::builder(ProviderId::new(PROVIDER), "unassigned")
                .resource_type("imu.sample_source")
                .build(),
            resources: vec![
                ResourceRecord::builder(
                    ResourceId::new("cost-board.imu-0"),
                    "imu.sample_source",
                    ProviderId::new(PROVIDER),
                )
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .build(),
            ],
        }));
        provider.sync();
        provider.send(&status(0));
        provider.sync();
        provider
    });
    let query = StatusQuery::all();
    let deadline = Instant::now() + WAIT;
    while app.query_status(&query).is_empty() {
        assert!(
            Instant::now() < deadline,
            "the provider's status never arrived"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    // Idle: what the node allocates per second with one quiet session.
    let before = snapshot();
    let wakeups_before = worker_wakeups();
    let idle_for = Duration::from_secs(1);
    std::thread::sleep(idle_for);
    let after = snapshot();
    let idle_wakeups = worker_wakeups() - wakeups_before;
    let idle = (after.0 - before.0, after.1 - before.1);

    // Status updates from the provider, nobody watching.
    let frames: Vec<Vec<u8>> = (1..=MESSAGES)
        .map(|v| provider.encode(&status(v)))
        .collect();
    let (written, read) = (provider.written, provider.read);
    let before = snapshot();
    let mut provider = client(move || {
        for frame in &frames {
            provider.write(frame);
        }
        provider.sync();
        provider
    });
    let after = snapshot();
    let publish = per(
        MESSAGES,
        before,
        after,
        provider.written - written,
        provider.read - read,
    );

    // The same updates with a client watching: each is fanned out to it.
    let socket = path.clone();
    let mut watcher = client(move || {
        let mut watcher = Session::connect(&socket, "cost-console", Roles::CLIENT);
        watcher.send(&Message::StatusWatch(StatusRequest {
            req: 1,
            filter: StatusQuery::subject(StatusSubject::Provider(ProviderId::new(PROVIDER))),
        }));
        watcher.until(|message| matches!(message, Message::StatusChanges(c) if !c.more));
        watcher
    });
    let frames: Vec<Vec<u8>> = (1..=MESSAGES)
        .map(|v| provider.encode(&status(MESSAGES + v)))
        .collect();
    let (written, watcher_read) = (provider.written, watcher.read);
    let last = TypedConfigValue::UInt(2 * MESSAGES);
    let before = snapshot();
    let (provider, watcher, changes) = client(move || {
        let writer = std::thread::spawn(move || {
            IGNORED.with(|ignored| ignored.set(true));
            for frame in &frames {
                provider.write(frame);
            }
            provider
        });
        let changes = watcher.until_counting(
            |message| matches!(message, Message::StatusChanges(_)),
            |message| {
                matches!(message, Message::StatusChanges(c)
                    if c.updated.iter().any(|entry| entry.value == last))
            },
        );
        (writer.join().expect("writer"), watcher, changes)
    });
    let after = snapshot();
    let watched = per(
        MESSAGES,
        before,
        after,
        provider.written - written,
        watcher.read - watcher_read,
    );

    // Stream batches from the provider to a subscriber.
    let mut provider = provider;
    let mut watcher = watcher;
    let (provider, mut watcher) = client(move || {
        provider.send(&Message::StreamDeclare(StreamDeclare {
            req: 9,
            id: "imu".into(),
            series: vec![SeriesDecl::new("imu.accel_x", SeriesKind::F32, 1_000)],
        }));
        provider.until(|message| matches!(message, Message::StreamDemand(_)));
        watcher.send(&Message::StreamSubscribe(Box::new(StreamSubscribe {
            req: 2,
            subscription: StreamSubscription::all(StreamRef {
                owner: StatusSubject::Provider(ProviderId::new(PROVIDER)),
                id: "imu".into(),
            })
            .with_backfill(Backfill::None),
        })));
        watcher.until(|message| matches!(message, Message::StreamCatalog(_)));
        provider.until(|message| matches!(message, Message::StreamDemand(d) if d.subscribers == 1));
        (provider, watcher)
    });
    let mut provider = provider;
    let t0 = 1_700_000_000_000_000u64;
    let frames: Vec<Vec<u8>> = (0..BATCHES)
        .map(|batch| {
            let mut builder = SampleChunkBuilder::new(0, SeriesKind::F32);
            for i in 0..SAMPLES {
                builder.push(t0 + (batch * SAMPLES + i) * 1_000, i as f64);
            }
            provider.encode(&Message::StreamPublish(StreamPublish {
                id: "imu".into(),
                chunks: vec![builder.take()],
            }))
        })
        .collect();
    let (written, sub_read) = (provider.written, watcher.read);
    let before = snapshot();
    let (provider, watcher, batches) = client(move || {
        let writer = std::thread::spawn(move || {
            IGNORED.with(|ignored| ignored.set(true));
            for frame in &frames {
                provider.write(frame);
            }
            provider
        });
        // The hub may merge batches into one message: wait for the last batch's sequence.
        let batches = watcher.until_counting(
            |message| matches!(message, Message::StreamData(_)),
            |message| matches!(message, Message::StreamData(d) if d.cursor.seq >= BATCHES),
        );
        (writer.join().expect("writer"), watcher, batches)
    });
    let after = snapshot();
    let streamed = per(
        BATCHES,
        before,
        after,
        provider.written - written,
        watcher.read - sub_read,
    );

    // Stream batches nobody subscribes to (a device's telemetry kept for later readers): the
    // node keeps them in the stream's history. A status update after them, which the watcher
    // sees, marks the last batch handled: frames of a session are handled in order.
    let mut provider = provider;
    let (provider, watcher) = client(move || {
        provider.send(&Message::StreamDeclare(StreamDeclare {
            req: 10,
            id: "temp".into(),
            series: vec![SeriesDecl::new("temp.board", SeriesKind::F32, 1_000)],
        }));
        provider.until(|message| matches!(message, Message::StreamDemand(d) if d.id == "temp"));
        (provider, watcher)
    });
    let mut provider = provider;
    let mut frames: Vec<Vec<u8>> = (0..BATCHES)
        .map(|batch| {
            let mut builder = SampleChunkBuilder::new(0, SeriesKind::F32);
            for i in 0..SAMPLES {
                builder.push(t0 + (batch * SAMPLES + i) * 1_000, i as f64);
            }
            provider.encode(&Message::StreamPublish(StreamPublish {
                id: "temp".into(),
                chunks: vec![builder.take()],
            }))
        })
        .collect();
    let fence = 3 * MESSAGES;
    frames.push(provider.encode(&status(fence)));
    let fence = TypedConfigValue::UInt(fence);
    let written = provider.written;
    let mut watcher = watcher;
    let before = snapshot();
    let (provider, watcher) = client(move || {
        let writer = std::thread::spawn(move || {
            IGNORED.with(|ignored| ignored.set(true));
            for frame in &frames {
                provider.write(frame);
            }
            provider
        });
        watcher.until(|message| {
            matches!(message, Message::StatusChanges(c)
                if c.updated.iter().any(|entry| entry.value == fence))
        });
        (writer.join().expect("writer"), watcher)
    });
    let after = snapshot();
    let unwatched = per(BATCHES, before, after, provider.written - written, 0);

    println!(
        "node idle, one session: {:.0} allocs/s, {:.0} B/s allocated, {:.0} wake-ups/s",
        idle.0 as f64 / idle_for.as_secs_f64(),
        idle.1 as f64 / idle_for.as_secs_f64(),
        idle_wakeups as f64 / idle_for.as_secs_f64()
    );
    print("status update (no watcher)", &publish);
    print("status update fanned out to 1 watch", &watched);
    print(&format!("stream batch ({SAMPLES} f32 samples)"), &streamed);
    print("stream batch, no subscriber", &unwatched);
    println!(
        "watch answers: {changes} for {MESSAGES} updates; stream messages: {batches} for {BATCHES} batches"
    );

    // Blow-up ceilings only (see the module docs), except the unwatched status update: it
    // reuses its read buffer, decoded batch and stored entry, so it allocates only while the
    // pools fill (0.1 per update over this run). The ceiling catches a per-message allocation
    // coming back.
    assert!(
        publish.allocations < 1.0,
        "status update: {:.1} allocs",
        publish.allocations
    );
    assert!(
        watched.allocations < 400.0,
        "fan-out: {:.1} allocs",
        watched.allocations
    );
    // An unsubscribed batch is decoded into the session's recycled chunks and kept in the
    // stream's history in place; the fence's fan-out is spread over the batches.
    assert!(
        unwatched.allocations < 1.0,
        "unsubscribed stream batch: {:.1} allocs",
        unwatched.allocations
    );
    assert!(
        streamed.allocations < 400.0,
        "stream batch: {:.1} allocs",
        streamed.allocations
    );
    // Idle allocates nothing today (link status is updated in place, only when it changed); a
    // few allocations of slack keep the test calm on a busy machine.
    assert!(idle.0 <= 10, "idle: {} allocs in a second", idle.0);
    // An idle link sleeps until its next deadline (the lease check once a second, the session's
    // loss timeout): 1 to 4 wake-ups a second today, against 20 with the old fixed tick. Only
    // voluntary switches are counted, so load does not raise it much; 10 keeps it calm.
    assert!(
        idle_wakeups <= 10,
        "idle: {idle_wakeups} worker wake-ups in a second"
    );

    drop((provider, watcher));
    runtime.block_on(gateway.shutdown());
    let _ = std::fs::remove_file(&path);
}
