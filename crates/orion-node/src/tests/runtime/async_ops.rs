use super::*;

#[tokio::test(flavor = "current_thread")]
async fn async_snapshot_adoption_does_not_block_the_runtime_thread() {
    let state_dir = temp_state_dir("async-snapshot-adoption");
    let app = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-async-snapshot",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = DesiredClusterState::default();
    desired.put_workload(
        WorkloadRecord::builder("workload.pose", "graph.exec.v1", "artifact.pose")
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .build(),
    );
    let snapshot = orion::control_plane::StateSnapshot {
        state: orion::control_plane::ClusterStateEnvelope::new(
            desired,
            Default::default(),
            Default::default(),
        ),
    };

    set_test_persist_delay(Duration::from_millis(50));
    let tick_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let tick_count_task = tick_count.clone();
    let stop_task = stop.clone();
    let heartbeat = tokio::spawn(async move {
        while !stop_task.load(std::sync::atomic::Ordering::Relaxed) {
            tick_count_task.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    app.adopt_remote_snapshot_async_for_test(snapshot)
        .await
        .expect("async snapshot adoption should succeed");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    heartbeat.await.expect("heartbeat should join");
    clear_test_persist_delay();

    assert!(
        tick_count.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "background task should keep running while async persistence waits"
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tick_async_keeps_state_reads_available_while_provider_snapshot_blocks() {
    let app = Arc::new(
        NodeApp::builder()
            .config(test_node_config_with_auth(
                "node-a",
                "tick-async-provider-lock",
                crate::PeerAuthenticationMode::Disabled,
            ))
            .try_build()
            .expect("node app should build"),
    );
    let (provider, started_rx) =
        BlockingProvider::new("provider.blocking", "resource.blocking", "imu.sample");
    app.register_provider(provider.clone())
        .expect("provider registration should succeed");

    let tick_app = Arc::clone(&app);
    let tick = tokio::spawn(async move { tick_app.tick_async().await });

    started_rx
        .recv_timeout(Duration::from_millis(250))
        .expect("provider snapshot should start");

    let snapshot_app = Arc::clone(&app);
    let (snapshot_tx, snapshot_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let revision = snapshot_app.state_snapshot().state.desired.revision;
        let _ = snapshot_tx.send(revision);
    });

    let revision = snapshot_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("state snapshot should not block while provider snapshot is running");
    assert!(revision > Revision::ZERO);

    provider.release();
    tick.await
        .expect("tick task should join")
        .expect("tick should complete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tick_async_keeps_state_reads_available_while_executor_snapshot_blocks() {
    let app = Arc::new(
        NodeApp::builder()
            .config(test_node_config_with_auth(
                "node-a",
                "tick-async-executor-lock",
                crate::PeerAuthenticationMode::Disabled,
            ))
            .try_build()
            .expect("node app should build"),
    );
    let (executor, started_rx) = BlockingExecutor::new("executor.blocking", "graph.exec.v1");
    app.register_executor(executor.clone())
        .expect("executor registration should succeed");

    let tick_app = Arc::clone(&app);
    let tick = tokio::spawn(async move { tick_app.tick_async().await });

    started_rx
        .recv_timeout(Duration::from_millis(250))
        .expect("executor snapshot should start");

    let snapshot_app = Arc::clone(&app);
    let (snapshot_tx, snapshot_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let revision = snapshot_app.state_snapshot().state.desired.revision;
        let _ = snapshot_tx.send(revision);
    });

    let revision = snapshot_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("state snapshot should not block while executor snapshot is running");
    assert!(revision > Revision::ZERO);

    executor.release();
    tick.await
        .expect("tick task should join")
        .expect("tick should complete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tick_async_makes_progress_under_concurrent_mutation_load() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "tick-async-mutation-load",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    app.register_provider(TestProvider)
        .expect("provider registration should succeed");
    app.register_executor(TestExecutor::new())
        .expect("executor registration should succeed");

    let mutator_app = app.clone();
    let mutator = tokio::spawn(async move {
        for index in 0..24_u32 {
            let mut desired = mutator_app.state_snapshot().state.desired;
            desired.put_node(
                orion::control_plane::NodeRecord::builder(format!("node-churn-{index}")).build(),
            );
            desired.put_artifact(
                orion::control_plane::ArtifactRecord::builder(format!("artifact-churn-{index}"))
                    .build(),
            );
            mutator_app.replace_desired(desired);
            tokio::task::yield_now().await;
        }
    });

    let ticker_app = app.clone();
    let ticker = tokio::spawn(async move {
        for _ in 0..24 {
            ticker_app.tick_async().await?;
            tokio::task::yield_now().await;
        }
        Ok::<(), NodeError>(())
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        mutator.await.expect("mutator task should join");
        ticker
            .await
            .expect("ticker task should join")
            .expect("tick loop should succeed");
    })
    .await
    .expect("tick and mutation load should complete without hanging");

    assert!(
        app.state_snapshot().state.desired.revision > Revision::ZERO,
        "concurrent mutation load should advance desired revision"
    );
}
