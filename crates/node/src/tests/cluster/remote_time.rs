//! The `operator` time source over a real `orion+tcp` session (`docs/time-sync.md`).

use super::remote_operator::{enrolled_operator, operator_node};
use super::*;
use crate::time_sync::{TimeSyncConfig, TimeSyncMode};
use orion::control_plane::{OperatorPolicy, StatusQuery};
use orion_client::remote::{OperatorIdentity, RemoteError};

fn report_from_operator() -> TimeSyncConfig {
    TimeSyncConfig {
        mode: TimeSyncMode::Report,
        servers: vec!["operator".to_owned()],
        serve: None,
        serve_authority: false,
        min_poll: Duration::from_secs(64),
        max_poll: Duration::from_secs(1024),
        synced_file: None,
    }
}

fn policy(actions: &[&str]) -> OperatorPolicy {
    OperatorPolicy {
        read: true,
        actions: Some(actions.iter().map(|name| (*name).to_owned()).collect()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_operator_allowed_clock_set_is_the_nodes_time_source() {
    let node = operator_node("node-time", None).await;
    let operator = enrolled_operator(
        &node,
        OperatorIdentity::generate("clock").expect("identity"),
        policy(&["clock.set"]),
    )
    .await;
    let sync = node
        .app
        .start_time_sync(report_from_operator())
        .expect("sync starts")
        .expect("sync is on");

    // Generous: the offer is consumed by the sync task of a possibly busy machine.
    let found = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            operator.offer_time(3).await.expect("the offer is taken");
            let entries = operator
                .status(StatusQuery::default())
                .await
                .expect("status");
            if entries.iter().any(|entry| {
                entry.key.as_str() == "time.source"
                    && entry.value == TypedConfigValue::String("operator".to_owned())
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(found.is_ok(), "the node never used the operator's clock");
    sync.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_operator_without_clock_set_may_not_offer_its_clock() {
    let node = operator_node("node-time-refused", None).await;
    let operator = enrolled_operator(
        &node,
        OperatorIdentity::generate("other").expect("identity"),
        policy(&["locate"]),
    )
    .await;
    let sync = node
        .app
        .start_time_sync(report_from_operator())
        .expect("sync starts")
        .expect("sync is on");
    let refused = operator.offer_time(3).await;
    assert!(
        matches!(&refused, Err(RemoteError::Rejected(why)) if why.contains("clock.set")),
        "{refused:?}"
    );
    sync.shutdown().await;
}
