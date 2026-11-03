//! Update intents over peer sync: an intent written on one node reaches another through the
//! summary and diff exchange, a delete travels as a tombstone, and a late copy of an old write
//! does not bring the intent back (`docs/update-recovery.md`, "How this interacts with HLC").

use super::*;
use orion::{
    HlcTimestamp,
    control_plane::{
        DesiredStateMutation, DesiredStateSection, SyncDiffRequest, UpdateIntentRecord,
    },
};
use orion_core::UpdateIntentId;

fn update_node(id: &'static str) -> NodeApp {
    NodeApp::builder()
        .config(test_node_config_with_auth(
            id,
            id,
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build")
}

async fn serve_node(
    node: &NodeApp,
) -> (
    HttpClient,
    tokio::task::JoinHandle<Result<(), HttpTransportError>>,
) {
    let (addr, server) = node
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("HTTP server should start");
    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");
    (client, server)
}

/// `to` asks `from` for the update intents it is missing or behind on, and merges the answer.
async fn pull_intents(from: &HttpClient, to: &NodeApp, to_client: &HttpClient) {
    let summary = to
        .desired_state_summary_for_sections(&[DesiredStateSection::UpdateIntents])
        .expect("summary");
    let answer = from
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::SyncDiffRequest(SyncDiffRequest {
                node_id: NodeId::new("node-b"),
                desired_revision: to.current_desired_revision(),
                desired_summary: summary,
                sections: vec![DesiredStateSection::UpdateIntents],
                object_selectors: Vec::new(),
            }),
        )))
        .await
        .expect("diff request should succeed");
    let batch = match answer {
        HttpResponsePayload::Mutations(batch) => batch,
        other => panic!("expected stamped versions, got {other:?}"),
    };
    assert!(batch.is_stamped());
    to_client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(batch),
        )))
        .await
        .expect("merge should succeed");
}

async fn push_stamped(client: &HttpClient, mutation: DesiredStateMutation, stamp: HlcTimestamp) {
    client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(MutationBatch::stamped(
                Revision::ZERO,
                vec![(mutation, stamp)],
            )),
        )))
        .await
        .expect("stamped push should succeed");
}

#[tokio::test]
async fn update_intents_converge_across_sync_including_deletes() {
    let a = update_node("node-a-intent-sync");
    let b = update_node("node-b-intent-sync");
    let (a_client, a_server) = serve_node(&a).await;
    let (b_client, b_server) = serve_node(&b).await;

    // Stamps a little in the past (inside the tombstone retention) and now.
    let now = NodeApp::wall_clock_ms();
    let written_at = HlcTimestamp::new(now - 1_000, 0, 1);
    let deleted_at = HlcTimestamp::new(now, 0, 1);
    let intent = UpdateIntentRecord::new("node-b-intent-sync", "os", "2.0");
    let id: UpdateIntentId = intent.intent_id.clone();

    // The requester writes the intent to node A.
    push_stamped(
        &a_client,
        DesiredStateMutation::PutUpdateIntent(intent.clone()),
        written_at,
    )
    .await;
    assert!(a.current_desired_state().update_intents.contains_key(&id));

    // Node B pulls it from A.
    pull_intents(&a_client, &b, &b_client).await;
    assert_eq!(
        b.current_desired_state().update_intents.get(&id),
        Some(&intent)
    );
    assert_eq!(
        a.desired_metadata_for_test()
            .expect("metadata")
            .2
            .update_intents,
        b.desired_metadata_for_test()
            .expect("metadata")
            .2
            .update_intents,
        "the update intent section fingerprints must agree"
    );

    // The requester deletes it on A. B pulls the tombstone.
    push_stamped(
        &a_client,
        DesiredStateMutation::RemoveUpdateIntent(id.clone()),
        deleted_at,
    )
    .await;
    assert!(!a.current_desired_state().update_intents.contains_key(&id));
    pull_intents(&a_client, &b, &b_client).await;
    let b_state = b.current_desired_state();
    assert!(!b_state.update_intents.contains_key(&id));
    assert_eq!(
        b_state.tombstones.update_intents.get(&id),
        Some(&deleted_at)
    );
    assert_eq!(
        a.desired_metadata_for_test()
            .expect("metadata")
            .2
            .update_intents,
        b.desired_metadata_for_test()
            .expect("metadata")
            .2
            .update_intents,
    );

    // A late copy of the old write (a peer that kept it) does not resurrect the intent.
    push_stamped(
        &b_client,
        DesiredStateMutation::PutUpdateIntent(intent.clone()),
        written_at,
    )
    .await;
    assert!(!b.current_desired_state().update_intents.contains_key(&id));

    a_server.abort();
    b_server.abort();
}
