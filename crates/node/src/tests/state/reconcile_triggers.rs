//! Reconcile passes are triggered by changes, not by every re-published snapshot.

use super::*;
use orion::control_plane::ClientHello;

const PROVIDER: &str = "provider.noop";

fn trigger_app(node_id: &str) -> NodeApp {
    NodeApp::builder()
        .config(NodeConfig::for_local_node(NodeId::new(node_id)))
        .try_build()
        .expect("node app should build")
}

fn provider_state(app: &NodeApp, label: &str) -> ControlMessage {
    ControlMessage::ProviderState(ProviderStateUpdate {
        provider: ProviderRecord::builder(PROVIDER, app.config.node_id.clone())
            .resource_type(ResourceType::new("camera.frame"))
            .build(),
        resources: vec![
            ResourceRecord::builder("resource.noop.camera", "camera.frame", PROVIDER)
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .label(label)
                .build(),
        ],
    })
}

fn reconciles(app: &NodeApp) -> u64 {
    app.observability_snapshot().reconcile.success_count
}

#[test]
fn identical_provider_snapshot_does_not_trigger_reconcile() {
    let app = trigger_app("node.noop.reconcile");
    let source = LocalAddress::new("noop-provider");
    app.apply_local_control_message(
        &source,
        ControlMessage::ClientHello(ClientHello {
            client_name: "noop-provider".into(),
            role: ClientRole::Provider,
        }),
    )
    .expect("hello should be accepted");

    // The first snapshot creates the provider record and changes observed state: it reconciles.
    let before_first = reconciles(&app);
    app.apply_local_control_message(&source, provider_state(&app, "generation=0"))
        .expect("first provider state should apply");
    let after_first = reconciles(&app);
    assert!(
        after_first > before_first,
        "a new provider record and a changed snapshot must reconcile"
    );

    // Re-publishing the identical snapshot changes nothing, so no pass runs.
    for _ in 0..5 {
        app.apply_local_control_message(&source, provider_state(&app, "generation=0"))
            .expect("repeated provider state should apply");
    }
    assert_eq!(
        reconciles(&app),
        after_first,
        "identical provider snapshots must not trigger reconcile passes"
    );

    // A real change still reconciles.
    app.apply_local_control_message(&source, provider_state(&app, "generation=1"))
        .expect("changed provider state should apply");
    assert!(reconciles(&app) > after_first);
}
