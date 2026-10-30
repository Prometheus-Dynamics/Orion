//! Removing a provider retracts the observed resources it last reported, both for a local removal
//! and for a removal merged from a peer. The owner's observed slice then carries the retraction to
//! other nodes (`docs/peer-sync.md`, "Observed state").

use super::*;
use orion::control_plane::{ClientHello, ClientRole, DesiredStateMutation, MutationBatch};

const PROVIDER: &str = "provider.retract";

fn retraction_app() -> NodeApp {
    NodeApp::builder()
        .config(NodeConfig::for_local_node(NodeId::new("node-a")))
        .try_build()
        .expect("node app should build")
}

/// Registers a provider client and publishes one resource for it.
fn publish_provider(app: &NodeApp, source: &LocalAddress) {
    app.apply_local_control_message(
        source,
        ControlMessage::ClientHello(ClientHello {
            client_name: "retract-provider".into(),
            role: ClientRole::Provider,
        }),
    )
    .expect("hello should be accepted");
    app.apply_local_control_message(
        source,
        ControlMessage::ProviderState(ProviderStateUpdate {
            provider: ProviderRecord::builder(PROVIDER, app.config.node_id.clone())
                .resource_type(ResourceType::new("camera.frame"))
                .build(),
            resources: vec![
                ResourceRecord::builder("resource.retract.camera", "camera.frame", PROVIDER)
                    .health(HealthState::Healthy)
                    .availability(AvailabilityState::Available)
                    .build(),
            ],
        }),
    )
    .expect("provider state should apply");
}

fn observed_resources(app: &NodeApp) -> usize {
    app.state_snapshot().state.observed.resources.len()
}

fn provider_is_desired(app: &NodeApp) -> bool {
    app.state_snapshot()
        .state
        .desired
        .providers
        .contains_key(&ProviderId::new(PROVIDER))
}

#[test]
fn a_local_provider_removal_retracts_what_the_provider_last_reported() {
    let app = retraction_app();
    let source = LocalAddress::new("retract-provider");
    publish_provider(&app, &source);
    assert_eq!(observed_resources(&app), 1);
    let observed_before = app.state_snapshot().state.observed.revision;

    // Removing another provider leaves this one's resources alone.
    let base = app.state_snapshot().state.desired.revision;
    app.apply_local_control_message(
        &source,
        ControlMessage::Mutations(MutationBatch::new(
            base,
            vec![DesiredStateMutation::RemoveProvider(ProviderId::new(
                "provider.other",
            ))],
        )),
    )
    .expect("removing an unknown provider should apply");
    assert_eq!(observed_resources(&app), 1);

    let base = app.state_snapshot().state.desired.revision;
    app.apply_local_control_message(
        &source,
        ControlMessage::Mutations(MutationBatch::new(
            base,
            vec![DesiredStateMutation::RemoveProvider(ProviderId::new(
                PROVIDER,
            ))],
        )),
    )
    .expect("provider removal should apply");

    assert!(!provider_is_desired(&app));
    assert_eq!(
        observed_resources(&app),
        0,
        "resources of the removed provider are retracted"
    );
    assert_ne!(
        app.state_snapshot().state.observed.revision,
        observed_before,
        "the retraction changes observed state, so the owner's slice is pushed"
    );
}

#[test]
fn a_provider_removal_merged_from_a_peer_retracts_its_resources_too() {
    let app = retraction_app();
    let source = LocalAddress::new("retract-provider");
    publish_provider(&app, &source);
    assert_eq!(observed_resources(&app), 1);

    // A peer's tombstone, stamped after this node's own write of the provider.
    let stamp = HlcTimestamp::new(NodeApp::wall_clock_ms() + 1_000, 0, 9);
    app.apply_control_message(
        None,
        ControlMessage::Mutations(MutationBatch::stamped(
            Revision::ZERO,
            vec![(
                DesiredStateMutation::RemoveProvider(ProviderId::new(PROVIDER)),
                stamp,
            )],
        )),
    )
    .expect("the peer removal should merge");

    assert!(!provider_is_desired(&app));
    assert_eq!(observed_resources(&app), 0);
}
