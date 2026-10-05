//! Generic actions over local IPC: provider handlers, node handlers, rejection, timeouts, and
//! authorization (`docs/actions.md`).

use super::*;
use crate::actions::{ActionContext, ActionOutcome};
use crate::{ControlRequest, ControlSurface};
use orion::control_plane::{
    ActionQuery, ActionReport, ActionRequest, ActionResult, ActionState, ActionTarget,
    ClientEventKind, ClientEventPoll, ClientHello, ClientRole, ProviderStateUpdate,
};
use orion_core::ClientName;
use std::collections::BTreeMap;

const PROVIDER: &str = "provider.actions";
const RESOURCE: &str = "provider.actions.camera";

pub(crate) fn hello(app: &NodeApp, source: &str, role: ClientRole) -> LocalAddress {
    let source = LocalAddress::new(source);
    app.apply_local_control_message(
        &source,
        ControlMessage::ClientHello(ClientHello {
            client_name: ClientName::new(source.as_str()),
            role,
        }),
    )
    .expect("hello should be accepted");
    source
}

pub(crate) fn publish_provider(app: &NodeApp, source: &LocalAddress, provider: &str) {
    app.apply_local_control_message(
        source,
        ControlMessage::ProviderState(ProviderStateUpdate {
            provider: ProviderRecord::builder(
                ProviderId::new(provider),
                app.config.node_id.clone(),
            )
            .resource_type(ResourceType::new("camera.frame"))
            .build(),
            resources: vec![
                ResourceRecord::builder(
                    orion::ResourceId::new(format!("{provider}.camera")),
                    "camera.frame",
                    ProviderId::new(provider),
                )
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .build(),
            ],
        }),
    )
    .expect("provider state should apply");
}

pub(crate) fn watch_requests(app: &NodeApp, source: &LocalAddress, provider: &str) {
    let response = app
        .apply_local_control_message(
            source,
            ControlMessage::WatchActionRequests(vec![ActionTarget::Provider(ProviderId::new(
                provider,
            ))]),
        )
        .expect("handler registration should succeed");
    assert_eq!(response, ControlMessage::Accepted);
}

pub(crate) fn polled_requests(app: &NodeApp, source: &LocalAddress) -> Vec<ActionRequest> {
    match app
        .apply_local_control_message(
            source,
            ControlMessage::PollClientEvents(ClientEventPoll {
                after_sequence: 0,
                max_events: 64,
            }),
        )
        .expect("poll should succeed")
    {
        ControlMessage::ClientEvents(events) => events
            .into_iter()
            .filter_map(|event| match event.event {
                ClientEventKind::ActionRequest(request) => Some(*request),
                _ => None,
            })
            .collect(),
        other => panic!("unexpected poll response: {other:?}"),
    }
}

fn run(app: &NodeApp, source: &LocalAddress, request: ActionRequest) -> ActionResult {
    match app
        .apply_local_control_message(source, ControlMessage::RunAction(Box::new(request)))
        .expect("action should be submitted")
    {
        ControlMessage::ActionResults(mut results) => results.remove(0),
        other => panic!("unexpected run response: {other:?}"),
    }
}

fn report(
    app: &NodeApp,
    source: &LocalAddress,
    result: ActionReport,
) -> Result<ControlMessage, NodeError> {
    app.apply_local_control_message(source, ControlMessage::ReportActionResult(Box::new(result)))
}

fn current(app: &NodeApp, action_id: &str) -> ActionResult {
    app.query_actions(&ActionQuery::action(action_id))
        .pop()
        .expect("action should be tracked")
}

pub(crate) async fn wait_final(app: &NodeApp, action_id: &str) -> ActionResult {
    for _ in 0..500 {
        let result = current(app, action_id);
        if result.state.is_terminal() {
            return result;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "action {action_id} did not finish: {:?}",
        current(app, action_id)
    );
}

fn actions_app(name: &str) -> NodeApp {
    NodeApp::builder()
        .config(test_node_config(NodeId::new(name), name))
        .try_build()
        .expect("node app should build")
}

#[test]
fn provider_handles_resource_actions_and_reports_progress_and_output() {
    let app = actions_app("node-actions-provider");
    let provider = hello(&app, "camera-provider", ClientRole::Provider);
    publish_provider(&app, &provider, PROVIDER);
    let handler = hello(&app, "camera-provider-actions", ClientRole::Provider);
    watch_requests(&app, &handler, PROVIDER);
    let operator = hello(&app, "operator", ClientRole::ControlPlane);
    app.apply_local_control_message(&operator, ControlMessage::WatchActions(ActionQuery::all()))
        .expect("watch should be accepted");

    let accepted = run(
        &app,
        &operator,
        ActionRequest::new(
            "locate-1",
            ActionTarget::Resource(orion::ResourceId::new(RESOURCE)),
            "locate",
        )
        .with_arg("duration_ms", TypedConfigValue::UInt(5_000))
        .with_requested_by("spoofed"),
    );
    assert_eq!(accepted.state, ActionState::Accepted);
    assert_eq!(accepted.handled_by, NodeId::new("node-actions-provider"));
    assert_eq!(
        accepted.requested_by, "local:operator",
        "the node stamps the authenticated requester"
    );

    let requests = polled_requests(&app, &handler);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].name, "locate");
    assert_eq!(
        requests[0].args["duration_ms"],
        TypedConfigValue::UInt(5_000)
    );

    // Only the handler the action was delivered to may report.
    let stranger = hello(&app, "other-provider", ClientRole::Provider);
    assert!(
        report(
            &app,
            &stranger,
            ActionReport::new("locate-1", ActionState::Succeeded)
        )
        .is_err()
    );

    report(
        &app,
        &handler,
        ActionReport::new(
            "locate-1",
            ActionState::Running {
                progress: Some(1_500),
            },
        ),
    )
    .expect("progress is accepted");
    assert_eq!(
        current(&app, "locate-1").state,
        ActionState::Running {
            progress: Some(1_000)
        }
    );
    report(
        &app,
        &handler,
        ActionReport::new("locate-1", ActionState::Succeeded)
            .with_output("blinks", TypedConfigValue::UInt(12)),
    )
    .expect("outcome is accepted");
    let done = current(&app, "locate-1");
    assert_eq!(done.state, ActionState::Succeeded);
    assert_eq!(done.output["blinks"], TypedConfigValue::UInt(12));
    // A late report cannot change a final result.
    report(
        &app,
        &handler,
        ActionReport::new(
            "locate-1",
            ActionState::Failed {
                reason: "late".into(),
            },
        ),
    )
    .expect("late reports are ignored");
    assert_eq!(current(&app, "locate-1").state, ActionState::Succeeded);

    // The watcher saw the lifecycle, coalesced to the newest result per action.
    let watched = match app
        .apply_local_control_message(
            &operator,
            ControlMessage::PollClientEvents(ClientEventPoll {
                after_sequence: 0,
                max_events: 64,
            }),
        )
        .expect("poll")
    {
        ControlMessage::ClientEvents(events) => events
            .into_iter()
            .filter_map(|event| match event.event {
                ClientEventKind::ActionResults(results) => Some(results),
                _ => None,
            })
            .flatten()
            .collect::<Vec<_>>(),
        other => panic!("unexpected poll response: {other:?}"),
    };
    assert_eq!(watched.len(), 1);
    assert_eq!(watched[0].state, ActionState::Succeeded);

    // Resubmitting the same request returns the existing result; a different one is refused.
    let again = run(
        &app,
        &operator,
        ActionRequest::new(
            "locate-1",
            ActionTarget::Resource(orion::ResourceId::new(RESOURCE)),
            "locate",
        )
        .with_arg("duration_ms", TypedConfigValue::UInt(5_000)),
    );
    assert_eq!(again.state, ActionState::Succeeded);
    let conflicting = app.apply_local_control_message(
        &operator,
        ControlMessage::RunAction(Box::new(ActionRequest::new(
            "locate-1",
            ActionTarget::Provider(ProviderId::new(PROVIDER)),
            "reboot",
        ))),
    );
    assert!(conflicting.is_err(), "{conflicting:?}");
}

#[test]
fn actions_without_a_handler_are_rejected_and_recorded() {
    let app = actions_app("node-actions-none");
    let operator = hello(&app, "operator", ClientRole::ControlPlane);
    let provider = hello(&app, "silent-provider", ClientRole::Provider);
    publish_provider(&app, &provider, PROVIDER);
    for (id, target, needle) in [
        (
            "a-node",
            ActionTarget::Node(NodeId::new("node-actions-none")),
            "no handler for action `reboot`",
        ),
        (
            "a-provider",
            ActionTarget::Provider(ProviderId::new(PROVIDER)),
            "no action handler is registered",
        ),
        (
            "a-unknown",
            ActionTarget::Resource(orion::ResourceId::new("missing")),
            "unknown resource missing",
        ),
    ] {
        let result = run(&app, &operator, ActionRequest::new(id, target, "reboot"));
        match &result.state {
            ActionState::Rejected { reason } => assert!(reason.contains(needle), "{reason}"),
            other => panic!("expected a rejection for {id}, got {other:?}"),
        }
    }
    assert_eq!(app.query_actions(&ActionQuery::all()).len(), 3);
    // Handlers register only for local providers and with the matching role.
    let control = hello(&app, "control", ClientRole::ControlPlane);
    assert!(
        app.apply_local_control_message(
            &control,
            ControlMessage::WatchActionRequests(vec![ActionTarget::Provider(ProviderId::new(
                PROVIDER
            ))]),
        )
        .is_err()
    );
    assert!(
        app.apply_local_control_message(
            &provider,
            ControlMessage::WatchActionRequests(vec![ActionTarget::Provider(ProviderId::new(
                "provider.elsewhere"
            ))]),
        )
        .is_err()
    );
}

#[tokio::test]
async fn node_handlers_run_report_progress_and_fail() {
    let app = NodeApp::builder()
        .config(test_node_config(
            NodeId::new("node-actions-handler"),
            "node-actions-handler",
        ))
        .with_action_handler(
            "restart-unit",
            |request: ActionRequest, context: ActionContext| async move {
                let Some(TypedConfigValue::String(unit)) = request.args.get("unit").cloned() else {
                    return ActionOutcome::rejected("argument `unit` is required");
                };
                context.progress(Some(50), BTreeMap::new());
                if unit == "broken.service" {
                    return ActionOutcome::failed("unit failed to start");
                }
                ActionOutcome::Succeeded(BTreeMap::from([(
                    "unit".to_owned(),
                    TypedConfigValue::String(unit),
                )]))
            },
        )
        .try_build()
        .expect("node app should build");
    assert_eq!(app.action_handler_names(), vec!["restart-unit"]);
    let node = ActionTarget::Node(NodeId::new("node-actions-handler"));

    let accepted = app
        .run_action(
            ActionRequest::new("r1", node.clone(), "restart-unit")
                .with_arg("unit", TypedConfigValue::String("camera.service".into())),
            "embedder",
        )
        .expect("action should be submitted");
    assert_eq!(accepted.requested_by, "local:embedder");
    let done = wait_final(&app, "r1").await;
    assert_eq!(done.state, ActionState::Succeeded);
    assert_eq!(
        done.output["unit"],
        TypedConfigValue::String("camera.service".into())
    );

    app.run_action(
        ActionRequest::new("r2", node.clone(), "restart-unit")
            .with_arg("unit", TypedConfigValue::String("broken.service".into())),
        "embedder",
    )
    .expect("submitted");
    assert_eq!(
        wait_final(&app, "r2").await.state,
        ActionState::Failed {
            reason: "unit failed to start".into()
        }
    );
    app.run_action(ActionRequest::new("r3", node, "restart-unit"), "embedder")
        .expect("submitted");
    assert!(matches!(
        wait_final(&app, "r3").await.state,
        ActionState::Rejected { .. }
    ));
}

#[tokio::test]
async fn actions_time_out_at_their_deadline() {
    let app = NodeApp::builder()
        .config(test_node_config(
            NodeId::new("node-actions-timeout"),
            "node-actions-timeout",
        ))
        .with_action_handler("hang", |_request, _context| async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            ActionOutcome::succeeded()
        })
        .try_build()
        .expect("node app should build");
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let sweeper = {
        let app = app.clone();
        tokio::spawn(async move { app.run_action_expiry_for_test(shutdown_rx).await })
    };

    app.run_action(
        ActionRequest::new(
            "t1",
            ActionTarget::Node(NodeId::new("node-actions-timeout")),
            "hang",
        )
        .with_deadline_ms(50),
        "embedder",
    )
    .expect("submitted");
    // A client-handled action whose handler never answers times out the same way.
    let provider = hello(&app, "slow-provider", ClientRole::Provider);
    publish_provider(&app, &provider, PROVIDER);
    watch_requests(&app, &provider, PROVIDER);
    app.run_action(
        ActionRequest::new("t2", ActionTarget::Provider(ProviderId::new(PROVIDER)), "x")
            .with_deadline_ms(50),
        "embedder",
    )
    .expect("submitted");

    assert_eq!(wait_final(&app, "t1").await.state, ActionState::TimedOut);
    assert_eq!(wait_final(&app, "t2").await.state, ActionState::TimedOut);
    assert!(
        report(
            &app,
            &provider,
            ActionReport::new("t2", ActionState::Succeeded)
        )
        .is_ok(),
        "late reports are accepted and ignored"
    );
    assert_eq!(current(&app, "t2").state, ActionState::TimedOut);
    let _ = shutdown_tx.send(true);
    let _ = sweeper.await;
}

#[test]
fn authorization_requires_the_control_plane_role_and_enrolled_peers() {
    let app = actions_app("node-actions-authz");
    let provider = hello(&app, "authz-provider", ClientRole::Provider);
    let request = ActionRequest::new(
        "z1",
        ActionTarget::Node(NodeId::new("node-actions-authz")),
        "reboot",
    );
    let denied = app.serve_local_control_message(
        ControlSurface::LocalIpc,
        provider.clone(),
        LocalAddress::new("orion"),
        ControlMessage::RunAction(Box::new(request.clone())),
    );
    assert!(
        matches!(denied, Err(NodeError::ClientRoleMismatch { .. })),
        "{denied:?}"
    );
    let operator = hello(&app, "authz-operator", ClientRole::ControlPlane);
    let denied = app.serve_local_control_message(
        ControlSurface::LocalIpc,
        operator,
        LocalAddress::new("orion"),
        ControlMessage::ReportActionResult(Box::new(ActionReport::new(
            "z1",
            ActionState::Succeeded,
        ))),
    );
    assert!(denied.is_err(), "{denied:?}");

    // An unauthenticated peer request is refused before it reaches the action registry.
    let denied = app.serve_control_request(ControlRequest::from_http_payload(
        HttpRequestPayload::Control(Box::new(ControlMessage::RunAction(Box::new(request)))),
    ));
    assert!(
        matches!(&denied, Err(NodeError::Authorization(_))),
        "{denied:?}"
    );
    assert!(app.query_actions(&ActionQuery::all()).is_empty());
}

fn claim(
    app: &NodeApp,
    source: &LocalAddress,
    names: &[&str],
) -> Result<ControlMessage, NodeError> {
    app.apply_local_control_message(
        source,
        ControlMessage::ClaimNodeActions(names.iter().map(|name| (*name).to_owned()).collect()),
    )
}

#[test]
fn claimed_node_actions_go_to_the_claiming_client_and_conflicts_are_refused() {
    let app = NodeApp::builder()
        .config(test_node_config(
            NodeId::new("node-actions-claims"),
            "node-actions-claims",
        ))
        .with_action_handler("locate", |_request, _context| async {
            ActionOutcome::succeeded()
        })
        .try_build()
        .expect("node app should build");
    let manager = hello(&app, "device-manager", ClientRole::Provider);
    let other = hello(&app, "other-manager", ClientRole::Executor);
    let operator = hello(&app, "operator", ClientRole::ControlPlane);

    // In-process handlers take their names: claiming one is refused.
    let refused = claim(&app, &manager, &["locate"]);
    assert!(
        matches!(&refused, Err(NodeError::Action(message)) if message.contains("in-process")),
        "{refused:?}"
    );
    assert_eq!(
        claim(&app, &manager, &["update", "reboot"]).expect("claim"),
        ControlMessage::Accepted
    );
    // A name held by another live client is refused; the holder may claim again.
    let refused = claim(&app, &other, &["update"]);
    assert!(
        matches!(&refused, Err(NodeError::Action(message)) if message.contains("already claimed")),
        "{refused:?}"
    );
    assert!(claim(&app, &manager, &["update"]).is_ok());

    let node = ActionTarget::Node(NodeId::new("node-actions-claims"));
    let accepted = run(
        &app,
        &operator,
        ActionRequest::new("u1", node.clone(), "update")
            .with_arg(
                "transfer_id",
                TypedConfigValue::String("transfer-42".into()),
            )
            .with_arg("size", TypedConfigValue::UInt(1 << 20)),
    );
    assert_eq!(accepted.state, ActionState::Accepted);
    let delivered = polled_requests(&app, &manager);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].name, "update");

    // The claiming client may mirror progress under `action.*` keys of the node subject, but
    // not publish other node keys.
    let progress = ActionResult::new(
        "u1",
        node.clone(),
        "update",
        NodeId::new("node-actions-claims"),
        ActionState::Running {
            progress: Some(400),
        },
    );
    app.apply_local_control_message(
        &manager,
        ControlMessage::PublishStatus(progress.status_entries()),
    )
    .expect("a claim holder publishes action status for the node");
    assert!(
        app.apply_local_control_message(
            &manager,
            ControlMessage::PublishStatus(vec![orion::control_plane::StatusEntry::new(
                orion::control_plane::StatusSubject::Node(NodeId::new("node-actions-claims")),
                "host.uptime_seconds",
                TypedConfigValue::UInt(1),
            )]),
        )
        .is_err()
    );
    // A handler that must reboot reports success with a phase before it goes down.
    report(
        &app,
        &manager,
        ActionReport::new("u1", ActionState::Succeeded)
            .with_output("phase", TypedConfigValue::String("rebooting".into())),
    )
    .expect("report");
    assert_eq!(
        current(&app, "u1").output["phase"],
        TypedConfigValue::String("rebooting".into())
    );

    // Losing the handler releases its claims and fails what was still waiting for it.
    run(
        &app,
        &operator,
        ActionRequest::new("u2", node.clone(), "reboot"),
    );
    app.release_action_handler(&manager);
    assert_eq!(
        current(&app, "u2").state,
        ActionState::Failed {
            reason: "handler disconnected".into()
        }
    );
    let rejected = run(&app, &operator, ActionRequest::new("u3", node, "update"));
    assert!(matches!(rejected.state, ActionState::Rejected { .. }));
    assert!(
        claim(&app, &other, &["update"]).is_ok(),
        "the name is free again"
    );
}
