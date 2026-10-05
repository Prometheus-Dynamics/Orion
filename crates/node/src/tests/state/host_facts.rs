use super::*;
use crate::HostFactsSource;
use orion::control_plane::{
    HostFacts, HostMetricsSample, HostTemperature, NodeHostFacts, StatusQuery, StatusSubject,
    render_observability_metrics,
};
use std::sync::Mutex;

/// An injectable source whose sample tests change between refreshes.
struct FakeHost(Mutex<HostFacts>);

impl FakeHost {
    fn new() -> Self {
        Self(Mutex::new(HostFacts {
            identity: NodeHostFacts {
                hostname: Some("edge-1".into()),
                os_id: Some("debian".into()),
                os_version: Some("12".into()),
                kernel_release: Some("6.6.31".into()),
                architecture: Some("aarch64".into()),
                boot_id: Some("boot-1".into()),
                cpu_count: Some(4),
                memory_total_bytes: Some(8 << 30),
                ..NodeHostFacts::default()
            },
            metrics: HostMetricsSample {
                uptime_seconds: Some(120),
                load_1_milli: Some(250),
                memory_available_bytes: Some(6 << 30),
                temperatures: vec![HostTemperature::new("cpu-thermal", 48_500)],
                ..HostMetricsSample::default()
            },
            sampled_at_ms: 0,
        }))
    }

    fn update(&self, change: impl FnOnce(&mut HostFacts)) {
        change(&mut self.0.lock().expect("fake host lock"));
    }
}

impl HostFactsSource for FakeHost {
    fn sample(&self) -> HostFacts {
        self.0.lock().expect("fake host lock").clone()
    }
}

fn host_app(name: &str) -> NodeApp {
    NodeApp::builder()
        .config(test_node_config(NodeId::new(name), name))
        .try_build()
        .expect("node app should build")
}

#[test]
fn identity_goes_to_the_observed_record_only_when_it_changes() {
    let app = host_app("node-host");
    let host = FakeHost::new();

    assert!(app.refresh_host_facts_from(&host));
    let published = app.published_host_facts().expect("identity is published");
    assert_eq!(published.hostname.as_deref(), Some("edge-1"));
    assert_eq!(published.cpu_count, Some(4));

    // Volatile metrics change without touching the record.
    host.update(|facts| {
        facts.metrics.uptime_seconds = Some(180);
        facts.metrics.temperatures = vec![HostTemperature::new("cpu-thermal", 51_000)];
    });
    assert!(!app.refresh_host_facts_from(&host));

    // A reboot (new boot id) is an identity change.
    host.update(|facts| facts.identity.boot_id = Some("boot-2".into()));
    assert!(app.refresh_host_facts_from(&host));
    assert_eq!(
        app.published_host_facts()
            .and_then(|facts| facts.boot_id)
            .as_deref(),
        Some("boot-2")
    );
}

#[test]
fn volatile_metrics_go_to_the_status_lane_and_observability() {
    let app = host_app("node-host-status");
    let host = FakeHost::new();
    app.refresh_host_facts_from(&host);

    let entries = app.query_status(&StatusQuery::subject(StatusSubject::Node(NodeId::new(
        "node-host-status",
    ))));
    let value = |key: &str| {
        entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| entry.value.clone())
    };
    assert_eq!(
        value("host.uptime_seconds"),
        Some(TypedConfigValue::UInt(120))
    );
    assert_eq!(value("host.load1_milli"), Some(TypedConfigValue::UInt(250)));
    assert_eq!(
        value("host.temperature.cpu-thermal"),
        Some(TypedConfigValue::Int(48_500))
    );
    assert_eq!(
        value("host.load5_milli"),
        None,
        "unknown values are left out"
    );
    let ttl = entries[0].ttl_ms;
    assert_eq!(
        ttl,
        3 * app
            .config
            .runtime_tuning
            .host_facts
            .refresh_interval
            .as_millis() as u64
    );

    let snapshot = app.observability_snapshot();
    let facts = snapshot.host_facts.as_ref().expect("sample is reported");
    assert!(facts.sampled_at_ms > 0, "the node stamps the sample time");
    let metrics = render_observability_metrics(&snapshot);
    assert!(
        metrics.contains(
            "orion_node_host_info{node_id=\"node-host-status\",hostname=\"edge-1\",os_id=\"debian\""
        ),
        "{metrics}"
    );
    assert!(metrics.contains("orion_node_host_cpu_count{node_id=\"node-host-status\"} 4"));
    assert!(metrics.contains(
        "orion_node_host_temperature_celsius{node_id=\"node-host-status\",sensor=\"cpu-thermal\"} 48.5"
    ));
}

#[test]
fn clients_cannot_publish_node_status() {
    let app = host_app("node-host-owner");
    let source = LocalAddress::new("provider-client");
    app.apply_local_control_message(
        &source,
        ControlMessage::ClientHello(ClientHello {
            client_name: orion_core::ClientName::new("provider-client"),
            role: ClientRole::Provider,
        }),
    )
    .expect("hello");
    let result = app.apply_local_control_message(
        &source,
        ControlMessage::PublishStatus(vec![orion::control_plane::StatusEntry::new(
            StatusSubject::Node(NodeId::new("node-host-owner")),
            "host.uptime_seconds",
            TypedConfigValue::UInt(1),
        )]),
    );
    assert!(result.is_err(), "{result:?}");
}

struct BoardOverlay;

impl HostFactsSource for BoardOverlay {
    fn sample(&self) -> HostFacts {
        let mut facts = HostFacts::default();
        facts
            .identity
            .labels
            .insert("board.serial".into(), "SN-7".into());
        facts.identity.image_version = Some("3.1.0".into());
        facts
    }
}

#[test]
fn builder_sources_and_overlays_are_merged() {
    let app = NodeApp::builder()
        .config(test_node_config(
            NodeId::new("node-host-builder"),
            "node-host-builder",
        ))
        .with_host_facts_source(FakeHost::new())
        .with_host_facts_overlay(BoardOverlay)
        .try_build()
        .expect("node app should build");
    assert!(app.refresh_host_facts());
    let published = app.published_host_facts().expect("published");
    assert_eq!(published.hostname.as_deref(), Some("edge-1"));
    assert_eq!(published.image_version.as_deref(), Some("3.1.0"));
    assert_eq!(published.labels["board.serial"], "SN-7");
    let observed = app.state_snapshot().state.observed;
    assert_eq!(
        observed.nodes[&NodeId::new("node-host-builder")]
            .host
            .as_ref()
            .and_then(|host| host.hostname.as_deref()),
        Some("edge-1")
    );
}
