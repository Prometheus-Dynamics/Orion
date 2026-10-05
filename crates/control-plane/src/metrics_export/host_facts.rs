//! Prometheus text export for host facts (`docs/host-facts.md`) and link gateway counters
//! (`docs/link-protocol.md`).

use super::format::{gauge, metric_help, metric_type, sample};
use crate::{HostFacts, LinkStatusSnapshot, TypedConfigValue};
use orion_core::NodeId;

/// Renders the host-facts families on their own.
pub fn render_host_facts_metrics(node_id: &NodeId, facts: Option<&HostFacts>) -> String {
    let mut out = String::new();
    append_host_facts_metrics(&mut out, node_id, facts);
    out
}

/// Renders the link families on their own.
pub fn render_link_metrics(node_id: &NodeId, links: &[LinkStatusSnapshot]) -> String {
    let mut out = String::new();
    append_link_metrics(&mut out, node_id, links);
    out
}

/// Host identity as an info gauge plus CPU count, temperatures, and numeric extra metrics.
/// Uptime, load, and memory are the `orion_host_*` families of the host section.
pub(super) fn append_host_facts_metrics(
    out: &mut String,
    node_id: &NodeId,
    facts: Option<&HostFacts>,
) {
    let Some(facts) = facts else {
        return;
    };
    let node = node_id.as_str();
    let identity = &facts.identity;
    let text = |value: &Option<String>| value.clone().unwrap_or_default();
    let (hostname, os_id, os_version) = (
        text(&identity.hostname),
        text(&identity.os_id),
        text(&identity.os_version),
    );
    let (image_name, image_version) = (text(&identity.image_name), text(&identity.image_version));
    let (kernel, arch) = (text(&identity.kernel_release), text(&identity.architecture));
    let board_model = text(&identity.board_model);
    gauge(
        out,
        "orion_node_host_info",
        "Host identity facts of the node (always 1).",
        &[
            ("node_id", node),
            ("hostname", &hostname),
            ("os_id", &os_id),
            ("os_version", &os_version),
            ("image_name", &image_name),
            ("image_version", &image_version),
            ("kernel_release", &kernel),
            ("architecture", &arch),
            ("board_model", &board_model),
        ],
        1,
    );
    if let Some(cpus) = identity.cpu_count {
        gauge(
            out,
            "orion_node_host_cpu_count",
            "Online logical CPUs of the host.",
            &[("node_id", node)],
            cpus,
        );
    }
    if !facts.metrics.temperatures.is_empty() {
        let name = "orion_node_host_temperature_celsius";
        metric_help(
            out,
            name,
            "Host temperature sensor reading in degrees Celsius.",
        );
        metric_type(out, name, "gauge");
        for reading in &facts.metrics.temperatures {
            sample(
                out,
                name,
                &[("node_id", node), ("sensor", reading.sensor.as_str())],
                f64::from(reading.millidegrees_c) / 1000.0,
            );
        }
    }
    let numeric: Vec<(&str, f64)> = facts
        .metrics
        .extra
        .iter()
        .filter_map(|(key, value)| {
            let value = match value {
                TypedConfigValue::Int(value) => *value as f64,
                TypedConfigValue::UInt(value) => *value as f64,
                TypedConfigValue::Bool(value) => f64::from(u8::from(*value)),
                TypedConfigValue::String(_) | TypedConfigValue::Bytes(_) => return None,
            };
            Some((key.as_str(), value))
        })
        .collect();
    if !numeric.is_empty() {
        let name = "orion_node_host_metric";
        metric_help(
            out,
            name,
            "Numeric extra host metric reported by the host-facts source.",
        );
        metric_type(out, name, "gauge");
        for (key, value) in numeric {
            sample(out, name, &[("node_id", node), ("key", key)], value);
        }
    }
}

pub(super) fn append_link_metrics(
    out: &mut String,
    node_id: &NodeId,
    links: &[LinkStatusSnapshot],
) {
    if links.is_empty() {
        return;
    }
    let node = node_id.as_str();
    let family = |out: &mut String, name: &str, help: &str, kind: &str| {
        metric_help(out, name, help);
        metric_type(out, name, kind);
    };
    family(
        out,
        "orion_link_open",
        "1 when the link's port or socket is open.",
        "gauge",
    );
    for link in links {
        let labels = [("node_id", node), ("link", link.name.as_str())];
        sample(out, "orion_link_open", &labels, u8::from(link.open));
    }
    family(
        out,
        "orion_link_devices",
        "Devices with an accepted provider snapshot on the link.",
        "gauge",
    );
    for link in links {
        let labels = [("node_id", node), ("link", link.name.as_str())];
        sample(out, "orion_link_devices", &labels, link.devices.len());
    }
    family(
        out,
        "orion_link_frames_total",
        "Link frames received (rx) and sent (tx).",
        "counter",
    );
    for link in links {
        for (direction, value) in [("rx", link.frames_rx), ("tx", link.frames_tx)] {
            let labels = [
                ("node_id", node),
                ("link", link.name.as_str()),
                ("direction", direction),
            ];
            sample(out, "orion_link_frames_total", &labels, value);
        }
    }
    family(
        out,
        "orion_link_bytes_total",
        "Link bytes received (rx) and sent (tx).",
        "counter",
    );
    for link in links {
        for (direction, value) in [("rx", link.bytes_rx), ("tx", link.bytes_tx)] {
            let labels = [
                ("node_id", node),
                ("link", link.name.as_str()),
                ("direction", direction),
            ];
            sample(out, "orion_link_bytes_total", &labels, value);
        }
    }
    family(
        out,
        "orion_link_errors_total",
        "Link errors by kind: crc, framing, dropped, decode, io.",
        "counter",
    );
    for link in links {
        for (kind, value) in [
            ("crc", link.crc_errors),
            ("framing", link.framing_errors),
            ("dropped", link.dropped),
            ("decode", link.decode_errors),
            ("io", link.io_errors),
        ] {
            let labels = [
                ("node_id", node),
                ("link", link.name.as_str()),
                ("kind", kind),
            ];
            sample(out, "orion_link_errors_total", &labels, value);
        }
    }
    family(
        out,
        "orion_link_rejects_total",
        "Refused device hellos, provider snapshots, and status batches.",
        "counter",
    );
    for link in links {
        for (kind, value) in [
            ("hello", link.hello_rejects),
            ("snapshot", link.snapshot_rejects),
            ("status", link.status_rejects),
        ] {
            let labels = [
                ("node_id", node),
                ("link", link.name.as_str()),
                ("kind", kind),
            ];
            sample(out, "orion_link_rejects_total", &labels, value);
        }
    }
    for (name, help, value) in [
        (
            "orion_link_sessions_total",
            "Device sessions opened on the link.",
            (|link: &LinkStatusSnapshot| link.sessions) as fn(&LinkStatusSnapshot) -> u64,
        ),
        (
            "orion_link_device_timeouts_total",
            "Devices lost to missed heartbeats.",
            |link| link.device_timeouts,
        ),
        (
            "orion_link_status_batches_total",
            "Device status batches accepted into the status lane.",
            |link| link.status_batches,
        ),
    ] {
        family(out, name, help, "counter");
        for link in links {
            let labels = [("node_id", node), ("link", link.name.as_str())];
            sample(out, name, &labels, value(link));
        }
    }
}
