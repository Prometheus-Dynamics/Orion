//! Rendering host facts (`docs/host-facts.md`) and link status (`docs/link-protocol.md`).

use orion_control_plane::{HostMetricsSample, LinkStatusSnapshot, NodeHostFacts};

fn text_or_dash(value: Option<&String>) -> &str {
    value.map_or("-", String::as_str)
}

fn display_or_dash<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

/// Text values with spaces are quoted so `key=value` lines stay splittable.
fn quoted(value: &str) -> String {
    if value.contains(char::is_whitespace) {
        format!("{value:?}")
    } else {
        value.to_owned()
    }
}

/// `key=value` host fields for one-line node summaries.
pub(crate) fn render_host_fields(host: Option<&NodeHostFacts>) -> String {
    let field =
        |pick: fn(&NodeHostFacts) -> Option<&String>| quoted(text_or_dash(host.and_then(pick)));
    format!(
        "hostname={} os={} os_version={} image={} image_version={} kernel={} arch={}",
        field(|host| host.hostname.as_ref()),
        field(|host| host.os_id.as_ref()),
        field(|host| host.os_version.as_ref()),
        field(|host| host.image_name.as_ref()),
        field(|host| host.image_version.as_ref()),
        field(|host| host.kernel_release.as_ref()),
        field(|host| host.architecture.as_ref()),
    )
}

/// `key: value` host lines for `describe node`: identity facts from the observed record, plus
/// volatile metrics when the queried node is the described one.
pub(crate) fn print_host_lines(host: Option<&NodeHostFacts>, metrics: Option<&HostMetricsSample>) {
    let text =
        |pick: fn(&NodeHostFacts) -> Option<&String>| text_or_dash(host.and_then(pick)).to_owned();
    println!("host_hostname: {}", text(|host| host.hostname.as_ref()));
    println!("host_os_id: {}", text(|host| host.os_id.as_ref()));
    println!("host_os_name: {}", text(|host| host.os_name.as_ref()));
    println!("host_os_version: {}", text(|host| host.os_version.as_ref()));
    println!("host_image_name: {}", text(|host| host.image_name.as_ref()));
    println!(
        "host_image_version: {}",
        text(|host| host.image_version.as_ref())
    );
    println!(
        "host_kernel_release: {}",
        text(|host| host.kernel_release.as_ref())
    );
    println!(
        "host_architecture: {}",
        text(|host| host.architecture.as_ref())
    );
    println!("host_boot_id: {}", text(|host| host.boot_id.as_ref()));
    println!(
        "host_cpu_count: {}",
        display_or_dash(host.and_then(|host| host.cpu_count))
    );
    println!(
        "host_memory_total_bytes: {}",
        display_or_dash(host.and_then(|host| host.memory_total_bytes))
    );
    let labels = host
        .map(|host| {
            host.labels
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    println!(
        "host_labels: {}",
        if labels.is_empty() {
            "-".to_owned()
        } else {
            labels.join(",")
        }
    );
    println!(
        "host_uptime_seconds: {}",
        display_or_dash(metrics.and_then(|metrics| metrics.uptime_seconds))
    );
    let load = metrics.map(|metrics| {
        [
            metrics.load_1_milli,
            metrics.load_5_milli,
            metrics.load_15_milli,
        ]
        .map(|value| {
            value.map_or_else(
                || "-".to_owned(),
                |milli| format!("{:.2}", milli as f64 / 1000.0),
            )
        })
        .join(",")
    });
    println!("host_load: {}", load.as_deref().unwrap_or("-"));
    println!(
        "host_memory_available_bytes: {}",
        display_or_dash(metrics.and_then(|metrics| metrics.memory_available_bytes))
    );
    let temperatures = metrics
        .map(|metrics| {
            metrics
                .temperatures
                .iter()
                .map(|reading| {
                    format!(
                        "{}={:.1}C",
                        reading.sensor,
                        f64::from(reading.millidegrees_c) / 1000.0
                    )
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    println!(
        "host_temperatures: {}",
        if temperatures.is_empty() {
            "-".to_owned()
        } else {
            temperatures.join(",")
        }
    );
}

/// `orionctl get links` summary lines.
pub(crate) fn render_links_summary(links: &[LinkStatusSnapshot]) -> String {
    let mut out = format!("links count={}\n", links.len());
    for link in links {
        out.push_str(&format!(
            "link name={} open={} devices={} frames_rx={} frames_tx={} bytes_rx={} bytes_tx={} \
             crc_errors={} framing_errors={} dropped={} decode_errors={} sessions={} \
             device_timeouts={} hello_rejects={} snapshot_rejects={} status_batches={} \
             status_rejects={} io_errors={} last_error={}\n",
            link.name,
            link.open,
            if link.devices.is_empty() {
                "-".to_owned()
            } else {
                link.devices.join(",")
            },
            link.frames_rx,
            link.frames_tx,
            link.bytes_rx,
            link.bytes_tx,
            link.crc_errors,
            link.framing_errors,
            link.dropped,
            link.decode_errors,
            link.sessions,
            link.device_timeouts,
            link.hello_rejects,
            link.snapshot_rejects,
            link.status_batches,
            link.status_rejects,
            link.io_errors,
            link.last_error
                .as_deref()
                .map(|error| format!("{error:?}"))
                .unwrap_or_else(|| "-".to_owned()),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_fields_quote_spaces_and_dash_missing_values() {
        let host = NodeHostFacts {
            hostname: Some("edge-1".into()),
            os_id: Some("debian".into()),
            image_name: Some("edge image".into()),
            ..NodeHostFacts::default()
        };
        assert_eq!(
            render_host_fields(Some(&host)),
            "hostname=edge-1 os=debian os_version=- image=\"edge image\" image_version=- \
             kernel=- arch=-"
        );
        assert!(render_host_fields(None).starts_with("hostname=- os=-"));
    }

    #[test]
    fn links_summary_lists_one_line_per_link() {
        let link = LinkStatusSnapshot {
            name: "serial:/dev/ttyUSB0".into(),
            open: true,
            devices: vec!["imu-board".into()],
            frames_rx: 10,
            last_error: Some("read failed".into()),
            ..LinkStatusSnapshot::default()
        };
        let text = render_links_summary(&[link]);
        assert!(text.starts_with(
            "links count=1\nlink name=serial:/dev/ttyUSB0 open=true devices=imu-board frames_rx=10 "
        ));
        assert!(text.ends_with("last_error=\"read failed\"\n"));
        assert_eq!(render_links_summary(&[]), "links count=0\n");
    }
}
