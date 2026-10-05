use super::*;
use orion::control_plane::TypedConfigValue;

const OS_RELEASE: &str = r#"PRETTY_NAME="Debian GNU/Linux 12 (bookworm)"
NAME="Debian GNU/Linux"
VERSION_ID="12"
ID=debian
# comment
IMAGE_ID=edge-image
IMAGE_VERSION='2024.10.1'
"#;

const MEMINFO: &str = "MemTotal:        8040532 kB\n\
MemFree:          512000 kB\n\
MemAvailable:    6123456 kB\n\
SwapTotal:             0 kB\n";

fn temp_root(label: &str) -> PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "orion-host-facts-{label}-{}-{unique}",
        std::process::id()
    ))
}

fn write(root: &Path, path: &str, contents: &str) {
    let path = root.join(path);
    fs::create_dir_all(path.parent().expect("fixture path has a parent"))
        .expect("fixture dir is created");
    fs::write(path, contents).expect("fixture file is written");
}

#[test]
fn parses_os_release_fields() {
    let release = parse_os_release(OS_RELEASE);
    assert_eq!(release.id.as_deref(), Some("debian"));
    assert_eq!(release.name.as_deref(), Some("Debian GNU/Linux"));
    assert_eq!(release.version_id.as_deref(), Some("12"));
    assert_eq!(release.image_id.as_deref(), Some("edge-image"));
    assert_eq!(release.image_version.as_deref(), Some("2024.10.1"));
    assert_eq!(parse_os_release(""), OsRelease::default());
}

#[test]
fn parses_proc_fixtures() {
    assert_eq!(parse_uptime_seconds("12345.67 98765.43\n"), Some(12_345));
    assert_eq!(parse_uptime_seconds("garbage"), None);
    assert_eq!(
        parse_loadavg("0.52 1.05 2.50 2/345 6789\n"),
        (Some(520), Some(1_050), Some(2_500))
    );
    assert_eq!(parse_loadavg(""), (None, None, None));
    assert_eq!(meminfo_bytes(MEMINFO, "MemTotal"), Some(8_040_532 * 1024));
    assert_eq!(
        meminfo_bytes(MEMINFO, "MemAvailable"),
        Some(6_123_456 * 1024)
    );
    assert_eq!(meminfo_bytes(MEMINFO, "Buffers"), None);
    assert_eq!(parse_cpu_list("0-3\n"), Some(4));
    assert_eq!(parse_cpu_list("0-3,6,8-9"), Some(7));
    assert_eq!(parse_cpu_list("0"), Some(1));
    assert_eq!(parse_cpu_list("x"), None);
    assert_eq!(parse_cpu_list(""), None);
}

#[test]
fn image_files_accept_key_values_or_a_bare_version() {
    assert_eq!(
        parse_image_file("IMAGE_NAME=gateway\nVERSION=\"3.2\"\n"),
        ImageInfo {
            name: Some("gateway".into()),
            version: Some("3.2".into()),
        }
    );
    assert_eq!(
        parse_image_file("\n# built by ci\n1.4.0-rc2\n"),
        ImageInfo {
            name: None,
            version: Some("1.4.0-rc2".into()),
        }
    );
}

#[test]
fn thermal_zones_are_labeled_by_type_and_disambiguated() {
    let readings = thermal_readings([
        ("thermal_zone0", Some("cpu-thermal\n"), "48312\n"),
        ("thermal_zone1", Some("gpu-thermal"), "-1500"),
        ("thermal_zone2", Some("gpu-thermal"), "40000"),
        ("thermal_zone3", None, "39000"),
        ("thermal_zone4", Some("broken"), "n/a"),
    ]);
    assert_eq!(
        readings,
        vec![
            HostTemperature::new("cpu-thermal", 48_312),
            HostTemperature::new("gpu-thermal/thermal_zone1", -1_500),
            HostTemperature::new("gpu-thermal/thermal_zone2", 40_000),
            HostTemperature::new("thermal_zone3", 39_000),
        ]
    );
}

#[test]
fn linux_source_reads_a_fixture_root() {
    let root = temp_root("linux");
    write(&root, "etc/os-release", OS_RELEASE);
    write(&root, "proc/sys/kernel/hostname", "edge-1\n");
    write(&root, "proc/sys/kernel/osrelease", "6.6.31-v8+\n");
    write(
        &root,
        "proc/sys/kernel/random/boot_id",
        "0d6a3c62-6e1b-4b8e-9df5-3a0c2a4b5c6d\n",
    );
    write(&root, "sys/devices/system/cpu/online", "0-3\n");
    write(&root, "proc/meminfo", MEMINFO);
    write(&root, "proc/uptime", "3600.12 1000.00\n");
    write(&root, "proc/loadavg", "0.10 0.20 0.30 1/100 42\n");
    write(
        &root,
        "sys/class/thermal/thermal_zone0/type",
        "cpu-thermal\n",
    );
    write(&root, "sys/class/thermal/thermal_zone0/temp", "51234\n");
    write(&root, "sys/class/thermal/thermal_zone10/temp", "30000\n");
    write(&root, "sys/class/thermal/cooling_device0/type", "fan\n");
    write(&root, "opt/image/version", "IMAGE_VERSION=2024.11.0\n");
    write(
        &root,
        "proc/device-tree/serial-number",
        "10000000abcdef12\0",
    );
    write(
        &root,
        "proc/device-tree/model",
        "Raspberry Pi Compute Module 5 Rev 1.0\0",
    );
    write(&root, "sys/class/dmi/id/product_name", "ignored\n");
    write(
        &root,
        "etc/machine-id",
        "4f6c2a0e3b8d4c1f9a7e5d2b1c0a9f8e\n",
    );

    let facts = LinuxHostFactsSource::with_root(&root)
        .with_image_files([
            PathBuf::from("/missing/version"),
            PathBuf::from("/opt/image/version"),
        ])
        .sample();
    let identity = &facts.identity;
    assert_eq!(identity.hostname.as_deref(), Some("edge-1"));
    assert_eq!(identity.os_id.as_deref(), Some("debian"));
    assert_eq!(identity.os_version.as_deref(), Some("12"));
    assert_eq!(identity.image_name.as_deref(), Some("edge-image"));
    assert_eq!(
        identity.image_version.as_deref(),
        Some("2024.11.0"),
        "a configured image file wins over os-release"
    );
    assert_eq!(identity.kernel_release.as_deref(), Some("6.6.31-v8+"));
    assert_eq!(
        identity.architecture.as_deref(),
        Some(std::env::consts::ARCH)
    );
    assert_eq!(
        identity.boot_id.as_deref(),
        Some("0d6a3c62-6e1b-4b8e-9df5-3a0c2a4b5c6d")
    );
    assert_eq!(identity.board_serial.as_deref(), Some("10000000abcdef12"));
    assert_eq!(
        identity.board_model.as_deref(),
        Some("Raspberry Pi Compute Module 5 Rev 1.0"),
        "the device tree wins over DMI"
    );
    assert_eq!(
        identity.machine_id.as_deref(),
        Some("4f6c2a0e3b8d4c1f9a7e5d2b1c0a9f8e")
    );
    assert_eq!(identity.cpu_count, Some(4));
    assert_eq!(identity.memory_total_bytes, Some(8_040_532 * 1024));
    let metrics = &facts.metrics;
    assert_eq!(metrics.uptime_seconds, Some(3_600));
    assert_eq!(metrics.load_1_milli, Some(100));
    assert_eq!(metrics.load_15_milli, Some(300));
    assert_eq!(metrics.memory_available_bytes, Some(6_123_456 * 1024));
    assert_eq!(
        metrics.temperatures,
        vec![
            HostTemperature::new("cpu-thermal", 51_234),
            HostTemperature::new("thermal_zone10", 30_000),
        ]
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn firmware_strings_drop_trailing_nuls_and_fall_back_to_dmi() {
    assert_eq!(firmware_string(b"abc\0\0").as_deref(), Some("abc"));
    assert_eq!(firmware_string(b" SN 01 \n").as_deref(), Some("SN 01"));
    assert_eq!(firmware_string(b"\0\n"), None);

    let root = temp_root("dmi");
    write(&root, "sys/class/dmi/id/product_serial", "VMware-56 4d\n");
    write(
        &root,
        "sys/class/dmi/id/product_name",
        "Standard PC (Q35)\n",
    );
    let facts = LinuxHostFactsSource::with_root(&root).sample();
    assert_eq!(facts.identity.board_serial.as_deref(), Some("VMware-56 4d"));
    assert_eq!(
        facts.identity.board_model.as_deref(),
        Some("Standard PC (Q35)")
    );
    assert_eq!(facts.identity.machine_id, None);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn missing_files_leave_facts_empty() {
    let root = temp_root("empty");
    let facts = LinuxHostFactsSource::with_root(&root).sample();
    assert_eq!(facts.identity.hostname, None);
    assert_eq!(facts.identity.os_id, None);
    assert_eq!(facts.metrics.uptime_seconds, None);
    assert!(facts.metrics.temperatures.is_empty());
}

struct BoardFacts;

impl HostFactsSource for BoardFacts {
    fn sample(&self) -> HostFacts {
        let mut facts = HostFacts::default();
        facts
            .identity
            .labels
            .insert("board.serial".into(), "SN-0042".into());
        facts.metrics.temperatures = vec![HostTemperature::new("pmic", 35_500)];
        facts
            .metrics
            .extra
            .insert("fan.rpm".into(), TypedConfigValue::UInt(2_400));
        facts
    }
}

struct FixedBase;

impl HostFactsSource for FixedBase {
    fn sample(&self) -> HostFacts {
        let mut facts = HostFacts::default();
        facts.identity.hostname = Some("edge-1".into());
        facts.metrics.temperatures = vec![HostTemperature::new("cpu", 40_000)];
        facts
    }
}

#[test]
fn overlays_merge_labels_temperatures_and_extra_metrics() {
    let source =
        LayeredHostFactsSource::new(Arc::new(FixedBase)).with_overlay(Arc::new(BoardFacts));
    let facts = source.sample();
    assert_eq!(facts.identity.hostname.as_deref(), Some("edge-1"));
    assert_eq!(facts.identity.labels["board.serial"], "SN-0042");
    assert_eq!(
        facts.metrics.temperatures,
        vec![
            HostTemperature::new("cpu", 40_000),
            HostTemperature::new("pmic", 35_500)
        ]
    );
    assert_eq!(
        facts.metrics.extra["fan.rpm"],
        TypedConfigValue::UInt(2_400)
    );
}
