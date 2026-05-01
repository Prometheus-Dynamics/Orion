use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct Threshold<T> {
    warn: T,
    fail: T,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct PerfConfig {
    idle_cpu_percent: Threshold<f64>,
    local_persistence_replay_ms: BTreeMap<String, Threshold<f64>>,
    release_memory_baseline_bytes: BTreeMap<String, Threshold<u64>>,
    release_cold_replay_ms: BTreeMap<String, BTreeMap<String, Threshold<f64>>>,
    cluster_sync_avg_ms: BTreeMap<String, Threshold<f64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Severity {
    Warn,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Issue {
    severity: Severity,
    message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Args {
    config: PathBuf,
    config_format: Option<StructuredFormat>,
    logs: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StructuredFormat {
    Json,
    Yaml,
    Toml,
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<ExitCode, String> {
    let args = parse_args(env::args().skip(1))?;
    let config = load_config(&args.config, args.config_format)?;
    let issues = collect_issues(&config, &args.logs)?;
    let summary = render_summary(&issues);

    println!("{summary}");
    if let Ok(path) = env::var("GITHUB_STEP_SUMMARY") {
        fs::write(path, &summary).map_err(|error| error.to_string())?;
    }

    if issues.iter().any(|issue| issue.severity == Severity::Fail) {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

fn parse_args<I>(mut args: I) -> Result<Args, String>
where
    I: Iterator<Item = String>,
{
    let Some(flag) = args.next() else {
        return Err(usage());
    };
    if flag != "--config" {
        return Err(usage());
    }

    let Some(config) = args.next() else {
        return Err(usage());
    };
    let mut config_format = None;
    let mut logs = Vec::new();
    while let Some(arg) = args.next() {
        if arg == "--config-format" {
            let Some(value) = args.next() else {
                return Err(usage());
            };
            config_format = Some(parse_structured_format(&value)?);
            continue;
        }
        logs.push(PathBuf::from(arg));
    }
    if logs.is_empty() {
        return Err(usage());
    }

    Ok(Args {
        config: PathBuf::from(config),
        config_format,
        logs,
    })
}

fn usage() -> String {
    "usage: orion-perf-check --config <path> [--config-format json|yaml|toml] <log>...".to_owned()
}

fn load_config(
    path: &Path,
    explicit_format: Option<StructuredFormat>,
) -> Result<PerfConfig, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let format = match explicit_format {
        Some(format) => format,
        None => infer_structured_format(path)?,
    };
    match format {
        StructuredFormat::Json => serde_json::from_str(&text).map_err(|error| error.to_string()),
        StructuredFormat::Yaml => serde_yaml::from_str(&text).map_err(|error| error.to_string()),
        StructuredFormat::Toml => toml::from_str(&text).map_err(|error| error.to_string()),
    }
}

fn parse_structured_format(value: &str) -> Result<StructuredFormat, String> {
    match value {
        "json" => Ok(StructuredFormat::Json),
        "yaml" => Ok(StructuredFormat::Yaml),
        "toml" => Ok(StructuredFormat::Toml),
        other => Err(format!(
            "unsupported config format `{other}`; expected json, yaml, or toml"
        )),
    }
}

fn infer_structured_format(path: &Path) -> Result<StructuredFormat, String> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("could not infer config format for {}", path.display()))?;
    match extension {
        "json" => Ok(StructuredFormat::Json),
        "yaml" | "yml" => Ok(StructuredFormat::Yaml),
        "toml" => Ok(StructuredFormat::Toml),
        other => Err(format!(
            "unsupported config extension `.{other}` for {}",
            path.display()
        )),
    }
}

fn collect_issues(config: &PerfConfig, logs: &[PathBuf]) -> Result<Vec<Issue>, String> {
    let mut issues = Vec::new();

    for log_path in logs {
        let text = fs::read_to_string(log_path).map_err(|error| error.to_string())?;
        for line in text.lines() {
            if let Some((workloads, elapsed_ms)) = parse_local_replay(line)
                && let Some(thresholds) = config.local_persistence_replay_ms.get(workloads)
            {
                check_threshold(
                    &mut issues,
                    &format!("local_persistence_replay[{workloads}]"),
                    elapsed_ms,
                    thresholds,
                    "ms",
                );
            }

            if let Some(cpu_percent) = parse_idle_cpu(line) {
                check_threshold(
                    &mut issues,
                    "idle_cpu_percent",
                    cpu_percent,
                    &config.idle_cpu_percent,
                    "%",
                );
            }

            if let Some((workloads, rss_bytes)) = parse_release_memory(line)
                && let Some(thresholds) = config.release_memory_baseline_bytes.get(workloads)
            {
                check_threshold(
                    &mut issues,
                    &format!("release_memory_baseline[{workloads}]"),
                    rss_bytes,
                    thresholds,
                    "B",
                );
            }

            if let Some((scenario, workloads, elapsed_ms)) = parse_release_replay(line)
                && let Some(thresholds) = config
                    .release_cold_replay_ms
                    .get(scenario)
                    .and_then(|scenarios| scenarios.get(workloads))
            {
                check_threshold(
                    &mut issues,
                    &format!("release_cold_replay[{scenario}][{workloads}]"),
                    elapsed_ms,
                    thresholds,
                    "ms",
                );
            }

            if let Some((node_count, avg_ms)) = parse_cluster_sync(line)
                && let Some(thresholds) = config.cluster_sync_avg_ms.get(node_count)
            {
                check_threshold(
                    &mut issues,
                    &format!("cluster_sync_avg[{node_count}]"),
                    avg_ms,
                    thresholds,
                    "ms",
                );
            }
        }
    }

    Ok(issues)
}

fn check_threshold<T>(
    issues: &mut Vec<Issue>,
    label: &str,
    value: T,
    thresholds: &Threshold<T>,
    unit: &str,
) where
    T: PartialOrd + Copy + std::fmt::Display,
{
    let issue = if value >= thresholds.fail {
        Some(Issue {
            severity: Severity::Fail,
            message: format!(
                "{label}={value}{unit} exceeded fail threshold {}{unit}",
                thresholds.fail
            ),
        })
    } else if value >= thresholds.warn {
        Some(Issue {
            severity: Severity::Warn,
            message: format!(
                "{label}={value}{unit} exceeded warn threshold {}{unit}",
                thresholds.warn
            ),
        })
    } else {
        None
    };

    if let Some(issue) = issue {
        issues.push(issue);
    }
}

fn parse_local_replay(line: &str) -> Option<(&str, f64)> {
    if !line.contains("persistence_replay ") {
        return None;
    }
    Some((
        token_value(line, "workloads=")?,
        token_value(line, "elapsed_ms=")?.parse().ok()?,
    ))
}

fn parse_idle_cpu(line: &str) -> Option<f64> {
    if !line.contains("idle_cpu_baseline ") {
        return None;
    }
    token_value(line, "cpu_percent=")?.parse().ok()
}

fn parse_release_memory(line: &str) -> Option<(&str, u64)> {
    if !line.contains("release_memory_baseline ") {
        return None;
    }
    Some((
        token_value(line, "workloads=")?,
        token_value(line, "rss_bytes=")?.parse().ok()?,
    ))
}

fn parse_release_replay(line: &str) -> Option<(&str, &str, f64)> {
    if !line.contains("release_cold_replay ") {
        return None;
    }
    Some((
        token_value(line, "scenario=")?,
        token_value(line, "workloads=")?,
        token_value(line, "elapsed_ms=")?.parse().ok()?,
    ))
}

fn parse_cluster_sync(line: &str) -> Option<(&str, f64)> {
    if !line.contains("cluster_sync_time ") {
        return None;
    }
    Some((
        token_value(line, "node_count=")?,
        token_value(line, "avg_ms=")?.parse().ok()?,
    ))
}

fn token_value<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(prefix))
}

fn render_summary(issues: &[Issue]) -> String {
    let warns = issues
        .iter()
        .filter(|issue| issue.severity == Severity::Warn)
        .map(|issue| issue.message.as_str())
        .collect::<Vec<_>>();
    let fails = issues
        .iter()
        .filter(|issue| issue.severity == Severity::Fail)
        .map(|issue| issue.message.as_str())
        .collect::<Vec<_>>();

    let mut lines = vec!["# Orion Perf Check".to_owned(), String::new()];
    if issues.is_empty() {
        lines.push("All configured perf thresholds passed.".to_owned());
    } else {
        if !warns.is_empty() {
            lines.push("## Warnings".to_owned());
            for warning in warns {
                lines.push(format!("- {warning}"));
            }
            lines.push(String::new());
        }
        if !fails.is_empty() {
            lines.push("## Failures".to_owned());
            for failure in fails {
                lines.push(format!("- {failure}"));
            }
            lines.push(String::new());
        }
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config_json() -> &'static str {
        r#"{
          "idle_cpu_percent": { "warn": 2.0, "fail": 5.0 },
          "local_persistence_replay_ms": {
            "10": { "warn": 1.0, "fail": 2.0 }
          },
          "release_memory_baseline_bytes": {
            "50": { "warn": 10, "fail": 20 }
          },
          "release_cold_replay_ms": {
            "current_snapshot": {
              "100": { "warn": 4.5, "fail": 6.5 }
            }
          },
          "cluster_sync_avg_ms": {
            "3": { "warn": 125.0, "fail": 150.0 }
          }
        }"#
    }

    fn sample_config() -> PerfConfig {
        serde_json::from_str(sample_config_json()).expect("sample config should parse")
    }

    #[test]
    fn load_config_supports_json_yaml_and_toml() {
        let dir = env::temp_dir().join(format!("orion-perf-check-load-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let json_path = dir.join("config.json");
        let yaml_path = dir.join("config.yaml");
        let toml_path = dir.join("config.toml");

        fs::write(&json_path, sample_config_json()).expect("json config should write");
        fs::write(
            &yaml_path,
            serde_yaml::to_string(&sample_config()).expect("yaml config should serialize"),
        )
        .expect("yaml config should write");
        fs::write(
            &toml_path,
            toml::to_string_pretty(&sample_config()).expect("toml config should serialize"),
        )
        .expect("toml config should write");

        assert_eq!(
            load_config(&json_path, None).expect("json config should load"),
            sample_config()
        );
        assert_eq!(
            load_config(&yaml_path, None).expect("yaml config should load"),
            sample_config()
        );
        assert_eq!(
            load_config(&toml_path, None).expect("toml config should load"),
            sample_config()
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn parse_args_accepts_explicit_config_format() {
        let args = parse_args(
            [
                "--config".to_owned(),
                "config".to_owned(),
                "--config-format".to_owned(),
                "yaml".to_owned(),
                "perf.log".to_owned(),
            ]
            .into_iter(),
        )
        .expect("args should parse");
        assert_eq!(args.config_format, Some(StructuredFormat::Yaml));
    }

    #[test]
    fn parses_expected_perf_lines() {
        assert_eq!(
            parse_local_replay("persistence_replay workloads=10 elapsed_ms=1.25"),
            Some(("10", 1.25))
        );
        assert_eq!(
            parse_idle_cpu(
                "idle_cpu_baseline pid=12 window_secs=2 cpu_seconds=0.1 cpu_percent=3.250"
            ),
            Some(3.25)
        );
        assert_eq!(
            parse_release_memory("release_memory_baseline workloads=50 rss_bytes=1234"),
            Some(("50", 1234))
        );
        assert_eq!(
            parse_release_replay(
                "release_cold_replay scenario=current_snapshot workloads=100 elapsed_ms=5.25"
            ),
            Some(("current_snapshot", "100", 5.25))
        );
        assert_eq!(
            parse_cluster_sync("cluster_sync_time node_count=3 waves=5 total_ms=600 avg_ms=120"),
            Some(("3", 120.0))
        );
    }

    #[test]
    fn collects_warn_and_fail_issues_from_logs() {
        let config = sample_config();
        let logs = [PathBuf::from("perf-a.log"), PathBuf::from("perf-b.log")];

        let dir = env::temp_dir().join(format!("orion-perf-check-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let log_a = dir.join(&logs[0]);
        let log_b = dir.join(&logs[1]);
        fs::write(
            &log_a,
            "persistence_replay workloads=10 elapsed_ms=1.25\nrelease_memory_baseline workloads=50 rss_bytes=25\n",
        )
        .expect("log a should write");
        fs::write(
            &log_b,
            "idle_cpu_baseline pid=1 window_secs=2 cpu_seconds=0.1 cpu_percent=3.250\nrelease_cold_replay scenario=current_snapshot workloads=100 elapsed_ms=7.0\ncluster_sync_time node_count=3 waves=5 total_ms=700 avg_ms=140\n",
        )
        .expect("log b should write");

        let issues = collect_issues(&config, &[log_a, log_b]).expect("issues should collect");
        assert_eq!(issues.len(), 5);
        assert!(issues.iter().any(|issue| issue.severity == Severity::Warn));
        assert!(issues.iter().any(|issue| issue.severity == Severity::Fail));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn summary_renders_pass_and_failure_sections() {
        let pass = render_summary(&[]);
        assert!(pass.contains("All configured perf thresholds passed."));

        let summary = render_summary(&[
            Issue {
                severity: Severity::Warn,
                message: "warn issue".to_owned(),
            },
            Issue {
                severity: Severity::Fail,
                message: "fail issue".to_owned(),
            },
        ]);
        assert!(summary.contains("## Warnings"));
        assert!(summary.contains("- warn issue"));
        assert!(summary.contains("## Failures"));
        assert!(summary.contains("- fail issue"));
    }
}
