//! Host and process facts sampled from `/proc`. Every reader degrades to `None` when the host
//! does not expose the file or field, so non-Linux builds report empty process counters.

use orion::control_plane::{HostMetricsSnapshot, ProcessMemorySnapshot};
use std::fs;

/// Samples host metrics and process memory from one read of the process status files.
pub(crate) fn sample_host_and_process_memory() -> (HostMetricsSnapshot, ProcessMemorySnapshot) {
    let status = fs::read_to_string("/proc/self/status")
        .map(|contents| parse_proc_status(&contents))
        .unwrap_or_default();
    let smaps = fs::read_to_string("/proc/self/smaps_rollup")
        .map(|contents| parse_smaps_rollup(&contents))
        .unwrap_or_default();
    let host = HostMetricsSnapshot {
        hostname: read_trimmed("/proc/sys/kernel/hostname"),
        os_name: std::env::consts::OS.to_owned(),
        os_version: os_release_value("VERSION_ID"),
        kernel_version: read_trimmed("/proc/sys/kernel/osrelease"),
        architecture: std::env::consts::ARCH.to_owned(),
        uptime_seconds: proc_uptime_seconds(),
        load_1_milli: proc_loadavg_milli(0),
        load_5_milli: proc_loadavg_milli(1),
        load_15_milli: proc_loadavg_milli(2),
        memory_total_bytes: proc_meminfo_kib("MemTotal").map(kib_to_bytes),
        memory_available_bytes: proc_meminfo_kib("MemAvailable").map(kib_to_bytes),
        swap_total_bytes: proc_meminfo_kib("SwapTotal").map(kib_to_bytes),
        swap_free_bytes: proc_meminfo_kib("SwapFree").map(kib_to_bytes),
        process_id: std::process::id(),
        process_rss_bytes: process_rss_pages()
            .map(|pages| {
                pages
                    .saturating_mul(page_size_bytes())
                    .min(u64::MAX as usize) as u64
            })
            .or(status.vm_rss_bytes),
        process_pss_bytes: smaps.pss_bytes,
        process_private_dirty_bytes: smaps.private_dirty_bytes,
        process_anonymous_bytes: smaps.anonymous_bytes,
        process_vm_size_bytes: status.vm_size_bytes,
        process_vm_data_bytes: status.vm_data_bytes,
        process_vm_hwm_bytes: status.vm_hwm_bytes,
        process_threads: status.threads,
        process_fd_count: process_fd_count(),
        // Filled by `NodeApp::fill_live_host_metrics` (shared CPU baseline) and from host facts.
        cpu_busy_milli: None,
        cpu_core_busy_milli: Vec::new(),
        cpu_window_ms: None,
        temperatures: Vec::new(),
    };
    let process = ProcessMemorySnapshot {
        vm_rss_bytes: status.vm_rss_bytes,
        vm_hwm_bytes: status.vm_hwm_bytes,
        rss_anon_bytes: status.rss_anon_bytes,
        rss_file_bytes: status.rss_file_bytes,
        rss_shmem_bytes: status.rss_shmem_bytes,
        vm_data_bytes: status.vm_data_bytes,
        pss_bytes: smaps.pss_bytes,
        pss_anon_bytes: smaps.pss_anon_bytes,
        pss_file_bytes: smaps.pss_file_bytes,
        private_dirty_bytes: smaps.private_dirty_bytes,
        threads: status.threads,
    };
    (host, process)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProcStatus {
    pub(crate) vm_size_bytes: Option<u64>,
    pub(crate) vm_data_bytes: Option<u64>,
    pub(crate) vm_rss_bytes: Option<u64>,
    pub(crate) vm_hwm_bytes: Option<u64>,
    pub(crate) rss_anon_bytes: Option<u64>,
    pub(crate) rss_file_bytes: Option<u64>,
    pub(crate) rss_shmem_bytes: Option<u64>,
    pub(crate) threads: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SmapsRollup {
    pub(crate) pss_bytes: Option<u64>,
    pub(crate) pss_anon_bytes: Option<u64>,
    pub(crate) pss_file_bytes: Option<u64>,
    pub(crate) private_dirty_bytes: Option<u64>,
    pub(crate) anonymous_bytes: Option<u64>,
}

/// Parses the fields Orion reports from a `/proc/<pid>/status` document.
pub(crate) fn parse_proc_status(contents: &str) -> ProcStatus {
    let mut status = ProcStatus::default();
    for line in contents.lines() {
        let Some((key, _)) = line.split_once(':') else {
            continue;
        };
        let slot = match key {
            "VmSize" => &mut status.vm_size_bytes,
            "VmData" => &mut status.vm_data_bytes,
            "VmRSS" => &mut status.vm_rss_bytes,
            "VmHWM" => &mut status.vm_hwm_bytes,
            "RssAnon" => &mut status.rss_anon_bytes,
            "RssFile" => &mut status.rss_file_bytes,
            "RssShmem" => &mut status.rss_shmem_bytes,
            "Threads" => {
                status.threads = proc_plain_line(line, key);
                continue;
            }
            _ => continue,
        };
        *slot = proc_kib_line(line, key).map(kib_to_bytes);
    }
    status
}

/// Parses the rollup totals from a `/proc/<pid>/smaps_rollup` document.
pub(crate) fn parse_smaps_rollup(contents: &str) -> SmapsRollup {
    let mut rollup = SmapsRollup::default();
    for line in contents.lines() {
        let Some((key, _)) = line.split_once(':') else {
            continue;
        };
        let slot = match key {
            "Pss" => &mut rollup.pss_bytes,
            "Pss_Anon" => &mut rollup.pss_anon_bytes,
            "Pss_File" => &mut rollup.pss_file_bytes,
            "Private_Dirty" => &mut rollup.private_dirty_bytes,
            "Anonymous" => &mut rollup.anonymous_bytes,
            _ => continue,
        };
        *slot = proc_kib_line(line, key).map(kib_to_bytes);
    }
    rollup
}

fn read_trimmed(path: &str) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn os_release_value(key: &str) -> Option<String> {
    let contents = fs::read_to_string("/etc/os-release").ok()?;
    contents.lines().find_map(|line| {
        let (line_key, value) = line.split_once('=')?;
        (line_key == key).then(|| value.trim_matches('"').to_owned())
    })
}

fn proc_uptime_seconds() -> Option<u64> {
    let contents = fs::read_to_string("/proc/uptime").ok()?;
    let first = contents.split_whitespace().next()?;
    let whole_seconds = first.split('.').next()?;
    whole_seconds.parse().ok()
}

fn proc_loadavg_milli(index: usize) -> Option<u64> {
    let contents = fs::read_to_string("/proc/loadavg").ok()?;
    let value = contents.split_whitespace().nth(index)?;
    decimal_to_milli(value)
}

fn proc_meminfo_kib(key: &str) -> Option<u64> {
    let contents = fs::read_to_string("/proc/meminfo").ok()?;
    contents.lines().find_map(|line| proc_kib_line(line, key))
}

fn process_rss_pages() -> Option<usize> {
    let contents = fs::read_to_string("/proc/self/statm").ok()?;
    contents.split_whitespace().nth(1)?.parse().ok()
}

fn proc_kib_line(line: &str, key: &str) -> Option<u64> {
    let value = proc_plain_line(line, key)?;
    line.split_whitespace()
        .nth(2)
        .filter(|unit| *unit == "kB")?;
    Some(value)
}

fn proc_plain_line(line: &str, key: &str) -> Option<u64> {
    let (line_key, rest) = line.split_once(':')?;
    if line_key != key {
        return None;
    }
    rest.split_whitespace().next()?.parse().ok()
}

fn process_fd_count() -> Option<u64> {
    let entries = fs::read_dir("/proc/self/fd").ok()?;
    Some(entries.filter_map(Result::ok).count() as u64)
}

fn page_size_bytes() -> usize {
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(size).unwrap_or(0)
}

fn decimal_to_milli(value: &str) -> Option<u64> {
    let (whole, fractional) = value.split_once('.').unwrap_or((value, ""));
    let whole = whole.parse::<u64>().ok()?;
    let mut digits = fractional.chars().take(3).collect::<String>();
    while digits.len() < 3 {
        digits.push('0');
    }
    let fractional = digits.parse::<u64>().ok()?;
    Some(whole.saturating_mul(1000).saturating_add(fractional))
}

fn kib_to_bytes(kib: u64) -> u64 {
    kib.saturating_mul(1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROC_STATUS_FIXTURE: &str = "Name:\torion-node\n\
Umask:\t0022\n\
State:\tS (sleeping)\n\
Pid:\t4242\n\
VmPeak:\t  212340 kB\n\
VmSize:\t  208112 kB\n\
VmLck:\t       0 kB\n\
VmHWM:\t   43520 kB\n\
VmRSS:\t   42112 kB\n\
RssAnon:\t   30208 kB\n\
RssFile:\t   11776 kB\n\
RssShmem:\t     128 kB\n\
VmData:\t   61440 kB\n\
VmStk:\t     132 kB\n\
Threads:\t9\n\
SigQ:\t0/15187\n";

    const SMAPS_ROLLUP_FIXTURE: &str = "55d0c0000000-7ffd1a3ff000 ---p 00000000 00:00 0                          [rollup]\n\
Rss:               42112 kB\n\
Pss:               40960 kB\n\
Pss_Dirty:         30000 kB\n\
Pss_Anon:          30208 kB\n\
Pss_File:          10624 kB\n\
Pss_Shmem:           128 kB\n\
Shared_Clean:       2048 kB\n\
Private_Dirty:     30080 kB\n\
Anonymous:         30208 kB\n\
Swap:                  0 kB\n";

    #[test]
    fn parses_proc_status_memory_fields() {
        let status = parse_proc_status(PROC_STATUS_FIXTURE);
        assert_eq!(status.vm_size_bytes, Some(208_112 * 1024));
        assert_eq!(status.vm_data_bytes, Some(61_440 * 1024));
        assert_eq!(status.vm_rss_bytes, Some(42_112 * 1024));
        assert_eq!(status.vm_hwm_bytes, Some(43_520 * 1024));
        assert_eq!(status.rss_anon_bytes, Some(30_208 * 1024));
        assert_eq!(status.rss_file_bytes, Some(11_776 * 1024));
        assert_eq!(status.rss_shmem_bytes, Some(128 * 1024));
        assert_eq!(status.threads, Some(9));
    }

    #[test]
    fn parses_smaps_rollup_without_confusing_prefixed_keys() {
        let rollup = parse_smaps_rollup(SMAPS_ROLLUP_FIXTURE);
        assert_eq!(rollup.pss_bytes, Some(40_960 * 1024));
        assert_eq!(rollup.pss_anon_bytes, Some(30_208 * 1024));
        assert_eq!(rollup.pss_file_bytes, Some(10_624 * 1024));
        assert_eq!(rollup.private_dirty_bytes, Some(30_080 * 1024));
        assert_eq!(rollup.anonymous_bytes, Some(30_208 * 1024));
    }

    #[test]
    fn missing_or_malformed_proc_fields_stay_absent() {
        let status = parse_proc_status("Name:\torion\nVmRSS:\tlots kB\nVmHWM:\t12\nThreads:\tx\n");
        assert_eq!(status, ProcStatus::default());
        let rollup = parse_smaps_rollup("Rss: 10 kB\n");
        assert_eq!(rollup, SmapsRollup::default());
        assert_eq!(parse_proc_status(""), ProcStatus::default());
    }

    #[test]
    fn older_kernels_without_split_pss_report_only_total_pss() {
        let rollup = parse_smaps_rollup("Rss: 100 kB\nPss: 90 kB\nPrivate_Dirty: 12 kB\n");
        assert_eq!(rollup.pss_bytes, Some(90 * 1024));
        assert_eq!(rollup.pss_anon_bytes, None);
        assert_eq!(rollup.pss_file_bytes, None);
        assert_eq!(rollup.private_dirty_bytes, Some(12 * 1024));
    }

    #[test]
    fn decimal_load_average_converts_to_milli_units() {
        assert_eq!(decimal_to_milli("0.52"), Some(520));
        assert_eq!(decimal_to_milli("3"), Some(3000));
        assert_eq!(decimal_to_milli("1.23456"), Some(1234));
        assert_eq!(decimal_to_milli("x"), None);
    }
}
