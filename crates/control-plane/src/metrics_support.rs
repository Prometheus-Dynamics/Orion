use crate::LatencyMetricsSnapshot;
use std::time::Duration;

pub const LATENCY_BUCKET_LE_1_MS: u64 = 1;
pub const LATENCY_BUCKET_LE_5_MS: u64 = 5;
pub const LATENCY_BUCKET_LE_10_MS: u64 = 10;
pub const LATENCY_BUCKET_LE_50_MS: u64 = 50;
pub const LATENCY_BUCKET_LE_100_MS: u64 = 100;
pub const LATENCY_BUCKET_LE_500_MS: u64 = 500;
pub const COMMUNICATION_RECENT_WINDOW_MS: u64 = 300_000;
pub const COMMUNICATION_RECENT_SAMPLE_LIMIT: usize = 4_096;
pub const ESTIMATED_WIRE_OVERHEAD_BYTES: u64 = 64;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LatencyMetricBuckets {
    samples_total: u64,
    total_duration_ms: u64,
    last_duration_ms: Option<u64>,
    max_duration_ms: u64,
    bucket_le_1_ms: u64,
    bucket_le_5_ms: u64,
    bucket_le_10_ms: u64,
    bucket_le_50_ms: u64,
    bucket_le_100_ms: u64,
    bucket_le_500_ms: u64,
    bucket_gt_500_ms: u64,
}

impl LatencyMetricBuckets {
    pub fn record(&mut self, duration: Duration) {
        let duration_ms = duration_ms_u64(duration);
        self.samples_total = self.samples_total.saturating_add(1);
        self.total_duration_ms = self.total_duration_ms.saturating_add(duration_ms);
        self.last_duration_ms = Some(duration_ms);
        self.max_duration_ms = self.max_duration_ms.max(duration_ms);
        match duration_ms {
            0..=LATENCY_BUCKET_LE_1_MS => {
                self.bucket_le_1_ms = self.bucket_le_1_ms.saturating_add(1);
            }
            2..=LATENCY_BUCKET_LE_5_MS => {
                self.bucket_le_5_ms = self.bucket_le_5_ms.saturating_add(1);
            }
            6..=LATENCY_BUCKET_LE_10_MS => {
                self.bucket_le_10_ms = self.bucket_le_10_ms.saturating_add(1);
            }
            11..=LATENCY_BUCKET_LE_50_MS => {
                self.bucket_le_50_ms = self.bucket_le_50_ms.saturating_add(1);
            }
            51..=LATENCY_BUCKET_LE_100_MS => {
                self.bucket_le_100_ms = self.bucket_le_100_ms.saturating_add(1);
            }
            101..=LATENCY_BUCKET_LE_500_MS => {
                self.bucket_le_500_ms = self.bucket_le_500_ms.saturating_add(1);
            }
            _ => self.bucket_gt_500_ms = self.bucket_gt_500_ms.saturating_add(1),
        }
    }

    pub fn snapshot(&self) -> LatencyMetricsSnapshot {
        LatencyMetricsSnapshot {
            samples_total: self.samples_total,
            total_duration_ms: self.total_duration_ms,
            last_duration_ms: self.last_duration_ms,
            max_duration_ms: self.max_duration_ms,
            bucket_le_1_ms: self.bucket_le_1_ms,
            bucket_le_5_ms: self.bucket_le_5_ms,
            bucket_le_10_ms: self.bucket_le_10_ms,
            bucket_le_50_ms: self.bucket_le_50_ms,
            bucket_le_100_ms: self.bucket_le_100_ms,
            bucket_le_500_ms: self.bucket_le_500_ms,
            bucket_gt_500_ms: self.bucket_gt_500_ms,
        }
    }
}

pub fn duration_ms_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

pub fn estimate_wire_bytes(payload_bytes: u64) -> u64 {
    payload_bytes.saturating_add(ESTIMATED_WIRE_OVERHEAD_BYTES)
}
