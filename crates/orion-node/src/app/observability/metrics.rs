use orion::control_plane::{OperationFailureCategory, OperationMetricsSnapshot};
use std::time::Duration;

#[derive(Clone, Debug, Default)]
pub(crate) struct OperationMetrics {
    success_count: u64,
    failure_count: u64,
    total_duration_ms: u64,
    last_duration_ms: Option<u64>,
    max_duration_ms: u64,
    last_error_category: Option<OperationFailureCategory>,
    last_error: Option<String>,
}

impl OperationMetrics {
    pub(crate) fn record_success(&mut self, duration: Duration) {
        let duration_ms = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        self.success_count = self.success_count.saturating_add(1);
        self.total_duration_ms = self.total_duration_ms.saturating_add(duration_ms);
        self.last_duration_ms = Some(duration_ms);
        self.max_duration_ms = self.max_duration_ms.max(duration_ms);
        self.last_error_category = None;
        self.last_error = None;
    }

    pub(crate) fn record_failure(
        &mut self,
        duration: Duration,
        category: OperationFailureCategory,
        error: impl Into<String>,
    ) {
        let duration_ms = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        self.failure_count = self.failure_count.saturating_add(1);
        self.total_duration_ms = self.total_duration_ms.saturating_add(duration_ms);
        self.last_duration_ms = Some(duration_ms);
        self.max_duration_ms = self.max_duration_ms.max(duration_ms);
        self.last_error_category = Some(category);
        self.last_error = Some(error.into());
    }

    pub(crate) fn snapshot(&self) -> OperationMetricsSnapshot {
        OperationMetricsSnapshot {
            success_count: self.success_count,
            failure_count: self.failure_count,
            total_duration_ms: self.total_duration_ms,
            last_duration_ms: self.last_duration_ms,
            max_duration_ms: self.max_duration_ms,
            last_error_category: self.last_error_category,
            last_error: self.last_error.clone(),
        }
    }
}
