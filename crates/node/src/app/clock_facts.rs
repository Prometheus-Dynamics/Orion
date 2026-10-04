//! Publishing the node's clock facts into its observed node record.

use super::NodeApp;
use crate::clock::{ClockStatusSource, clock_facts_from_reading, clock_facts_need_publish};
use orion::control_plane::{NodeClockFacts, NodeRecord};

/// Published facts are refreshed at least every this many clock checks, so `checked_at_ms` tells
/// readers how fresh they are even when nothing changes.
const CLOCK_REPUBLISH_AFTER_CHECKS: u32 = 30;

impl NodeApp {
    /// Reads the clock from `source`, records the sample for observability, and republishes it in
    /// this node's observed record when it changed meaningfully. Returns whether the observed
    /// record changed.
    ///
    /// `spawn_clock_facts_loop` calls this with [`crate::KernelClockStatusSource`] every
    /// `clock_refresh_interval`; embedders and tests can call it with their own source.
    pub fn refresh_clock_facts_from(&self, source: &dyn ClockStatusSource) -> bool {
        let tuning = &self.config.runtime_tuning;
        let facts = clock_facts_from_reading(
            source.read(),
            tuning.clock_source.as_ref(),
            tuning.clock_timebase.as_deref(),
            Self::current_time_ms(),
        );
        self.observability_lock().clock = Some(facts.clone());

        let max_age_ms = u64::try_from(
            tuning
                .clock_refresh_interval
                .saturating_mul(CLOCK_REPUBLISH_AFTER_CHECKS)
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        self.with_store_mut(|store| {
            let node_id = store.local_node_id.clone();
            let published = store
                .observed
                .nodes
                .get(&node_id)
                .and_then(|record| record.clock.as_ref());
            if !clock_facts_need_publish(published, &facts, max_age_ms) {
                return false;
            }
            let template = store.desired.nodes.get(&node_id).cloned();
            let record = store
                .observed
                .nodes
                .entry(node_id)
                .or_insert_with_key(|node_id| {
                    let mut record =
                        template.unwrap_or_else(|| NodeRecord::builder(node_id.clone()).build());
                    record.clock = None;
                    record
                });
            record.clock = Some(facts);
            true
        })
    }

    /// Clock facts currently published in this node's observed record.
    pub fn published_clock_facts(&self) -> Option<NodeClockFacts> {
        let store = self.store_read();
        store
            .observed
            .nodes
            .get(&store.local_node_id)
            .and_then(|record| record.clock.clone())
    }
}
