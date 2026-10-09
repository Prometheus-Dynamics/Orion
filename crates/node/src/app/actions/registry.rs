//! Bounded in-memory registry of tracked actions (`docs/actions.md`).

use orion::{
    NodeId,
    control_plane::{ActionQuery, ActionRequest, ActionResult, ActionState, TypedConfigValue},
    transport::ipc::LocalAddress,
};
use std::collections::BTreeMap;

/// Longest action id, action name, argument or output key, in bytes.
pub(crate) const MAX_ACTION_TEXT_BYTES: usize = 128;
/// Most arguments of a request and entries of a result's output.
pub(crate) const MAX_ACTION_MAP_ENTRIES: usize = 32;
/// Longest string value of an argument or output entry.
pub(crate) const MAX_ACTION_STRING_BYTES: usize = 1024;
/// Longest byte value of an argument or output entry (hardware I/O: an SPI transaction is up to
/// 64 KiB).
pub(crate) const MAX_ACTION_BYTES_VALUE: usize = 64 * 1024;
/// Most bytes of keys and values in one request's arguments or one result's output, so a single
/// action cannot balloon a frame or the registry.
pub(crate) const MAX_ACTION_PAYLOAD_BYTES: usize = 256 * 1024;

/// Where an accepted action runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ActionRoute {
    /// Refused before it ran.
    Rejected,
    /// A node-side `ActionHandler`.
    Node,
    /// The local client registered as the target's action handler.
    Client(LocalAddress),
    /// Forwarded to the peer that owns the target.
    Remote(NodeId),
}

#[derive(Debug)]
pub(crate) struct ActionEntry {
    pub(crate) request: ActionRequest,
    pub(crate) result: ActionResult,
    pub(crate) deadline_at_ms: u64,
    pub(crate) route: ActionRoute,
    /// The peer that forwarded the request, which may query it.
    pub(crate) origin_peer: Option<NodeId>,
    /// Node-handler or forwarding task, aborted when the action times out.
    pub(crate) task: Option<tokio::task::AbortHandle>,
}

#[derive(Debug, Default)]
pub(crate) struct ActionRegistry {
    entries: BTreeMap<String, ActionEntry>,
}

fn value_len(value: &TypedConfigValue) -> usize {
    match value {
        TypedConfigValue::String(value) => value.len(),
        TypedConfigValue::Bytes(value) => value.len(),
        _ => 8,
    }
}

/// Why `value` is over its size limit, if it is.
fn value_too_long(value: &TypedConfigValue) -> Option<String> {
    match value {
        TypedConfigValue::String(text) if text.len() > MAX_ACTION_STRING_BYTES => Some(format!(
            "is a string longer than {MAX_ACTION_STRING_BYTES} bytes"
        )),
        TypedConfigValue::Bytes(bytes) if bytes.len() > MAX_ACTION_BYTES_VALUE => {
            Some(format!("is longer than {MAX_ACTION_BYTES_VALUE} bytes"))
        }
        _ => None,
    }
}

/// Checks the size limits of a request.
pub(crate) fn validate_request(request: &ActionRequest) -> Result<(), String> {
    for (what, text) in [
        ("action id", &request.action_id),
        ("action name", &request.name),
    ] {
        if text.trim().is_empty() {
            return Err(format!("{what} must not be empty"));
        }
        if text.len() > MAX_ACTION_TEXT_BYTES {
            return Err(format!(
                "{what} is longer than {MAX_ACTION_TEXT_BYTES} bytes"
            ));
        }
    }
    if request.args.len() > MAX_ACTION_MAP_ENTRIES {
        return Err(format!(
            "an action takes at most {MAX_ACTION_MAP_ENTRIES} arguments"
        ));
    }
    for (key, value) in &request.args {
        if key.is_empty() || key.len() > MAX_ACTION_TEXT_BYTES {
            return Err(format!(
                "argument names must be 1 to {MAX_ACTION_TEXT_BYTES} bytes"
            ));
        }
        if let Some(reason) = value_too_long(value) {
            return Err(format!("argument `{key}` {reason}"));
        }
    }
    let total: usize = request
        .args
        .iter()
        .map(|(key, value)| key.len() + value_len(value))
        .sum();
    if total > MAX_ACTION_PAYLOAD_BYTES {
        return Err(format!(
            "the arguments add up to {total} bytes; at most {MAX_ACTION_PAYLOAD_BYTES} are allowed"
        ));
    }
    Ok(())
}

/// Merges `output` into `target`, dropping entries beyond the size limits.
pub(crate) fn merge_output(
    target: &mut BTreeMap<String, TypedConfigValue>,
    output: BTreeMap<String, TypedConfigValue>,
) {
    for (key, value) in output {
        if key.is_empty() || key.len() > MAX_ACTION_TEXT_BYTES || value_too_long(&value).is_some() {
            continue;
        }
        if target.len() >= MAX_ACTION_MAP_ENTRIES && !target.contains_key(&key) {
            continue;
        }
        let total: usize = target
            .iter()
            .filter(|(existing, _)| *existing != &key)
            .map(|(key, value)| key.len() + value_len(value))
            .sum();
        if total + key.len() + value_len(&value) > MAX_ACTION_PAYLOAD_BYTES {
            continue;
        }
        target.insert(key, value);
    }
}

/// Whether two requests ask for the same thing (a resubmission of the same action).
pub(crate) fn same_request(a: &ActionRequest, b: &ActionRequest) -> bool {
    a.target == b.target && a.name == b.name && a.args == b.args
}

impl ActionRegistry {
    pub(crate) fn get(&self, action_id: &str) -> Option<&ActionEntry> {
        self.entries.get(action_id)
    }

    pub(crate) fn get_mut(&mut self, action_id: &str) -> Option<&mut ActionEntry> {
        self.entries.get_mut(action_id)
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn insert(&mut self, entry: ActionEntry) {
        self.entries.insert(entry.result.action_id.clone(), entry);
    }

    /// Drops final results older than `ttl_ms`.
    pub(crate) fn evict_expired(&mut self, now_ms: u64, ttl_ms: u64) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, entry| {
            !entry.result.state.is_terminal()
                || entry.result.updated_at_ms.saturating_add(ttl_ms) > now_ms
        });
        before - self.entries.len()
    }

    /// Evicts the oldest final results until one more entry fits under `max`. Returns `false` when
    /// every tracked action is still running.
    pub(crate) fn make_room(&mut self, max: usize) -> bool {
        while self.entries.len() >= max {
            let oldest = self
                .entries
                .values()
                .filter(|entry| entry.result.state.is_terminal())
                .min_by_key(|entry| entry.result.updated_at_ms)
                .map(|entry| entry.result.action_id.clone());
            match oldest {
                Some(action_id) => {
                    self.entries.remove(&action_id);
                }
                None => return false,
            }
        }
        true
    }

    /// Moves a non-final action to `state`, merging `output`. Returns the updated result, or
    /// `None` when the action is unknown or already final.
    pub(crate) fn update(
        &mut self,
        action_id: &str,
        state: ActionState,
        output: BTreeMap<String, TypedConfigValue>,
        now_ms: u64,
    ) -> Option<ActionResult> {
        let entry = self.entries.get_mut(action_id)?;
        if entry.result.state.is_terminal() {
            return None;
        }
        merge_output(&mut entry.result.output, output);
        entry.result.state = state;
        entry.result.updated_at_ms = now_ms;
        if entry.result.state.is_terminal() {
            entry.task = None;
        }
        Some(entry.result.clone())
    }

    /// Marks every non-final action whose deadline passed as `TimedOut`, returning the results
    /// and the tasks to abort.
    pub(crate) fn time_out(
        &mut self,
        now_ms: u64,
    ) -> (Vec<ActionResult>, Vec<tokio::task::AbortHandle>) {
        let mut results = Vec::new();
        let mut tasks = Vec::new();
        for entry in self.entries.values_mut() {
            if entry.result.state.is_terminal() || entry.deadline_at_ms > now_ms {
                continue;
            }
            entry.result.state = ActionState::TimedOut;
            entry.result.updated_at_ms = now_ms;
            tasks.extend(entry.task.take());
            results.push(entry.result.clone());
        }
        (results, tasks)
    }

    /// Next time `time_out` or `evict_expired` has work: the earliest deadline of a running
    /// action or expiry of a final result.
    pub(crate) fn next_wakeup_ms(&self, ttl_ms: u64) -> Option<u64> {
        self.entries
            .values()
            .map(|entry| {
                if entry.result.state.is_terminal() {
                    entry.result.updated_at_ms.saturating_add(ttl_ms)
                } else {
                    entry.deadline_at_ms
                }
            })
            .min()
    }

    /// Results matching `query`; with `peer`, only actions that peer forwarded.
    pub(crate) fn query(&self, query: &ActionQuery, peer: Option<&NodeId>) -> Vec<ActionResult> {
        let mut results: Vec<ActionResult> = self
            .entries
            .values()
            .filter(|entry| peer.is_none_or(|peer| entry.origin_peer.as_ref() == Some(peer)))
            .filter(|entry| query.matches(&entry.result))
            .map(|entry| entry.result.clone())
            .collect();
        results
            .sort_by(|a, b| (a.created_at_ms, &a.action_id).cmp(&(b.created_at_ms, &b.action_id)));
        results
    }

    /// Ids of non-final actions routed to `address`.
    pub(crate) fn running_for_client(&self, address: &LocalAddress) -> Vec<String> {
        self.entries
            .values()
            .filter(|entry| {
                entry.route == ActionRoute::Client(address.clone())
                    && !entry.result.state.is_terminal()
            })
            .map(|entry| entry.result.action_id.clone())
            .collect()
    }

    /// Accepted actions routed to `address` (for a handler that subscribes after they arrived).
    pub(crate) fn pending_for_client(&self, address: &LocalAddress) -> Vec<ActionRequest> {
        self.entries
            .values()
            .filter(|entry| {
                entry.route == ActionRoute::Client(address.clone())
                    && entry.result.state == ActionState::Accepted
            })
            .map(|entry| entry.request.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion::control_plane::ActionTarget;

    fn entry(id: &str, state: ActionState, updated_at_ms: u64, deadline_at_ms: u64) -> ActionEntry {
        let target = ActionTarget::Node(NodeId::new("node-a"));
        let request = ActionRequest::new(id, target.clone(), "locate");
        let mut result = ActionResult::new(id, target, "locate", NodeId::new("node-a"), state);
        result.updated_at_ms = updated_at_ms;
        result.created_at_ms = updated_at_ms;
        ActionEntry {
            request,
            result,
            deadline_at_ms,
            route: ActionRoute::Node,
            origin_peer: None,
            task: None,
        }
    }

    #[test]
    fn room_is_made_by_evicting_the_oldest_final_results_only() {
        let mut registry = ActionRegistry::default();
        registry.insert(entry("a", ActionState::Succeeded, 10, 100));
        registry.insert(entry("b", ActionState::Accepted, 5, 100));
        registry.insert(entry("c", ActionState::TimedOut, 20, 100));
        assert!(registry.make_room(3));
        assert!(
            registry.get("a").is_none(),
            "oldest final result goes first"
        );
        assert_eq!(registry.len(), 2);
        assert!(registry.make_room(2));
        assert!(registry.get("c").is_none());
        assert!(!registry.make_room(1), "running actions are never evicted");
        assert!(registry.get("b").is_some());
    }

    #[test]
    fn deadlines_time_out_running_actions_and_ttl_evicts_final_ones() {
        let mut registry = ActionRegistry::default();
        registry.insert(entry("run", ActionState::Accepted, 0, 50));
        registry.insert(entry("done", ActionState::Succeeded, 10, 0));
        assert_eq!(registry.next_wakeup_ms(100), Some(50));
        let (timed_out, _) = registry.time_out(60);
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].state, ActionState::TimedOut);
        assert_eq!(
            registry.update("run", ActionState::Succeeded, BTreeMap::new(), 70),
            None,
            "a final action ignores later reports"
        );
        assert_eq!(registry.evict_expired(110, 100), 1);
        assert!(registry.get("done").is_none());
        assert!(registry.get("run").is_some());
    }

    #[test]
    fn requests_and_outputs_are_bounded() {
        let mut request =
            ActionRequest::new("id", ActionTarget::Node(NodeId::new("node-a")), "locate");
        assert!(validate_request(&request).is_ok());
        request.name = "x".repeat(MAX_ACTION_TEXT_BYTES + 1);
        assert!(validate_request(&request).is_err());
        request.name = "locate".into();
        request.action_id = " ".into();
        assert!(validate_request(&request).is_err());
        request.action_id = "id".into();
        // Byte values may be as large as one SPI transaction; strings stay small.
        let spi = request.clone().with_arg(
            "tx",
            TypedConfigValue::Bytes(vec![0; MAX_ACTION_BYTES_VALUE]),
        );
        assert!(validate_request(&spi).is_ok());
        let too_big = request.clone().with_arg(
            "tx",
            TypedConfigValue::Bytes(vec![0; MAX_ACTION_BYTES_VALUE + 1]),
        );
        assert!(validate_request(&too_big).is_err());
        let long_string = request.clone().with_arg(
            "text",
            TypedConfigValue::String("x".repeat(MAX_ACTION_STRING_BYTES + 1)),
        );
        assert!(validate_request(&long_string).is_err());
        // The arguments together are bounded too.
        let mut total = request.clone();
        for index in 0..5 {
            total = total.with_arg(
                format!("tx{index}"),
                TypedConfigValue::Bytes(vec![0; MAX_ACTION_BYTES_VALUE]),
            );
        }
        assert!(validate_request(&total).is_err());

        let mut output = BTreeMap::new();
        let blobs: BTreeMap<_, _> = (0..5)
            .map(|index| {
                (
                    format!("rx{index}"),
                    TypedConfigValue::Bytes(vec![0; MAX_ACTION_BYTES_VALUE]),
                )
            })
            .collect();
        merge_output(&mut output, blobs);
        assert_eq!(output.len(), 3, "the output stops at the payload bound");

        let mut output = BTreeMap::new();
        let big: BTreeMap<_, _> = (0..MAX_ACTION_MAP_ENTRIES + 4)
            .map(|index| (format!("k{index:03}"), TypedConfigValue::UInt(index as u64)))
            .collect();
        merge_output(&mut output, big);
        assert_eq!(output.len(), MAX_ACTION_MAP_ENTRIES);
    }
}
