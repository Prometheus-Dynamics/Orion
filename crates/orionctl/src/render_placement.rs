//! Placement and cross-node binding rendering (`docs/placement.md`).

use orion_control_plane::{PlacementReason, ResourceBinding, WorkloadRecord};

/// Why the workload is (or is not) on its node: `explicit`, `placed`, `failover-from:<node>`,
/// `pending` (placement-managed, not yet assigned) or `manual` (no placement, not assigned).
pub(crate) fn render_assignment(workload: &WorkloadRecord) -> String {
    if workload.has_explicit_assignment() {
        return "explicit".to_owned();
    }
    match workload.placement.as_ref() {
        None => "manual".to_owned(),
        Some(placement) => match (&workload.assigned_node_id, &placement.decision) {
            (None, _) => "pending".to_owned(),
            (Some(_), Some(decision)) => match &decision.reason {
                PlacementReason::Placed => "placed".to_owned(),
                PlacementReason::Failover { from } => format!("failover-from:{from}"),
            },
            (Some(_), None) => "explicit".to_owned(),
        },
    }
}

/// Placement constraints: `any`, or `selector=k=v,k2;colocate=<resource>`; `-` for none.
pub(crate) fn render_placement(workload: &WorkloadRecord) -> String {
    let Some(placement) = workload.placement.as_ref() else {
        return "-".to_owned();
    };
    let mut parts = Vec::new();
    if !placement.node_selector.is_empty() {
        let terms: Vec<String> = placement
            .node_selector
            .iter()
            .map(ToString::to_string)
            .collect();
        parts.push(format!("selector={}", terms.join(",")));
    }
    if let Some(resource_id) = placement.colocate_with_resource.as_ref() {
        parts.push(format!("colocate={resource_id}"));
    }
    if parts.is_empty() {
        "any".to_owned()
    } else {
        parts.join(";")
    }
}

/// One binding: `<resource>@<node>`, plus `(remote <endpoints>)` and `unavailable` for cross-node
/// bindings.
pub(crate) fn render_binding(binding: &ResourceBinding) -> String {
    match binding.remote.as_ref() {
        None => format!("{}@{}", binding.resource_id, binding.node_id),
        Some(remote) => format!(
            "{}@{} remote endpoints=[{}]{}",
            binding.resource_id,
            binding.node_id,
            remote.endpoints.join(","),
            if remote.available { "" } else { " unavailable" }
        ),
    }
}

/// Comma-separated `<resource>@<node>` list (`*` marks remote bindings), `-` when empty.
pub(crate) fn render_binding_list(bindings: &[ResourceBinding]) -> String {
    if bindings.is_empty() {
        return "-".to_owned();
    }
    bindings
        .iter()
        .map(|binding| {
            let marker = match binding.remote.as_ref() {
                Some(remote) if remote.available => "*",
                Some(_) => "*!",
                None => "",
            };
            format!("{}@{}{marker}", binding.resource_id, binding.node_id)
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_control_plane::{PlacementDecision, WorkloadPlacement};

    fn workload() -> WorkloadRecord {
        WorkloadRecord::builder("workload.a", "graph.exec.v1", "artifact.a").build()
    }

    #[test]
    fn assignment_reasons_render() {
        let mut manual = workload();
        assert_eq!(render_assignment(&manual), "manual");
        manual.assigned_node_id = Some("node-a".into());
        assert_eq!(render_assignment(&manual), "explicit");

        let mut placed = workload();
        placed.placement = Some(WorkloadPlacement::any());
        assert_eq!(render_assignment(&placed), "pending");
        placed.assigned_node_id = Some("node-b".into());
        placed.placement.as_mut().unwrap().decision = Some(PlacementDecision {
            node_id: "node-b".into(),
            reason: PlacementReason::Failover {
                from: "node-a".into(),
            },
        });
        assert_eq!(render_assignment(&placed), "failover-from:node-a");
        assert_eq!(render_placement(&placed), "any");
    }

    #[test]
    fn bindings_render_with_node_and_remote_markers() {
        let bindings = [
            ResourceBinding::new("resource.local", "node-a"),
            ResourceBinding::remote("resource.cam", "node-b", vec!["tcp://b:1".into()], false),
        ];
        assert_eq!(
            render_binding_list(&bindings),
            "resource.local@node-a,resource.cam@node-b*!"
        );
        assert_eq!(
            render_binding(&bindings[1]),
            "resource.cam@node-b remote endpoints=[tcp://b:1] unavailable"
        );
    }
}
