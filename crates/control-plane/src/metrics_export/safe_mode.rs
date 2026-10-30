use super::format::gauge;
use orion_core::NodeId;

/// `orion_safe_mode`: 1 while the node runs in safe mode (`docs/update-recovery.md`), with the
/// cause kind as label, and 0 otherwise. `cause` is that kind (for example `requested`), not the
/// full reason, which stays in health and readiness.
pub fn render_safe_mode_metrics(node_id: &NodeId, cause: Option<&str>) -> String {
    let mut out = String::new();
    gauge(
        &mut out,
        "orion_safe_mode",
        "1 while the node runs in safe mode, with the cause as label; 0 otherwise.",
        &[
            ("node_id", node_id.as_str()),
            ("cause", cause.unwrap_or("")),
        ],
        u8::from(cause.is_some()),
    );
    out
}
