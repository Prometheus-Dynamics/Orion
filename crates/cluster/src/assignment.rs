use orion_control_plane::{DesiredClusterState, WorkloadRecord};
use orion_core::NodeId;

#[derive(Default)]
pub struct ClusterCoordinator;

impl ClusterCoordinator {
    pub fn assign(
        &self,
        state: &mut DesiredClusterState,
        workload_id: &orion_core::WorkloadId,
        node_id: NodeId,
    ) -> bool {
        let Some(workload) = state.workloads.get_mut(workload_id) else {
            return false;
        };

        workload.assigned_node_id = Some(node_id);
        state.bump_revision();
        true
    }

    pub fn assign_in_place(&self, workload: &mut WorkloadRecord, node_id: NodeId) {
        workload.assigned_node_id = Some(node_id);
    }
}
