use super::{NodeApp, NodeError};
use orion::control_plane::{
    MaintenanceAction, MaintenanceCommand, MaintenanceMode, MaintenanceState, MaintenanceStatus,
};

impl NodeApp {
    pub fn maintenance_status(&self) -> MaintenanceStatus {
        MaintenanceStatus {
            state: self.maintenance_state_read().clone(),
            peer_sync_paused: self.peer_sync_paused(),
            remote_desired_state_blocked: self.remote_desired_state_blocked(),
        }
    }

    pub(crate) fn peer_sync_paused(&self) -> bool {
        matches!(
            self.maintenance_state_read().mode,
            MaintenanceMode::Isolated
        )
    }

    pub(crate) fn remote_desired_state_blocked(&self) -> bool {
        matches!(
            self.maintenance_state_read().mode,
            MaintenanceMode::Isolated
        )
    }

    pub(crate) fn update_maintenance(
        &self,
        command: MaintenanceCommand,
    ) -> Result<MaintenanceStatus, NodeError> {
        let new_state = {
            let current = self.maintenance_state_read().clone();
            Self::next_maintenance_state(current, command)
        };
        self.set_maintenance_state(new_state.clone())?;
        let _ = self.tick()?;
        Ok(self.maintenance_status())
    }

    fn next_maintenance_state(
        current: MaintenanceState,
        command: MaintenanceCommand,
    ) -> MaintenanceState {
        match command.action {
            MaintenanceAction::Cordon => MaintenanceState {
                mode: MaintenanceMode::Cordoned,
                allow_runtime_types: current.allow_runtime_types,
                allow_workload_ids: current.allow_workload_ids,
            },
            MaintenanceAction::Drain => MaintenanceState {
                mode: MaintenanceMode::Draining,
                allow_runtime_types: current.allow_runtime_types,
                allow_workload_ids: current.allow_workload_ids,
            },
            MaintenanceAction::Enter => MaintenanceState {
                mode: MaintenanceMode::Maintenance,
                allow_runtime_types: command.allow_runtime_types,
                allow_workload_ids: command.allow_workload_ids,
            },
            MaintenanceAction::Isolate => MaintenanceState {
                mode: MaintenanceMode::Isolated,
                allow_runtime_types: command.allow_runtime_types,
                allow_workload_ids: command.allow_workload_ids,
            },
            MaintenanceAction::Exit => MaintenanceState::normal(),
        }
    }

    pub(crate) fn set_maintenance_state(&self, state: MaintenanceState) -> Result<(), NodeError> {
        {
            let mut maintenance = self.maintenance_state_lock();
            *maintenance = state.clone();
        }
        self.with_store_mut(|store| {
            store.set_maintenance(state.clone());
        });
        if let Some(storage) = &self.storage {
            storage.save_maintenance_state(&state)?;
        }
        Ok(())
    }
}
