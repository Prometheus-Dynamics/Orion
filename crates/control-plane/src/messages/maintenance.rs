use orion_core::{RuntimeType, WorkloadId};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Default,
)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceMode {
    #[default]
    Normal,
    Cordoned,
    Draining,
    Maintenance,
    Isolated,
}

impl fmt::Display for MaintenanceMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Normal => "normal",
            Self::Cordoned => "cordoned",
            Self::Draining => "draining",
            Self::Maintenance => "maintenance",
            Self::Isolated => "isolated",
        };
        f.write_str(value)
    }
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Default,
)]
pub struct MaintenanceState {
    pub mode: MaintenanceMode,
    pub allow_runtime_types: Vec<RuntimeType>,
    pub allow_workload_ids: Vec<WorkloadId>,
}

impl MaintenanceState {
    pub fn normal() -> Self {
        Self::default()
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceAction {
    Cordon,
    Drain,
    Enter,
    Isolate,
    Exit,
}

impl fmt::Display for MaintenanceAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Cordon => "cordon",
            Self::Drain => "drain",
            Self::Enter => "enter",
            Self::Isolate => "isolate",
            Self::Exit => "exit",
        };
        f.write_str(value)
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MaintenanceCommand {
    pub action: MaintenanceAction,
    pub allow_runtime_types: Vec<RuntimeType>,
    pub allow_workload_ids: Vec<WorkloadId>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MaintenanceStatus {
    pub state: MaintenanceState,
    pub peer_sync_paused: bool,
    pub remote_desired_state_blocked: bool,
}
