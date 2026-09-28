//! Camera-pipeline provider/executor fixtures shared by the runtime composition and validation
//! tests.

use orion::{
    CapabilityDef, ConfigSchemaDef, ResourceType,
    control_plane::{
        AvailabilityState, DesiredState, ExecutorRecord, HealthState, LeaseState, ProviderRecord,
        ResourceRecord, WorkloadObservedState, WorkloadRecord,
    },
    runtime::{
        ExecutorCommand, ExecutorIntegration, ExecutorSnapshot, ProviderIntegration,
        ProviderSnapshot,
    },
};
use std::sync::{Arc, Mutex};

pub(super) struct CameraControllerConfigV1;
impl ConfigSchemaDef for CameraControllerConfigV1 {
    const SCHEMA_ID: &'static str = "camera.controller.config.v1";
}

pub(super) struct CaptureConfigurable;
impl CapabilityDef for CaptureConfigurable {
    const CAPABILITY_ID: &'static str = "capture.configurable";
}

#[derive(Clone)]
pub(super) struct CameraProvider;

impl ProviderIntegration for CameraProvider {
    fn provider_record(&self) -> ProviderRecord {
        ProviderRecord::builder("provider.camera", "node-a")
            .resource_type(ResourceType::new("camera.device"))
            .build()
    }

    fn snapshot(&self) -> ProviderSnapshot {
        ProviderSnapshot {
            provider: self.provider_record(),
            resources: vec![
                ResourceRecord::builder(
                    "resource.camera.raw.front",
                    "camera.device",
                    "provider.camera",
                )
                .ownership_mode(
                    orion::control_plane::ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
                )
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .lease_state(LeaseState::Unleased)
                .build(),
            ],
        }
    }
}

#[derive(Clone)]
pub(super) struct CameraControllerExecutor {
    pub(super) commands: Arc<Mutex<Vec<ExecutorCommand>>>,
}

impl CameraControllerExecutor {
    pub(super) fn new() -> Self {
        Self {
            commands: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl ExecutorIntegration for CameraControllerExecutor {
    fn executor_record(&self) -> ExecutorRecord {
        ExecutorRecord::builder("executor.camera-stack", "node-a")
            .runtime_type("camera.controller.v1")
            .build()
    }

    fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            executor: self.executor_record(),
            workloads: Vec::new(),
            resources: Vec::new(),
        }
    }

    fn apply_command(&self, command: &ExecutorCommand) -> Result<(), orion::runtime::RuntimeError> {
        self.commands
            .lock()
            .expect("camera executor command log should not be poisoned")
            .push(command.clone());
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct CameraPipelineExecutor {
    pub(super) commands: Arc<Mutex<Vec<ExecutorCommand>>>,
}

impl CameraPipelineExecutor {
    pub(super) fn new() -> Self {
        Self {
            commands: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl ExecutorIntegration for CameraPipelineExecutor {
    fn executor_record(&self) -> ExecutorRecord {
        ExecutorRecord::builder("executor.camera-stack", "node-a")
            .runtime_type("camera.controller.v1")
            .runtime_type("vision.consumer.v1")
            .build()
    }

    fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            executor: self.executor_record(),
            workloads: vec![
                WorkloadRecord::builder(
                    "workload.camera-controller",
                    "camera.controller.v1",
                    "artifact.camera",
                )
                .desired_state(DesiredState::Running)
                .observed_state(WorkloadObservedState::Running)
                .assigned_to("node-a")
                .require_resource_with_ownership(
                    "camera.device",
                    1,
                    orion::control_plane::ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
                )
                .bind_resource("resource.camera.raw.front", "node-a")
                .build(),
            ],
            resources: vec![
                ResourceRecord::builder(
                    "resource.camera.stream.front",
                    "camera.frame_stream",
                    "provider.camera",
                )
                .realized_by_executor("executor.camera-stack")
                .ownership_mode(orion::control_plane::ResourceOwnershipMode::SharedRead)
                .realized_for_workload("workload.camera-controller")
                .source_resource("resource.camera.raw.front")
                .source_workload("workload.camera-controller")
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .build(),
            ],
        }
    }

    fn apply_command(&self, command: &ExecutorCommand) -> Result<(), orion::runtime::RuntimeError> {
        self.commands
            .lock()
            .expect("camera executor command log should not be poisoned")
            .push(command.clone());
        Ok(())
    }
}
