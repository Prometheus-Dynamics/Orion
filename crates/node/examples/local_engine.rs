use orion::{
    ArtifactId, NodeId, WorkloadId,
    control_plane::{
        ArtifactRecord, DesiredClusterState, DesiredState, ExecutorRecord, ProviderRecord,
        WorkloadObservedState, WorkloadRecord,
    },
    runtime::{
        ExecutorCommand, ExecutorIntegration, ExecutorSnapshot, ProviderIntegration,
        ProviderSnapshot,
    },
};
use orion_node::{NodeApp, NodeConfig};
use std::sync::{Arc, Mutex};

struct LocalProvider {
    node_id: NodeId,
}

impl ProviderIntegration for LocalProvider {
    fn provider_record(&self) -> ProviderRecord {
        ProviderRecord::builder("provider.local", self.node_id.clone())
            .resource_type("imu.sample")
            .build()
    }

    fn snapshot(&self) -> ProviderSnapshot {
        ProviderSnapshot {
            provider: self.provider_record(),
            resources: vec![
                orion::control_plane::ResourceRecord::builder(
                    "resource.imu-1",
                    "imu.sample",
                    "provider.local",
                )
                .availability(orion::control_plane::AvailabilityState::Available)
                .health(orion::control_plane::HealthState::Healthy)
                .build(),
            ],
        }
    }
}

struct LocalExecutor {
    node_id: NodeId,
    commands: Arc<Mutex<Vec<ExecutorCommand>>>,
}

impl ExecutorIntegration for LocalExecutor {
    fn executor_record(&self) -> ExecutorRecord {
        ExecutorRecord::builder("executor.engine", self.node_id.clone())
            .runtime_type("graph.exec.v1")
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
            .expect("local example command log should not be poisoned")
            .push(command.clone());
        Ok(())
    }
}

fn main() {
    let app = NodeApp::try_new(
        NodeConfig::for_local_node("node-a").with_http_bind_addr(
            "127.0.0.1:9100"
                .parse()
                .expect("socket address should parse"),
        ),
    )
    .expect("node app should build");

    let commands = Arc::new(Mutex::new(Vec::new()));
    app.register_provider(LocalProvider {
        node_id: NodeId::new("node-a"),
    })
    .expect("provider should register");
    app.register_executor(LocalExecutor {
        node_id: NodeId::new("node-a"),
        commands: commands.clone(),
    })
    .expect("executor should register");

    let mut desired = DesiredClusterState::default();
    desired.put_artifact(ArtifactRecord::builder(ArtifactId::new("artifact.pose")).build());
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            "graph.exec.v1",
            ArtifactId::new("artifact.pose"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-a"))
        .require_resource("imu.sample", 1)
        .observed_state(WorkloadObservedState::Pending)
        .build(),
    );
    app.replace_desired(desired);

    let report = app.tick().expect("tick should succeed");
    println!(
        "orion-node example: desired_revision={} commands={}",
        report.desired_revision,
        report.commands.len()
    );
}
