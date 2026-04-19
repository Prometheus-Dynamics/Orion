#![cfg(all(feature = "macros", feature = "runtime", feature = "control-plane"))]

use orion::{
    NodeId, OrionExecutor, OrionProvider, RuntimeType, orion_resource_type, orion_runtime_type,
    runtime::{ExecutorDescriptor, ProviderDescriptor},
};

#[orion_runtime_type("graph.exec.v1")]
struct GraphExecV1;

#[orion_resource_type("imu.sample")]
struct ImuSample;

#[derive(OrionExecutor)]
#[orion_executor(id = "executor.engine", runtime_types = ["graph.exec.v1"])]
struct DerivedExecutor {
    node_id: NodeId,
}

#[derive(OrionProvider)]
#[orion_provider(id = "provider.peripherals", resource_types = ["imu.sample"])]
struct DerivedProvider {
    node_id: NodeId,
}

#[test]
fn facade_macros_expand_without_direct_workspace_imports() {
    let executor = DerivedExecutor {
        node_id: NodeId::new("node-a"),
    };
    let provider = DerivedProvider {
        node_id: NodeId::new("node-a"),
    };

    assert_eq!(
        GraphExecV1::runtime_type(),
        RuntimeType::of::<GraphExecV1>()
    );
    assert_eq!(executor.executor_record().runtime_types.len(), 1);
    assert_eq!(provider.provider_record().resource_types.len(), 1);
    assert_eq!(ImuSample::resource_type().as_str(), "imu.sample");
}
