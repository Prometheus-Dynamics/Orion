use std::{path::PathBuf, sync::Arc};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use orion_control_plane::{ControlMessage, PeerHello};
use orion_core::{NodeId, Revision};
use orion_transport_ipc::{
    ControlEnvelope, IpcTransportError, LocalAddress, UnixControlClient, UnixControlHandler,
    UnixControlServer,
};

struct EchoHandler;

impl UnixControlHandler for EchoHandler {
    fn handle_control(
        &self,
        envelope: ControlEnvelope,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        Ok(ControlEnvelope {
            source: envelope.destination,
            destination: envelope.source,
            message: envelope.message,
        })
    }
}

fn socket_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "orion-ipc-bench-{}-{}.sock",
        std::process::id(),
        Revision::new(1).get()
    ))
}

fn payload_with_node_id_len(len: usize) -> ControlEnvelope {
    let node_id = "n".repeat(len.max(1));
    ControlEnvelope {
        source: LocalAddress::new("bench-client"),
        destination: LocalAddress::new("bench-server"),
        message: ControlMessage::Hello(PeerHello {
            node_id: NodeId::new(node_id),
            desired_revision: Revision::new(7),
            desired_fingerprint: 0,
            desired_section_fingerprints: orion_control_plane::DesiredStateSectionFingerprints {
                nodes: 0,
                artifacts: 0,
                workloads: 0,
                resources: 0,
                providers: 0,
                executors: 0,
                leases: 0,
            },
            observed_revision: Revision::new(7),
            applied_revision: Revision::new(7),
            transport_binding_version: None,
            transport_binding_public_key: None,
            transport_tls_cert_pem: None,
            transport_binding_signature: None,
        }),
    }
}

fn benchmark_ipc_latency(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime should build");
    let path = socket_path();
    let server = runtime
        .block_on(UnixControlServer::bind(&path, Arc::new(EchoHandler)))
        .expect("IPC server should bind");
    let task = runtime.spawn(server.serve());
    let client = UnixControlClient::new(&path);

    let mut group = c.benchmark_group("ipc_control");
    for (label, node_id_len) in [("tiny", 8usize), ("large", 4096usize)] {
        let payload = payload_with_node_id_len(node_id_len);
        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &payload,
            |b, payload| {
                b.to_async(&runtime).iter(|| client.send(payload.clone()));
            },
        );
    }
    group.finish();

    task.abort();
    let _ = std::fs::remove_file(path);
}

criterion_group!(benches, benchmark_ipc_latency);
criterion_main!(benches);
