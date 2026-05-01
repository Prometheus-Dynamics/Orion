use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use orion_control_plane::{ControlMessage, PeerHello};
use orion_core::{NodeId, Revision};
use orion_transport_http::{
    HttpClient, HttpControlHandler, HttpRequestPayload, HttpResponsePayload, HttpServer,
    HttpTransportError,
};

struct AcceptHandler;

impl HttpControlHandler for AcceptHandler {
    fn handle_payload(
        &self,
        _payload: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        Ok(HttpResponsePayload::Accepted)
    }
}

fn benchmark_http_roundtrip(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime should build");
    let service = Arc::new(AcceptHandler);
    let (addr, server, listener) = runtime
        .block_on(HttpServer::bind(
            "127.0.0.1:0".parse().expect("socket address should parse"),
            service,
        ))
        .expect("HTTP server should bind");
    let server_task = runtime.spawn(server.serve(listener));
    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");
    let payload = HttpRequestPayload::Control(Box::new(ControlMessage::Hello(PeerHello {
        node_id: NodeId::new("bench-node"),
        desired_revision: Revision::ZERO,
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
        observed_revision: Revision::ZERO,
        applied_revision: Revision::ZERO,
        transport_binding_version: None,
        transport_binding_public_key: None,
        transport_tls_cert_pem: None,
        transport_binding_signature: None,
    })));

    let mut group = c.benchmark_group("http_control");
    group.bench_with_input(
        BenchmarkId::from_parameter("hello_roundtrip"),
        &payload,
        |b, payload| {
            b.to_async(&runtime).iter(|| client.send(payload));
        },
    );
    group.finish();

    server_task.abort();
}

criterion_group!(benches, benchmark_http_roundtrip);
criterion_main!(benches);
