use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use orion_core::{NodeId, ProtocolVersion, ResourceId};
use orion_data_plane::{ChannelBinding, LinkType, PeerCapabilities, RemoteBinding, TransportType};
use orion_transport_quic::{
    QuicChannel, QuicEndpoint, QuicFrame, QuicFrameClient, QuicFrameHandler, QuicFrameServer,
    QuicTransportError,
};

struct EchoHandler;

impl QuicFrameHandler for EchoHandler {
    fn handle_frame(&self, frame: QuicFrame) -> Result<QuicFrame, QuicTransportError> {
        Ok(QuicFrame {
            source: frame.destination.clone(),
            destination: frame.source.clone(),
            ..frame
        })
    }
}

fn frame(payload_len: usize) -> QuicFrame {
    let link = PeerCapabilities {
        node_id: NodeId::new("node-a"),
        control_versions: vec![ProtocolVersion::new(1, 0)],
        data_versions: vec![ProtocolVersion::new(1, 0)],
        transports: vec![TransportType::QuicStream],
        link_types: vec![LinkType::ReliableMultiplexed],
        features: Vec::new(),
    }
    .negotiate_with(&PeerCapabilities {
        node_id: NodeId::new("node-b"),
        control_versions: vec![ProtocolVersion::new(1, 0)],
        data_versions: vec![ProtocolVersion::new(1, 0)],
        transports: vec![TransportType::QuicStream],
        link_types: vec![LinkType::ReliableMultiplexed],
        features: Vec::new(),
    })
    .expect("peer capabilities should negotiate");

    QuicFrame {
        source: QuicEndpoint::new("127.0.0.1", 5100).with_server_name("node-a.local"),
        destination: QuicEndpoint::new("127.0.0.1", 5200).with_server_name("node-b.local"),
        connection_id: 7,
        channel: QuicChannel::Stream(3),
        link,
        binding: RemoteBinding::Channel(ChannelBinding {
            remote_node_id: NodeId::new("node-b"),
            resource_id: ResourceId::new("resource.video"),
            transport: TransportType::QuicStream,
            link_type: LinkType::ReliableMultiplexed,
        }),
        payload: vec![11; payload_len],
    }
}

fn benchmark_quic_perf(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime should build");
    let (server, endpoint) = runtime
        .block_on(QuicFrameServer::bind(
            QuicEndpoint::new("127.0.0.1", 0).with_server_name("node-b.local"),
            Arc::new(EchoHandler),
        ))
        .expect("QUIC server should bind");
    let task = runtime.spawn(server.serve());
    let client = {
        let _guard = runtime.enter();
        QuicFrameClient::new(endpoint).expect("QUIC client should build")
    };

    let small = frame(64);
    c.bench_function("quic_latency_small_frame", |b| {
        b.to_async(&runtime).iter(|| client.send(small.clone()));
    });

    let mut group = c.benchmark_group("quic_throughput");
    for payload_len in [64usize, 1024, 16 * 1024] {
        let payload = frame(payload_len);
        group.throughput(Throughput::Bytes(payload_len as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(payload_len),
            &payload,
            |b, payload| {
                b.to_async(&runtime).iter(|| client.send(payload.clone()));
            },
        );
    }
    group.finish();

    task.abort();
}

criterion_group!(benches, benchmark_quic_perf);
criterion_main!(benches);
