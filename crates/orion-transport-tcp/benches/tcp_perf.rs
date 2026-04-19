use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use orion_core::{NodeId, ProtocolVersion, ResourceId};
use orion_data_plane::{ChannelBinding, LinkType, PeerCapabilities, RemoteBinding, TransportType};
use orion_transport_tcp::{
    TcpEndpoint, TcpFrame, TcpFrameClient, TcpFrameHandler, TcpFrameServer, TcpTransportError,
};

struct EchoHandler;

impl TcpFrameHandler for EchoHandler {
    fn handle_frame(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
        Ok(TcpFrame {
            source: frame.destination.clone(),
            destination: frame.source.clone(),
            ..frame
        })
    }
}

fn frame(payload_len: usize) -> TcpFrame {
    let link = PeerCapabilities {
        node_id: NodeId::new("node-a"),
        control_versions: vec![ProtocolVersion::new(1, 0)],
        data_versions: vec![ProtocolVersion::new(1, 0)],
        transports: vec![TransportType::TcpStream],
        link_types: vec![LinkType::ReliableOrdered],
        features: Vec::new(),
    }
    .negotiate_with(&PeerCapabilities {
        node_id: NodeId::new("node-b"),
        control_versions: vec![ProtocolVersion::new(1, 0)],
        data_versions: vec![ProtocolVersion::new(1, 0)],
        transports: vec![TransportType::TcpStream],
        link_types: vec![LinkType::ReliableOrdered],
        features: Vec::new(),
    })
    .expect("peer capabilities should negotiate");

    TcpFrame {
        source: TcpEndpoint::new("127.0.0.1", 4100),
        destination: TcpEndpoint::new("127.0.0.1", 4200),
        link,
        binding: RemoteBinding::Channel(ChannelBinding {
            remote_node_id: NodeId::new("node-b"),
            resource_id: ResourceId::new("resource.camera"),
            transport: TransportType::TcpStream,
            link_type: LinkType::ReliableOrdered,
        }),
        payload: vec![7; payload_len],
    }
}

fn benchmark_tcp_perf(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime should build");
    let (addr, server, listener) = runtime
        .block_on(TcpFrameServer::bind("127.0.0.1:0", Arc::new(EchoHandler)))
        .expect("TCP server should bind");
    let task = runtime.spawn(server.serve(listener));
    let client = TcpFrameClient::new(addr);

    let small = frame(64);
    c.bench_function("tcp_latency_small_frame", |b| {
        b.to_async(&runtime).iter(|| client.send(small.clone()));
    });

    let mut group = c.benchmark_group("tcp_throughput");
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

criterion_group!(benches, benchmark_tcp_perf);
criterion_main!(benches);
