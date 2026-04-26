mod address;
mod control;
mod data;
mod error;
mod memory;
mod unix;

pub use address::LocalAddress;
pub use control::{ControlEnvelope, LocalControlTransport, UnixPeerIdentity};
pub use data::{DataEnvelope, LocalDataTransport};
pub use error::IpcTransportError;
pub use memory::IpcTransport;
pub use unix::{
    UnixControlClient, UnixControlHandler, UnixControlServer, UnixControlStreamClient,
    read_control_frame, read_control_frame_with_limit, read_control_frame_with_limit_metered,
    write_control_frame, write_control_frame_with_limit, write_control_frame_with_limit_metered,
};

#[cfg(test)]
mod tests {
    use super::*;
    use orion_control_plane::{ControlMessage, PeerHello};
    use orion_core::{CompatibilityState, NodeId, ProtocolVersion, ResourceId, Revision};
    use orion_core::{decode_from_slice, encode_to_vec};
    use orion_data_plane::{
        ChannelBinding, LinkType, PeerCapabilities, PeerLink, RemoteBinding, TransportType,
    };

    fn peer_link() -> PeerLink {
        PeerCapabilities {
            node_id: NodeId::new("node-a"),
            control_versions: vec![ProtocolVersion::new(1, 0)],
            data_versions: vec![ProtocolVersion::new(1, 0)],
            transports: vec![TransportType::Ipc],
            link_types: vec![LinkType::LocalSharedMemory],
            features: Vec::new(),
        }
        .negotiate_with(&PeerCapabilities {
            node_id: NodeId::new("node-b"),
            control_versions: vec![ProtocolVersion::new(1, 0)],
            data_versions: vec![ProtocolVersion::new(1, 0)],
            transports: vec![TransportType::Ipc],
            link_types: vec![LinkType::LocalSharedMemory],
            features: Vec::new(),
        })
        .expect("peer capabilities should negotiate")
    }

    #[test]
    fn control_transport_delivers_messages_fifo() {
        let transport = IpcTransport::new();
        let source = LocalAddress::new("engine");
        let destination = LocalAddress::new("orion");

        assert!(transport.register_control_endpoint(source.clone()));
        assert!(transport.register_control_endpoint(destination.clone()));

        assert!(transport.send_control(ControlEnvelope {
            source: source.clone(),
            destination: destination.clone(),
            message: ControlMessage::Hello(PeerHello {
                node_id: NodeId::new("node-a"),
                desired_revision: Revision::new(1),
                desired_fingerprint: 0,
                desired_section_fingerprints:
                    orion_control_plane::DesiredStateSectionFingerprints {
                        nodes: 0,
                        artifacts: 0,
                        workloads: 0,
                        resources: 0,
                        providers: 0,
                        executors: 0,
                        leases: 0,
                    },
                observed_revision: Revision::new(1),
                applied_revision: Revision::new(1),
                transport_binding_version: None,
                transport_binding_public_key: None,
                transport_tls_cert_pem: None,
                transport_binding_signature: None,
            }),
        }));

        let envelope = transport
            .recv_control(&destination)
            .expect("control message should be delivered");
        assert_eq!(envelope.source, source);
        assert!(matches!(envelope.message, ControlMessage::Hello(_)));
        assert!(transport.recv_control(&destination).is_none());
    }

    #[test]
    fn data_transport_delivers_remote_binding_payload() {
        let transport = IpcTransport::new();
        let source = LocalAddress::new("provider");
        let destination = LocalAddress::new("executor");

        assert!(transport.register_data_endpoint(source.clone()));
        assert!(transport.register_data_endpoint(destination.clone()));

        assert!(transport.send_data(DataEnvelope {
            source,
            destination: destination.clone(),
            link: peer_link(),
            binding: RemoteBinding::Channel(ChannelBinding {
                remote_node_id: NodeId::new("node-b"),
                resource_id: ResourceId::new("resource.imu"),
                transport: TransportType::Ipc,
                link_type: LinkType::LocalSharedMemory,
            }),
            payload: vec![1, 2, 3, 4],
        }));

        let envelope = transport
            .recv_data(&destination)
            .expect("data message should be delivered");
        assert_eq!(envelope.payload, vec![1, 2, 3, 4]);
        assert_eq!(envelope.link.compatibility, CompatibilityState::Preferred);
    }

    #[tokio::test]
    async fn control_stream_rejects_oversized_frame_before_allocation() {
        use orion_transport_common::DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES;
        use tokio::io::AsyncWriteExt;

        let (mut client, mut server) = tokio::io::duplex(16);
        let writer = tokio::spawn(async move {
            client
                .write_u32_le((DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES + 1) as u32)
                .await
                .expect("oversized frame length should write");
        });

        let err = read_control_frame(&mut server)
            .await
            .expect_err("oversized frame should be rejected");
        assert!(matches!(err, IpcTransportError::DecodeFailed(_)));
        writer.await.expect("writer should join");
    }

    #[test]
    fn sends_fail_when_destination_endpoint_is_missing() {
        let transport = IpcTransport::new();
        let source = LocalAddress::new("provider");
        transport.register_control_endpoint(source.clone());
        transport.register_data_endpoint(source.clone());

        assert!(!transport.send_control(ControlEnvelope {
            source: source.clone(),
            destination: LocalAddress::new("missing"),
            message: ControlMessage::Hello(PeerHello {
                node_id: NodeId::new("node-a"),
                desired_revision: Revision::new(0),
                desired_fingerprint: 0,
                desired_section_fingerprints:
                    orion_control_plane::DesiredStateSectionFingerprints {
                        nodes: 0,
                        artifacts: 0,
                        workloads: 0,
                        resources: 0,
                        providers: 0,
                        executors: 0,
                        leases: 0,
                    },
                observed_revision: Revision::new(0),
                applied_revision: Revision::new(0),
                transport_binding_version: None,
                transport_binding_public_key: None,
                transport_tls_cert_pem: None,
                transport_binding_signature: None,
            }),
        }));

        assert!(!transport.send_data(DataEnvelope {
            source,
            destination: LocalAddress::new("missing"),
            link: peer_link(),
            binding: RemoteBinding::Channel(ChannelBinding {
                remote_node_id: NodeId::new("node-b"),
                resource_id: ResourceId::new("resource.imu"),
                transport: TransportType::Ipc,
                link_type: LinkType::LocalSharedMemory,
            }),
            payload: vec![9],
        }));
    }

    #[test]
    fn registering_same_endpoint_twice_reports_existing_endpoint() {
        let transport = IpcTransport::new();
        let address = LocalAddress::new("orion");

        assert!(transport.register_control_endpoint(address.clone()));
        assert!(!transport.register_control_endpoint(address.clone()));

        assert!(transport.register_data_endpoint(address.clone()));
        assert!(!transport.register_data_endpoint(address));
    }

    #[tokio::test]
    async fn unix_control_transport_roundtrips_real_messages() {
        use std::sync::Arc;

        struct LoopbackHandler;

        impl UnixControlHandler for LoopbackHandler {
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

        let socket_path = std::env::temp_dir().join(format!(
            "orion-ipc-test-{}-{}.sock",
            std::process::id(),
            Revision::new(1).get()
        ));

        let server = UnixControlServer::bind(&socket_path, Arc::new(LoopbackHandler))
            .await
            .expect("unix control server should bind");
        let task = tokio::spawn(server.serve());

        let client = UnixControlClient::new(&socket_path);
        let response = client
            .send(ControlEnvelope {
                source: LocalAddress::new("engine"),
                destination: LocalAddress::new("orion"),
                message: ControlMessage::Hello(PeerHello {
                    node_id: NodeId::new("node-a"),
                    desired_revision: Revision::new(1),
                    desired_fingerprint: 0,
                    desired_section_fingerprints:
                        orion_control_plane::DesiredStateSectionFingerprints {
                            nodes: 0,
                            artifacts: 0,
                            workloads: 0,
                            resources: 0,
                            providers: 0,
                            executors: 0,
                            leases: 0,
                        },
                    observed_revision: Revision::new(1),
                    applied_revision: Revision::new(1),
                    transport_binding_version: None,
                    transport_binding_public_key: None,
                    transport_tls_cert_pem: None,
                    transport_binding_signature: None,
                }),
            })
            .await
            .expect("unix control request should roundtrip");

        assert_eq!(response.source.as_str(), "orion");
        assert_eq!(response.destination.as_str(), "engine");
        assert!(matches!(response.message, ControlMessage::Hello(_)));

        task.abort();
        let _ = std::fs::remove_file(socket_path);
    }

    #[tokio::test]
    async fn unix_control_transport_rejects_malformed_frames_without_poisoning_server() {
        use std::sync::Arc;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::UnixStream,
        };

        struct LoopbackHandler;

        impl UnixControlHandler for LoopbackHandler {
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

        let socket_path = std::env::temp_dir().join(format!(
            "orion-ipc-malformed-{}-{}.sock",
            std::process::id(),
            Revision::new(2).get()
        ));

        let server = UnixControlServer::bind(&socket_path, Arc::new(LoopbackHandler))
            .await
            .expect("unix control server should bind");
        let task = tokio::spawn(server.serve());

        let mut raw = UnixStream::connect(&socket_path)
            .await
            .expect("raw unix client should connect");
        raw.write_all(br#"{ definitely not valid json"#)
            .await
            .expect("raw malformed bytes should send");
        raw.shutdown()
            .await
            .expect("raw unix client should shutdown write side");

        let mut response = Vec::new();
        raw.read_to_end(&mut response)
            .await
            .expect("raw unix client should read server close");
        assert!(
            response.is_empty(),
            "malformed requests should not get a response payload"
        );

        let client = UnixControlClient::new(&socket_path);
        let roundtrip = client
            .send(ControlEnvelope {
                source: LocalAddress::new("engine"),
                destination: LocalAddress::new("orion"),
                message: ControlMessage::Hello(PeerHello {
                    node_id: NodeId::new("node-a"),
                    desired_revision: Revision::new(3),
                    desired_fingerprint: 0,
                    desired_section_fingerprints:
                        orion_control_plane::DesiredStateSectionFingerprints {
                            nodes: 0,
                            artifacts: 0,
                            workloads: 0,
                            resources: 0,
                            providers: 0,
                            executors: 0,
                            leases: 0,
                        },
                    observed_revision: Revision::new(3),
                    applied_revision: Revision::new(3),
                    transport_binding_version: None,
                    transport_binding_public_key: None,
                    transport_tls_cert_pem: None,
                    transport_binding_signature: None,
                }),
            })
            .await
            .expect("server should still accept a valid request after malformed bytes");

        assert_eq!(roundtrip.source.as_str(), "orion");
        assert!(matches!(roundtrip.message, ControlMessage::Hello(_)));

        task.abort();
        let _ = std::fs::remove_file(socket_path);
    }

    #[tokio::test]
    async fn unix_control_transport_graceful_shutdown_aborts_stalled_connections() {
        use std::sync::Arc;
        use tokio::{
            net::UnixStream,
            sync::oneshot,
            time::{Duration, timeout},
        };

        struct LoopbackHandler;

        impl UnixControlHandler for LoopbackHandler {
            fn handle_control(
                &self,
                envelope: ControlEnvelope,
            ) -> Result<ControlEnvelope, IpcTransportError> {
                Ok(envelope)
            }
        }

        let socket_path = std::env::temp_dir().join(format!(
            "orion-ipc-shutdown-test-{}-{}.sock",
            std::process::id(),
            Revision::new(2).get()
        ));
        let server = UnixControlServer::bind(&socket_path, Arc::new(LoopbackHandler))
            .await
            .expect("unix control server should bind");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(server.serve_with_shutdown(async move {
            let _ = shutdown_rx.await;
        }));

        let stalled_client = UnixStream::connect(&socket_path)
            .await
            .expect("stalled unix client should connect");
        let _ = &stalled_client;

        shutdown_tx
            .send(())
            .expect("shutdown signal should be sent");
        timeout(Duration::from_millis(200), task)
            .await
            .expect("unix control server should shut down promptly")
            .expect("unix control server join should succeed")
            .expect("unix control server shutdown should succeed");

        let _ = std::fs::remove_file(socket_path);
    }

    #[tokio::test]
    async fn unix_control_transport_accepts_partial_writes_and_reconnects() {
        use std::sync::Arc;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::UnixStream,
        };

        struct LoopbackHandler;

        impl UnixControlHandler for LoopbackHandler {
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

        let socket_path = std::env::temp_dir().join(format!(
            "orion-ipc-partial-{}-{}.sock",
            std::process::id(),
            Revision::new(4).get()
        ));

        let server = UnixControlServer::bind(&socket_path, Arc::new(LoopbackHandler))
            .await
            .expect("unix control server should bind");
        let task = tokio::spawn(server.serve());

        let envelope = ControlEnvelope {
            source: LocalAddress::new("engine"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::Hello(PeerHello {
                node_id: NodeId::new("node-a"),
                desired_revision: Revision::new(4),
                desired_fingerprint: 0,
                desired_section_fingerprints:
                    orion_control_plane::DesiredStateSectionFingerprints {
                        nodes: 0,
                        artifacts: 0,
                        workloads: 0,
                        resources: 0,
                        providers: 0,
                        executors: 0,
                        leases: 0,
                    },
                observed_revision: Revision::new(4),
                applied_revision: Revision::new(4),
                transport_binding_version: None,
                transport_binding_public_key: None,
                transport_tls_cert_pem: None,
                transport_binding_signature: None,
            }),
        };
        let payload = encode_to_vec(&envelope).expect("control envelope should encode");

        let mut raw = UnixStream::connect(&socket_path)
            .await
            .expect("raw unix client should connect");
        let midpoint = payload.len() / 2;
        raw.write_all(&payload[..midpoint])
            .await
            .expect("first partial write should succeed");
        tokio::task::yield_now().await;
        raw.write_all(&payload[midpoint..])
            .await
            .expect("second partial write should succeed");
        raw.shutdown()
            .await
            .expect("raw unix client should shutdown write side");

        let mut response = Vec::new();
        raw.read_to_end(&mut response)
            .await
            .expect("raw unix client should read response");
        let response: ControlEnvelope =
            decode_from_slice(&response).expect("response should decode");
        assert_eq!(response.source.as_str(), "orion");

        let client = UnixControlClient::new(&socket_path);
        let second = client
            .send(envelope)
            .await
            .expect("same transport should accept a second request");
        assert_eq!(second.destination.as_str(), "engine");

        task.abort();
        let _ = std::fs::remove_file(socket_path);
    }

    #[tokio::test]
    async fn unix_control_transport_survives_interrupted_clients_and_concurrent_pressure() {
        use std::sync::Arc;
        use tokio::io::AsyncWriteExt;

        struct LoopbackHandler;

        impl UnixControlHandler for LoopbackHandler {
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

        let socket_path = std::env::temp_dir().join(format!(
            "orion-ipc-pressure-{}-{}.sock",
            std::process::id(),
            Revision::new(5).get()
        ));

        let server = UnixControlServer::bind(&socket_path, Arc::new(LoopbackHandler))
            .await
            .expect("unix control server should bind");
        let task = tokio::spawn(server.serve());

        let interrupted_payload = encode_to_vec(&ControlEnvelope {
            source: LocalAddress::new("engine"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::Hello(PeerHello {
                node_id: NodeId::new("node-interrupted"),
                desired_revision: Revision::new(5),
                desired_fingerprint: 0,
                desired_section_fingerprints:
                    orion_control_plane::DesiredStateSectionFingerprints {
                        nodes: 0,
                        artifacts: 0,
                        workloads: 0,
                        resources: 0,
                        providers: 0,
                        executors: 0,
                        leases: 0,
                    },
                observed_revision: Revision::new(5),
                applied_revision: Revision::new(5),
                transport_binding_version: None,
                transport_binding_public_key: None,
                transport_tls_cert_pem: None,
                transport_binding_signature: None,
            }),
        })
        .expect("interrupted envelope should encode");
        let mut interrupted = tokio::net::UnixStream::connect(&socket_path)
            .await
            .expect("interrupted unix client should connect");
        interrupted
            .write_all(&interrupted_payload)
            .await
            .expect("interrupted payload should send");
        drop(interrupted);

        let mut joins = Vec::new();
        for index in 0..16 {
            let path = socket_path.clone();
            joins.push(tokio::spawn(async move {
                let client = UnixControlClient::new(path);
                client
                    .send(ControlEnvelope {
                        source: LocalAddress::new(format!("engine-{index}")),
                        destination: LocalAddress::new("orion"),
                        message: ControlMessage::Hello(PeerHello {
                            node_id: NodeId::new(format!("node-{index}")),
                            desired_revision: Revision::new(index as u64),
                            desired_fingerprint: 0,
                            desired_section_fingerprints:
                                orion_control_plane::DesiredStateSectionFingerprints {
                                    nodes: 0,
                                    artifacts: 0,
                                    workloads: 0,
                                    resources: 0,
                                    providers: 0,
                                    executors: 0,
                                    leases: 0,
                                },
                            observed_revision: Revision::new(index as u64),
                            applied_revision: Revision::new(index as u64),
                            transport_binding_version: None,
                            transport_binding_public_key: None,
                            transport_tls_cert_pem: None,
                            transport_binding_signature: None,
                        }),
                    })
                    .await
            }));
        }

        for join in joins {
            let response = join
                .await
                .expect("concurrent IPC task should join")
                .expect("concurrent IPC request should succeed");
            assert_eq!(response.source.as_str(), "orion");
        }

        task.abort();
        let _ = std::fs::remove_file(socket_path);
    }
}
