//! CI guard: the archived (rkyv) layout of the control-protocol types must match the layout
//! recorded next to `CONTROL_PROTOCOL_VERSION` in `crates/core/src/protocol.rs`.
//!
//! rkyv archives are layout-exact, so any layout change breaks mixed orionctl/client/node
//! versions. This test forces such changes to come with a protocol version bump, which turns the
//! breakage into a clear `ProtocolMismatch` error instead of an rkyv decode failure.
//!
//! The fingerprint hashes `size_of`/`align_of` of every archived protocol type (fixed across
//! platforms: rkyv is configured `little_endian` + `pointer_width_64`). It catches added,
//! removed, or resized fields and variants anywhere in the listed types. Pure reorders or swaps
//! between same-sized field types are not visible here and still need a manual version bump.

use orion_core::{CONTROL_PROTOCOL_LAYOUT_FINGERPRINT, CONTROL_PROTOCOL_VERSION};
use std::mem::{align_of, size_of};

macro_rules! archived_layouts {
    ($($ty:ty),* $(,)?) => {
        vec![$((
            stringify!($ty),
            size_of::<rkyv::Archived<$ty>>(),
            align_of::<rkyv::Archived<$ty>>(),
        )),*]
    };
}

/// Every rkyv type that can appear on the local IPC or HTTP control wire.
fn protocol_layouts() -> Vec<(&'static str, usize, usize)> {
    use orion_auth::*;
    use orion_control_plane::*;
    use orion_core::*;
    use orion_transport_http::HttpResponsePayload;
    use orion_transport_ipc::{ControlEnvelope, LocalAddress, UnixPeerIdentity};

    archived_layouts![
        // orion-core
        Revision,
        HlcTimestamp,
        ProtocolVersion,
        CompatibilityState,
        FeatureFlag,
        NodeId,
        WorkloadId,
        ResourceId,
        ArtifactId,
        ProviderId,
        ExecutorId,
        ClientName,
        SessionId,
        PeerBaseUrl,
        PublicKeyHex,
        RuntimeType,
        ResourceType,
        ConfigSchemaId,
        CapabilityId,
        // envelopes
        ControlEnvelope,
        LocalAddress,
        UnixPeerIdentity,
        HttpResponsePayload,
        AuthenticatedPeerRequest,
        NodeTransportBinding,
        PeerRequestAuth,
        PeerRequestPayload,
        // control messages
        ControlMessage,
        ClientEvent,
        ClientEventKind,
        ClientEventPoll,
        ClientHello,
        ClientRole,
        ClientSession,
        ExecutorStateUpdate,
        ExecutorWorkloadQuery,
        ObservedStateUpdate,
        PeerEnrollment,
        PeerIdentityUpdate,
        ProviderLeaseQuery,
        ProviderStateUpdate,
        StateWatch,
        StatusSubject,
        StatusEntry,
        StatusKey,
        StatusQuery,
        StatusChange,
        MaintenanceAction,
        MaintenanceCommand,
        MaintenanceMode,
        MaintenanceState,
        MaintenanceStatus,
        MutationBatch,
        DesiredStateMutation,
        PeerHello,
        StateSnapshot,
        SyncRequest,
        SyncSummaryRequest,
        SyncDiffRequest,
        DesiredStateObjectSelector,
        DesiredStateSection,
        DesiredStateSectionFingerprints,
        DesiredStateSummary,
        DesiredObjectStamps,
        // observability
        NodeObservabilitySnapshot,
        NodeHealthSnapshot,
        NodeHealthStatus,
        NodeReadinessSnapshot,
        NodeReadinessStatus,
        AuditLogBackpressureMode,
        ClientSessionMetricsSnapshot,
        CommunicationEndpointScope,
        CommunicationEndpointSnapshot,
        CommunicationFailureCountSnapshot,
        CommunicationFailureKind,
        CommunicationMetricsSnapshot,
        CommunicationRecentMetricsSnapshot,
        CommunicationStageMetricsSnapshot,
        CommunicationTransportKind,
        HostMetricsSnapshot,
        HttpMutualTlsMode,
        LatencyMetricsSnapshot,
        ObservabilityEvent,
        ObservabilityEventKind,
        OperationFailureCategory,
        OperationMetricsSnapshot,
        PeerSyncErrorKind,
        PeerSyncStatus,
        PeerTrustRecord,
        PeerTrustSnapshot,
        PersistenceMetricsSnapshot,
        TransportMetricsSnapshot,
        NodeResourceUsageSnapshot,
        LocalStreamUsageSnapshot,
        MutationHistoryUsageSnapshot,
        ProcessMemorySnapshot,
        RegistryUsageSnapshot,
        StateSectionCounts,
        StateSizeSnapshot,
        WorkerQueueUsageSnapshot,
        DesiredStateMergeSnapshot,
        ObservedPersistenceUsageSnapshot,
        StatusLaneUsageSnapshot,
        // records and state
        AppliedClusterState,
        ClusterStateEnvelope,
        DesiredClusterState,
        ObservedClusterState,
        ArtifactRecord,
        ExecutorRecord,
        NodeRecord,
        NodeClockFacts,
        ClockSourceKind,
        ProviderRecord,
        LeaseRecord,
        ResourceActionResult,
        ResourceActionStatus,
        ResourceCapability,
        ResourceConfigState,
        ResourceOwnershipMode,
        ResourceRecord,
        ResourceState,
        ResourceBinding,
        TypedConfigValue,
        WorkloadConfig,
        WorkloadRecord,
        WorkloadRequirement,
        AvailabilityState,
        DesiredState,
        HealthState,
        LeaseState,
        RestartPolicy,
        WorkloadObservedState,
    ]
}

/// FNV-1a over `(name, size, align)`; stable across Rust versions and platforms.
fn fingerprint(layouts: &[(&str, usize, usize)]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut feed = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    for (name, size, align) in layouts {
        feed(name.as_bytes());
        feed(&[0]);
        feed(&(*size as u64).to_le_bytes());
        feed(&(*align as u64).to_le_bytes());
    }
    hash
}

#[test]
fn control_protocol_layout_matches_recorded_fingerprint() {
    let layouts = protocol_layouts();
    let actual = fingerprint(&layouts);
    if actual != CONTROL_PROTOCOL_LAYOUT_FINGERPRINT {
        let table = layouts
            .iter()
            .map(|(name, size, align)| format!("  {name}: size={size} align={align}"))
            .collect::<Vec<_>>()
            .join("\n");
        panic!(
            "the archived layout of the Orion control protocol changed \
             (fingerprint {actual:#018x}, recorded {CONTROL_PROTOCOL_LAYOUT_FINGERPRINT:#018x} for \
             CONTROL_PROTOCOL_VERSION {CONTROL_PROTOCOL_VERSION}).\n\
             Mixed orionctl/client/orion-node versions can no longer decode each other's messages: \
             bump CONTROL_PROTOCOL_VERSION and update the fingerprint \
             (CONTROL_PROTOCOL_LAYOUT_FINGERPRINT = {actual:#018x}) in crates/core/src/protocol.rs, \
             and note the bump in CHANGELOG.md. If you only edited the type list in this test \
             without changing any wire type, update the fingerprint alone.\n\
             Current layouts:\n{table}"
        );
    }
}
