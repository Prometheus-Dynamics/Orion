//! Foundational identities, type names, version primitives, compatibility
//! primitives, and shared errors for Orion.
//!
//! This crate is intentionally small. It should contain only the vocabulary that
//! the rest of Orion depends on, not control-plane object graphs or runtime
//! lifecycle semantics.

mod archive;
mod compat;
mod error;
mod ids;
mod types;
mod version;

pub use archive::{
    ArchiveEncode, decode_from_slice, decode_from_slice_with, decode_length_prefixed,
    encode_length_prefixed, encode_to_vec,
};
pub use compat::{CompatibilityState, FeatureFlag};
pub use error::OrionError;
pub use ids::{
    ArtifactId, ClientName, ExecutorId, NodeId, PeerBaseUrl, ProviderId, PublicKeyHex, ResourceId,
    SessionId, WorkloadId,
};
pub use types::{
    CapabilityDef, CapabilityId, ConfigSchemaDef, ConfigSchemaId, ResourceType, ResourceTypeDef,
    RuntimeType, RuntimeTypeDef,
};
pub use version::{ProtocolVersion, Revision};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_roundtrip_strings() {
        let node_id = NodeId::new("node-a");
        let workload_id = WorkloadId::new("workload.pose");

        assert_eq!(node_id.as_str(), "node-a");
        assert_eq!(workload_id.as_str(), "workload.pose");
        assert_eq!(node_id.to_string(), "node-a");
    }

    #[test]
    fn types_roundtrip_strings() {
        let runtime_type = RuntimeType::new("graph.exec.v1");
        let resource_type = ResourceType::new("imu.sample_source");

        assert_eq!(runtime_type.as_str(), "graph.exec.v1");
        assert_eq!(resource_type.as_str(), "imu.sample_source");
    }

    #[test]
    fn ids_and_types_reject_empty_values() {
        assert!(NodeId::try_new("").is_err());
        assert!(RuntimeType::try_new("   ").is_err());
        assert!(FeatureFlag::try_new("").is_err());
    }

    #[test]
    fn revision_advances_monotonically() {
        let revision = Revision::ZERO.next().next();
        assert_eq!(revision.get(), 2);
    }

    #[test]
    fn protocol_version_and_compatibility_are_shared_primitives() {
        let version = ProtocolVersion::new(1, 2);
        let compatibility = CompatibilityState::Downgraded;

        assert_eq!(version.major, 1);
        assert_eq!(version.minor, 2);
        assert_eq!(compatibility, CompatibilityState::Downgraded);
    }

    #[test]
    fn shared_error_variants_remain_core_level() {
        let err = OrionError::UnsupportedRuntimeType(RuntimeType::new("graph.exec.v1"));
        assert_eq!(err.to_string(), "unsupported runtime type: graph.exec.v1");
    }
}
