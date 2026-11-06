//! The link's own body types for the records it carries.
//!
//! The wire is defined here, not by the `orion-control-plane` records: each record converts to a
//! mirror type whose fields and variants are in wire order, and postcard encodes the mirror. A
//! record that gains a field does not change the wire. The conversions destructure every field and
//! match every variant, so such a change fails to compile here until it is decided whether the
//! wire carries it (an appended body field or a new kind, see
//! `docs/design/stable-interface.md`), and what a decoded record gets when it does not.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

use orion_control_plane::{
    AvailabilityState, HealthState, LeaseRecord, LeaseState, ProviderRecord, ResourceActionResult,
    ResourceActionStatus, ResourceCapability, ResourceConfigState, ResourceOwnershipMode,
    ResourceRecord, ResourceState, TypedConfigValue,
};
use orion_core::{
    CapabilityId, ExecutorId, NodeId, ProviderId, ResourceId, ResourceType, WorkloadId,
};
use serde::{Deserialize, Serialize};

use super::codec::bounded_vec;

#[derive(Serialize, Deserialize)]
pub(crate) struct ProviderWire {
    provider_id: ProviderId,
    node_id: NodeId,
    #[serde(deserialize_with = "bounded_vec")]
    resource_types: Vec<ResourceType>,
}

impl From<&ProviderRecord> for ProviderWire {
    fn from(record: &ProviderRecord) -> Self {
        let ProviderRecord {
            provider_id,
            node_id,
            resource_types,
        } = record;
        Self {
            provider_id: provider_id.clone(),
            node_id: node_id.clone(),
            resource_types: resource_types.clone(),
        }
    }
}

impl From<ProviderWire> for ProviderRecord {
    fn from(wire: ProviderWire) -> Self {
        Self {
            provider_id: wire.provider_id,
            node_id: wire.node_id,
            resource_types: wire.resource_types,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ResourceWire {
    resource_id: ResourceId,
    resource_type: ResourceType,
    provider_id: ProviderId,
    realized_by_executor_id: Option<ExecutorId>,
    ownership_mode: OwnershipWire,
    realized_for_workload_id: Option<WorkloadId>,
    source_resource_id: Option<ResourceId>,
    source_workload_id: Option<WorkloadId>,
    health: HealthWire,
    availability: AvailabilityWire,
    lease_state: LeaseStateWire,
    #[serde(deserialize_with = "bounded_vec")]
    capabilities: Vec<CapabilityWire>,
    #[serde(deserialize_with = "bounded_vec")]
    labels: Vec<String>,
    #[serde(deserialize_with = "bounded_vec")]
    endpoints: Vec<String>,
    state: Option<StateWire>,
}

impl From<&ResourceRecord> for ResourceWire {
    fn from(record: &ResourceRecord) -> Self {
        let ResourceRecord {
            resource_id,
            resource_type,
            provider_id,
            realized_by_executor_id,
            ownership_mode,
            realized_for_workload_id,
            source_resource_id,
            source_workload_id,
            health,
            availability,
            lease_state,
            capabilities,
            labels,
            endpoints,
            state,
        } = record;
        Self {
            resource_id: resource_id.clone(),
            resource_type: resource_type.clone(),
            provider_id: provider_id.clone(),
            realized_by_executor_id: realized_by_executor_id.clone(),
            ownership_mode: ownership_mode.into(),
            realized_for_workload_id: realized_for_workload_id.clone(),
            source_resource_id: source_resource_id.clone(),
            source_workload_id: source_workload_id.clone(),
            health: (*health).into(),
            availability: (*availability).into(),
            lease_state: (*lease_state).into(),
            capabilities: capabilities.iter().map(CapabilityWire::from).collect(),
            labels: labels.clone(),
            endpoints: endpoints.clone(),
            state: state.as_ref().map(StateWire::from),
        }
    }
}

impl From<ResourceWire> for ResourceRecord {
    fn from(wire: ResourceWire) -> Self {
        Self {
            resource_id: wire.resource_id,
            resource_type: wire.resource_type,
            provider_id: wire.provider_id,
            realized_by_executor_id: wire.realized_by_executor_id,
            ownership_mode: wire.ownership_mode.into(),
            realized_for_workload_id: wire.realized_for_workload_id,
            source_resource_id: wire.source_resource_id,
            source_workload_id: wire.source_workload_id,
            health: wire.health.into(),
            availability: wire.availability.into(),
            lease_state: wire.lease_state.into(),
            capabilities: wire.capabilities.into_iter().map(Into::into).collect(),
            labels: wire.labels,
            endpoints: wire.endpoints,
            state: wire.state.map(Into::into),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum OwnershipWire {
    Exclusive,
    SharedRead,
    SharedLimited { max_consumers: u32 },
}

impl From<&ResourceOwnershipMode> for OwnershipWire {
    fn from(mode: &ResourceOwnershipMode) -> Self {
        match mode {
            ResourceOwnershipMode::Exclusive => Self::Exclusive,
            ResourceOwnershipMode::SharedRead => Self::SharedRead,
            ResourceOwnershipMode::SharedLimited { max_consumers } => Self::SharedLimited {
                max_consumers: *max_consumers,
            },
        }
    }
}

impl From<OwnershipWire> for ResourceOwnershipMode {
    fn from(wire: OwnershipWire) -> Self {
        match wire {
            OwnershipWire::Exclusive => Self::Exclusive,
            OwnershipWire::SharedRead => Self::SharedRead,
            OwnershipWire::SharedLimited { max_consumers } => Self::SharedLimited { max_consumers },
        }
    }
}

/// Maps a fieldless record enum to its mirror and back, variant by variant.
macro_rules! unit_enum_wire {
    ($wire:ident <=> $record:ident { $($variant:ident),+ $(,)? }) => {
        #[derive(Clone, Copy, Serialize, Deserialize)]
        enum $wire {
            $($variant),+
        }

        impl From<$record> for $wire {
            fn from(value: $record) -> Self {
                match value {
                    $($record::$variant => Self::$variant),+
                }
            }
        }

        impl From<$wire> for $record {
            fn from(wire: $wire) -> Self {
                match wire {
                    $($wire::$variant => Self::$variant),+
                }
            }
        }
    };
}

unit_enum_wire!(HealthWire <=> HealthState { Healthy, Degraded, Failed, Unknown });
unit_enum_wire!(AvailabilityWire <=> AvailabilityState { Available, Busy, Unavailable, Unknown });
unit_enum_wire!(LeaseStateWire <=> LeaseState { Unleased, Leased, Contended });
unit_enum_wire!(ActionStatusWire <=> ResourceActionStatus { Applied, Read, Failed });

#[derive(Serialize, Deserialize)]
struct CapabilityWire {
    capability_id: CapabilityId,
    detail: Option<String>,
}

impl From<&ResourceCapability> for CapabilityWire {
    fn from(capability: &ResourceCapability) -> Self {
        let ResourceCapability {
            capability_id,
            detail,
        } = capability;
        Self {
            capability_id: capability_id.clone(),
            detail: detail.clone(),
        }
    }
}

impl From<CapabilityWire> for ResourceCapability {
    fn from(wire: CapabilityWire) -> Self {
        Self {
            capability_id: wire.capability_id,
            detail: wire.detail,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StateWire {
    observed_at_ms: u64,
    action_result: Option<ActionResultWire>,
    config: Option<ConfigStateWire>,
}

impl From<&ResourceState> for StateWire {
    fn from(state: &ResourceState) -> Self {
        let ResourceState {
            observed_at_ms,
            action_result,
            config,
        } = state;
        Self {
            observed_at_ms: *observed_at_ms,
            action_result: action_result.as_ref().map(ActionResultWire::from),
            config: config.as_ref().map(ConfigStateWire::from),
        }
    }
}

impl From<StateWire> for ResourceState {
    fn from(wire: StateWire) -> Self {
        Self {
            observed_at_ms: wire.observed_at_ms,
            action_result: wire.action_result.map(Into::into),
            config: wire.config.map(Into::into),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ActionResultWire {
    action_kind: String,
    status: ActionStatusWire,
    data: Option<ValueWire>,
    error: Option<String>,
}

impl From<&ResourceActionResult> for ActionResultWire {
    fn from(result: &ResourceActionResult) -> Self {
        let ResourceActionResult {
            action_kind,
            status,
            data,
            error,
        } = result;
        Self {
            action_kind: action_kind.clone(),
            status: status.clone().into(),
            data: data.as_ref().map(ValueWire::from),
            error: error.clone(),
        }
    }
}

impl From<ActionResultWire> for ResourceActionResult {
    fn from(wire: ActionResultWire) -> Self {
        Self {
            action_kind: wire.action_kind,
            status: wire.status.into(),
            data: wire.data.map(Into::into),
            error: wire.error,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ConfigStateWire {
    payload: BTreeMap<String, ValueWire>,
}

impl From<&ResourceConfigState> for ConfigStateWire {
    fn from(config: &ResourceConfigState) -> Self {
        let ResourceConfigState { payload } = config;
        Self {
            payload: payload
                .iter()
                .map(|(key, value)| (key.clone(), value.into()))
                .collect(),
        }
    }
}

impl From<ConfigStateWire> for ResourceConfigState {
    fn from(wire: ConfigStateWire) -> Self {
        Self {
            payload: wire
                .payload
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
        }
    }
}

/// [`TypedConfigValue`] on the wire. `F64` came last, so the other variants keep their indices.
#[derive(Serialize, Deserialize)]
pub(crate) enum ValueWire {
    Bool(bool),
    Int(i64),
    UInt(u64),
    String(String),
    Bytes(Vec<u8>),
    F64(f64),
}

impl From<&TypedConfigValue> for ValueWire {
    fn from(value: &TypedConfigValue) -> Self {
        match value {
            TypedConfigValue::Bool(value) => Self::Bool(*value),
            TypedConfigValue::Int(value) => Self::Int(*value),
            TypedConfigValue::UInt(value) => Self::UInt(*value),
            TypedConfigValue::String(value) => Self::String(value.clone()),
            TypedConfigValue::Bytes(value) => Self::Bytes(value.clone()),
            TypedConfigValue::F64(value) => Self::F64(*value),
        }
    }
}

impl From<ValueWire> for TypedConfigValue {
    fn from(wire: ValueWire) -> Self {
        match wire {
            ValueWire::Bool(value) => Self::Bool(value),
            ValueWire::Int(value) => Self::Int(value),
            ValueWire::UInt(value) => Self::UInt(value),
            ValueWire::String(value) => Self::String(value),
            ValueWire::Bytes(value) => Self::Bytes(value),
            ValueWire::F64(value) => Self::F64(value),
        }
    }
}

/// `serde(with)` for a [`TypedConfigValue`] field of a link body.
pub(crate) mod value {
    use super::{TypedConfigValue, ValueWire};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        value: &TypedConfigValue,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        ValueWire::from(value).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<TypedConfigValue, D::Error> {
        ValueWire::deserialize(deserializer).map(Into::into)
    }
}

/// A lease on the wire. Cross-node `holders` are not carried; devices see the first holder in
/// `holder_*`, and a decoded lease has none.
#[derive(Serialize, Deserialize)]
pub(crate) struct LeaseWire {
    resource_id: ResourceId,
    lease_state: LeaseStateWire,
    holder_node_id: Option<NodeId>,
    holder_workload_id: Option<WorkloadId>,
}

impl From<&LeaseRecord> for LeaseWire {
    fn from(record: &LeaseRecord) -> Self {
        let LeaseRecord {
            resource_id,
            lease_state,
            holder_node_id,
            holder_workload_id,
            holders: _,
        } = record;
        Self {
            resource_id: resource_id.clone(),
            lease_state: (*lease_state).into(),
            holder_node_id: holder_node_id.clone(),
            holder_workload_id: holder_workload_id.clone(),
        }
    }
}

impl From<LeaseWire> for LeaseRecord {
    fn from(wire: LeaseWire) -> Self {
        Self {
            resource_id: wire.resource_id,
            lease_state: wire.lease_state.into(),
            holder_node_id: wire.holder_node_id,
            holder_workload_id: wire.holder_workload_id,
            holders: Vec::new(),
        }
    }
}
