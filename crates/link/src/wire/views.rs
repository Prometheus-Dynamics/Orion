//! Borrowed device → host bodies. Each encodes to the same bytes postcard produces for the
//! corresponding `orion-control-plane` record (field order, `Option` tags, and enum variant
//! indices included).

use super::codec::{Encode, Writer};
use super::{Roles, sealed};

/// Opens (or reopens) a session. Device → host, kind [`super::kind::HELLO`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloView<'a> {
    /// Stable device name; the host may restrict accepted names.
    pub device_name: &'a str,
    /// Announced roles.
    pub roles: Roles,
    /// Largest frame (header + payload + CRC) the device can receive and send.
    pub max_frame: u32,
}

impl Encode for HelloView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.device_name.encode(w);
        self.roles.encode(w);
        self.max_frame.encode(w);
    }
}

/// `ProviderRecord` as borrowed data.
///
/// The gateway owns `node_id` and overwrites it, so any non-empty placeholder works.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderView<'a> {
    /// `ProviderRecord::provider_id`.
    pub provider_id: &'a str,
    /// `ProviderRecord::node_id` (overwritten by the gateway).
    pub node_id: &'a str,
    /// `ProviderRecord::resource_types`.
    pub resource_types: &'a [&'a str],
}

impl<'a> ProviderView<'a> {
    /// A provider with no resource types.
    #[must_use]
    pub const fn new(provider_id: &'a str, node_id: &'a str) -> Self {
        Self {
            provider_id,
            node_id,
            resource_types: &[],
        }
    }

    /// Sets the resource types.
    #[must_use]
    pub const fn with_resource_types(mut self, resource_types: &'a [&'a str]) -> Self {
        self.resource_types = resource_types;
        self
    }
}

impl Encode for ProviderView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.provider_id.encode(w);
        self.node_id.encode(w);
        self.resource_types.encode(w);
    }
}

macro_rules! unit_enum {
    ($(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident = $index:literal,)+ }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(u8)]
        pub enum $name {
            $($(#[$vmeta])* $variant = $index,)+
        }

        impl $name {
            /// The postcard variant index.
            #[must_use]
            pub const fn index(self) -> u8 {
                self as u8
            }

            /// The variant with postcard index `index`.
            #[must_use]
            pub const fn from_index(index: u32) -> Option<Self> {
                match index {
                    $($index => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }

        impl Encode for $name {
            fn encode(&self, w: &mut Writer<'_>) {
                w.byte(self.index());
            }
        }
    };
}

unit_enum!(
    /// `HealthState`.
    Health {
        /// `Healthy`.
        Healthy = 0,
        /// `Degraded`.
        Degraded = 1,
        /// `Failed`.
        Failed = 2,
        /// `Unknown`.
        Unknown = 3,
    }
);

unit_enum!(
    /// `AvailabilityState`.
    Availability {
        /// `Available`.
        Available = 0,
        /// `Busy`.
        Busy = 1,
        /// `Unavailable`.
        Unavailable = 2,
        /// `Unknown`.
        Unknown = 3,
    }
);

unit_enum!(
    /// `LeaseState`.
    LeaseState {
        /// `Unleased`.
        Unleased = 0,
        /// `Leased`.
        Leased = 1,
        /// `Contended`.
        Contended = 2,
    }
);

unit_enum!(
    /// `ResourceActionStatus`.
    ActionStatus {
        /// `Applied`.
        Applied = 0,
        /// `Read`.
        Read = 1,
        /// `Failed`.
        Failed = 2,
    }
);

/// `ResourceOwnershipMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Ownership {
    /// `Exclusive`.
    Exclusive,
    /// `SharedRead`.
    SharedRead,
    /// `SharedLimited { max_consumers }`.
    SharedLimited {
        /// Maximum concurrent consumers.
        max_consumers: u32,
    },
}

impl Encode for Ownership {
    fn encode(&self, w: &mut Writer<'_>) {
        match self {
            Self::Exclusive => w.byte(0),
            Self::SharedRead => w.byte(1),
            Self::SharedLimited { max_consumers } => {
                w.byte(2);
                max_consumers.encode(w);
            }
        }
    }
}

/// `TypedConfigValue` as borrowed data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Value<'a> {
    /// `Bool`.
    Bool(bool),
    /// `Int`.
    Int(i64),
    /// `UInt`.
    UInt(u64),
    /// `String`.
    String(&'a str),
    /// `Bytes`.
    Bytes(&'a [u8]),
}

impl Encode for Value<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        match *self {
            Self::Bool(value) => {
                w.byte(0);
                value.encode(w);
            }
            Self::Int(value) => {
                w.byte(1);
                w.zigzag(value);
            }
            Self::UInt(value) => {
                w.byte(2);
                w.varint(value);
            }
            Self::String(value) => {
                w.byte(3);
                value.encode(w);
            }
            Self::Bytes(value) => {
                w.byte(4);
                w.bytes(value);
            }
        }
    }
}

/// `ResourceCapability`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityView<'a> {
    /// `capability_id`.
    pub capability_id: &'a str,
    /// `detail`.
    pub detail: Option<&'a str>,
}

impl Encode for CapabilityView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.capability_id.encode(w);
        self.detail.encode(w);
    }
}

/// One entry of `ResourceConfigState::payload` (a `BTreeMap`). Give entries sorted by key, with
/// unique keys, to match the record encoding byte for byte; hosts accept any order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigField<'a> {
    /// Key.
    pub key: &'a str,
    /// Value.
    pub value: Value<'a>,
}

/// `ResourceActionResult`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionResultView<'a> {
    /// `action_kind`.
    pub action_kind: &'a str,
    /// `status`.
    pub status: ActionStatus,
    /// `data`.
    pub data: Option<Value<'a>>,
    /// `error`.
    pub error: Option<&'a str>,
}

impl Encode for ActionResultView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.action_kind.encode(w);
        self.status.encode(w);
        self.data.encode(w);
        self.error.encode(w);
    }
}

/// `ResourceState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceStateView<'a> {
    /// `observed_at_ms`.
    pub observed_at_ms: u64,
    /// `action_result`.
    pub action_result: Option<ActionResultView<'a>>,
    /// `config` (`Some` encodes a `ResourceConfigState` with these fields).
    pub config: Option<&'a [ConfigField<'a>]>,
}

impl Encode for ResourceStateView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.observed_at_ms.encode(w);
        self.action_result.encode(w);
        match self.config {
            None => w.byte(0),
            Some(fields) => {
                w.byte(1);
                w.len_prefix(fields.len());
                for field in fields {
                    field.key.encode(w);
                    field.value.encode(w);
                }
            }
        }
    }
}

/// `ResourceRecord` as borrowed data. Start from [`ResourceView::new`] (the record builder's
/// defaults) and set what the device knows; everything is `const`, so a fixed resource can be a
/// `static`.
///
/// `state` is held as `&dyn StateBody` so the encoder for resource state (action results,
/// typed config values) is only linked into firmware that actually sets one.
#[derive(Debug, Clone, Copy)]
pub struct ResourceView<'a> {
    /// `resource_id`.
    pub resource_id: &'a str,
    /// `resource_type`.
    pub resource_type: &'a str,
    /// `provider_id`.
    pub provider_id: &'a str,
    /// `realized_by_executor_id`.
    pub realized_by_executor_id: Option<&'a str>,
    /// `ownership_mode`.
    pub ownership_mode: Ownership,
    /// `realized_for_workload_id`.
    pub realized_for_workload_id: Option<&'a str>,
    /// `source_resource_id`.
    pub source_resource_id: Option<&'a str>,
    /// `source_workload_id`.
    pub source_workload_id: Option<&'a str>,
    /// `health`.
    pub health: Health,
    /// `availability`.
    pub availability: Availability,
    /// `lease_state`.
    pub lease_state: LeaseState,
    /// `capabilities`.
    pub capabilities: &'a [CapabilityView<'a>],
    /// `labels`.
    pub labels: &'a [&'a str],
    /// `endpoints`.
    pub endpoints: &'a [&'a str],
    /// `state` (a [`ResourceStateView`]).
    pub state: Option<&'a dyn StateBody>,
}

impl<'a> ResourceView<'a> {
    /// A resource with the record builder's defaults: exclusive, health and availability
    /// unknown, unleased, and nothing else set.
    #[must_use]
    pub const fn new(resource_id: &'a str, resource_type: &'a str, provider_id: &'a str) -> Self {
        Self {
            resource_id,
            resource_type,
            provider_id,
            realized_by_executor_id: None,
            ownership_mode: Ownership::Exclusive,
            realized_for_workload_id: None,
            source_resource_id: None,
            source_workload_id: None,
            health: Health::Unknown,
            availability: Availability::Unknown,
            lease_state: LeaseState::Unleased,
            capabilities: &[],
            labels: &[],
            endpoints: &[],
            state: None,
        }
    }

    /// Sets the health.
    #[must_use]
    pub const fn with_health(mut self, health: Health) -> Self {
        self.health = health;
        self
    }

    /// Sets the availability.
    #[must_use]
    pub const fn with_availability(mut self, availability: Availability) -> Self {
        self.availability = availability;
        self
    }

    /// Sets the labels.
    #[must_use]
    pub const fn with_labels(mut self, labels: &'a [&'a str]) -> Self {
        self.labels = labels;
        self
    }

    /// Sets the endpoints.
    #[must_use]
    pub const fn with_endpoints(mut self, endpoints: &'a [&'a str]) -> Self {
        self.endpoints = endpoints;
        self
    }

    /// Sets the capabilities.
    #[must_use]
    pub const fn with_capabilities(mut self, capabilities: &'a [CapabilityView<'a>]) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Sets the lease state.
    #[must_use]
    pub const fn with_lease_state(mut self, lease_state: LeaseState) -> Self {
        self.lease_state = lease_state;
        self
    }

    /// Sets the ownership mode.
    #[must_use]
    pub const fn with_ownership(mut self, ownership_mode: Ownership) -> Self {
        self.ownership_mode = ownership_mode;
        self
    }

    /// Sets the resource state.
    #[must_use]
    pub const fn with_state(mut self, state: &'a dyn StateBody) -> Self {
        self.state = Some(state);
        self
    }
}

impl Encode for ResourceView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.resource_id.encode(w);
        self.resource_type.encode(w);
        self.provider_id.encode(w);
        self.realized_by_executor_id.encode(w);
        self.ownership_mode.encode(w);
        self.realized_for_workload_id.encode(w);
        self.source_resource_id.encode(w);
        self.source_workload_id.encode(w);
        self.health.encode(w);
        self.availability.encode(w);
        self.lease_state.encode(w);
        self.capabilities.encode(w);
        self.labels.encode(w);
        self.endpoints.encode(w);
        self.state.encode(w);
    }
}

/// `StatusEntry` as borrowed data: one volatile status value, kind [`super::kind::STATUS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusView<'a> {
    /// Key, unique per device (at most 128 bytes on the node).
    pub key: &'a str,
    /// The value.
    pub value: Value<'a>,
    /// Requested time-to-live in milliseconds (`0`: the node's maximum).
    pub ttl_ms: u32,
}

impl<'a> StatusView<'a> {
    /// A status entry with the node's maximum TTL.
    #[must_use]
    pub const fn new(key: &'a str, value: Value<'a>) -> Self {
        Self {
            key,
            value,
            ttl_ms: 0,
        }
    }

    /// Requests a time-to-live.
    #[must_use]
    pub const fn with_ttl_ms(mut self, ttl_ms: u32) -> Self {
        self.ttl_ms = ttl_ms;
        self
    }
}

impl Encode for StatusView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.key.encode(w);
        self.value.encode(w);
        self.ttl_ms.encode(w);
    }
}

/// A `ProviderState` body: the provider and every resource it offers.
#[derive(Debug, Clone, Copy)]
pub struct ProviderStateView<'a, P: ?Sized, R> {
    /// The provider.
    pub provider: &'a P,
    /// Its resources.
    pub resources: &'a [R],
}

impl<P: Encode + ?Sized, R: Encode> Encode for ProviderStateView<'_, P, R> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.provider.encode(w);
        self.resources.encode(w);
    }
}

/// Something that encodes as a `ResourceState`: [`ResourceStateView`]. Sealed.
pub trait StateBody: Encode + core::fmt::Debug + sealed::Sealed {}

impl sealed::Sealed for ResourceStateView<'_> {}
impl StateBody for ResourceStateView<'_> {}

/// Something that encodes as a `ProviderRecord`: [`ProviderView`], or (feature `alloc`) the
/// record itself. Sealed.
pub trait ProviderBody: Encode + sealed::Sealed {}
/// Something that encodes as a `ResourceRecord`: [`ResourceView`], or (feature `alloc`) the
/// record itself. Sealed.
pub trait ResourceBody: Encode + sealed::Sealed {}
/// Something that encodes as a `StatusEntry`: [`StatusView`], or (feature `alloc`) the entry
/// itself. Sealed.
pub trait StatusBody: Encode + sealed::Sealed {}

impl sealed::Sealed for ProviderView<'_> {}
impl ProviderBody for ProviderView<'_> {}
impl sealed::Sealed for ResourceView<'_> {}
impl ResourceBody for ResourceView<'_> {}
impl sealed::Sealed for StatusView<'_> {}
impl StatusBody for StatusView<'_> {}
