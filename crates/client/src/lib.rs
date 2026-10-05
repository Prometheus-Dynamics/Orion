//! Rust client SDK for talking to an Orion daemon.
//!
//! Preferred entrypoints:
//!
//! - Use the typed client wrappers such as [`ControlPlaneClient`], [`ProviderClient`], and
//!   [`ExecutorClient`] once a session is established.
//! - Use the higher-level local app/client helpers for same-host IPC flows.
//! - Keep `ClientSession` as the lower-level escape hatch when a workflow needs custom plumbing.
//! - Use the consumer views ([`AssignedWorkload`], [`BoundResource`], [`assigned_workloads`],
//!   [`resources_bound_to`], and `LocalExecutorService::watch_assigned_workloads`) instead of
//!   walking cluster-state envelopes by hand.
//! - Publish volatile, latest-value status (TTL'd, in node memory only) with
//!   `LocalProviderService::publish_status` / `LocalExecutorService::publish_status`, and read it
//!   with `query_status` or `watch_status` ([`StatusWatch`]).
//! - Handle actions for a provider or executor with `watch_action_requests`
//!   ([`ActionRequestWatch`]); submit and follow them with `LocalControlPlaneClient::run_action`,
//!   `query_actions`, `wait_for_action`, and [`ActionWatch`] (see `docs/actions.md`).
//! - Talk to nodes over the network as a remote operator (desktop and fleet tools) with
//!   [`remote::RemoteOperator`] (feature `remote`, `docs/remote-operator.md`).
//! - Import [`prelude`] to get the client API plus the core and control-plane types executor and
//!   provider apps commonly need, without depending on `orion-core` or `orion-control-plane`.

#[cfg(feature = "ipc")]
mod app;
#[cfg(feature = "ipc")]
mod control_plane;
mod error;
#[cfg(feature = "ipc")]
mod executor;
mod graph;
#[cfg(feature = "ipc")]
mod provider;
#[cfg(feature = "remote")]
pub mod remote;
mod resource;
#[cfg(feature = "ipc")]
mod response;
#[cfg(feature = "ipc")]
mod session;
#[cfg(feature = "ipc")]
mod stream;
mod view;

pub mod prelude;

#[cfg(feature = "ipc")]
pub use app::{
    ActionRequestWatch, AssignedWorkloadWatch, AssignedWorkloadsUpdate, ExecutorApp,
    LocalExecutorApp, LocalExecutorClient, LocalExecutorEvent, LocalExecutorService,
    LocalExecutorSubscription, LocalNodeRuntime, LocalProviderApp, LocalProviderClient,
    LocalProviderEvent, LocalProviderService, LocalProviderSubscription, LocalRuntimePublisher,
    LocalRuntimePublisherBuilder, LocalServiceRetryPolicy, ProviderApp, StatusWatch,
};
#[cfg(feature = "ipc")]
pub use control_plane::{
    ActionWatch, ControlPlaneClient, ControlPlaneEventStream, LocalControlPlaneClient,
};
pub use error::ClientError;
#[cfg(feature = "ipc")]
pub use executor::{ExecutorClient, ExecutorEventStream};
pub use graph::{
    GraphPayload, GraphPayloadSource, GraphReference, GraphResolutionError, ResolvedGraphPayload,
    graph_reference_for_workload, resolve_graph_reference, resolve_workload_graph,
};
pub use orion_control_plane::ClientRole;
#[cfg(feature = "ipc")]
pub use provider::{ProviderClient, ProviderEventStream};
pub use resource::{DerivedResource, ProviderResource, ResourceClaim};
#[cfg(feature = "ipc")]
pub use session::{
    ClientIdentity, ClientSession, DEFAULT_DAEMON_ADDRESS, SessionConfig, default_ipc_socket_path,
    default_ipc_stream_socket_path,
};

pub use view::{
    AssignedWorkload, BoundResource, assigned_workloads, assigned_workloads_for_executor,
    assigned_workloads_from_records, is_assigned_to, resources_bound_to,
};

#[cfg(all(test, feature = "ipc"))]
mod tests;
