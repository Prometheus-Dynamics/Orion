//! Rust client SDK for talking to an Orion daemon.
//!
//! Preferred entrypoints:
//!
//! - Use the typed client wrappers such as [`ControlPlaneClient`], [`ProviderClient`], and
//!   [`ExecutorClient`] once a session is established.
//! - Use the higher-level local app/client helpers for same-host IPC flows.
//! - Keep `ClientSession` as the lower-level escape hatch when a workflow needs custom plumbing.

#[cfg(feature = "ipc")]
mod app;
#[cfg(feature = "ipc")]
mod control_plane;
mod error;
#[cfg(feature = "ipc")]
mod executor;
#[cfg(feature = "ipc")]
mod provider;
mod resource;
#[cfg(feature = "ipc")]
mod session;
#[cfg(feature = "ipc")]
mod stream;

#[cfg(feature = "ipc")]
pub use app::{
    ExecutorApp, LocalExecutorApp, LocalExecutorClient, LocalNodeRuntime, LocalProviderApp,
    LocalProviderClient, ProviderApp,
};
#[cfg(feature = "ipc")]
pub use control_plane::{ControlPlaneClient, ControlPlaneEventStream, LocalControlPlaneClient};
pub use error::ClientError;
#[cfg(feature = "ipc")]
pub use executor::{ExecutorClient, ExecutorEventStream};
pub use orion_control_plane::ClientRole;
#[cfg(feature = "ipc")]
pub use provider::{ProviderClient, ProviderEventStream};
pub use resource::{DerivedResource, ProviderResource, ResourceClaim};
#[cfg(feature = "ipc")]
pub use session::{
    ClientIdentity, ClientSession, DEFAULT_DAEMON_ADDRESS, SessionConfig, default_ipc_socket_path,
    default_ipc_stream_socket_path,
};

#[cfg(all(test, feature = "ipc"))]
mod tests;
