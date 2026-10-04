//! Typed views over the endpoint strings advertised in [`ResourceRecord::endpoints`].
//!
//! Endpoints are persisted and exchanged as plain `scheme://payload` strings; the types in
//! this module only exist at parse time, so adding variants here never changes the wire or
//! storage format of resource records.
//!
//! Built-in schemes (`shm`, `ipc`, `unix`, `tcp`, `http`, `https`) parse into dedicated
//! variants. Any other syntactically valid scheme parses into [`ResourceEndpoint::Custom`],
//! which downstream crates can interpret by implementing [`CustomEndpointScheme`].
//!
//! [`ResourceRecord::endpoints`]: super::ResourceRecord::endpoints

use alloc::{
    borrow::ToOwned,
    format,
    string::{String, ToString},
};
use core::{fmt, str::FromStr};
use orion_core::ResourceId;
#[cfg(feature = "std")]
use std::{fs, io, path::PathBuf};
use thiserror::Error;

/// Schemes that always parse into a dedicated built-in [`ResourceEndpoint`] variant.
pub const BUILTIN_ENDPOINT_SCHEMES: &[&str] = &["shm", "ipc", "unix", "tcp", "http", "https"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedMemoryEndpoint {
    pub name: String,
}

/// Filesystem access to the shared-memory payload is host-only and requires the `std` feature.
#[cfg(feature = "std")]
impl SharedMemoryEndpoint {
    pub fn path(&self) -> PathBuf {
        self.path_in(Self::default_root())
    }

    pub fn path_in(&self, root: impl Into<PathBuf>) -> PathBuf {
        root.into().join(&self.name)
    }

    pub fn read_bytes(&self) -> Result<Vec<u8>, io::Error> {
        self.read_bytes_from(Self::default_root())
    }

    pub fn read_bytes_from(&self, root: impl Into<PathBuf>) -> Result<Vec<u8>, io::Error> {
        fs::read(self.path_in(root))
    }

    pub fn read_string(&self) -> Result<String, io::Error> {
        self.read_string_from(Self::default_root())
    }

    pub fn read_string_from(&self, root: impl Into<PathBuf>) -> Result<String, io::Error> {
        fs::read_to_string(self.path_in(root))
    }

    fn default_root() -> PathBuf {
        std::env::var_os("ORION_SHM_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/dev/shm"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IpcEndpoint {
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnixEndpoint {
    pub path: String,
}

/// Filesystem access to the socket/file path is host-only and requires the `std` feature.
#[cfg(feature = "std")]
impl UnixEndpoint {
    pub fn path_buf(&self) -> PathBuf {
        PathBuf::from(&self.path)
    }

    pub fn read_bytes(&self) -> Result<Vec<u8>, io::Error> {
        fs::read(self.path_buf())
    }

    pub fn read_string(&self) -> Result<String, io::Error> {
        fs::read_to_string(self.path_buf())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpEndpoint {
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpEndpoint {
    pub url: String,
}

/// An endpoint whose scheme is not one of Orion's built-in schemes, for example
/// `styx-frame-lease+unix:///run/helios/cam0.sock`.
///
/// `scheme` is stored in canonical (ASCII lowercase) form and `payload` is everything after
/// `://`, verbatim. Values produced by [`ResourceEndpoint::parse`] or [`CustomEndpoint::new`]
/// always hold a valid, non-built-in scheme and a non-empty payload.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CustomEndpoint {
    pub scheme: String,
    pub payload: String,
}

impl CustomEndpoint {
    /// Builds a custom endpoint, validating the scheme syntax and rejecting built-in schemes
    /// (which would not round-trip back into a `Custom` value).
    pub fn new(
        scheme: impl Into<String>,
        payload: impl Into<String>,
    ) -> Result<Self, ResourceEndpointError> {
        let scheme = scheme.into();
        let payload = payload.into();
        let endpoint = format!("{scheme}://{payload}");
        if !is_valid_endpoint_scheme(&scheme) {
            return Err(ResourceEndpointError::InvalidScheme { endpoint, scheme });
        }
        if payload.is_empty() {
            return Err(ResourceEndpointError::EmptyPayload { endpoint });
        }
        let scheme = scheme.to_ascii_lowercase();
        if BUILTIN_ENDPOINT_SCHEMES.contains(&scheme.as_str()) {
            return Err(ResourceEndpointError::ReservedScheme { endpoint, scheme });
        }
        Ok(Self { scheme, payload })
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn payload(&self) -> &str {
        &self.payload
    }

    /// The scheme without its transport suffix: `styx-frame-lease` for
    /// `styx-frame-lease+unix`. Equal to the whole scheme when there is no `+`.
    pub fn base_scheme(&self) -> &str {
        self.scheme
            .rsplit_once('+')
            .map_or(self.scheme.as_str(), |(base, _)| base)
    }

    /// The transport named after the last `+` in the scheme: `unix` for
    /// `styx-frame-lease+unix`. `None` when the scheme has no `+`.
    pub fn transport_suffix(&self) -> Option<&str> {
        self.scheme.rsplit_once('+').map(|(_, suffix)| suffix)
    }

    /// Whether this endpoint uses `scheme` (compared ASCII case-insensitively).
    pub fn has_scheme(&self, scheme: &str) -> bool {
        self.scheme.eq_ignore_ascii_case(scheme)
    }
}

impl fmt::Display for CustomEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}", self.scheme, self.payload)
    }
}

/// A parsed resource endpoint.
///
/// Marked `#[non_exhaustive]` so that future built-in schemes can be added without breaking
/// downstream `match` expressions.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResourceEndpoint {
    SharedMemory(SharedMemoryEndpoint),
    Ipc(IpcEndpoint),
    Unix(UnixEndpoint),
    Tcp(TcpEndpoint),
    Http(HttpEndpoint),
    Https(HttpEndpoint),
    Custom(CustomEndpoint),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ResourceEndpointError {
    #[error("resource endpoint `{endpoint}` is missing a scheme")]
    MissingScheme { endpoint: String },
    #[error("resource endpoint `{endpoint}` has an empty payload")]
    EmptyPayload { endpoint: String },
    /// No longer produced by [`ResourceEndpoint::parse`], which now accepts any valid scheme
    /// as [`ResourceEndpoint::Custom`]. Retained for API compatibility and for consumers that
    /// want to reject schemes they do not understand.
    #[error("resource endpoint `{endpoint}` uses unsupported scheme `{scheme}`")]
    UnsupportedScheme { endpoint: String, scheme: String },
    /// The scheme is not RFC 3986 syntax: an ASCII letter followed by ASCII letters, digits,
    /// `+`, `-` or `.`.
    #[error("resource endpoint `{endpoint}` has invalid scheme `{scheme}`")]
    InvalidScheme { endpoint: String, scheme: String },
    /// A [`CustomEndpoint`] was constructed with a built-in scheme.
    #[error("resource endpoint `{endpoint}` uses built-in scheme `{scheme}` as a custom scheme")]
    ReservedScheme { endpoint: String, scheme: String },
    #[error("resource `{resource_id}` has no {endpoint_type} endpoint")]
    EndpointTypeNotFound {
        resource_id: ResourceId,
        endpoint_type: &'static str,
    },
}

pub trait TypedResourceEndpoint: Sized {
    const TYPE_NAME: &'static str;

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self>;
}

/// Typed interpretation of a custom endpoint scheme.
///
/// Every `CustomEndpointScheme` is automatically a [`TypedResourceEndpoint`], so it can be
/// looked up with [`ResourceRecord::endpoint`](super::ResourceRecord::endpoint):
///
/// ```
/// use orion_control_plane::{CustomEndpointScheme, ResourceRecord};
///
/// struct FrameLeaseEndpoint {
///     socket_path: String,
/// }
///
/// impl CustomEndpointScheme for FrameLeaseEndpoint {
///     const SCHEME: &'static str = "styx-frame-lease+unix";
///
///     fn from_payload(payload: &str) -> Option<Self> {
///         Some(Self { socket_path: payload.to_owned() })
///     }
/// }
///
/// let resource = ResourceRecord::builder("resource.cam0", "camera.frames", "provider.helios")
///     .endpoint("styx-frame-lease+unix:///run/helios/cam0.sock")
///     .build();
/// let endpoint = resource.endpoint::<FrameLeaseEndpoint>().unwrap();
/// assert_eq!(endpoint.socket_path, "/run/helios/cam0.sock");
/// ```
///
/// `SCHEME` must not be a built-in scheme (those never parse as custom endpoints) and is
/// compared ASCII case-insensitively.
pub trait CustomEndpointScheme: Sized {
    const SCHEME: &'static str;
    /// Human-readable name used in [`ResourceEndpointError::EndpointTypeNotFound`].
    const TYPE_NAME: &'static str = Self::SCHEME;

    /// Interprets the part after `://`. Return `None` to treat the endpoint as not matching.
    fn from_payload(payload: &str) -> Option<Self>;

    fn from_custom(endpoint: &CustomEndpoint) -> Option<Self> {
        if endpoint.has_scheme(Self::SCHEME) {
            Self::from_payload(&endpoint.payload)
        } else {
            None
        }
    }

    /// Builds the endpoint string for `payload` under this scheme.
    fn endpoint_string(payload: impl fmt::Display) -> String {
        format!("{}://{payload}", Self::SCHEME)
    }
}

impl<T: CustomEndpointScheme> TypedResourceEndpoint for T {
    const TYPE_NAME: &'static str = <T as CustomEndpointScheme>::TYPE_NAME;

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        endpoint.as_custom().and_then(T::from_custom)
    }
}

/// RFC 3986 scheme syntax: `ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )`.
pub fn is_valid_endpoint_scheme(scheme: &str) -> bool {
    let mut bytes = scheme.bytes();
    bytes
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
}

impl ResourceEndpoint {
    /// Parses a `scheme://payload` endpoint string.
    ///
    /// Scheme matching is ASCII case-insensitive; custom schemes are stored lowercased.
    pub fn parse(endpoint: impl AsRef<str>) -> Result<Self, ResourceEndpointError> {
        let endpoint = endpoint.as_ref();
        let (scheme, payload) =
            endpoint
                .split_once("://")
                .ok_or_else(|| ResourceEndpointError::MissingScheme {
                    endpoint: endpoint.to_owned(),
                })?;
        if scheme.is_empty() {
            return Err(ResourceEndpointError::MissingScheme {
                endpoint: endpoint.to_owned(),
            });
        }
        if !is_valid_endpoint_scheme(scheme) {
            return Err(ResourceEndpointError::InvalidScheme {
                endpoint: endpoint.to_owned(),
                scheme: scheme.to_owned(),
            });
        }
        if payload.is_empty() {
            return Err(ResourceEndpointError::EmptyPayload {
                endpoint: endpoint.to_owned(),
            });
        }

        let scheme = scheme.to_ascii_lowercase();
        let payload = payload.to_owned();
        Ok(match scheme.as_str() {
            "shm" => Self::SharedMemory(SharedMemoryEndpoint { name: payload }),
            "ipc" => Self::Ipc(IpcEndpoint { address: payload }),
            "unix" => Self::Unix(UnixEndpoint { path: payload }),
            "tcp" => Self::Tcp(TcpEndpoint { address: payload }),
            "http" => Self::Http(HttpEndpoint {
                url: endpoint.to_owned(),
            }),
            "https" => Self::Https(HttpEndpoint {
                url: endpoint.to_owned(),
            }),
            _ => Self::Custom(CustomEndpoint { scheme, payload }),
        })
    }

    /// The endpoint scheme, e.g. `shm`, `https` or `styx-frame-lease+unix`.
    pub fn scheme(&self) -> &str {
        match self {
            Self::SharedMemory(_) => "shm",
            Self::Ipc(_) => "ipc",
            Self::Unix(_) => "unix",
            Self::Tcp(_) => "tcp",
            Self::Http(_) => "http",
            Self::Https(_) => "https",
            Self::Custom(custom) => &custom.scheme,
        }
    }

    /// The part after `://`.
    pub fn payload(&self) -> &str {
        match self {
            Self::SharedMemory(endpoint) => &endpoint.name,
            Self::Ipc(endpoint) => &endpoint.address,
            Self::Unix(endpoint) => &endpoint.path,
            Self::Tcp(endpoint) => &endpoint.address,
            Self::Http(endpoint) | Self::Https(endpoint) => endpoint
                .url
                .split_once("://")
                .map_or(endpoint.url.as_str(), |(_, payload)| payload),
            Self::Custom(endpoint) => &endpoint.payload,
        }
    }

    pub fn is_custom(&self) -> bool {
        matches!(self, Self::Custom(_))
    }

    pub fn as_custom(&self) -> Option<&CustomEndpoint> {
        match self {
            Self::Custom(endpoint) => Some(endpoint),
            _ => None,
        }
    }
}

/// Formats the endpoint back into its `scheme://payload` string. `parse(endpoint.to_string())`
/// yields an equal value. HTTP(S) endpoints print their stored URL verbatim.
impl fmt::Display for ResourceEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(endpoint) | Self::Https(endpoint) => f.write_str(&endpoint.url),
            Self::Custom(endpoint) => endpoint.fmt(f),
            _ => write!(f, "{}://{}", self.scheme(), self.payload()),
        }
    }
}

impl FromStr for ResourceEndpoint {
    type Err = ResourceEndpointError;

    fn from_str(endpoint: &str) -> Result<Self, Self::Err> {
        Self::parse(endpoint)
    }
}

impl From<CustomEndpoint> for ResourceEndpoint {
    fn from(endpoint: CustomEndpoint) -> Self {
        Self::Custom(endpoint)
    }
}

impl From<&ResourceEndpoint> for String {
    fn from(endpoint: &ResourceEndpoint) -> Self {
        endpoint.to_string()
    }
}

impl From<ResourceEndpoint> for String {
    fn from(endpoint: ResourceEndpoint) -> Self {
        endpoint.to_string()
    }
}

impl TypedResourceEndpoint for SharedMemoryEndpoint {
    const TYPE_NAME: &'static str = "shared memory";

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        match endpoint {
            ResourceEndpoint::SharedMemory(endpoint) => Some(endpoint.clone()),
            _ => None,
        }
    }
}

impl TypedResourceEndpoint for IpcEndpoint {
    const TYPE_NAME: &'static str = "ipc";

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        match endpoint {
            ResourceEndpoint::Ipc(endpoint) => Some(endpoint.clone()),
            _ => None,
        }
    }
}

impl TypedResourceEndpoint for UnixEndpoint {
    const TYPE_NAME: &'static str = "unix";

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        match endpoint {
            ResourceEndpoint::Unix(endpoint) => Some(endpoint.clone()),
            _ => None,
        }
    }
}

impl TypedResourceEndpoint for TcpEndpoint {
    const TYPE_NAME: &'static str = "tcp";

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        match endpoint {
            ResourceEndpoint::Tcp(endpoint) => Some(endpoint.clone()),
            _ => None,
        }
    }
}

impl TypedResourceEndpoint for HttpEndpoint {
    const TYPE_NAME: &'static str = "http/https";

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        match endpoint {
            ResourceEndpoint::Http(endpoint) | ResourceEndpoint::Https(endpoint) => {
                Some(endpoint.clone())
            }
            _ => None,
        }
    }
}

/// Matches any custom endpoint; prefer a [`CustomEndpointScheme`] type to match one scheme.
impl TypedResourceEndpoint for CustomEndpoint {
    const TYPE_NAME: &'static str = "custom";

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        endpoint.as_custom().cloned()
    }
}

#[cfg(test)]
#[path = "resource_endpoints_tests.rs"]
mod tests;
