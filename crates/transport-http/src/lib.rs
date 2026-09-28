//! HTTP transport adapter for Orion control-plane traffic.
//!
//! The crate is layered so consumers that only need the wire protocol can avoid the HTTP stack:
//!
//! - Always available: the request/response payload types, [`ControlRoute`], [`HttpCodec`],
//!   [`HttpTransportError`], and the [`HttpControlHandler`] / [`HttpService`] handler traits plus
//!   the in-process [`HttpTransport`]. These only depend on the Orion protocol crates.
//! - `transport` feature (default): the real network client/server (`HttpClient`, `HttpServer`)
//!   and TLS configuration, which pull in axum, hyper, reqwest and rustls.

mod codec;
mod error;
mod handler;
mod message;
#[cfg(feature = "transport")]
mod protocol;
mod route;
#[cfg(feature = "transport")]
mod tls;
#[cfg(feature = "transport")]
mod transport;

pub use codec::HttpCodec;
pub use error::{HttpRequestFailureKind, HttpTransportError};
pub use handler::{HttpControlHandler, HttpService, HttpTransport, METRICS_PATH};
pub use message::{HttpRequest, HttpRequestPayload, HttpResponse, HttpResponsePayload};
pub use route::{ControlRoute, HttpMethod};
#[cfg(feature = "transport")]
pub use tls::{
    HttpClientTlsConfig, HttpServerClientAuth, HttpServerTlsConfig, HttpTlsTrustProvider,
};
#[cfg(feature = "transport")]
pub use transport::{HttpClient, HttpServer};

#[cfg(all(test, feature = "transport"))]
mod tests;
