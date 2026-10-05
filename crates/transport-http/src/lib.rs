//! HTTP transport adapter for Orion control-plane traffic.
//!
//! The crate is layered so consumers that only need the wire protocol can avoid the HTTP stack:
//!
//! - Always available: the request/response payload types, [`ControlRoute`], [`HttpCodec`],
//!   [`HttpTransportError`], and the [`HttpControlHandler`] / [`HttpService`] handler traits plus
//!   the in-process [`HttpTransport`]. These only depend on the Orion protocol crates.
//! - `client` feature: the network client ([`HttpClient`]) and [`HttpClientTlsConfig`], built on
//!   reqwest + rustls. It does not link the axum/hyper server stack, so CLIs such as `orionctl`
//!   enable only this.
//! - `server` feature: the network server ([`HttpServer`]) and server TLS / client-auth
//!   configuration, built on axum + hyper + tokio-rustls.
//! - `transport` feature (default): `client` + `server`, kept for compatibility.

#[cfg(feature = "client")]
mod client;
mod codec;
mod error;
mod handler;
mod message;
#[cfg(any(feature = "client", feature = "server"))]
mod protocol;
mod route;
#[cfg(feature = "server")]
mod server;
#[cfg(any(feature = "client", feature = "server"))]
mod tls;

#[cfg(feature = "client")]
pub use client::HttpClient;
pub use codec::HttpCodec;
pub use error::{HttpRequestFailureKind, HttpTransportError};
pub use handler::{HttpControlHandler, HttpService, HttpTransport, METRICS_PATH};
pub use message::{HttpRequest, HttpRequestPayload, HttpResponse, HttpResponsePayload};
pub use route::{ControlRoute, HttpMethod};
#[cfg(feature = "server")]
pub use server::HttpServer;
#[cfg(feature = "client")]
pub use tls::HttpClientTlsConfig;
#[cfg(feature = "server")]
pub use tls::{HttpServerClientAuth, HttpServerTlsConfig, HttpTlsTrustProvider};

#[cfg(all(test, feature = "client", feature = "server"))]
mod tests;
