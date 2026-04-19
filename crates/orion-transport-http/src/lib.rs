mod codec;
mod error;
mod message;
mod route;
mod tls;
mod transport;

pub use codec::HttpCodec;
pub use error::{HttpRequestFailureKind, HttpTransportError};
pub use message::{HttpRequest, HttpRequestPayload, HttpResponse, HttpResponsePayload};
pub use route::{ControlRoute, HttpMethod};
pub use tls::{
    HttpClientTlsConfig, HttpServerClientAuth, HttpServerTlsConfig, HttpTlsTrustProvider,
};
pub use transport::{HttpClient, HttpControlHandler, HttpServer, HttpService, HttpTransport};

#[cfg(test)]
mod tests;
