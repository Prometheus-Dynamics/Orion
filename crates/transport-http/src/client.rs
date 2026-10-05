//! HTTP control client (`client` feature): reqwest over rustls, no server stack.

use orion_core::CONTROL_PROTOCOL_HTTP_HEADER;
use orion_transport_common::{
    DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT, DEFAULT_HTTP_CLIENT_REQUEST_TIMEOUT,
};
use reqwest::Method;

use crate::{
    ControlRoute, HttpCodec, HttpRequestPayload, HttpResponse, HttpResponsePayload,
    HttpTransportError,
    protocol::{check_response_protocol, local_protocol_header_value},
    tls::{HttpClientTlsConfig, install_crypto_provider},
};

#[derive(Clone, Debug)]
pub struct HttpClient {
    base_url: String,
    client: reqwest::Client,
    codec: HttpCodec,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BaseUrlScheme {
    Http,
    Https,
}

impl HttpClient {
    /// Builds an HTTP client without panicking on TLS/client-builder failures.
    pub fn try_new(base_url: impl Into<String>) -> Result<Self, HttpTransportError> {
        Self::with_optional_tls(base_url, None)
    }

    pub fn with_tls(
        base_url: impl Into<String>,
        tls: HttpClientTlsConfig,
    ) -> Result<Self, HttpTransportError> {
        Self::with_optional_tls(base_url, Some(tls))
    }

    pub fn with_dangerous_tls(base_url: impl Into<String>) -> Result<Self, HttpTransportError> {
        install_crypto_provider();
        let client = default_http_client_builder()
            .danger_accept_invalid_certs(true)
            .build()
            .map_err(|err| HttpTransportError::Tls(err.to_string()))?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            client,
            codec: HttpCodec,
        })
    }

    fn with_optional_tls(
        base_url: impl Into<String>,
        tls: Option<HttpClientTlsConfig>,
    ) -> Result<Self, HttpTransportError> {
        install_crypto_provider();
        let (base_url, scheme) = normalize_base_url(base_url.into())?;
        let mut builder = default_http_client_builder();
        if scheme == BaseUrlScheme::Http {
            if tls.is_some() {
                return Err(HttpTransportError::Tls(
                    "plain HTTP targets do not use TLS configuration; remove TLS material or switch to https://"
                        .into(),
                ));
            }
            // Reqwest initializes the rustls platform verifier at client-build time even for
            // plain-HTTP clients unless the client is configured to use only explicit roots.
            // Give the builder an empty explicit root set so stripped rootfs images do not need
            // a system trust store just to construct an http:// client.
            builder = builder.tls_certs_only(Vec::<reqwest::Certificate>::new());
        }
        if let Some(tls) = tls {
            let cert = reqwest::Certificate::from_pem(&tls.root_cert_pem)
                .map_err(|err| HttpTransportError::Tls(err.to_string()))?;
            // Use only the caller-provided roots instead of treating them as "extra" roots.
            // On minimal images this avoids reqwest's platform-verifier initialization path.
            builder = builder.tls_certs_only([cert]);
            if let (Some(client_cert_pem), Some(client_key_pem)) =
                (tls.client_cert_pem, tls.client_key_pem)
            {
                let mut identity_pem = client_cert_pem;
                if !identity_pem.ends_with(b"\n") {
                    identity_pem.push(b'\n');
                }
                identity_pem.extend_from_slice(&client_key_pem);
                let identity = reqwest::Identity::from_pem(&identity_pem)
                    .map_err(|err| HttpTransportError::Tls(err.to_string()))?;
                builder = builder.identity(identity);
            }
        }

        Ok(Self {
            base_url,
            client: builder
                .build()
                .map_err(|err| HttpTransportError::Tls(err.to_string()))?,
            codec: HttpCodec,
        })
    }

    pub async fn send(
        &self,
        payload: &HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        let request = self.codec.encode_request(payload)?;
        let method = match request.method {
            crate::HttpMethod::Post => Method::POST,
            crate::HttpMethod::Get => Method::GET,
        };

        let response = self
            .client
            .request(method, format!("{}{}", self.base_url, request.path))
            .header(CONTROL_PROTOCOL_HTTP_HEADER, local_protocol_header_value())
            .body(request.body)
            .send()
            .await
            .map_err(HttpTransportError::request_failed_from_reqwest)?;

        let status = response.status().as_u16();
        check_response_protocol(status, response.headers())?;
        let body = response
            .bytes()
            .await
            .map_err(HttpTransportError::request_failed_from_reqwest)?;

        self.codec.decode_response(&HttpResponse {
            status,
            body: body.to_vec(),
        })
    }

    pub async fn get_route(
        &self,
        route: ControlRoute,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        if route.method() != crate::HttpMethod::Get {
            return Err(HttpTransportError::UnsupportedMethod(
                route.method().as_str().to_owned(),
            ));
        }

        let response = self
            .client
            .request(Method::GET, format!("{}{}", self.base_url, route.path()))
            .header(CONTROL_PROTOCOL_HTTP_HEADER, local_protocol_header_value())
            .send()
            .await
            .map_err(HttpTransportError::request_failed_from_reqwest)?;

        let status = response.status().as_u16();
        check_response_protocol(status, response.headers())?;
        let body = response
            .bytes()
            .await
            .map_err(HttpTransportError::request_failed_from_reqwest)?;

        self.codec.decode_response(&HttpResponse {
            status,
            body: body.to_vec(),
        })
    }
}

fn default_http_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .connect_timeout(DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT)
        .timeout(DEFAULT_HTTP_CLIENT_REQUEST_TIMEOUT)
}

fn normalize_base_url(base_url: String) -> Result<(String, BaseUrlScheme), HttpTransportError> {
    let parsed = reqwest::Url::parse(&base_url)
        .map_err(|err| HttpTransportError::InvalidBaseUrl(err.to_string()))?;
    let scheme = match parsed.scheme() {
        "http" => BaseUrlScheme::Http,
        "https" => BaseUrlScheme::Https,
        other => {
            return Err(HttpTransportError::InvalidBaseUrl(format!(
                "unsupported scheme `{other}`; expected http:// or https://"
            )));
        }
    };
    Ok((parsed.as_str().trim_end_matches('/').to_owned(), scheme))
}
