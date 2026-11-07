//! HTTP control client (`client` feature): HTTP/1.1 over tokio, with rustls for `https://`. No
//! reqwest and no hyper: a request is one write, a response one status line, its headers and a
//! `Content-Length` (or chunked) body, on a kept-alive connection.

use orion_core::CONTROL_PROTOCOL_HTTP_HEADER;
use orion_transport_common::{
    DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT, DEFAULT_HTTP_CLIENT_REQUEST_TIMEOUT,
};
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::{
    ControlRoute, HttpCodec, HttpMethod, HttpRequestFailureKind, HttpRequestPayload, HttpResponse,
    HttpResponsePayload, HttpTransportError,
    protocol::{check_response_protocol, local_protocol_header_value},
    tls::{HttpClientTlsConfig, client_tls_config, install_crypto_provider},
};

/// Idle connections kept per client.
const IDLE_MAX: usize = 4;
/// An idle connection older than this is not reused (the server may have closed it).
const IDLE_FOR: Duration = Duration::from_secs(30);
/// Largest response head (status line and headers).
const HEAD_MAX: usize = 16 * 1024;
/// Headers parsed in a response.
const HEADERS_MAX: usize = 32;
/// Largest response body.
const BODY_MAX: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct HttpClient {
    inner: Arc<Inner>,
}

struct Inner {
    target: Target,
    tls: Option<(TlsConnector, rustls::pki_types::ServerName<'static>)>,
    connect_timeout: Duration,
    request_timeout: Duration,
    idle: Mutex<Vec<(Conn, Instant)>>,
    codec: HttpCodec,
}

impl fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpClient")
            .field("base_url", &self.inner.target.base_url)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scheme {
    Http,
    Https,
}

/// A parsed base URL.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Target {
    scheme: Scheme,
    /// For the connect and the TLS server name (an IPv6 address without brackets).
    host: String,
    port: u16,
    /// The `Host` header.
    authority: String,
    /// Prepended to every route's path (no trailing `/`).
    prefix: String,
    base_url: String,
}

impl HttpClient {
    /// Builds an HTTP client without panicking on TLS failures.
    ///
    /// Connect timeout: `DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT` (500 ms). The timeout covers DNS,
    /// the TCP connect and the TLS handshake. Use [`HttpClient::try_new_with_connect_timeout`] to
    /// change it.
    pub fn try_new(base_url: impl Into<String>) -> Result<Self, HttpTransportError> {
        Self::build(
            base_url.into(),
            Trust::Platform,
            DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT,
        )
    }

    /// Like [`HttpClient::try_new`], with `connect_timeout` in place of the default (covers DNS,
    /// TCP connect and TLS handshake).
    pub fn try_new_with_connect_timeout(
        base_url: impl Into<String>,
        connect_timeout: Duration,
    ) -> Result<Self, HttpTransportError> {
        Self::build(base_url.into(), Trust::Platform, connect_timeout)
    }

    /// An `https://` client trusting only `tls.root_cert_pem`, with an optional client identity.
    pub fn with_tls(
        base_url: impl Into<String>,
        tls: HttpClientTlsConfig,
    ) -> Result<Self, HttpTransportError> {
        Self::build(
            base_url.into(),
            Trust::Explicit(tls),
            DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT,
        )
    }

    /// Like [`HttpClient::with_tls`], with `connect_timeout` in place of the default.
    pub fn with_tls_and_connect_timeout(
        base_url: impl Into<String>,
        tls: HttpClientTlsConfig,
        connect_timeout: Duration,
    ) -> Result<Self, HttpTransportError> {
        Self::build(base_url.into(), Trust::Explicit(tls), connect_timeout)
    }

    /// A client that accepts any server certificate (bootstrap before the node's CA is known).
    pub fn with_dangerous_tls(base_url: impl Into<String>) -> Result<Self, HttpTransportError> {
        Self::build(
            base_url.into(),
            Trust::Any,
            DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT,
        )
    }

    fn build(
        base_url: String,
        trust: Trust,
        connect_timeout: Duration,
    ) -> Result<Self, HttpTransportError> {
        install_crypto_provider();
        let target = Target::parse(&base_url)?;
        let tls = match (target.scheme, trust) {
            (Scheme::Http, Trust::Explicit(_)) => {
                return Err(HttpTransportError::Tls(
                    "plain HTTP targets do not use TLS configuration; remove TLS material or switch to https://"
                        .into(),
                ));
            }
            // Plain HTTP needs no trust store: stripped images without CA roots still work.
            (Scheme::Http, _) => None,
            (Scheme::Https, trust) => {
                let config = client_tls_config(trust.into())?;
                let name = rustls::pki_types::ServerName::try_from(target.host.clone())
                    .map_err(|err| HttpTransportError::InvalidBaseUrl(err.to_string()))?;
                Some((TlsConnector::from(Arc::new(config)), name))
            }
        };
        Ok(Self {
            inner: Arc::new(Inner {
                target,
                tls,
                connect_timeout,
                request_timeout: DEFAULT_HTTP_CLIENT_REQUEST_TIMEOUT,
                idle: Mutex::new(Vec::new()),
                codec: HttpCodec,
            }),
        })
    }

    pub async fn send(
        &self,
        payload: &HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        let request = self.inner.codec.encode_request(payload)?;
        self.exchange(request.method, &request.path, &request.body)
            .await
    }

    pub async fn get_route(
        &self,
        route: ControlRoute,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        if route.method() != HttpMethod::Get {
            return Err(HttpTransportError::UnsupportedMethod(
                route.method().as_str().to_owned(),
            ));
        }
        self.exchange(HttpMethod::Get, route.path(), &[]).await
    }

    async fn exchange(
        &self,
        method: HttpMethod,
        path: &str,
        body: &[u8],
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        let head = self.inner.request_head(method, path, body.len());
        let deadline = tokio::time::Instant::now() + self.inner.request_timeout;
        let response = tokio::time::timeout_at(deadline, self.inner.round_trip(&head, body))
            .await
            .map_err(|_| HttpTransportError::RequestFailed {
                kind: HttpRequestFailureKind::Timeout,
                message: format!(
                    "{}{path}: no response within {} ms",
                    self.inner.target.base_url,
                    self.inner.request_timeout.as_millis()
                ),
            })??;
        check_response_protocol(response.status, &response.headers)?;
        self.inner.codec.decode_response(&HttpResponse {
            status: response.status,
            body: response.body,
        })
    }
}

enum Trust {
    /// The platform's trust store (`https://` without explicit roots).
    Platform,
    Explicit(HttpClientTlsConfig),
    Any,
}

impl From<Trust> for crate::tls::ClientTrust {
    fn from(trust: Trust) -> Self {
        match trust {
            Trust::Platform => Self::Platform,
            Trust::Explicit(tls) => Self::Explicit(tls),
            Trust::Any => Self::Any,
        }
    }
}

struct Response {
    status: u16,
    headers: http::HeaderMap,
    body: Vec<u8>,
    keep_alive: bool,
}

impl Inner {
    fn request_head(&self, method: HttpMethod, path: &str, body_len: usize) -> Vec<u8> {
        let protocol = local_protocol_header_value();
        let mut head = Vec::with_capacity(160 + path.len());
        head.extend_from_slice(method.as_str().as_bytes());
        head.push(b' ');
        head.extend_from_slice(self.target.prefix.as_bytes());
        head.extend_from_slice(path.as_bytes());
        head.extend_from_slice(b" HTTP/1.1\r\nhost: ");
        head.extend_from_slice(self.target.authority.as_bytes());
        head.extend_from_slice(b"\r\n");
        head.extend_from_slice(CONTROL_PROTOCOL_HTTP_HEADER.as_bytes());
        head.extend_from_slice(b": ");
        head.extend_from_slice(protocol.as_bytes());
        head.extend_from_slice(b"\r\ncontent-length: ");
        head.extend_from_slice(body_len.to_string().as_bytes());
        head.extend_from_slice(b"\r\n\r\n");
        head
    }

    /// Sends the request on an idle connection, or a new one. A reused connection the server
    /// closed before answering (it closes idle connections whenever it likes) is retried once on
    /// a new one: nothing was read from it, so the server never served the request.
    async fn round_trip(&self, head: &[u8], body: &[u8]) -> Result<Response, HttpTransportError> {
        if let Some(mut conn) = self.take_idle() {
            match exchange_on(&mut conn, head, body).await {
                Ok(response) => {
                    self.keep(conn, &response);
                    return Ok(response);
                }
                Err(Failure::Closed) => {}
                Err(Failure::Io(error)) => return Err(self.request_error(error)),
                Err(Failure::Protocol(message)) => {
                    return Err(HttpTransportError::request_failed(message));
                }
            }
        }
        let mut conn = self.connect().await?;
        match exchange_on(&mut conn, head, body).await {
            Ok(response) => {
                self.keep(conn, &response);
                Ok(response)
            }
            Err(Failure::Closed) => Err(HttpTransportError::request_failed(format!(
                "{}: connection closed before the response",
                self.target.base_url
            ))),
            Err(Failure::Io(error)) => Err(self.request_error(error)),
            Err(Failure::Protocol(message)) => Err(HttpTransportError::request_failed(message)),
        }
    }

    fn take_idle(&self) -> Option<Conn> {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while let Some((conn, since)) = idle.pop() {
            if since.elapsed() < IDLE_FOR {
                return Some(conn);
            }
        }
        None
    }

    fn keep(&self, conn: Conn, response: &Response) {
        if !response.keep_alive {
            return;
        }
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if idle.len() < IDLE_MAX {
            idle.push((conn, Instant::now()));
        }
    }

    async fn connect(&self) -> Result<Conn, HttpTransportError> {
        let target = &self.target;
        let connect = async {
            let stream = TcpStream::connect((target.host.as_str(), target.port)).await?;
            stream.set_nodelay(true)?;
            match &self.tls {
                None => Ok(Conn::Plain(stream)),
                Some((connector, name)) => connector
                    .connect(name.clone(), stream)
                    .await
                    .map(|tls| Conn::Tls(Box::new(tls))),
            }
        };
        match tokio::time::timeout(self.connect_timeout, connect).await {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(error)) => Err(HttpTransportError::RequestFailed {
                kind: HttpRequestFailureKind::Connectivity,
                message: format!("{}: {error}", target.base_url),
            }),
            Err(_) => Err(HttpTransportError::RequestFailed {
                kind: HttpRequestFailureKind::Timeout,
                message: format!(
                    "{}: connect timed out after {} ms",
                    target.base_url,
                    self.connect_timeout.as_millis()
                ),
            }),
        }
    }

    fn request_error(&self, error: io::Error) -> HttpTransportError {
        let kind = match error.kind() {
            io::ErrorKind::TimedOut => HttpRequestFailureKind::Timeout,
            io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::UnexpectedEof => HttpRequestFailureKind::Connectivity,
            _ => HttpRequestFailureKind::Other,
        };
        HttpTransportError::RequestFailed {
            kind,
            message: format!("{}: {error}", self.target.base_url),
        }
    }
}

enum Failure {
    /// The connection closed before any byte of the response.
    Closed,
    Io(io::Error),
    Protocol(String),
}

impl From<io::Error> for Failure {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

async fn exchange_on(conn: &mut Conn, head: &[u8], body: &[u8]) -> Result<Response, Failure> {
    // One write for small requests (one TLS record, one segment).
    if body.len() <= 16 * 1024 {
        let mut request = Vec::with_capacity(head.len() + body.len());
        request.extend_from_slice(head);
        request.extend_from_slice(body);
        write_request(conn, &request).await?;
    } else {
        write_request(conn, head).await?;
        write_request(conn, body).await?;
    }
    conn.flush().await.map_err(closed_or_io)?;
    read_response(conn).await
}

async fn write_request(conn: &mut Conn, bytes: &[u8]) -> Result<(), Failure> {
    conn.write_all(bytes).await.map_err(closed_or_io)
}

/// A write to (or read from) a connection the server already closed fails with a reset or a
/// broken pipe.
fn is_closed(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
    )
}

fn closed_or_io(error: io::Error) -> Failure {
    if is_closed(&error) {
        Failure::Closed
    } else {
        Failure::Io(error)
    }
}

async fn read_response(conn: &mut Conn) -> Result<Response, Failure> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let (status, headers, head_len) = loop {
        let n = match conn.read(&mut chunk).await {
            Ok(0) if buf.is_empty() => return Err(Failure::Closed),
            Err(error) if buf.is_empty() && is_closed(&error) => return Err(Failure::Closed),
            Ok(0) => {
                return Err(Failure::Protocol(
                    "connection closed inside the response head".into(),
                ));
            }
            Ok(n) => n,
            Err(error) => return Err(Failure::Io(error)),
        };
        buf.extend_from_slice(&chunk[..n]);
        let mut parsed = [httparse::EMPTY_HEADER; HEADERS_MAX];
        let mut response = httparse::Response::new(&mut parsed);
        match response.parse(&buf) {
            Ok(httparse::Status::Complete(head_len)) => {
                let status = response.code.unwrap_or(0);
                let mut headers = http::HeaderMap::with_capacity(response.headers.len());
                for header in response.headers.iter() {
                    if let (Ok(name), Ok(value)) = (
                        http::HeaderName::from_bytes(header.name.as_bytes()),
                        http::HeaderValue::from_bytes(header.value),
                    ) {
                        headers.append(name, value);
                    }
                }
                break (status, headers, head_len);
            }
            Ok(httparse::Status::Partial) if buf.len() < HEAD_MAX => {}
            Ok(httparse::Status::Partial) => {
                return Err(Failure::Protocol("response head too large".into()));
            }
            Err(error) => return Err(Failure::Protocol(format!("malformed response: {error}"))),
        }
    };
    let mut rest = buf.split_off(head_len);
    let close = header_has(&headers, http::header::CONNECTION, "close");
    let chunked = header_has(&headers, http::header::TRANSFER_ENCODING, "chunked");
    let length = headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<usize>().ok());
    let no_body = status == 204 || status == 304 || (100..200).contains(&status);
    let (body, keep_alive) = if no_body {
        (Vec::new(), !close)
    } else if chunked {
        (read_chunked(conn, rest).await?, !close)
    } else if let Some(length) = length {
        if length > BODY_MAX {
            return Err(Failure::Protocol(format!(
                "response body of {length} bytes"
            )));
        }
        while rest.len() < length {
            let n = conn.read(&mut chunk).await?;
            if n == 0 {
                return Err(Failure::Protocol(
                    "connection closed inside the body".into(),
                ));
            }
            rest.extend_from_slice(&chunk[..n]);
        }
        rest.truncate(length);
        (rest, !close)
    } else {
        // Delimited by the end of the connection.
        loop {
            let n = conn.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            if rest.len() + n > BODY_MAX {
                return Err(Failure::Protocol("response body too large".into()));
            }
            rest.extend_from_slice(&chunk[..n]);
        }
        (rest, false)
    };
    Ok(Response {
        status,
        headers,
        body,
        keep_alive,
    })
}

fn header_has(headers: &http::HeaderMap, name: http::HeaderName, token: &str) -> bool {
    headers.get_all(name).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case(token))
        })
    })
}

/// A chunked body: `size[;ext]\r\n data \r\n` ... `0\r\n` trailers `\r\n`.
async fn read_chunked(conn: &mut Conn, mut buf: Vec<u8>) -> Result<Vec<u8>, Failure> {
    let mut body = Vec::new();
    let mut pos = 0usize;
    let mut chunk = [0u8; 4096];
    loop {
        // Size line.
        let line_end = loop {
            if let Some(i) = find_crlf(&buf[pos..]) {
                break pos + i;
            }
            fill(conn, &mut buf, &mut chunk).await?;
        };
        let line = std::str::from_utf8(&buf[pos..line_end])
            .map_err(|_| Failure::Protocol("malformed chunk size".into()))?;
        let size = usize::from_str_radix(line.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|_| Failure::Protocol("malformed chunk size".into()))?;
        pos = line_end + 2;
        if size == 0 {
            // Trailers, up to the empty line.
            loop {
                let end = loop {
                    if let Some(i) = find_crlf(&buf[pos..]) {
                        break pos + i;
                    }
                    fill(conn, &mut buf, &mut chunk).await?;
                };
                let empty = end == pos;
                pos = end + 2;
                if empty {
                    return Ok(body);
                }
            }
        }
        if body.len() + size > BODY_MAX {
            return Err(Failure::Protocol("response body too large".into()));
        }
        while buf.len() < pos + size + 2 {
            fill(conn, &mut buf, &mut chunk).await?;
        }
        body.extend_from_slice(&buf[pos..pos + size]);
        pos += size + 2;
    }
}

async fn fill(conn: &mut Conn, buf: &mut Vec<u8>, chunk: &mut [u8]) -> Result<(), Failure> {
    let n = conn.read(chunk).await?;
    if n == 0 {
        return Err(Failure::Protocol(
            "connection closed inside the body".into(),
        ));
    }
    buf.extend_from_slice(&chunk[..n]);
    Ok(())
}

fn find_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|pair| pair == b"\r\n")
}

impl Target {
    fn parse(base_url: &str) -> Result<Self, HttpTransportError> {
        let invalid =
            |reason: &str| HttpTransportError::InvalidBaseUrl(format!("{base_url}: {reason}"));
        let (scheme, rest) = base_url
            .split_once("://")
            .ok_or_else(|| invalid("expected http:// or https://"))?;
        let scheme = match scheme.to_ascii_lowercase().as_str() {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            other => {
                return Err(HttpTransportError::InvalidBaseUrl(format!(
                    "unsupported scheme `{other}`; expected http:// or https://"
                )));
            }
        };
        let (authority, path) = match rest.find(['/', '?', '#']) {
            Some(i) => rest.split_at(i),
            None => (rest, ""),
        };
        if path.contains(['?', '#']) {
            return Err(invalid("a base URL has no query or fragment"));
        }
        if authority.is_empty() || authority.contains('@') {
            return Err(invalid("expected a host"));
        }
        let default_port = match scheme {
            Scheme::Http => 80,
            Scheme::Https => 443,
        };
        let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
            let (host, after) = bracketed
                .split_once(']')
                .ok_or_else(|| invalid("unclosed IPv6 address"))?;
            host.parse::<std::net::Ipv6Addr>()
                .map_err(|_| invalid("malformed IPv6 address"))?;
            let port = match after.strip_prefix(':') {
                Some(port) => port.parse().map_err(|_| invalid("malformed port"))?,
                None if after.is_empty() => default_port,
                None => return Err(invalid("malformed authority")),
            };
            (host.to_owned(), port)
        } else {
            match authority.rsplit_once(':') {
                Some((host, port)) => (
                    host.to_owned(),
                    port.parse().map_err(|_| invalid("malformed port"))?,
                ),
                None => (authority.to_owned(), default_port),
            }
        };
        if host.is_empty() {
            return Err(invalid("expected a host"));
        }
        let host = host.to_ascii_lowercase();
        let authority = authority.to_ascii_lowercase();
        let prefix = path.trim_end_matches('/').to_owned();
        let scheme_name = match scheme {
            Scheme::Http => "http",
            Scheme::Https => "https",
        };
        let base_url = format!("{scheme_name}://{authority}{prefix}");
        Ok(Self {
            scheme,
            host,
            port,
            authority,
            prefix,
            base_url,
        })
    }
}

/// A plain or TLS connection.
enum Conn {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests;
