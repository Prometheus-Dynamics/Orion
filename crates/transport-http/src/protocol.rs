//! Control-protocol version header for the HTTP control surface.
//!
//! HTTP bodies are rkyv archives (layout-exact), so every request from [`crate::HttpClient`] and
//! every response from [`crate::HttpServer`] carries [`CONTROL_PROTOCOL_HTTP_HEADER`] with
//! [`CONTROL_PROTOCOL_VERSION`]. Both sides compare it before decoding a body: the server answers
//! a skewed request with `409 Conflict` (still carrying its own header), and the client turns a
//! skewed response into [`HttpTransportError::ProtocolMismatch`].

#[cfg(feature = "server")]
use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use http::{HeaderMap, HeaderValue};
use orion_core::{CONTROL_PROTOCOL_HTTP_HEADER, CONTROL_PROTOCOL_VERSION, ControlProtocolMismatch};

use crate::HttpTransportError;

pub(crate) fn local_protocol_header_value() -> HeaderValue {
    HeaderValue::from(CONTROL_PROTOCOL_VERSION)
}

fn parse_protocol_header(headers: &HeaderMap) -> Option<Result<u16, ()>> {
    headers.get(CONTROL_PROTOCOL_HTTP_HEADER).map(|value| {
        value
            .to_str()
            .ok()
            .and_then(|value| value.trim().parse::<u16>().ok())
            .ok_or(())
    })
}

#[cfg(feature = "server")]
/// Server side: validates the version announced by a control request before its body is decoded.
pub(crate) fn check_request_protocol(headers: &HeaderMap) -> Result<(), HttpTransportError> {
    match parse_protocol_header(headers) {
        Some(Ok(remote)) => ControlProtocolMismatch::check(remote).map_err(Into::into),
        Some(Err(())) => Err(HttpTransportError::DecodeRequest(format!(
            "invalid {CONTROL_PROTOCOL_HTTP_HEADER} header"
        ))),
        None => Err(HttpTransportError::DecodeRequest(format!(
            "missing {CONTROL_PROTOCOL_HTTP_HEADER} header; Orion HTTP control clients must \
             announce their control protocol version"
        ))),
    }
}

#[cfg(feature = "client")]
/// Client side: validates the version announced by a response before its body is decoded.
///
/// Responses without the header are only accepted when they carry no archived body (non-200), so
/// a proxy error page still surfaces as an unexpected status rather than a protocol error.
pub(crate) fn check_response_protocol(
    status: u16,
    headers: &HeaderMap,
) -> Result<(), HttpTransportError> {
    match parse_protocol_header(headers) {
        Some(Ok(remote)) => ControlProtocolMismatch::check(remote).map_err(Into::into),
        Some(Err(())) => Err(HttpTransportError::DecodeResponse(format!(
            "invalid {CONTROL_PROTOCOL_HTTP_HEADER} response header"
        ))),
        None if status == 200 => Err(HttpTransportError::DecodeResponse(format!(
            "response is missing the {CONTROL_PROTOCOL_HTTP_HEADER} header; the server is not an \
             orion-node speaking control protocol v{CONTROL_PROTOCOL_VERSION}"
        ))),
        None => Ok(()),
    }
}

#[cfg(feature = "server")]
/// Response for a request whose protocol check failed.
pub(crate) fn protocol_rejection(error: &HttpTransportError) -> Response {
    let status = if matches!(error, HttpTransportError::ProtocolMismatch { .. }) {
        StatusCode::CONFLICT
    } else {
        StatusCode::BAD_REQUEST
    };
    (status, error.to_string()).into_response()
}

#[cfg(feature = "server")]
/// Middleware stamping every server response with this build's protocol version.
pub(crate) async fn stamp_protocol_header(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(CONTROL_PROTOCOL_HTTP_HEADER, local_protocol_header_value());
    response
}

#[cfg(all(test, feature = "client", feature = "server"))]
mod tests {
    use super::*;

    fn headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTROL_PROTOCOL_HTTP_HEADER,
            HeaderValue::from_str(value).expect("header value"),
        );
        headers
    }

    #[test]
    fn request_check_requires_matching_header() {
        assert_eq!(
            check_request_protocol(&headers(&CONTROL_PROTOCOL_VERSION.to_string())),
            Ok(())
        );
        assert_eq!(
            check_request_protocol(&headers("999")),
            Err(HttpTransportError::ProtocolMismatch {
                local: CONTROL_PROTOCOL_VERSION,
                remote: 999
            })
        );
        assert!(matches!(
            check_request_protocol(&HeaderMap::new()),
            Err(HttpTransportError::DecodeRequest(_))
        ));
        assert!(matches!(
            check_request_protocol(&headers("v2")),
            Err(HttpTransportError::DecodeRequest(_))
        ));
    }

    #[test]
    fn response_check_detects_skew_before_decode() {
        assert_eq!(
            check_response_protocol(409, &headers("999")),
            Err(HttpTransportError::ProtocolMismatch {
                local: CONTROL_PROTOCOL_VERSION,
                remote: 999
            })
        );
        assert!(matches!(
            check_response_protocol(200, &HeaderMap::new()),
            Err(HttpTransportError::DecodeResponse(_))
        ));
        assert_eq!(check_response_protocol(502, &HeaderMap::new()), Ok(()));
    }
}
