use crate::{
    ControlRoute, HttpRequest, HttpRequestPayload, HttpResponse, HttpResponsePayload,
    HttpTransportError,
};
use orion_auth::AuthenticatedPeerRequest;
use orion_control_plane::{ControlMessage, ObservedStateUpdate};
use orion_core::{decode_from_slice_with, encode_to_vec};

#[derive(Clone, Debug, Default)]
pub struct HttpCodec;

impl HttpCodec {
    pub fn encode_request(
        &self,
        payload: &HttpRequestPayload,
    ) -> Result<HttpRequest, HttpTransportError> {
        let route = payload.route()?;
        let body = match payload {
            HttpRequestPayload::Control(message) => encode_to_vec(message.as_ref())
                .map_err(|err| HttpTransportError::EncodeRequest(err.to_string()))?,
            HttpRequestPayload::ObservedUpdate(update) => encode_to_vec(update)
                .map_err(|err| HttpTransportError::EncodeRequest(err.to_string()))?,
            HttpRequestPayload::AuthenticatedPeer(request) => encode_to_vec(request)
                .map_err(|err| HttpTransportError::EncodeRequest(err.to_string()))?,
        };

        Ok(HttpRequest {
            method: route.method(),
            path: route.path().to_owned(),
            body,
        })
    }

    pub fn decode_request(
        &self,
        request: &HttpRequest,
    ) -> Result<HttpRequestPayload, HttpTransportError> {
        match ControlRoute::parse(&request.path, request.method) {
            Some(ControlRoute::Hello)
            | Some(ControlRoute::Sync)
            | Some(ControlRoute::Snapshot)
            | Some(ControlRoute::Observability)
            | Some(ControlRoute::Mutations) => {
                if let Ok(request) = decode_from_slice_with::<AuthenticatedPeerRequest, _, _>(
                    &request.body,
                    HttpTransportError::DecodeRequest,
                ) {
                    return Ok(HttpRequestPayload::AuthenticatedPeer(request));
                }
                let message = decode_from_slice_with::<ControlMessage, _, _>(
                    &request.body,
                    HttpTransportError::DecodeRequest,
                )?;
                Ok(HttpRequestPayload::Control(Box::new(message)))
            }
            Some(ControlRoute::ObservedUpdate) => {
                if let Ok(request) = decode_from_slice_with::<AuthenticatedPeerRequest, _, _>(
                    &request.body,
                    HttpTransportError::DecodeRequest,
                ) {
                    return Ok(HttpRequestPayload::AuthenticatedPeer(request));
                }
                let update = decode_from_slice_with::<ObservedStateUpdate, _, _>(
                    &request.body,
                    HttpTransportError::DecodeRequest,
                )?;
                Ok(HttpRequestPayload::ObservedUpdate(update))
            }
            Some(ControlRoute::Health) | Some(ControlRoute::Readiness) => Err(
                HttpTransportError::UnsupportedMethod(request.method.as_str().to_owned()),
            ),
            None if request.method != crate::HttpMethod::Post => Err(
                HttpTransportError::UnsupportedMethod(request.method.as_str().to_owned()),
            ),
            None => Err(HttpTransportError::UnsupportedPath(request.path.clone())),
        }
    }

    pub fn encode_response(
        &self,
        payload: &HttpResponsePayload,
    ) -> Result<HttpResponse, HttpTransportError> {
        match payload {
            HttpResponsePayload::Accepted => Ok(HttpResponse {
                status: 202,
                body: Vec::new(),
            }),
            payload @ (HttpResponsePayload::Hello(_)
            | HttpResponsePayload::Summary(_)
            | HttpResponsePayload::Snapshot(_)
            | HttpResponsePayload::Mutations(_)
            | HttpResponsePayload::Observability(_)
            | HttpResponsePayload::Health(_)
            | HttpResponsePayload::Readiness(_)) => Ok(HttpResponse {
                status: 200,
                body: encode_to_vec(payload)
                    .map_err(|err| HttpTransportError::EncodeResponse(err.to_string()))?,
            }),
        }
    }

    pub fn decode_response(
        &self,
        response: &HttpResponse,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        match response.status {
            202 => Ok(HttpResponsePayload::Accepted),
            200 => decode_from_slice_with::<HttpResponsePayload, _, _>(
                &response.body,
                HttpTransportError::DecodeResponse,
            ),
            status => Err(HttpTransportError::UnexpectedStatus(status)),
        }
    }
}
