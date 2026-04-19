use crate::{
    ControlRoute, HttpRequest, HttpRequestPayload, HttpResponse, HttpResponsePayload,
    HttpTransportError,
};
use orion_auth::{AuthenticatedPeerRequest, PeerRequestPayload};
use orion_control_plane::{ControlMessage, ObservedStateUpdate};
use orion_core::{decode_from_slice_with, encode_to_vec};

const REQUEST_KIND_CONTROL: u8 = 1;
const REQUEST_KIND_OBSERVED_UPDATE: u8 = 2;
const REQUEST_KIND_AUTHENTICATED_PEER: u8 = 3;

#[derive(Clone, Debug, Default)]
pub struct HttpCodec;

impl HttpCodec {
    fn encode_request_body(&self, kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut body = Vec::with_capacity(1 + payload.len());
        body.push(kind);
        body.extend_from_slice(payload);
        body
    }

    fn split_request_body<'a>(&self, body: &'a [u8]) -> Result<(u8, &'a [u8]), HttpTransportError> {
        let Some((kind, payload)) = body.split_first() else {
            return Err(HttpTransportError::DecodeRequest(
                "request body missing wire-format prefix".into(),
            ));
        };
        Ok((*kind, payload))
    }

    fn decode_authenticated_request_exact(
        &self,
        body: &[u8],
    ) -> Result<AuthenticatedPeerRequest, HttpTransportError> {
        let decoded = decode_from_slice_with::<AuthenticatedPeerRequest, _, _>(
            body,
            HttpTransportError::DecodeRequest,
        )?;
        let canonical = encode_to_vec(&decoded)
            .map_err(|err| HttpTransportError::DecodeRequest(err.to_string()))?;
        if canonical != body {
            return Err(HttpTransportError::DecodeRequest(
                "request body did not match expected payload type".into(),
            ));
        }
        Ok(decoded)
    }

    fn decode_control_message_exact(
        &self,
        body: &[u8],
    ) -> Result<ControlMessage, HttpTransportError> {
        let decoded = decode_from_slice_with::<ControlMessage, _, _>(
            body,
            HttpTransportError::DecodeRequest,
        )?;
        let canonical = encode_to_vec(&decoded)
            .map_err(|err| HttpTransportError::DecodeRequest(err.to_string()))?;
        if canonical != body {
            return Err(HttpTransportError::DecodeRequest(
                "request body did not match expected payload type".into(),
            ));
        }
        Ok(decoded)
    }

    fn decode_observed_update_exact(
        &self,
        body: &[u8],
    ) -> Result<ObservedStateUpdate, HttpTransportError> {
        let decoded = decode_from_slice_with::<ObservedStateUpdate, _, _>(
            body,
            HttpTransportError::DecodeRequest,
        )?;
        let canonical = encode_to_vec(&decoded)
            .map_err(|err| HttpTransportError::DecodeRequest(err.to_string()))?;
        if canonical != body {
            return Err(HttpTransportError::DecodeRequest(
                "request body did not match expected payload type".into(),
            ));
        }
        Ok(decoded)
    }

    fn authenticated_request_route(
        &self,
        request: &AuthenticatedPeerRequest,
    ) -> Result<ControlRoute, HttpTransportError> {
        match &request.payload {
            PeerRequestPayload::Control(message) => {
                HttpRequestPayload::Control(message.clone()).route()
            }
            PeerRequestPayload::ObservedUpdate(update) => {
                HttpRequestPayload::ObservedUpdate(update.clone()).route()
            }
        }
    }

    pub fn encode_request(
        &self,
        payload: &HttpRequestPayload,
    ) -> Result<HttpRequest, HttpTransportError> {
        let route = payload.route()?;
        let body = match payload {
            HttpRequestPayload::Control(message) => self.encode_request_body(
                REQUEST_KIND_CONTROL,
                &encode_to_vec(message.as_ref())
                    .map_err(|err| HttpTransportError::EncodeRequest(err.to_string()))?,
            ),
            HttpRequestPayload::ObservedUpdate(update) => self.encode_request_body(
                REQUEST_KIND_OBSERVED_UPDATE,
                &encode_to_vec(update)
                    .map_err(|err| HttpTransportError::EncodeRequest(err.to_string()))?,
            ),
            HttpRequestPayload::AuthenticatedPeer(request) => self.encode_request_body(
                REQUEST_KIND_AUTHENTICATED_PEER,
                &encode_to_vec(request)
                    .map_err(|err| HttpTransportError::EncodeRequest(err.to_string()))?,
            ),
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
        let (kind, body) = self.split_request_body(&request.body)?;
        match ControlRoute::parse(&request.path, request.method) {
            Some(ControlRoute::Hello)
            | Some(ControlRoute::Sync)
            | Some(ControlRoute::Snapshot)
            | Some(ControlRoute::Observability)
            | Some(ControlRoute::Mutations) => {
                let route = ControlRoute::parse(&request.path, request.method)
                    .expect("already matched a concrete control route");
                match kind {
                    REQUEST_KIND_AUTHENTICATED_PEER => {
                        let request = self.decode_authenticated_request_exact(body)?;
                        if self.authenticated_request_route(&request)? == route {
                            Ok(HttpRequestPayload::AuthenticatedPeer(request))
                        } else {
                            Err(HttpTransportError::DecodeRequest(
                                "request body did not match expected payload type".into(),
                            ))
                        }
                    }
                    REQUEST_KIND_CONTROL => {
                        let message = self.decode_control_message_exact(body)?;
                        if HttpRequestPayload::Control(Box::new(message.clone())).route()? != route
                        {
                            return Err(HttpTransportError::DecodeRequest(
                                "request body did not match expected payload type".into(),
                            ));
                        }
                        Ok(HttpRequestPayload::Control(Box::new(message)))
                    }
                    _ => Err(HttpTransportError::DecodeRequest(
                        "request body did not match expected payload type".into(),
                    )),
                }
            }
            Some(ControlRoute::ObservedUpdate) => match kind {
                REQUEST_KIND_AUTHENTICATED_PEER => {
                    let request = self.decode_authenticated_request_exact(body)?;
                    if self.authenticated_request_route(&request)? == ControlRoute::ObservedUpdate {
                        Ok(HttpRequestPayload::AuthenticatedPeer(request))
                    } else {
                        Err(HttpTransportError::DecodeRequest(
                            "request body did not match expected payload type".into(),
                        ))
                    }
                }
                REQUEST_KIND_OBSERVED_UPDATE => {
                    let update = self.decode_observed_update_exact(body)?;
                    Ok(HttpRequestPayload::ObservedUpdate(update))
                }
                _ => Err(HttpTransportError::DecodeRequest(
                    "request body did not match expected payload type".into(),
                )),
            },
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
