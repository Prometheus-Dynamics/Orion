use orion_client::{LocalControlPlaneClient, default_ipc_socket_path};
use orion_control_plane::{ControlMessage, StateSnapshot, SyncRequest};
use orion_core::{NodeId, Revision};
use orion_transport_http::{
    ControlRoute, HttpClient, HttpClientTlsConfig, HttpRequestPayload, HttpResponsePayload,
};

use crate::cli::{HttpTargetArgs, HttpTargetScheme, LocalControlArgs, StateQueryArgs};

pub(crate) trait HttpTargetExt {
    fn client(&self) -> Result<HttpClient, String>;
    fn scheme(&self) -> Result<HttpTargetScheme, String>;
    fn validate_tls_args(&self, scheme: HttpTargetScheme) -> Result<(), String>;
    fn map_client_construction_error(&self, scheme: HttpTargetScheme, error: &str) -> String;
}

impl HttpTargetExt for HttpTargetArgs {
    fn client(&self) -> Result<HttpClient, String> {
        let scheme = self.scheme()?;
        self.validate_tls_args(scheme)?;
        match self.ca_cert.as_deref() {
            None => HttpClient::try_new(self.http.clone())
                .map_err(|error| self.map_client_construction_error(scheme, &error.to_string())),
            Some(ca_cert) => {
                let client_identity =
                    match (self.client_cert.as_deref(), self.client_key.as_deref()) {
                        (Some(client_cert), Some(client_key)) => Some((
                            std::fs::read(client_cert).map_err(|error| {
                                format!(
                                    "failed to read client cert {}: {error}",
                                    client_cert.display()
                                )
                            })?,
                            std::fs::read(client_key).map_err(|error| {
                                format!(
                                    "failed to read client key {}: {error}",
                                    client_key.display()
                                )
                            })?,
                        )),
                        (None, None) => None,
                        _ => unreachable!("validate_tls_args enforces paired client identity"),
                    };

                HttpClient::with_tls(
                    self.http.clone(),
                    HttpClientTlsConfig {
                        root_cert_pem: std::fs::read(ca_cert).map_err(|error| {
                            format!("failed to read CA cert {}: {error}", ca_cert.display())
                        })?,
                        client_cert_pem: client_identity.as_ref().map(|(cert, _)| cert.clone()),
                        client_key_pem: client_identity.map(|(_, key)| key),
                    },
                )
                .map_err(|error| self.map_client_construction_error(scheme, &error.to_string()))
            }
        }
    }

    fn scheme(&self) -> Result<HttpTargetScheme, String> {
        let Some((scheme, _rest)) = self.http.split_once("://") else {
            return Err(format!(
                "invalid --http URL `{}`; expected http:// or https://",
                self.http
            ));
        };
        match scheme {
            "http" => Ok(HttpTargetScheme::Http),
            "https" => Ok(HttpTargetScheme::Https),
            other => Err(format!(
                "unsupported --http URL scheme `{other}`; expected http:// or https://"
            )),
        }
    }

    fn validate_tls_args(&self, scheme: HttpTargetScheme) -> Result<(), String> {
        if self.client_cert.is_some() ^ self.client_key.is_some() {
            return Err("HTTP client TLS requires both --client-cert and --client-key".to_owned());
        }

        if scheme == HttpTargetScheme::Http {
            if self.ca_cert.is_some() || self.client_cert.is_some() || self.client_key.is_some() {
                return Err(
                    "plain http:// targets do not use TLS material; remove --ca-cert/--client-cert/--client-key or switch to https://"
                        .to_owned(),
                );
            }
            return Ok(());
        }

        if self.ca_cert.is_none() && (self.client_cert.is_some() || self.client_key.is_some()) {
            return Err(
                "HTTPS trust material requires --ca-cert; client auth does not replace server trust"
                    .to_owned(),
            );
        }

        Ok(())
    }

    fn map_client_construction_error(&self, scheme: HttpTargetScheme, error: &str) -> String {
        if scheme == HttpTargetScheme::Https
            && self.ca_cert.is_none()
            && error == "failed to configure HTTP TLS: builder error"
        {
            return "failed to initialize HTTPS trust verification from the system trust store; this image may be missing CA roots. Provide --ca-cert for the Orion server CA, or install a system CA bundle."
                .to_owned();
        }

        if scheme == HttpTargetScheme::Http
            && error == "failed to configure HTTP TLS: builder error"
        {
            return "failed to initialize a plain HTTP client; this indicates a runtime/build issue, not missing TLS credentials."
                .to_owned();
        }

        error.to_owned()
    }
}

impl HttpTargetArgs {
    pub(crate) async fn get_route(
        &self,
        route: ControlRoute,
    ) -> Result<HttpResponsePayload, String> {
        self.client()?
            .get_route(route)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn send_control(
        &self,
        message: ControlMessage,
    ) -> Result<HttpResponsePayload, String> {
        self.client()?
            .send(&HttpRequestPayload::Control(Box::new(message)))
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn fetch_snapshot(&self) -> Result<StateSnapshot, String> {
        match self
            .send_control(ControlMessage::SyncRequest(SyncRequest {
                node_id: NodeId::new(self.client_name.clone()),
                desired_revision: Revision::new(u64::MAX),
                desired_fingerprint: 0,
                desired_summary: None,
                sections: Vec::new(),
                object_selectors: Vec::new(),
            }))
            .await?
        {
            HttpResponsePayload::Snapshot(snapshot) => Ok(snapshot),
            other => Err(format!("expected snapshot response, got {other:?}")),
        }
    }
}

impl StateQueryArgs {
    pub(crate) async fn fetch_snapshot(&self) -> Result<StateSnapshot, String> {
        if self.http.is_some() {
            return self.http_target()?.fetch_snapshot().await;
        }
        self.local_client()?
            .fetch_state_snapshot()
            .await
            .map_err(|error| error.to_string())
    }

    fn http_target(&self) -> Result<HttpTargetArgs, String> {
        let http = self
            .http
            .clone()
            .ok_or_else(|| "HTTP target was not configured".to_owned())?;
        if self.socket.is_some() {
            return Err("choose either --http or --socket, not both".to_owned());
        }
        Ok(HttpTargetArgs {
            http,
            ca_cert: self.ca_cert.clone(),
            client_cert: self.client_cert.clone(),
            client_key: self.client_key.clone(),
            client_name: self.client_name.clone(),
            output: self.output,
        })
    }

    fn local_client(&self) -> Result<LocalControlPlaneClient, String> {
        if self.http.is_some() && self.socket.is_some() {
            return Err("choose either --http or --socket, not both".to_owned());
        }
        if self.http.is_none()
            && (self.ca_cert.is_some() || self.client_cert.is_some() || self.client_key.is_some())
        {
            return Err("--ca-cert/--client-cert/--client-key require --http".to_owned());
        }
        let socket_path = self.socket.clone().unwrap_or_else(default_ipc_socket_path);
        LocalControlPlaneClient::connect_at(&socket_path, self.client_name.clone())
            .map_err(|error| error.to_string())
    }
}

impl LocalControlArgs {
    pub(crate) fn client(&self) -> Result<LocalControlPlaneClient, String> {
        LocalControlPlaneClient::connect_at(&self.socket, self.client_name.clone())
            .map_err(|error| error.to_string())
    }
}
