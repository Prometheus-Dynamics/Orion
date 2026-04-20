use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use orion_control_plane::{
    ControlMessage, DesiredState, DesiredStateSectionFingerprints, MutationBatch, PeerHello,
    RestartPolicy, StateSnapshot, SyncRequest,
};
use orion_core::{NodeId, Revision};
use orion_transport_http::{
    ControlRoute, HttpClient, HttpClientTlsConfig, HttpRequestPayload, HttpResponsePayload,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HttpTargetScheme {
    Http,
    Https,
}

#[derive(Parser, Debug)]
#[command(name = "orionctl")]
#[command(about = "Operator CLI for the Orion daemon.")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    Hello(HttpTargetArgs),
    Health(HttpTargetArgs),
    Readiness(HttpTargetArgs),
    Observability(HttpTargetArgs),
    Snapshot(SnapshotArgs),
    Mutate {
        #[command(subcommand)]
        command: MutateCommand,
    },
    Trust {
        #[command(subcommand)]
        command: TrustCommand,
    },
    WatchState(WatchStateArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum MutateCommand {
    PutArtifact(PutArtifactArgs),
    PutWorkload(PutWorkloadArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum TrustCommand {
    List(LocalControlArgs),
    Enroll(TrustEnrollArgs),
    Revoke(TrustNodeArgs),
    ReplaceKey(TrustReplaceKeyArgs),
    RotateHttpTls(LocalControlArgs),
}

#[derive(Args, Debug)]
pub(crate) struct HttpTargetArgs {
    #[arg(long)]
    pub(crate) http: String,
    #[arg(long)]
    pub(crate) ca_cert: Option<PathBuf>,
    #[arg(long)]
    pub(crate) client_cert: Option<PathBuf>,
    #[arg(long)]
    pub(crate) client_key: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct SnapshotArgs {
    #[command(flatten)]
    pub(crate) target: HttpTargetArgs,
    #[arg(long, default_value = "orionctl.snapshot")]
    pub(crate) peer_node_id: String,
}

#[derive(Args, Debug)]
pub(crate) struct PutArtifactArgs {
    #[command(flatten)]
    pub(crate) target: HttpTargetArgs,
    #[arg(long)]
    pub(crate) artifact_id: String,
    #[arg(long)]
    pub(crate) content_type: Option<String>,
    #[arg(long)]
    pub(crate) size_bytes: Option<u64>,
}

#[derive(Args, Debug)]
pub(crate) struct PutWorkloadArgs {
    #[command(flatten)]
    pub(crate) target: HttpTargetArgs,
    #[arg(long)]
    pub(crate) workload_id: String,
    #[arg(long)]
    pub(crate) runtime_type: String,
    #[arg(long)]
    pub(crate) artifact_id: String,
    #[arg(long)]
    pub(crate) assigned_node: Option<String>,
    #[arg(long, value_enum, default_value_t = CliDesiredState::Stopped)]
    pub(crate) desired_state: CliDesiredState,
    #[arg(long, value_enum, default_value_t = CliRestartPolicy::Never)]
    pub(crate) restart_policy: CliRestartPolicy,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum CliDesiredState {
    Running,
    Stopped,
}

impl From<CliDesiredState> for DesiredState {
    fn from(value: CliDesiredState) -> Self {
        match value {
            CliDesiredState::Running => Self::Running,
            CliDesiredState::Stopped => Self::Stopped,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum CliRestartPolicy {
    Never,
    Always,
    #[value(alias = "on_failure")]
    OnFailure,
}

impl From<CliRestartPolicy> for RestartPolicy {
    fn from(value: CliRestartPolicy) -> Self {
        match value {
            CliRestartPolicy::Never => Self::Never,
            CliRestartPolicy::Always => Self::Always,
            CliRestartPolicy::OnFailure => Self::OnFailure,
        }
    }
}

#[derive(Args, Debug)]
pub(crate) struct WatchStateArgs {
    #[arg(long)]
    pub(crate) socket: PathBuf,
    #[arg(long)]
    pub(crate) client_name: String,
    #[arg(long, default_value_t = 0)]
    pub(crate) desired_revision: u64,
    #[arg(long, default_value_t = 1)]
    pub(crate) batches: u32,
}

#[derive(Args, Debug)]
pub(crate) struct LocalControlArgs {
    #[arg(long)]
    pub(crate) socket: PathBuf,
    #[arg(long)]
    pub(crate) client_name: String,
}

#[derive(Args, Debug)]
pub(crate) struct TrustEnrollArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    #[arg(long)]
    pub(crate) node_id: String,
    #[arg(long)]
    pub(crate) base_url: String,
    #[arg(long)]
    pub(crate) public_key: Option<String>,
    #[arg(long)]
    pub(crate) tls_root_cert: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct TrustNodeArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    #[arg(long)]
    pub(crate) node_id: String,
}

#[derive(Args, Debug)]
pub(crate) struct TrustReplaceKeyArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    #[arg(long)]
    pub(crate) node_id: String,
    #[arg(long)]
    pub(crate) public_key: String,
}

impl HttpTargetArgs {
    pub(crate) fn client(&self) -> Result<HttpClient, String> {
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

    pub(crate) async fn fetch_snapshot(&self, peer_node_id: &str) -> Result<StateSnapshot, String> {
        match self
            .send_control(ControlMessage::SyncRequest(SyncRequest {
                node_id: NodeId::new(peer_node_id),
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

    pub(crate) async fn apply_batch(&self, batch: MutationBatch) -> Result<(), String> {
        match self.send_control(ControlMessage::Mutations(batch)).await.map_err(|error| {
            if error.contains("authenticated peer identity is required") {
                format!(
                    "{error}\nDesired-state mutations over HTTP require an authenticated peer identity. This request was sent without one; if you are on-device, use local IPC instead."
                )
            } else {
                error
            }
        })? {
            HttpResponsePayload::Accepted | HttpResponsePayload::Mutations(_) => Ok(()),
            other => Err(format!(
                "expected accepted mutation response, got {other:?}"
            )),
        }
    }

    pub(crate) async fn hello(&self) -> Result<HttpResponsePayload, String> {
        self.send_control(ControlMessage::Hello(PeerHello {
            node_id: NodeId::new("orionctl.hello"),
            desired_revision: Revision::ZERO,
            desired_fingerprint: 0,
            desired_section_fingerprints: DesiredStateSectionFingerprints {
                nodes: 0,
                artifacts: 0,
                workloads: 0,
                resources: 0,
                providers: 0,
                executors: 0,
                leases: 0,
            },
            observed_revision: Revision::ZERO,
            applied_revision: Revision::ZERO,
            transport_binding_version: None,
            transport_binding_public_key: None,
            transport_tls_cert_pem: None,
            transport_binding_signature: None,
        }))
        .await
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

#[cfg(test)]
mod tests {
    use super::{HttpTargetArgs, HttpTargetScheme};

    fn target(http: &str) -> HttpTargetArgs {
        HttpTargetArgs {
            http: http.to_owned(),
            ca_cert: None,
            client_cert: None,
            client_key: None,
        }
    }

    #[test]
    fn plain_http_targets_reject_tls_material_early() {
        let mut args = target("http://127.0.0.1:9100");
        args.ca_cert = Some("ca.pem".into());
        let error = args
            .validate_tls_args(HttpTargetScheme::Http)
            .expect_err("plain HTTP should reject TLS material");
        assert!(error.contains("plain http:// targets do not use TLS material"));
    }

    #[test]
    fn client_identity_requires_matching_key() {
        let mut args = target("https://127.0.0.1:9100");
        args.client_cert = Some("client-cert.pem".into());
        let error = args
            .validate_tls_args(HttpTargetScheme::Https)
            .expect_err("partial client identity should fail");
        assert!(error.contains("requires both --client-cert and --client-key"));
    }

    #[test]
    fn https_builder_error_without_ca_cert_becomes_trust_guidance() {
        let args = target("https://127.0.0.1:9100");
        let error = args.map_client_construction_error(
            HttpTargetScheme::Https,
            "failed to configure HTTP TLS: builder error",
        );
        assert!(error.contains("missing CA roots"));
        assert!(error.contains("--ca-cert"));
    }
}
