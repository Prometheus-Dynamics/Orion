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
        match (
            self.ca_cert.as_deref(),
            self.client_cert.as_deref(),
            self.client_key.as_deref(),
        ) {
            (None, None, None) => HttpClient::try_new(self.http.clone()).map_err(|error| error.to_string()),
            (Some(ca_cert), client_cert, client_key) => {
                let client_identity = match (client_cert, client_key) {
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
                    _ => {
                        return Err(
                            "HTTP client TLS requires both --client-cert and --client-key"
                                .to_owned(),
                        )
                    }
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
                .map_err(|error| error.to_string())
            }
            (None, _, _) => Err(
                "HTTPS trust material requires --ca-cert; client auth does not replace server trust"
                    .to_owned(),
            ),
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
                    "{error}\nHTTP desired-state mutations require authenticated peer credentials; on-device admin writes should use local IPC."
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
}
