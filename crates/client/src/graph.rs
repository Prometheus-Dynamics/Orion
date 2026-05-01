use orion_control_plane::{
    ArtifactRecord, ConfigDecodeError, ResourceRecord, SharedMemoryEndpoint, StateSnapshot,
    TypedConfigValue, UnixEndpoint, WorkloadRecord,
};
use orion_core::{ArtifactId, ResourceId};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphPayload {
    Text(String),
    Bytes(Vec<u8>),
}

impl GraphPayload {
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text.as_str()),
            Self::Bytes(_) => None,
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Text(text) => text.into_bytes(),
            Self::Bytes(bytes) => bytes,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphReference {
    Inline(GraphPayload),
    ArtifactId(ArtifactId),
    ResourceId(ResourceId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphPayloadSource {
    Inline,
    Artifact(ArtifactId),
    Resource(ResourceId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedGraphPayload {
    pub source: GraphPayloadSource,
    pub payload: GraphPayload,
    pub content_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum GraphResolutionError {
    #[error("workload `{workload_id}` has no graph config")]
    MissingGraphConfig { workload_id: String },
    #[error("workload `{workload_id}` has invalid graph config: {source}")]
    InvalidGraphConfig {
        workload_id: String,
        #[source]
        source: ConfigDecodeError,
    },
    #[error("workload `{workload_id}` uses unsupported graph kind `{kind}`")]
    UnsupportedGraphKind { workload_id: String, kind: String },
    #[error("artifact `{artifact_id}` is missing from desired state")]
    MissingArtifact { artifact_id: ArtifactId },
    #[error("artifact `{artifact_id}` content is unavailable")]
    MissingArtifactContent { artifact_id: ArtifactId },
    #[error(
        "artifact `{artifact_id}` content type `{content_type}` is not supported for graph loading"
    )]
    UnsupportedArtifactContentType {
        artifact_id: ArtifactId,
        content_type: String,
    },
    #[error("resource `{resource_id}` is missing from state")]
    MissingResource { resource_id: ResourceId },
    #[error("resource `{resource_id}` has no graph payload in state")]
    MissingResourcePayload { resource_id: ResourceId },
    #[error(
        "resource `{resource_id}` only exposes endpoint-backed graph data; a custom reader is required"
    )]
    EndpointBackedResourceRequiresReader { resource_id: ResourceId },
    #[error("resource `{resource_id}` shared-memory graph payload could not be read: {message}")]
    SharedMemoryReadFailed {
        resource_id: ResourceId,
        message: String,
    },
    #[error("resource `{resource_id}` unix-path graph payload could not be read: {message}")]
    UnixReadFailed {
        resource_id: ResourceId,
        message: String,
    },
}

pub fn graph_reference_for_workload(
    workload: &WorkloadRecord,
) -> Result<GraphReference, GraphResolutionError> {
    let config =
        workload
            .config
            .as_ref()
            .ok_or_else(|| GraphResolutionError::MissingGraphConfig {
                workload_id: workload.workload_id.to_string(),
            })?;
    let view = config.view();
    let kind = view.required_string("graph.kind").map_err(|source| {
        GraphResolutionError::InvalidGraphConfig {
            workload_id: workload.workload_id.to_string(),
            source,
        }
    })?;

    match kind {
        "inline" => {
            if let Some(text) = view.optional_string("graph.inline").map_err(|source| {
                GraphResolutionError::InvalidGraphConfig {
                    workload_id: workload.workload_id.to_string(),
                    source,
                }
            })? {
                Ok(GraphReference::Inline(GraphPayload::Text(text.to_owned())))
            } else if let Some(bytes) = view.optional_bytes("graph.bytes").map_err(|source| {
                GraphResolutionError::InvalidGraphConfig {
                    workload_id: workload.workload_id.to_string(),
                    source,
                }
            })? {
                Ok(GraphReference::Inline(GraphPayload::Bytes(bytes.to_vec())))
            } else {
                Err(GraphResolutionError::InvalidGraphConfig {
                    workload_id: workload.workload_id.to_string(),
                    source: ConfigDecodeError::MissingField {
                        field: "graph.inline".to_owned(),
                        expected: "string",
                    },
                })
            }
        }
        "artifact" => Ok(GraphReference::ArtifactId(ArtifactId::new(
            view.required_string("graph.artifact_id")
                .map_err(|source| GraphResolutionError::InvalidGraphConfig {
                    workload_id: workload.workload_id.to_string(),
                    source,
                })?,
        ))),
        "resource" => Ok(GraphReference::ResourceId(ResourceId::new(
            view.required_string("graph.resource_id")
                .map_err(|source| GraphResolutionError::InvalidGraphConfig {
                    workload_id: workload.workload_id.to_string(),
                    source,
                })?,
        ))),
        other => Err(GraphResolutionError::UnsupportedGraphKind {
            workload_id: workload.workload_id.to_string(),
            kind: other.to_owned(),
        }),
    }
}

pub fn resolve_workload_graph<F>(
    workload: &WorkloadRecord,
    snapshot: &StateSnapshot,
    artifact_content: F,
) -> Result<ResolvedGraphPayload, GraphResolutionError>
where
    F: FnMut(&ArtifactId) -> Option<Vec<u8>>,
{
    let reference = graph_reference_for_workload(workload)?;
    resolve_graph_reference(&reference, snapshot, artifact_content)
}

pub fn resolve_graph_reference<F>(
    reference: &GraphReference,
    snapshot: &StateSnapshot,
    artifact_content: F,
) -> Result<ResolvedGraphPayload, GraphResolutionError>
where
    F: FnMut(&ArtifactId) -> Option<Vec<u8>>,
{
    match reference {
        GraphReference::Inline(payload) => Ok(ResolvedGraphPayload {
            source: GraphPayloadSource::Inline,
            payload: payload.clone(),
            content_type: Some("text/plain".to_owned()),
        }),
        GraphReference::ArtifactId(artifact_id) => {
            let artifact = snapshot
                .state
                .desired
                .artifacts
                .get(artifact_id)
                .ok_or_else(|| GraphResolutionError::MissingArtifact {
                    artifact_id: artifact_id.clone(),
                })?;
            resolve_artifact_graph(artifact, artifact_content)
        }
        GraphReference::ResourceId(resource_id) => {
            let resource = snapshot
                .state
                .observed
                .resources
                .get(resource_id)
                .or_else(|| snapshot.state.desired.resources.get(resource_id))
                .ok_or_else(|| GraphResolutionError::MissingResource {
                    resource_id: resource_id.clone(),
                })?;
            resolve_resource_graph(resource)
        }
    }
}

fn resolve_artifact_graph<F>(
    artifact: &ArtifactRecord,
    mut artifact_content: F,
) -> Result<ResolvedGraphPayload, GraphResolutionError>
where
    F: FnMut(&ArtifactId) -> Option<Vec<u8>>,
{
    if let Some(content_type) = artifact.content_type.as_deref()
        && !is_supported_graph_content_type(content_type)
    {
        return Err(GraphResolutionError::UnsupportedArtifactContentType {
            artifact_id: artifact.artifact_id.clone(),
            content_type: content_type.to_owned(),
        });
    }

    let bytes = artifact_content(&artifact.artifact_id).ok_or_else(|| {
        GraphResolutionError::MissingArtifactContent {
            artifact_id: artifact.artifact_id.clone(),
        }
    })?;
    Ok(ResolvedGraphPayload {
        source: GraphPayloadSource::Artifact(artifact.artifact_id.clone()),
        payload: graph_payload_from_bytes(bytes, artifact.content_type.as_deref()),
        content_type: artifact.content_type.clone(),
    })
}

fn resolve_resource_graph(
    resource: &ResourceRecord,
) -> Result<ResolvedGraphPayload, GraphResolutionError> {
    if let Some(state) = resource.state.as_ref() {
        if let Some(config) = state.config.as_ref() {
            if let Some(text) = config.view().optional_string("graph.inline").map_err(|_| {
                GraphResolutionError::MissingResourcePayload {
                    resource_id: resource.resource_id.clone(),
                }
            })? {
                return Ok(ResolvedGraphPayload {
                    source: GraphPayloadSource::Resource(resource.resource_id.clone()),
                    payload: GraphPayload::Text(text.to_owned()),
                    content_type: config
                        .view()
                        .optional_string("graph.content_type")
                        .ok()
                        .flatten()
                        .map(ToOwned::to_owned),
                });
            }
            if let Some(bytes) = config.view().optional_bytes("graph.bytes").map_err(|_| {
                GraphResolutionError::MissingResourcePayload {
                    resource_id: resource.resource_id.clone(),
                }
            })? {
                return Ok(ResolvedGraphPayload {
                    source: GraphPayloadSource::Resource(resource.resource_id.clone()),
                    payload: GraphPayload::Bytes(bytes.to_vec()),
                    content_type: config
                        .view()
                        .optional_string("graph.content_type")
                        .ok()
                        .flatten()
                        .map(ToOwned::to_owned),
                });
            }
        }

        if let Some(action_result) = state.action_result.as_ref()
            && let Some(value) = action_result.data.as_ref()
        {
            let payload = match value {
                TypedConfigValue::String(text) => Some(GraphPayload::Text(text.clone())),
                TypedConfigValue::Bytes(bytes) => Some(GraphPayload::Bytes(bytes.clone())),
                _ => None,
            };
            if let Some(payload) = payload {
                return Ok(ResolvedGraphPayload {
                    source: GraphPayloadSource::Resource(resource.resource_id.clone()),
                    payload,
                    content_type: None,
                });
            }
        }
    }

    if !resource.endpoints.is_empty() {
        if let Ok(shm) = resource.endpoint::<SharedMemoryEndpoint>() {
            let bytes =
                shm.read_bytes()
                    .map_err(|error| GraphResolutionError::SharedMemoryReadFailed {
                        resource_id: resource.resource_id.clone(),
                        message: error.to_string(),
                    })?;
            return Ok(ResolvedGraphPayload {
                source: GraphPayloadSource::Resource(resource.resource_id.clone()),
                payload: graph_payload_from_bytes(bytes, None),
                content_type: None,
            });
        }

        if let Ok(unix) = resource.endpoint::<UnixEndpoint>() {
            let bytes =
                unix.read_bytes()
                    .map_err(|error| GraphResolutionError::UnixReadFailed {
                        resource_id: resource.resource_id.clone(),
                        message: error.to_string(),
                    })?;
            return Ok(ResolvedGraphPayload {
                source: GraphPayloadSource::Resource(resource.resource_id.clone()),
                payload: graph_payload_from_bytes(bytes, None),
                content_type: None,
            });
        }

        return Err(GraphResolutionError::EndpointBackedResourceRequiresReader {
            resource_id: resource.resource_id.clone(),
        });
    }

    Err(GraphResolutionError::MissingResourcePayload {
        resource_id: resource.resource_id.clone(),
    })
}

fn graph_payload_from_bytes(bytes: Vec<u8>, content_type: Option<&str>) -> GraphPayload {
    if content_type.is_some_and(is_text_graph_content_type)
        && let Ok(text) = String::from_utf8(bytes.clone())
    {
        return GraphPayload::Text(text);
    }

    match String::from_utf8(bytes.clone()) {
        Ok(text) => GraphPayload::Text(text),
        Err(_) => GraphPayload::Bytes(bytes),
    }
}

fn is_supported_graph_content_type(content_type: &str) -> bool {
    is_text_graph_content_type(content_type) || content_type == "application/octet-stream"
}

fn is_text_graph_content_type(content_type: &str) -> bool {
    matches!(
        content_type,
        "application/json"
            | "application/yaml"
            | "application/x-yaml"
            | "application/toml"
            | "text/plain"
            | "text/yaml"
    ) || content_type.starts_with("text/")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use orion_control_plane::{
        AppliedClusterState, ArtifactRecord, ClusterStateEnvelope, DesiredClusterState,
        DesiredState, ObservedClusterState, ResourceConfigState, ResourceState, WorkloadConfig,
        WorkloadObservedState,
    };
    use orion_core::{ConfigSchemaId, RuntimeType, WorkloadId};

    use super::*;

    fn empty_snapshot() -> StateSnapshot {
        StateSnapshot {
            state: ClusterStateEnvelope {
                desired: DesiredClusterState::default(),
                observed: ObservedClusterState::default(),
                applied: AppliedClusterState::default(),
            },
        }
    }

    fn graph_workload() -> WorkloadRecord {
        WorkloadRecord::builder(
            WorkloadId::new("workload.graph"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.default"),
        )
        .desired_state(DesiredState::Running)
        .observed_state(WorkloadObservedState::Pending)
        .config(WorkloadConfig {
            schema_id: ConfigSchemaId::new("graph.workload.config.v1"),
            payload: BTreeMap::new(),
        })
        .build()
    }

    #[test]
    fn resolves_inline_graph_payload() {
        let mut workload = graph_workload();
        workload
            .config
            .as_mut()
            .expect("config should exist")
            .payload
            .extend([
                (
                    "graph.kind".to_owned(),
                    TypedConfigValue::String("inline".into()),
                ),
                (
                    "graph.inline".to_owned(),
                    TypedConfigValue::String("{\"nodes\":[]}".into()),
                ),
            ]);

        let payload = resolve_workload_graph(&workload, &empty_snapshot(), |_| None)
            .expect("inline graph should resolve");
        assert_eq!(payload.source, GraphPayloadSource::Inline);
        assert_eq!(payload.payload.as_text(), Some("{\"nodes\":[]}"));
    }

    #[test]
    fn resolves_artifact_backed_graph_payload() {
        let mut workload = graph_workload();
        workload
            .config
            .as_mut()
            .expect("config should exist")
            .payload
            .extend([
                (
                    "graph.kind".to_owned(),
                    TypedConfigValue::String("artifact".into()),
                ),
                (
                    "graph.artifact_id".to_owned(),
                    TypedConfigValue::String("artifact.graph".into()),
                ),
            ]);

        let mut snapshot = empty_snapshot();
        snapshot.state.desired.put_artifact(
            ArtifactRecord::builder("artifact.graph")
                .content_type("application/json")
                .build(),
        );

        let payload = resolve_workload_graph(&workload, &snapshot, |artifact_id| {
            (artifact_id == &ArtifactId::new("artifact.graph")).then(|| b"{\"graph\":1}".to_vec())
        })
        .expect("artifact graph should resolve");
        assert_eq!(
            payload.source,
            GraphPayloadSource::Artifact(ArtifactId::new("artifact.graph"))
        );
        assert_eq!(payload.payload.as_text(), Some("{\"graph\":1}"));
    }

    #[test]
    fn resolves_resource_backed_graph_payload_from_state_config() {
        let mut workload = graph_workload();
        workload
            .config
            .as_mut()
            .expect("config should exist")
            .payload
            .extend([
                (
                    "graph.kind".to_owned(),
                    TypedConfigValue::String("resource".into()),
                ),
                (
                    "graph.resource_id".to_owned(),
                    TypedConfigValue::String("resource.graph".into()),
                ),
            ]);

        let mut snapshot = empty_snapshot();
        snapshot.state.observed.put_resource(
            ResourceRecord::builder("resource.graph", "graph.payload", "provider.graph")
                .state(
                    ResourceState::new(1).with_config(
                        ResourceConfigState::new()
                            .field(
                                "graph.inline",
                                TypedConfigValue::String("{\"graph\":2}".into()),
                            )
                            .field(
                                "graph.content_type",
                                TypedConfigValue::String("application/json".into()),
                            ),
                    ),
                )
                .build(),
        );

        let payload = resolve_workload_graph(&workload, &snapshot, |_| None)
            .expect("resource graph should resolve");
        assert_eq!(
            payload.source,
            GraphPayloadSource::Resource(ResourceId::new("resource.graph"))
        );
        assert_eq!(payload.payload.as_text(), Some("{\"graph\":2}"));
    }

    #[test]
    fn resource_graph_with_non_shared_memory_endpoint_reports_custom_reader_requirement() {
        let mut snapshot = empty_snapshot();
        snapshot.state.observed.put_resource(
            ResourceRecord::builder("resource.graph", "graph.payload", "provider.graph")
                .endpoint("ipc://graph-control")
                .build(),
        );

        let error = resolve_graph_reference(
            &GraphReference::ResourceId(ResourceId::new("resource.graph")),
            &snapshot,
            |_| None,
        )
        .expect_err("endpoint-only resource should require custom reader");
        assert!(matches!(
            error,
            GraphResolutionError::EndpointBackedResourceRequiresReader { .. }
        ));
    }

    #[test]
    fn shared_memory_endpoint_backed_resource_graph_reads_payload() {
        let root =
            std::env::temp_dir().join(format!("orion-graph-shm-test-{}", std::process::id()));
        std::fs::create_dir_all(&root).expect("temp root should be creatable");
        let endpoint = SharedMemoryEndpoint {
            name: "graph-payload".to_owned(),
        };
        let path = endpoint.path_in(&root);
        std::fs::write(&path, b"{\"graph\":4}").expect("payload should be writable");

        let mut snapshot = empty_snapshot();
        snapshot.state.observed.put_resource(
            ResourceRecord::builder("resource.graph", "graph.payload", "provider.graph")
                .endpoint(format!("shm://{}", endpoint.name))
                .build(),
        );

        unsafe {
            std::env::set_var("ORION_SHM_ROOT", &root);
        }
        let payload = resolve_graph_reference(
            &GraphReference::ResourceId(ResourceId::new("resource.graph")),
            &snapshot,
            |_| None,
        );
        unsafe {
            std::env::remove_var("ORION_SHM_ROOT");
        }

        std::fs::remove_file(&path).expect("payload should be removable");
        std::fs::remove_dir(&root).expect("temp root should be removable");

        let payload = payload.expect("shared-memory payload should resolve");
        assert_eq!(payload.payload.as_text(), Some("{\"graph\":4}"));
    }

    #[test]
    fn unix_endpoint_backed_resource_graph_reads_payload() {
        let path =
            std::env::temp_dir().join(format!("orion-graph-unix-test-{}.txt", std::process::id()));
        std::fs::write(&path, b"{\"graph\":5}").expect("payload should be writable");

        let mut snapshot = empty_snapshot();
        snapshot.state.observed.put_resource(
            ResourceRecord::builder("resource.graph", "graph.payload", "provider.graph")
                .endpoint(format!("unix://{}", path.display()))
                .build(),
        );

        let payload = resolve_graph_reference(
            &GraphReference::ResourceId(ResourceId::new("resource.graph")),
            &snapshot,
            |_| None,
        )
        .expect("unix payload should resolve");

        std::fs::remove_file(&path).expect("payload should be removable");

        assert_eq!(payload.payload.as_text(), Some("{\"graph\":5}"));
    }
}
