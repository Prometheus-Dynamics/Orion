use crate::RuntimeError;
use orion_control_plane::{
    ProviderRecord, ResourceOwnershipMode, ResourceRecord, WorkloadRequirement,
};
use orion_core::{ResourceId, WorkloadId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderSnapshot {
    pub provider: ProviderRecord,
    pub resources: Vec<ResourceRecord>,
}

impl ProviderSnapshot {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        for resource in &self.resources {
            if resource.provider_id != self.provider.provider_id {
                return Err(RuntimeError::ProviderResourceMismatch {
                    provider_id: self.provider.provider_id.clone(),
                    resource_id: resource.resource_id.clone(),
                });
            }
        }
        Ok(())
    }
}

pub trait ProviderIntegration {
    /// Returns the provider's control-plane identity record.
    ///
    /// This method is called synchronously from node runtime code and should not perform blocking
    /// I/O or expensive computation.
    fn provider_record(&self) -> ProviderRecord;

    /// Captures the provider's current local snapshot.
    ///
    /// This method runs inline during reconcile state collection. Implementations should treat it
    /// as a cheap, synchronous snapshot read and avoid filesystem, network, or long-running work.
    fn snapshot(&self) -> ProviderSnapshot;

    /// Validates whether a resource can satisfy a workload requirement.
    ///
    /// This method is part of synchronous reconcile validation and should remain CPU-bounded and
    /// side-effect free.
    fn validate_resource_claim(
        &self,
        _resource: &ResourceRecord,
        _workload_id: &WorkloadId,
        _requirement: &WorkloadRequirement,
        _existing_claims: u32,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
}

pub trait ProviderDescriptor {
    /// Returns the provider's control-plane identity record.
    fn provider_record(&self) -> ProviderRecord;

    /// Returns the provider's currently available resources.
    ///
    /// This method is used to build [`ProviderIntegration::snapshot`] synchronously and should
    /// avoid blocking I/O.
    fn resources(&self) -> Vec<ResourceRecord> {
        Vec::new()
    }

    /// Validates whether a resource can satisfy a workload requirement.
    ///
    /// Descriptor-based implementations should keep this logic deterministic and side-effect free.
    fn validate_resource_claim(
        &self,
        resource: &ResourceRecord,
        _workload_id: &WorkloadId,
        requirement: &WorkloadRequirement,
        existing_claims: u32,
    ) -> Result<(), RuntimeError> {
        validate_requirement_against_resource(resource, requirement, existing_claims)
    }
}

impl<T> ProviderIntegration for T
where
    T: ProviderDescriptor,
{
    fn provider_record(&self) -> ProviderRecord {
        ProviderDescriptor::provider_record(self)
    }

    fn snapshot(&self) -> ProviderSnapshot {
        ProviderSnapshot {
            provider: self.provider_record(),
            resources: self.resources(),
        }
    }

    fn validate_resource_claim(
        &self,
        resource: &ResourceRecord,
        workload_id: &WorkloadId,
        requirement: &WorkloadRequirement,
        existing_claims: u32,
    ) -> Result<(), RuntimeError> {
        ProviderDescriptor::validate_resource_claim(
            self,
            resource,
            workload_id,
            requirement,
            existing_claims,
        )
    }
}

pub(crate) fn validate_claim_capacity(
    resource_id: &ResourceId,
    ownership_mode: &ResourceOwnershipMode,
    existing_claims: u32,
) -> Result<(), RuntimeError> {
    match ownership_mode {
        ResourceOwnershipMode::Exclusive
        | ResourceOwnershipMode::ExclusiveOwnerPublishesDerived => {
            if existing_claims > 0 {
                return Err(RuntimeError::ResourceOwnershipConflict {
                    resource_id: resource_id.clone(),
                    mode: ownership_mode.clone(),
                });
            }
        }
        ResourceOwnershipMode::SharedRead => {}
        ResourceOwnershipMode::SharedLimited { max_consumers } => {
            if existing_claims >= *max_consumers {
                return Err(RuntimeError::ResourceConsumerLimitExceeded {
                    resource_id: resource_id.clone(),
                    max_consumers: *max_consumers,
                });
            }
        }
    }
    Ok(())
}

pub fn validate_requirement_against_resource(
    resource: &ResourceRecord,
    requirement: &WorkloadRequirement,
    existing_claims: u32,
) -> Result<(), RuntimeError> {
    if let Some(expected_mode) = requirement.ownership_mode.as_ref()
        && resource.ownership_mode != *expected_mode
    {
        return Err(RuntimeError::OwnershipModeMismatch {
            resource_id: resource.resource_id.clone(),
            expected: expected_mode.clone(),
            found: resource.ownership_mode.clone(),
        });
    }

    for capability_id in &requirement.required_capabilities {
        if !resource
            .capabilities
            .iter()
            .any(|capability| &capability.capability_id == capability_id)
        {
            return Err(RuntimeError::MissingResourceCapability {
                resource_id: resource.resource_id.clone(),
                capability_id: capability_id.clone(),
            });
        }
    }

    validate_claim_capacity(
        &resource.resource_id,
        &resource.ownership_mode,
        existing_claims,
    )
}
