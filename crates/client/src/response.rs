use orion_control_plane::{
    ClientEvent, ControlMessage, ExecutorWorkloadQuery, LeaseRecord, MaintenanceStatus,
    NodeObservabilitySnapshot, PeerTrustSnapshot, ProviderLeaseQuery, StateSnapshot,
    WorkloadRecord,
};

use crate::ClientError;

pub(crate) fn expect_accepted(message: ControlMessage) -> Result<(), ClientError> {
    match message {
        ControlMessage::Accepted => Ok(()),
        ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
        _ => Err(ClientError::NoMessageAvailable),
    }
}

pub(crate) fn expect_client_events(
    message: ControlMessage,
) -> Result<Vec<ClientEvent>, ClientError> {
    match message {
        ControlMessage::ClientEvents(events) => Ok(events),
        ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
        _ => Err(ClientError::NoMessageAvailable),
    }
}

pub(crate) fn expect_state_snapshot(message: ControlMessage) -> Result<StateSnapshot, ClientError> {
    match message {
        ControlMessage::Snapshot(snapshot) => Ok(snapshot),
        ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
        _ => Err(ClientError::NoMessageAvailable),
    }
}

pub(crate) fn expect_peer_trust(message: ControlMessage) -> Result<PeerTrustSnapshot, ClientError> {
    match message {
        ControlMessage::PeerTrust(snapshot) => Ok(snapshot),
        ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
        _ => Err(ClientError::NoMessageAvailable),
    }
}

pub(crate) fn expect_observability(
    message: ControlMessage,
) -> Result<NodeObservabilitySnapshot, ClientError> {
    match message {
        ControlMessage::Observability(snapshot) => Ok(*snapshot),
        ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
        _ => Err(ClientError::NoMessageAvailable),
    }
}

pub(crate) fn expect_maintenance_status(
    message: ControlMessage,
) -> Result<MaintenanceStatus, ClientError> {
    match message {
        ControlMessage::MaintenanceStatus(status) => Ok(status),
        ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
        _ => Err(ClientError::NoMessageAvailable),
    }
}

pub(crate) fn expect_provider_leases(
    message: ControlMessage,
) -> Result<Vec<LeaseRecord>, ClientError> {
    match message {
        ControlMessage::ProviderLeases(leases) => Ok(leases),
        ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
        _ => Err(ClientError::NoMessageAvailable),
    }
}

pub(crate) fn expect_executor_workloads(
    message: ControlMessage,
) -> Result<Vec<WorkloadRecord>, ClientError> {
    match message {
        ControlMessage::ExecutorWorkloads(workloads) => Ok(workloads),
        ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
        _ => Err(ClientError::NoMessageAvailable),
    }
}

pub(crate) fn provider_lease_query(provider_id: orion_core::ProviderId) -> ControlMessage {
    ControlMessage::QueryProviderLeases(ProviderLeaseQuery { provider_id })
}

pub(crate) fn executor_workload_query(executor_id: orion_core::ExecutorId) -> ControlMessage {
    ControlMessage::QueryExecutorWorkloads(ExecutorWorkloadQuery { executor_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_helper_preserves_rejection_reason() {
        let err = expect_accepted(ControlMessage::Rejected("nope".into()))
            .expect_err("rejection should be surfaced");

        assert!(matches!(err, ClientError::Rejected(reason) if reason == "nope"));
    }

    #[test]
    fn typed_helpers_reject_unexpected_messages() {
        let err = expect_peer_trust(ControlMessage::Accepted)
            .expect_err("unexpected response should be rejected");

        assert!(matches!(err, ClientError::NoMessageAvailable));
    }
}
