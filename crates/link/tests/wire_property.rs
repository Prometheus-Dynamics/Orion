//! Property tests: the hand-written device codec (`wire`) against postcard + serde over the full
//! Orion records, for random records. Every view must encode to exactly the record's bytes, and
//! every lease set postcard encodes must decode into matching views.

#![cfg(feature = "alloc")]

#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeMap;

use common::Rng;
use orion_control_plane::{
    AvailabilityState, HealthState, LeaseRecord, LeaseState, ProviderRecord, ResourceActionResult,
    ResourceActionStatus, ResourceCapability, ResourceConfigState, ResourceOwnershipMode,
    ResourceRecord, ResourceState, TypedConfigValue,
};
use orion_core::{
    CapabilityId, ExecutorId, NodeId, ProviderId, ResourceId, ResourceType, WorkloadId,
};
use orion_link::frame;
use orion_link::message::{StatusEntry, encode_leases, encode_provider_state, encode_status};
use orion_link::wire::{
    self, ActionResultView, ActionStatus, Availability, CapabilityView, ConfigField, Health,
    LeaseView, Leases, Ownership, ProviderStateView, ProviderView, RawVarint, Reader,
    ResourceStateView, ResourceView, StatusView, Value, kind,
};

const CASES: usize = 2_000;

fn text(rng: &mut Rng) -> String {
    const PIECES: [&str; 8] = ["a", "imu", ".", "-", "é", "€", "😀", "0"];
    let mut out = String::from("x");
    for _ in 0..rng.below(12) {
        out.push_str(PIECES[rng.below(PIECES.len())]);
    }
    out
}

fn maybe<T>(rng: &mut Rng, f: impl FnOnce(&mut Rng) -> T) -> Option<T> {
    rng.chance(50).then(|| f(rng))
}

fn many<T>(rng: &mut Rng, max: usize, mut f: impl FnMut(&mut Rng) -> T) -> Vec<T> {
    (0..rng.below(max + 1)).map(|_| f(rng)).collect()
}

fn value(rng: &mut Rng) -> TypedConfigValue {
    match rng.below(5) {
        0 => TypedConfigValue::Bool(rng.chance(50)),
        1 => TypedConfigValue::Int(rng.next_u64() as i64 >> rng.below(64)),
        2 => TypedConfigValue::UInt(rng.next_u64() >> rng.below(64)),
        3 => TypedConfigValue::String(text(rng)),
        _ => TypedConfigValue::Bytes(many(rng, 20, Rng::byte)),
    }
}

fn resource(rng: &mut Rng) -> ResourceRecord {
    ResourceRecord {
        resource_id: ResourceId::try_new(text(rng)).unwrap(),
        resource_type: ResourceType::try_new(text(rng)).unwrap(),
        provider_id: ProviderId::try_new(text(rng)).unwrap(),
        realized_by_executor_id: maybe(rng, |r| ExecutorId::try_new(text(r)).unwrap()),
        ownership_mode: match rng.below(3) {
            0 => ResourceOwnershipMode::Exclusive,
            1 => ResourceOwnershipMode::SharedRead,
            _ => ResourceOwnershipMode::SharedLimited {
                max_consumers: rng.next_u64() as u32 >> rng.below(32),
            },
        },
        realized_for_workload_id: maybe(rng, |r| WorkloadId::try_new(text(r)).unwrap()),
        source_resource_id: maybe(rng, |r| ResourceId::try_new(text(r)).unwrap()),
        source_workload_id: maybe(rng, |r| WorkloadId::try_new(text(r)).unwrap()),
        health: [
            HealthState::Healthy,
            HealthState::Degraded,
            HealthState::Failed,
            HealthState::Unknown,
        ][rng.below(4)],
        availability: [
            AvailabilityState::Available,
            AvailabilityState::Busy,
            AvailabilityState::Unavailable,
            AvailabilityState::Unknown,
        ][rng.below(4)],
        lease_state: lease_state(rng),
        capabilities: many(rng, 3, |r| ResourceCapability {
            capability_id: CapabilityId::try_new(text(r)).unwrap(),
            detail: maybe(r, text),
        }),
        labels: many(rng, 3, text),
        endpoints: many(rng, 3, text),
        state: maybe(rng, |r| ResourceState {
            observed_at_ms: r.next_u64() >> r.below(64),
            action_result: maybe(r, |r| ResourceActionResult {
                action_kind: text(r),
                status: match r.below(3) {
                    0 => ResourceActionStatus::Applied,
                    1 => ResourceActionStatus::Read,
                    _ => ResourceActionStatus::Failed,
                },
                data: maybe(r, value),
                error: maybe(r, text),
            }),
            config: maybe(r, |r| ResourceConfigState {
                payload: (0..r.below(5))
                    .map(|_| (text(r), value(r)))
                    .collect::<BTreeMap<_, _>>(),
            }),
        }),
    }
}

fn lease_state(rng: &mut Rng) -> LeaseState {
    [
        LeaseState::Unleased,
        LeaseState::Leased,
        LeaseState::Contended,
    ][rng.below(3)]
}

fn value_view(value: &TypedConfigValue) -> Value<'_> {
    match value {
        TypedConfigValue::Bool(v) => Value::Bool(*v),
        TypedConfigValue::Int(v) => Value::Int(*v),
        TypedConfigValue::UInt(v) => Value::UInt(*v),
        TypedConfigValue::String(v) => Value::String(v),
        TypedConfigValue::Bytes(v) => Value::Bytes(v),
    }
}

/// The borrowed pieces a `ResourceView` points into.
struct Parts<'r> {
    capabilities: Vec<CapabilityView<'r>>,
    labels: Vec<&'r str>,
    endpoints: Vec<&'r str>,
    config: Option<Vec<ConfigField<'r>>>,
}

fn parts(record: &ResourceRecord) -> Parts<'_> {
    Parts {
        capabilities: record
            .capabilities
            .iter()
            .map(|c| CapabilityView {
                capability_id: c.capability_id.as_str(),
                detail: c.detail.as_deref(),
            })
            .collect(),
        labels: record.labels.iter().map(String::as_str).collect(),
        endpoints: record.endpoints.iter().map(String::as_str).collect(),
        config: record
            .state
            .as_ref()
            .and_then(|s| s.config.as_ref())
            .map(|config| {
                config
                    .payload
                    .iter()
                    .map(|(key, value)| ConfigField {
                        key,
                        value: value_view(value),
                    })
                    .collect()
            }),
    }
}

fn state_view<'r>(
    record: &'r ResourceRecord,
    parts: &'r Parts<'r>,
) -> Option<ResourceStateView<'r>> {
    let state = record.state.as_ref()?;
    Some(ResourceStateView {
        observed_at_ms: state.observed_at_ms,
        action_result: state.action_result.as_ref().map(|a| ActionResultView {
            action_kind: &a.action_kind,
            status: match a.status {
                ResourceActionStatus::Applied => ActionStatus::Applied,
                ResourceActionStatus::Read => ActionStatus::Read,
                ResourceActionStatus::Failed => ActionStatus::Failed,
            },
            data: a.data.as_ref().map(value_view),
            error: a.error.as_deref(),
        }),
        config: parts.config.as_deref(),
    })
}

fn resource_view<'r>(
    record: &'r ResourceRecord,
    parts: &'r Parts<'r>,
    state: Option<&'r ResourceStateView<'r>>,
) -> ResourceView<'r> {
    ResourceView {
        resource_id: record.resource_id.as_str(),
        resource_type: record.resource_type.as_str(),
        provider_id: record.provider_id.as_str(),
        realized_by_executor_id: record.realized_by_executor_id.as_deref(),
        ownership_mode: match record.ownership_mode {
            ResourceOwnershipMode::Exclusive => Ownership::Exclusive,
            ResourceOwnershipMode::SharedRead => Ownership::SharedRead,
            ResourceOwnershipMode::SharedLimited { max_consumers } => {
                Ownership::SharedLimited { max_consumers }
            }
        },
        realized_for_workload_id: record.realized_for_workload_id.as_deref(),
        source_resource_id: record.source_resource_id.as_deref(),
        source_workload_id: record.source_workload_id.as_deref(),
        health: Health::from_index(record.health as u32).unwrap(),
        availability: Availability::from_index(record.availability as u32).unwrap(),
        lease_state: wire::LeaseState::from_index(record.lease_state as u32).unwrap(),
        capabilities: &parts.capabilities,
        labels: &parts.labels,
        endpoints: &parts.endpoints,
        state: state.map(|s| s as &dyn wire::StateBody),
    }
}

fn payload(frame_bytes: &[u8]) -> &[u8] {
    frame::decode(frame_bytes).unwrap().payload()
}

#[test]
fn provider_state_views_encode_like_the_records() {
    let mut rng = Rng::new(1);
    let mut record_buf = vec![0u8; 1 << 16];
    let mut view_buf = vec![0u8; 1 << 16];
    for case in 0..CASES {
        let provider = ProviderRecord {
            provider_id: ProviderId::try_new(text(&mut rng)).unwrap(),
            node_id: NodeId::try_new(text(&mut rng)).unwrap(),
            resource_types: many(&mut rng, 3, |r| ResourceType::try_new(text(r)).unwrap()),
        };
        let resources = many(&mut rng, 4, resource);
        let seq = rng.next_u64() as u16;
        let len = encode_provider_state(&provider, &resources, seq, &mut record_buf).unwrap();

        let types: Vec<&str> = provider.resource_types.iter().map(|t| t.as_str()).collect();
        let provider_view =
            ProviderView::new(&provider.provider_id, &provider.node_id).with_resource_types(&types);
        let all_parts: Vec<Parts<'_>> = resources.iter().map(parts).collect();
        let states: Vec<Option<ResourceStateView<'_>>> = resources
            .iter()
            .zip(&all_parts)
            .map(|(r, p)| state_view(r, p))
            .collect();
        let views: Vec<ResourceView<'_>> = resources
            .iter()
            .zip(&all_parts)
            .zip(&states)
            .map(|((r, p), s)| resource_view(r, p, s.as_ref()))
            .collect();
        let body = ProviderStateView {
            provider: &provider_view,
            resources: &views,
        };
        let view_len = wire::encode_frame(kind::PROVIDER_STATE, seq, &body, &mut view_buf).unwrap();
        assert_eq!(&view_buf[..view_len], &record_buf[..len], "case {case}");
        assert_eq!(wire::encoded_len(&body), len - frame::FRAME_OVERHEAD);

        // The records themselves are device bodies too (feature `alloc`), with the same bytes.
        let body = ProviderStateView {
            provider: &provider,
            resources: &resources,
        };
        let records_len =
            wire::encode_frame(kind::PROVIDER_STATE, seq, &body, &mut view_buf).unwrap();
        assert_eq!(&view_buf[..records_len], &record_buf[..len], "case {case}");
    }
}

#[test]
fn status_views_encode_like_the_entries() {
    let mut rng = Rng::new(2);
    let mut record_buf = vec![0u8; 1 << 14];
    let mut view_buf = vec![0u8; 1 << 14];
    for case in 0..CASES {
        let entries: Vec<StatusEntry> = many(&mut rng, 6, |r| {
            StatusEntry::new(text(r), value(r)).with_ttl_ms(r.next_u64() as u32 >> r.below(32))
        });
        let views: Vec<StatusView<'_>> = entries
            .iter()
            .map(|e| StatusView::new(&e.key, value_view(&e.value)).with_ttl_ms(e.ttl_ms))
            .collect();
        let len = encode_status(&entries, 7, &mut record_buf).unwrap();
        let view_len = wire::encode_frame(kind::STATUS, 7, &views[..], &mut view_buf).unwrap();
        assert_eq!(&view_buf[..view_len], &record_buf[..len], "case {case}");
    }
}

#[test]
fn lease_sets_decode_into_matching_views() {
    let mut rng = Rng::new(3);
    let mut buf = vec![0u8; 1 << 14];
    for case in 0..CASES {
        let leases: Vec<LeaseRecord> = many(&mut rng, 6, |r| {
            let mut lease = LeaseRecord::builder(ResourceId::try_new(text(r)).unwrap())
                .lease_state(lease_state(r));
            if r.chance(50) {
                lease = lease.holder_node(NodeId::try_new(text(r)).unwrap());
            }
            if r.chance(50) {
                lease = lease.holder_workload(WorkloadId::try_new(text(r)).unwrap());
            }
            lease.build()
        });
        let len = encode_leases(&leases, 9, &mut buf).unwrap();
        let views: Vec<LeaseView<'_>> = Leases::decode(payload(&buf[..len])).unwrap().collect();
        let expected: Vec<LeaseView<'_>> = leases
            .iter()
            .map(|l| LeaseView {
                resource_id: l.resource_id.as_str(),
                lease_state: wire::LeaseState::from_index(l.lease_state as u32).unwrap(),
                holder_node_id: l.holder_node_id.as_deref(),
                holder_workload_id: l.holder_workload_id.as_deref(),
            })
            .collect();
        assert_eq!(views, expected, "case {case}");
        // Every truncation is rejected.
        let body = payload(&buf[..len]);
        for cut in 0..body.len() {
            assert!(
                Leases::decode(&body[..cut]).is_err(),
                "case {case} cut {cut}"
            );
        }
    }
}

#[test]
fn ping_values_are_echoed_verbatim() {
    let mut rng = Rng::new(4);
    let mut buf = [0u8; 32];
    for _ in 0..CASES {
        let now_ms = rng.next_u64() >> rng.below(64);
        let len = wire::encode_frame(kind::PING, 1, &now_ms, &mut buf).unwrap();
        let body = payload(&buf[..len]).to_vec();
        let mut raw = RawVarint::default();
        raw.read(&mut Reader::new(&body)).unwrap();
        assert_eq!(raw.as_bytes(), &body[..]);
        assert_eq!(wire::decode_u64(raw.as_bytes()), Ok(now_ms));
    }
}
