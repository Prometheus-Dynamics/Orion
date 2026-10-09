//! `TypedConfigValue::F64` on the control wire (rkyv), in JSON, and in its equality rules.

use orion_control_plane::{
    ActionRequest, ActionTarget, StatusEntry, StatusSubject, TypedConfigValue, WorkloadConfig,
};
use orion_core::{ConfigSchemaId, NodeId, decode_from_slice, encode_to_vec};
use std::collections::BTreeMap;

fn camera_context() -> BTreeMap<String, TypedConfigValue> {
    BTreeMap::from([
        ("fx".to_owned(), TypedConfigValue::F64(912.25)),
        ("fy".to_owned(), TypedConfigValue::F64(911.5)),
        ("cx".to_owned(), TypedConfigValue::F64(640.125)),
        ("k1".to_owned(), TypedConfigValue::F64(-0.031_25)),
        (
            "mount.pitch_rad".to_owned(),
            TypedConfigValue::F64(-0.2617993877991494),
        ),
        (
            "tiny".to_owned(),
            TypedConfigValue::F64(f64::MIN_POSITIVE / 2.0),
        ),
        (
            "lens".to_owned(),
            TypedConfigValue::String("fisheye".into()),
        ),
    ])
}

#[test]
fn f64_values_round_trip_through_the_control_wire_bit_for_bit() {
    let mut request =
        ActionRequest::new("a1", ActionTarget::Node(NodeId::new("node-a")), "configure");
    request.args = camera_context();
    let decoded: ActionRequest =
        decode_from_slice(&encode_to_vec(&request).expect("encode")).expect("decode");
    assert_eq!(decoded, request);

    for value in [f64::NAN, f64::INFINITY, -0.0, 1e-310, f64::MAX] {
        let entry = StatusEntry::new(
            StatusSubject::Node(NodeId::new("node-a")),
            "camera.value",
            TypedConfigValue::F64(value),
        );
        let decoded: StatusEntry =
            decode_from_slice(&encode_to_vec(&entry).expect("encode")).expect("decode");
        assert_eq!(
            decoded.value.as_f64().map(f64::to_bits),
            Some(value.to_bits()),
            "{value}"
        );
        assert_eq!(decoded, entry, "equality is bitwise, so NaN equals itself");
    }

    let config = WorkloadConfig {
        schema_id: ConfigSchemaId::new("camera.context.v1"),
        payload: camera_context(),
    };
    let decoded: WorkloadConfig =
        decode_from_slice(&encode_to_vec(&config).expect("encode")).expect("decode");
    assert_eq!(decoded, config);
}

#[test]
fn f64_values_round_trip_through_json() {
    let context = camera_context();
    let json = serde_json::to_string(&context).expect("json");
    assert!(json.contains(r#""fx":{"F64":912.25}"#), "{json}");
    let decoded: BTreeMap<String, TypedConfigValue> = serde_json::from_str(&json).expect("parse");
    assert_eq!(decoded, context);
}

#[test]
fn f64_equality_and_accessors() {
    let value = TypedConfigValue::F64(1.5);
    assert_eq!(value.kind_name(), "f64");
    assert_eq!(value.as_f64(), Some(1.5));
    assert_eq!(value.as_uint(), None);
    assert_eq!(
        TypedConfigValue::F64(f64::NAN),
        TypedConfigValue::F64(f64::NAN)
    );
    assert_ne!(TypedConfigValue::F64(0.0), TypedConfigValue::F64(-0.0));
    assert_ne!(TypedConfigValue::F64(1.0), TypedConfigValue::UInt(1));
    assert_eq!(TypedConfigValue::UInt(1).as_f64(), None);
}
