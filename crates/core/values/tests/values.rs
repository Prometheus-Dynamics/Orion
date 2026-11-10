use orion_measure::CountingAlloc;
use orion_measure::harness::assert_steady_no_allocs;
use orion_values::{
    Array, Error, FieldType, Fixed, Structured, Value, decode, encode, encode_array, offset_of,
    structured,
};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

structured! {
    /// Where something is and how sure the source is.
    pub struct Pose = "pose3d" {
        /// Metres.
        pub position: [f64; 3],
        /// A unit quaternion.
        pub rotation: [f64; 4],
        /// 0 to 1.
        pub confidence: f32,
        /// Which tag it came from.
        pub tag: u16,
        /// Whether it is usable.
        pub valid: bool,
    }
}

structured! {
    /// A pose with something added at the end by a newer build.
    pub struct PoseV2 = "pose3d" {
        pub position: [f64; 3],
        pub rotation: [f64; 4],
        pub confidence: f32,
        pub tag: u16,
        pub valid: bool,
        pub ambiguity: f32,
    }
}

structured! {
    /// A structured value inside another.
    pub struct Tracked = "tracked" {
        pub id: u32,
        pub pose: Pose,
    }
}

const POSE: Pose = Pose {
    position: [1.5, -2.25, 0.125],
    rotation: [0.5, 0.5, -0.5, 0.5],
    confidence: 0.75,
    tag: 17,
    valid: true,
};

/// The bytes of a structured value are part of every topic that carries it.
#[test]
fn the_layout_of_a_structured_value_is_pinned() {
    assert_eq!((Pose::SIZE, Pose::TYPE_NAME), (63, "pose3d"));
    let mut payload = [0xFF_u8; 63];
    assert_eq!(encode(&POSE, &mut payload), Ok(63));
    assert_eq!(payload[..8], 1.5_f64.to_le_bytes());
    assert_eq!(payload[16..24], 0.125_f64.to_le_bytes());
    assert_eq!(payload[48..56], 0.5_f64.to_le_bytes());
    assert_eq!(payload[56..60], 0.75_f32.to_le_bytes());
    assert_eq!(payload[60..], [17, 0, 1]);
    assert_eq!(
        Pose::MEMBERS,
        [
            ("position", 24),
            ("rotation", 32),
            ("confidence", 4),
            ("tag", 2),
            ("valid", 1)
        ]
    );
    // One number read from its offset, with no decoding of the rest.
    const TAG_AT: usize = offset_of(Pose::MEMBERS, "tag").expect("member");
    assert_eq!((TAG_AT, decode::<u16>(&payload[TAG_AT..])), (60, Ok(17)));
    assert_eq!(offset_of(Pose::MEMBERS, "nope"), None);
}

#[test]
fn a_structured_pose_round_trips_with_no_allocation() {
    let mut payload = [0_u8; 64];
    let mut seen = 0_u32;
    assert_steady_no_allocs("pose round trip", 8, 10_000, || {
        let len = encode(std::hint::black_box(&POSE), &mut payload).expect("encode");
        let pose: Pose = decode(std::hint::black_box(&payload[..len])).expect("decode");
        assert_eq!(pose, POSE);
        let value = Value::read(FieldType::Structured, &payload[..len]).expect("read");
        seen += u32::from(value.structured::<Pose>().expect("pose").tag);
    });
    assert_eq!(seen, 17 * 10_008);
}

#[test]
fn a_structured_type_grows_at_the_end() {
    let newer = PoseV2 {
        position: POSE.position,
        rotation: POSE.rotation,
        confidence: POSE.confidence,
        tag: POSE.tag,
        valid: POSE.valid,
        ambiguity: 0.2,
    };
    let mut payload = [0_u8; 67];
    assert_eq!(encode(&newer, &mut payload), Ok(67));
    // An older reader reads the part it knows. A newer reader refuses a payload too short for it.
    assert_eq!(decode::<Pose>(&payload), Ok(POSE));
    assert_eq!(decode::<PoseV2>(&payload[..63]), Err(Error::Malformed));
}

#[test]
fn structured_values_nest_and_short_buffers_are_refused() {
    let tracked = Tracked { id: 9, pose: POSE };
    assert_eq!(Tracked::SIZE, 67);
    let mut payload = [0_u8; 67];
    assert_eq!(
        encode(&tracked, &mut payload[..66]),
        Err(Error::BufferTooSmall)
    );
    assert_eq!(encode(&tracked, &mut payload), Ok(67));
    assert_eq!(decode::<Tracked>(&payload), Ok(tracked));
    assert_eq!(decode::<Tracked>(&payload[..66]), Err(Error::Malformed));
}

#[test]
fn every_number_keeps_its_bits() {
    fn round<T: Fixed + PartialEq + core::fmt::Debug>(value: T) {
        let mut out = [0_u8; 16];
        let len = encode(&value, &mut out).expect("encode");
        assert_eq!(len, T::SIZE);
        assert_eq!(decode::<T>(&out[..len]), Ok(value));
    }
    round(u8::MAX);
    round(i8::MIN);
    round(u16::MAX);
    round(i16::MIN);
    round(u32::MAX);
    round(i32::MIN);
    round(u64::MAX);
    round(i64::MIN);
    round(f32::MIN_POSITIVE);
    round(f64::NEG_INFINITY);
    round(true);
    round([1_u16, 2, 3]);
    round([[1.5_f32; 2]; 2]);
    let mut out = [0_u8; 8];
    encode(&f64::NAN, &mut out).expect("encode");
    assert!(decode::<f64>(&out).expect("decode").is_nan());
    assert_eq!(out, f64::NAN.to_le_bytes());
}

#[test]
fn every_type_reads_and_writes_as_a_value() {
    let mut floats = [0_u8; 24];
    encode_array(&[1.0_f64, -2.0, 3.5], &mut floats).expect("floats");
    let mut ints = [0_u8; 16];
    encode_array(&[i64::MIN, 7], &mut ints).expect("ints");
    let cases: [(FieldType, &[u8]); 8] = [
        (FieldType::Bool, &[1]),
        (FieldType::Int, &(-5_i64).to_le_bytes()),
        (FieldType::Float, &2.5_f64.to_le_bytes()),
        (FieldType::Text, "héllo".as_bytes()),
        (FieldType::Bytes, &[0, 255, 3]),
        (FieldType::FloatArray, &floats),
        (FieldType::IntArray, &ints),
        (FieldType::Structured, &[9; 5]),
    ];
    for (ty, payload) in cases {
        let value = Value::read(ty, payload).expect("read");
        assert_eq!(value.ty(), ty);
        let mut out = [0_u8; 32];
        let len = value.write(&mut out).expect("write");
        assert_eq!(&out[..len], payload, "{ty:?}");
        if !payload.is_empty() {
            assert_eq!(
                value.write(&mut out[..payload.len() - 1]),
                Err(Error::BufferTooSmall)
            );
        }
    }
    assert_eq!(
        Value::read(FieldType::Int, &(-5_i64).to_le_bytes()),
        Ok(Value::Int(-5))
    );
    assert_eq!(Value::read(FieldType::Text, b"hi"), Ok(Value::Text("hi")));
    assert_eq!(Value::Int(-5).as_f64(), Some(-5.0));
    assert_eq!(Value::Bool(true).as_f64(), Some(1.0));
    assert_eq!(Value::Text("x").as_f64(), None);
    let Value::FloatArray(array) = Value::read(FieldType::FloatArray, &floats).expect("read")
    else {
        panic!("not an array");
    };
    assert_eq!(
        (array.len(), array.get(2), array.get(3)),
        (3, Some(3.5), None)
    );
    assert!(array.iter().eq([1.0, -2.0, 3.5]));
    assert!(Array::<f64>::new(&[]).expect("empty").is_empty());
}

#[test]
fn payloads_that_do_not_fit_their_type_are_refused() {
    let bad: [(FieldType, &[u8]); 9] = [
        (FieldType::Bool, &[]),
        (FieldType::Bool, &[2]),
        (FieldType::Bool, &[1, 0]),
        (FieldType::Int, &[0; 7]),
        (FieldType::Float, &[0; 9]),
        (FieldType::Text, &[0xFF, 0xFE]),
        (FieldType::FloatArray, &[0; 12]),
        (FieldType::IntArray, &[0; 1]),
        (FieldType::Structured, &[]),
    ];
    for (ty, payload) in &bad[..8] {
        assert_eq!(Value::read(*ty, payload), Err(Error::Malformed), "{ty:?}");
    }
    // An empty structured payload reads; it is the typed read that says it is too short.
    let empty = Value::read(bad[8].0, bad[8].1).expect("read");
    assert_eq!(empty.structured::<Pose>(), Err(Error::Malformed));
    assert_eq!(Value::Int(1).structured::<Pose>(), Err(Error::Malformed));
    assert_eq!(
        Value::read(FieldType::Buffers, &[]),
        Err(Error::Unsupported)
    );
    assert_eq!(
        Value::read(FieldType::Unknown(200), &[]),
        Err(Error::Unsupported)
    );
}
