//! Value wire shapes reject illegal vector and file descriptors and preserve literal precision.

use odm_types::{FileDescriptor, FileManifestRef, Value, VectorValue};

fn valid_digest(seed: u8) -> String {
    let mut text = String::with_capacity(64);
    for i in 0..32 {
        let byte = seed.wrapping_add(i);
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

fn valid_file() -> FileDescriptor {
    FileDescriptor::new(
        valid_digest(0),
        32,
        Some("report.bin".into()),
        Some("application/octet-stream".into()),
        FileManifestRef { hash: valid_digest(1), size: 32 },
    )
    .expect("canonical descriptor")
}

#[test]
fn vector_deserialize_rejects_illegal_shapes() {
    for wire in [
        // zero dimension
        r#"{"dimension":0,"components":[]}"#.to_owned(),
        // dimension / component-count mismatch
        r#"{"dimension":2,"components":[1.0]}"#.to_owned(),
        // non-finite (1e40 overflows f32 into infinity)
        format!(r#"{{"dimension":1,"components":[{}]}}"#, 1e40_f64),
    ] {
        let parsed = serde_json::from_str::<VectorValue>(&wire);
        assert!(parsed.is_err(), "`VectorValue` accepted `{wire}`");
    }
    assert!(VectorValue::new(1, vec![f32::NAN]).is_none());
    assert!(VectorValue::new(1, vec![f32::INFINITY]).is_none());
}

#[test]
fn vector_deserialize_round_trips_valid_values() {
    let vector = VectorValue::new(3, vec![0.5, -0.0, 2.0]).expect("canonical vector");
    let wire = serde_json::to_string(&vector).expect("serialize");
    let decoded = serde_json::from_str::<VectorValue>(&wire).expect("deserialize");
    assert_eq!(decoded, vector);
    assert_eq!(decoded.components[1].to_bits(), (-0.0_f32).to_bits());
}

#[test]
fn vector_inside_value_deserialize_is_checked() {
    let illegal = serde_json::json!({"Vector": {"dimension": 0, "components": []}});
    assert!(serde_json::from_value::<Value>(illegal).is_err());
    let legal = serde_json::json!({"Vector": {"dimension": 1, "components": [1.0]}});
    let decoded = serde_json::from_value::<Value>(legal).expect("deserialize");
    assert!(matches!(decoded, Value::Vector(_)));
}

#[test]
fn file_deserialize_rejects_illegal_shapes() {
    let base = valid_file();
    let name = base.name.clone();
    let media_type = base.media_type.clone();
    let illegal = [
        // uppercase digest
        serde_json::json!({"digest": valid_digest(0).to_uppercase(), "octets": 32, "name": name, "mediaType": media_type, "manifest": {"hash": valid_digest(1), "size": 32}}),
        // short digest
        serde_json::json!({"digest": valid_digest(0)[..63], "octets": 32, "name": name, "mediaType": media_type, "manifest": {"hash": valid_digest(1), "size": 32}}),
        // uppercase manifest hash
        serde_json::json!({"digest": valid_digest(0), "octets": 32, "name": name, "mediaType": media_type, "manifest": {"hash": valid_digest(1).to_uppercase(), "size": 32}}),
        // blank labels
        serde_json::json!({"digest": valid_digest(0), "octets": 32, "name": "   ", "mediaType": media_type, "manifest": {"hash": valid_digest(1), "size": 32}}),
        serde_json::json!({"digest": valid_digest(0), "octets": 32, "name": name, "mediaType": "", "manifest": {"hash": valid_digest(1), "size": 32}}),
    ];
    for wire in illegal {
        let parsed = serde_json::from_value::<FileDescriptor>(wire.clone());
        assert!(parsed.is_err(), "`FileDescriptor` accepted `{wire}`");
    }
}

#[test]
fn file_deserialize_round_trips_valid_values() {
    let file = valid_file();
    let wire = serde_json::to_string(&file).expect("serialize");
    let decoded = serde_json::from_str::<FileDescriptor>(&wire).expect("deserialize");
    assert_eq!(decoded, file);
}

#[test]
fn file_inside_value_deserialize_is_checked() {
    let illegal = serde_json::json!({"File": {"digest": valid_digest(0)[..63], "octets": 1, "manifest": {"hash": valid_digest(1), "size": 1}}});
    assert!(serde_json::from_value::<Value>(illegal).is_err());
    let legal = serde_json::to_value(Value::File(valid_file())).expect("serialize");
    let decoded = serde_json::from_value::<Value>(legal).expect("deserialize");
    assert!(matches!(decoded, Value::File(_)));
}

#[test]
fn uint_and_float_pred_literals_keep_full_precision() {
    let (text, kind) = Value::UInt(u64::MAX).as_pred_literal();
    assert_eq!(text, "18446744073709551615");
    assert_eq!(kind, database_query::ir::LiteralKind::UInt);

    for float in [0.1_f64, -0.0, f64::MIN_POSITIVE / 2.0] {
        let (text, kind) = Value::Float(float).as_pred_literal();
        assert_eq!(kind, database_query::ir::LiteralKind::Float);
        let reparsed: f64 = text.parse().expect("canonical float text parses");
        assert_eq!(reparsed.to_bits(), float.to_bits(), "`{text}` must round-trip the exact bit pattern");
    }
}

#[test]
fn float_equality_matches_total_order() {
    assert_eq!(Value::Float(0.5), Value::Float(0.5));
    assert_ne!(Value::Float(0.0), Value::Float(-0.0));
    assert_eq!(Value::UInt(1), Value::UInt(1));
    assert_ne!(Value::UInt(1), Value::Int(1));
}
