//! 从 `src/types.rs` 迁出的测试。

use database_query::ir::LiteralKind;
use odm_adapter::adapter_sqlite::{SqliteValue, from_sql_value, from_sql_value_typed, literal_to_sql_value, to_sql_value};
use odm_types::Value;
use vos::types::{CanonicalScalar, ResolvedCanonicalType};

#[test]
fn float_binds_as_native_real_and_decodes_back() {
    let bound = to_sql_value(&Value::Float(-0.0)).expect("bind finite f64");
    assert!(matches!(bound, SqliteValue::Real(_)));
    let decoded = from_sql_value(bound).expect("decode real");
    match decoded {
        Value::Float(value) => assert_eq!(value.to_bits(), (-0.0_f64).to_bits()),
        other => panic!("expected `Value::Float`, got {other:?}"),
    }
}

#[test]
fn u64_within_sqlite_range_keeps_its_unsigned_carrier() {
    assert!(matches!(to_sql_value(&Value::UInt(7)), Ok(SqliteValue::Integer(7))));
    match from_sql_value_typed(SqliteValue::Integer(i64::MAX), &ResolvedCanonicalType::Scalar(CanonicalScalar::U64)) {
        Ok(Value::UInt(value)) => assert_eq!(value, i64::MAX as u64),
        other => panic!("expected `Value::UInt`, got {other:?}"),
    }
}

#[test]
fn values_sqlite_cannot_carry_are_rejected_instead_of_truncated() {
    assert!(to_sql_value(&Value::UInt(u64::MAX)).is_err());
    assert!(to_sql_value(&Value::Float(f64::NAN)).is_err());
    assert!(literal_to_sql_value("18446744073709551615", LiteralKind::UInt).is_err());
    assert!(literal_to_sql_value("NaN", LiteralKind::Float).is_err());
    assert!(to_sql_value(&Value::Vector(odm_types::VectorValue::new(2, vec![1.0, 2.0]).expect("vector"))).is_err());
    let digest = "00".repeat(32);
    let descriptor = odm_types::FileDescriptor::new(digest.clone(), 4, None, None, odm_types::FileManifestRef { hash: digest, size: 4 })
        .expect("descriptor");
    assert!(to_sql_value(&Value::File(descriptor)).is_err());
}

#[test]
fn bytes_bind_as_blob_and_decode_back_byte_for_byte() {
    let octets = vec![0x00_u8, 0xFF, 0xFE, b'i', b'r', b'i', b's'];
    let bound = to_sql_value(&Value::Bytes(octets.clone())).expect("bind bytes");
    assert!(matches!(&bound, SqliteValue::Blob(actual) if actual == &octets));
    match from_sql_value(bound.clone()).expect("decode blob") {
        Value::Bytes(actual) => assert_eq!(actual, octets),
        other => panic!("expected `Value::Bytes`, got {other:?}"),
    }
    match from_sql_value_typed(bound, &ResolvedCanonicalType::Scalar(CanonicalScalar::Bytes)) {
        Ok(Value::Bytes(actual)) => assert_eq!(actual, octets),
        other => panic!("expected typed `Value::Bytes`, got {other:?}"),
    }
}

#[test]
fn typed_bytes_result_rejects_text_cells_instead_of_stringifying() {
    let text_cell = SqliteValue::Text(b"plain".to_vec());
    assert!(from_sql_value_typed(text_cell, &ResolvedCanonicalType::Scalar(CanonicalScalar::Bytes)).is_err());
    let blob_cell = SqliteValue::Blob(vec![0x00]);
    assert!(from_sql_value_typed(blob_cell, &ResolvedCanonicalType::Scalar(CanonicalScalar::Utf8)).is_err());
}
