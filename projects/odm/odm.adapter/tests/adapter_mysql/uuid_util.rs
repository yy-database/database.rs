//! 从 `src/uuid_util.rs` 迁出的测试。

use odm_adapter::adapter_mysql::{try_parse_uuid_bytes, uuid_bytes_to_str};

#[test]
fn roundtrip_hyphenated() {
    let s = "550e8400-e29b-41d4-a716-446655440000";
    let b = try_parse_uuid_bytes(s).unwrap();
    assert_eq!(b.len(), 16);
    assert_eq!(uuid_bytes_to_str(&b).as_deref(), Some(s));
}

#[test]
fn roundtrip_compact() {
    let s = "550e8400e29b41d4a716446655440000";
    let b = try_parse_uuid_bytes(s).unwrap();
    assert_eq!(uuid_bytes_to_str(&b).as_deref(), Some("550e8400-e29b-41d4-a716-446655440000"));
}
