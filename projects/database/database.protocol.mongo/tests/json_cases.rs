//! Extended JSON 桥的逐字节回归用例。

use database_protocol_mongo::{
    bson::{BsonValue, encode_bson, parse_bson},
    json,
};
use serde_json::json;

#[test]
fn extended_json_round_trips_every_type() {
    let source = json!({
        "text": "hello",
        "int": {"$numberInt": "42"},
        "long": {"$numberLong": "9007199254740993"},
        "double": {"$numberDouble": "1.5"},
        "decimal": {"$numberDecimal": "1.5"},
        "flag": true,
        "empty": null,
        "undefined": {"$undefined": true},
        "oid": {"$oid": "000000000000000000000001"},
        "bin": {"$binary": {"base64": "AQID", "subType": "00"}},
        "date": {"$date": {"$numberLong": "1700000000000"}},
        "timestamp": {"$timestamp": {"t": 7, "i": 3}},
        "min": {"$minKey": 1},
        "max": {"$maxKey": 1},
        "symbol": {"$symbol": "sym"},
        "regex": {"$regularExpression": {"pattern": "abc", "options": "im"}},
        "code": {"$code": "function(){}"},
        "codeScope": {"$code": "function(){}", "$scope": {"a": {"$numberInt": "1"}}},
        "dbPointer": {"$dbPointer": {"$ref": "db.col", "$id": {"$oid": "000000000000000000000002"}}},
        "nested": {"a": [{"$numberInt": "1"}]}
    });
    let document = json::from_json(&serde_json::Value::Object(source.as_object().unwrap().clone())).expect("parse");
    let round_trip = json::to_json(&document);
    assert_eq!(round_trip, source);
    assert!(matches!(document, BsonValue::Document(_)));
}

#[test]
fn extended_json_text_keeps_bson_field_order() {
    let document = json::document_from_json(r#"{"z":{"$numberInt":"1"},"a":"tail"}"#).expect("parse");
    let text = json::document_to_json(&document);
    assert_eq!(text, r#"{"z":{"$numberInt":"1"},"a":"tail"}"#);
}

#[test]
fn extended_json_parse_then_encode_matches_bson_bytes() {
    let text = r#"{"a":{"$numberInt":"1"},"b":{"$numberLong":"9007199254740993"}}"#;
    let document = json::document_from_json(text).expect("parse");
    let bytes = encode_bson(&document);
    let decoded = parse_bson(&bytes).expect("decode");
    assert_eq!(decoded, document);
    assert_eq!(json::document_to_json(&decoded), text);
}

#[test]
fn relaxed_date_input_matches_the_canonical_form() {
    assert_eq!(json::from_json(&json!({"$date": {"$numberLong": "0"}})).expect("canonical"), BsonValue::DateTime(0));
    assert_eq!(json::from_json(&json!({"$date": "1970-01-01T00:00:00Z"})).expect("iso"), BsonValue::DateTime(0));
    assert_eq!(json::from_json(&json!({"$date": "2017-01-01T00:00:00.123+08:00"})).expect("offset"), BsonValue::DateTime(1_483_200_000_123));
    assert_eq!(json::from_json(&json!({"$date": 1})).expect("milliseconds"), BsonValue::DateTime(1));
}
