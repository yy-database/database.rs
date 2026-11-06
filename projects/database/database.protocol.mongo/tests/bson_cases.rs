//! 移植自 MongoDB 仓 `tests/cases/bson.json` 的 82 条字节用例。
//!
//! 期望值原样保留：BSON 文档按字节比对，解码结果按扩展 JSON 比对，解码后
//! 再编码必须与输入字节完全一致。断言不放宽。

use base64::{Engine as _, engine::general_purpose::STANDARD};
use database_protocol_mongo::{
    bson::{BsonDocument, BsonValue, double_bits, encode_bson, integral_value, parse_bson, value_to_key},
    wire::{decode_op_msg_command, decode_op_query_command, encode_op_msg_body},
};
use serde_json::{Map, Value};

/// 夹具内容内联，避免测试依赖运行目录。
const CASES: &str = include_str!("fixtures/bson.json");

/// 夹具里出现过的四种操作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    EncodeDocument,
    DecodeDocument,
    EncodeOpMsg,
    DecodeOpMsg,
    DecodeOpQuery,
}

impl Op {
    fn from_name(name: &str) -> Self {
        match name {
            "encodeDocument" => Self::EncodeDocument,
            "decodeDocument" => Self::DecodeDocument,
            "encodeOpMsg" => Self::EncodeOpMsg,
            "decodeOpMsg" => Self::DecodeOpMsg,
            "decodeOpQuery" => Self::DecodeOpQuery,
            other => panic!("unknown fixture op {other}"),
        }
    }
}

/// 逐条跑完夹具里的全部字节用例。
#[test]
fn ports_every_fixture_case() {
    let root: Value = serde_json::from_str(CASES).expect("fixture is valid json");
    let cases = root["cases"].as_array().expect("fixture has cases");
    assert_eq!(cases.len(), 82, "夹具条数必须与 MongoDB 仓一致");

    for case in cases {
        let id = case["id"].as_str().expect("case id");
        let program = case["program"].as_array().expect("case program");
        assert_eq!(program.len(), 1, "{id}: each case has one step");
        let step = &program[0];
        let op = Op::from_name(step["op"].as_str().expect("step op"));
        let expected = case["checks"][0]["equals"].clone();

        match op {
            Op::EncodeDocument => {
                let document = document_from_json(&step["document"]);
                let bytes = encode_bson(&document);
                assert_eq!(bytes_to_hex(&bytes), expected.as_str().unwrap(), "{id}: encoded bytes");
            }
            Op::EncodeOpMsg => {
                let document = document_from_json(&step["document"]);
                let bytes = encode_op_msg_body(&document);
                assert_eq!(bytes_to_hex(&bytes), expected.as_str().unwrap(), "{id}: encoded body");
            }
            Op::DecodeDocument => {
                let bytes = hex_to_bytes(step["hex"].as_str().expect("hex"));
                let document = parse_bson(&bytes).unwrap_or_else(|error| panic!("{id}: {error:?}"));
                assert_eq!(document_to_json(&document), expected, "{id}: decoded document");
                assert_eq!(encode_bson(&document), bytes, "{id}: re-encoded bytes must match input");
            }
            Op::DecodeOpMsg => {
                let bytes = hex_to_bytes(step["hex"].as_str().expect("hex"));
                let document = decode_op_msg_command(&bytes).unwrap_or_else(|error| panic!("{id}: {error:?}"));
                assert_eq!(document_to_json(&document), expected, "{id}: decoded OP_MSG body");
            }
            Op::DecodeOpQuery => {
                let bytes = hex_to_bytes(step["hex"].as_str().expect("hex"));
                let document = decode_op_query_command(&bytes).unwrap_or_else(|error| panic!("{id}: {error:?}"));
                assert_eq!(document_to_json(&document), expected, "{id}: decoded OP_QUERY body");
            }
        }
    }
}

/// `Undefined` (0x06) 编解码与往返。
#[test]
fn undefined_round_trips() {
    let mut document = BsonDocument::new();
    document.push("a", BsonValue::Undefined);
    let bytes = encode_bson(&document);
    assert_eq!(bytes_to_hex(&bytes), "0800000006610000");

    let parsed = parse_bson(&bytes).expect("undefined decodes");
    assert_eq!(parsed.fields(), &[("a".to_string(), BsonValue::Undefined)]);
    assert_eq!(encode_bson(&parsed), bytes, "undefined round-trips byte for byte");

    assert_ne!(value_to_key(&BsonValue::Undefined), value_to_key(&BsonValue::Null));
}

/// `1`、`1.0` 与 `NumberLong(1)` 落到同一个去重键。
#[test]
fn numbers_share_one_key_space() {
    let integer = value_to_key(&BsonValue::Int32(1));
    assert_eq!(integer, value_to_key(&BsonValue::Int64(1)));
    assert_eq!(integer, value_to_key(&BsonValue::Double(1.0)));

    // 小数 double 与超出 i64 范围的整数各自保留独立键。
    assert_ne!(value_to_key(&BsonValue::Double(1.5)), integer);
    assert_ne!(value_to_key(&BsonValue::Double(1e20)), integer);

    // -0.0 归并到 0.0。
    assert_eq!(value_to_key(&BsonValue::Double(-0.0)), value_to_key(&BsonValue::Double(0.0)));
    assert_eq!(double_bits(-0.0), 0);
    assert_eq!(integral_value(1e20), None);
    assert_eq!(integral_value(f64::NAN), None);
}

/// 负长度与非法长度一律拒绝，不做截断容错。
#[test]
fn rejects_negative_and_invalid_lengths() {
    // 顶层文档长度 -1。
    assert!(parse_bson(&hex_to_bytes("ffffffff00")).is_err());
    // 子文档长度 -4。
    assert!(parse_bson(&hex_to_bytes("0c000000036100ffffffff00")).is_err());
    // 字符串长度 -1。
    assert!(parse_bson(&hex_to_bytes("0c000000026100ffffffff00")).is_err());
    // 二进制长度 -1。
    assert!(parse_bson(&hex_to_bytes("0c000000056100ffffffff00")).is_err());
    // legacy 0x02 的内嵌长度必须与 外层长度 - 4 一致：外层 7，内嵌 2。
    assert!(parse_bson(&hex_to_bytes("14000000056100070000000202000000aabbcc00")).is_err());
    // legacy 0x02 外层长度装不下 4 字节内嵌长度。
    assert!(parse_bson(&hex_to_bytes("100000000561000300000002aabbcc00")).is_err());
    // 二进制声明的载荷长度超过剩余字节。
    assert!(parse_bson(&hex_to_bytes("0f0000000561000300000000010200")).is_err());
}

fn document_from_json(value: &Value) -> BsonDocument {
    let map = value.as_object().expect("document must be a json object");
    let mut document = BsonDocument::new();
    for (key, value) in map {
        document.push(key.clone(), value_from_json(value));
    }
    document
}

fn value_from_json(value: &Value) -> BsonValue {
    match value {
        Value::Object(map) => {
            if map.len() == 1 {
                if let Some(tag) = map.keys().next() {
                    if let Some(parsed) = tagged_value(tag, &map[tag]) {
                        return parsed;
                    }
                }
            }
            BsonValue::Document(document_from_json(value))
        }
        Value::Array(items) => BsonValue::Array(items.iter().map(value_from_json).collect()),
        Value::String(text) => BsonValue::String(text.clone()),
        Value::Bool(flag) => BsonValue::Bool(*flag),
        Value::Null => BsonValue::Null,
        other => panic!("fixture uses only canonical extended json, got {other}"),
    }
}

fn tagged_value(tag: &str, payload: &Value) -> Option<BsonValue> {
    match tag {
        "$numberInt" => Some(BsonValue::Int32(payload.as_str().expect("$numberInt is a string").parse().expect("i32"))),
        "$numberLong" => Some(BsonValue::Int64(payload.as_str().expect("$numberLong is a string").parse().expect("i64"))),
        "$numberDouble" => Some(BsonValue::Double(payload.as_str().expect("$numberDouble is a string").parse().expect("f64"))),
        "$oid" => {
            let bytes = hex_to_bytes(payload.as_str().expect("$oid is a string"));
            let bytes: [u8; 12] = bytes.try_into().expect("object id is 12 bytes");
            Some(BsonValue::ObjectId(bytes))
        }
        "$binary" => {
            let body = payload.as_object().expect("$binary is an object");
            let bytes = STANDARD.decode(body["base64"].as_str().expect("$binary.base64 is a string")).expect("valid base64");
            let subtype = u8::from_str_radix(body["subType"].as_str().expect("$binary.subType is a string"), 16).expect("valid subtype");
            Some(BsonValue::Binary(database_protocol_mongo::bson::BinaryData::new(subtype, bytes)))
        }
        "$undefined" => Some(BsonValue::Undefined),
        _ => None,
    }
}

fn document_to_json(document: &BsonDocument) -> Value {
    let mut map = Map::new();
    for (key, value) in document.fields() {
        map.insert(key.clone(), value_to_json(value));
    }
    Value::Object(map)
}

fn value_to_json(value: &BsonValue) -> Value {
    match value {
        BsonValue::Document(document) => document_to_json(document),
        BsonValue::Array(items) => Value::Array(items.iter().map(value_to_json).collect()),
        BsonValue::String(text) => Value::String(text.clone()),
        BsonValue::Int32(number) => tagged_json("$numberInt", number.to_string()),
        BsonValue::Int64(number) => tagged_json("$numberLong", number.to_string()),
        // `{:?}` 输出最短可往返表示，且保留 `0.0` / `-0.0` 的小数形态。
        BsonValue::Double(number) => tagged_json("$numberDouble", format!("{number:?}")),
        BsonValue::Bool(flag) => Value::Bool(*flag),
        BsonValue::Null => Value::Null,
        BsonValue::Undefined => {
            let mut map = Map::new();
            map.insert("$undefined".to_string(), Value::Bool(true));
            Value::Object(map)
        }
        BsonValue::ObjectId(bytes) => tagged_json("$oid", bytes_to_hex(bytes)),
        BsonValue::Binary(data) => {
            let mut binary = Map::new();
            binary.insert("base64".to_string(), Value::String(STANDARD.encode(&data.bytes)));
            binary.insert("subType".to_string(), Value::String(format!("{:02x}", data.subtype)));
            let mut wrapper = Map::new();
            wrapper.insert("$binary".to_string(), Value::Object(binary));
            Value::Object(wrapper)
        }
    }
}

fn tagged_json(tag: &str, payload: String) -> Value {
    let mut map = Map::new();
    map.insert(tag.to_string(), Value::String(payload));
    Value::Object(map)
}

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    assert!(hex.len() % 2 == 0, "hex input has an even number of digits");
    (0..hex.len() / 2).map(|index| u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).expect("valid hex")).collect()
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").expect("writing to a string cannot fail");
    }
    out
}
