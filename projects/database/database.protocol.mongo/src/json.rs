//! Extended JSON 桥，供非 Rust 宿主调用命令原语。
//!
//! BSON 压平成普通 JSON 会丢精度，因此值一律使用 MongoDB Extended JSON 形态：
//! `{"$oid": ...}`、`{"$numberInt": ...}`、`{"$numberLong": ...}`、
//! `{"$numberDouble": ...}`、`{"$binary": {...}}`、
//! `{"$date": {"$numberLong": ...}}`、`{"$timestamp": {...}}`、
//! `{"$minKey": 1}`、`{"$maxKey": 1}`、`{"$numberDecimal": ...}`、
//! `{"$regularExpression": {...}}`、`{"$code": ...}` 以及旧式的
//! `{"$undefined": true}`。
//!
//! 输出始终是规范形态。输入此外还接受宽松写法
//! `{"$date": "<ISO-8601>"}` 与 `{"$date": <毫秒数>}`。

use crate::{
    bson::{BinaryData, BsonDocument, BsonValue},
    decimal128::Decimal128,
};
use serde_json::{Map, Value};

/// 把一个 BSON 值转换成 Extended JSON 形态。
pub fn to_json(value: &BsonValue) -> Value {
    match value {
        BsonValue::Document(document) => Value::Object(to_json_document(document)),
        BsonValue::Array(items) => Value::Array(items.iter().map(to_json).collect()),
        BsonValue::String(text) => Value::String(text.clone()),
        BsonValue::Int32(number) => tagged("$numberInt", number.to_string()),
        BsonValue::Int64(number) => tagged("$numberLong", number.to_string()),
        BsonValue::Double(number) => tagged("$numberDouble", double_text(*number)),
        BsonValue::Decimal128(number) => tagged("$numberDecimal", number.as_str()),
        BsonValue::Bool(flag) => Value::Bool(*flag),
        BsonValue::Null => Value::Null,
        BsonValue::Undefined => {
            let mut wrapper = Map::new();
            wrapper.insert("$undefined".to_string(), Value::Bool(true));
            Value::Object(wrapper)
        }
        BsonValue::ObjectId(bytes) => tagged("$oid", hex_encode(bytes)),
        BsonValue::Binary(binary) => {
            let mut payload = Map::new();
            payload.insert("base64".to_string(), Value::String(base64_encode(&binary.bytes)));
            payload.insert("subType".to_string(), Value::String(format!("{:02x}", binary.subtype)));
            let mut wrapper = Map::new();
            wrapper.insert("$binary".to_string(), Value::Object(payload));
            Value::Object(wrapper)
        }
        BsonValue::DateTime(milliseconds) => {
            let mut payload = Map::new();
            payload.insert("$numberLong".to_string(), Value::String(milliseconds.to_string()));
            tagged_object("$date", Value::Object(payload))
        }
        BsonValue::Timestamp { t, i } => {
            let mut payload = Map::new();
            payload.insert("t".to_string(), Value::Number((*t).into()));
            payload.insert("i".to_string(), Value::Number((*i).into()));
            tagged_object("$timestamp", Value::Object(payload))
        }
        BsonValue::MinKey => tagged_number("$minKey", 1),
        BsonValue::MaxKey => tagged_number("$maxKey", 1),
        BsonValue::Symbol(text) => tagged("$symbol", text.clone()),
        BsonValue::Regex { pattern, options } => {
            let mut payload = Map::new();
            payload.insert("pattern".to_string(), Value::String(pattern.clone()));
            payload.insert("options".to_string(), Value::String(options.clone()));
            tagged_object("$regularExpression", Value::Object(payload))
        }
        BsonValue::Code(code) => tagged("$code", code.clone()),
        BsonValue::CodeWithScope { code, scope } => {
            let mut wrapper = Map::new();
            wrapper.insert("$code".to_string(), Value::String(code.clone()));
            wrapper.insert("$scope".to_string(), Value::Object(to_json_document(scope)));
            Value::Object(wrapper)
        }
        BsonValue::DbPointer { namespace, id } => {
            let mut identifier = Map::new();
            identifier.insert("$oid".to_string(), Value::String(hex_encode(id)));
            let mut payload = Map::new();
            payload.insert("$ref".to_string(), Value::String(namespace.clone()));
            payload.insert("$id".to_string(), Value::Object(identifier));
            tagged_object("$dbPointer", Value::Object(payload))
        }
    }
}

/// 把一个 BSON 文档转换成 Extended JSON 对象。
pub fn to_json_document(document: &BsonDocument) -> Map<String, Value> {
    let mut object = Map::new();
    for (key, value) in &document.fields {
        object.insert(key.clone(), to_json(value));
    }
    object
}

/// 把 Extended JSON 值解析回 BSON。
pub fn from_json(value: &Value) -> Result<BsonValue, String> {
    match value {
        Value::Null => Ok(BsonValue::Null),
        Value::Bool(flag) => Ok(BsonValue::Bool(*flag)),
        Value::String(text) => Ok(BsonValue::String(text.clone())),
        Value::Array(items) => Ok(BsonValue::Array(items.iter().map(from_json).collect::<Result<Vec<_>, _>>()?)),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                if i32::try_from(integer).is_ok() { Ok(BsonValue::Int32(integer as i32)) } else { Ok(BsonValue::Int64(integer)) }
            }
            else {
                number.as_f64().map(BsonValue::Double).ok_or_else(|| "number is not representable".to_string())
            }
        }
        Value::Object(object) => {
            // `CodeWithScope` 是唯一不采用单键包装的 Extended JSON 形态：
            // 它把 `$code` 与 `$scope` 并列携带。
            if object.contains_key("$scope") {
                let code = object.get("$code").and_then(Value::as_str).ok_or_else(|| "`$code` is required next to `$scope`".to_string())?;
                let scope = object.get("$scope").and_then(Value::as_object).ok_or_else(|| "`$scope` must be an object".to_string())?;
                return Ok(BsonValue::CodeWithScope { code: code.to_string(), scope: from_json_document(scope)? });
            }
            if object.len() == 1
                && let Some(tag) = object.keys().next()
                && let Some(parsed) = parse_tagged(tag, object.values().next().unwrap())?
            {
                return Ok(parsed);
            }
            let mut document = BsonDocument::new();
            for (key, item) in object {
                document.push(key.clone(), from_json(item)?);
            }
            Ok(BsonValue::Document(document))
        }
    }
}

/// 把 Extended JSON 对象解析成 BSON 文档。
pub fn from_json_document(object: &Map<String, Value>) -> Result<BsonDocument, String> {
    let mut document = BsonDocument::new();
    for (key, value) in object {
        document.push(key.clone(), from_json(value)?);
    }
    Ok(document)
}

/// 把 Extended JSON 文本解析成 BSON 命令文档。
pub fn document_from_json(text: &str) -> Result<BsonDocument, String> {
    let value: Value = serde_json::from_str(text).map_err(|error| format!("invalid JSON: {error}"))?;
    match from_json(&value)? {
        BsonValue::Document(document) => Ok(document),
        _ => Err("a JSON object is required".to_string()),
    }
}

/// 把 BSON 文档序列化成 Extended JSON 文本。
pub fn document_to_json(document: &BsonDocument) -> String {
    Value::Object(to_json_document(document)).to_string()
}

/// 把 BSON 值序列化成 Extended JSON 文本。
pub fn value_to_json_text(value: &BsonValue) -> String {
    to_json(value).to_string()
}

fn tagged(tag: &str, payload: String) -> Value {
    let mut object = Map::new();
    object.insert(tag.to_string(), Value::String(payload));
    Value::Object(object)
}

fn tagged_object(tag: &str, payload: Value) -> Value {
    let mut object = Map::new();
    object.insert(tag.to_string(), payload);
    Value::Object(object)
}

fn tagged_number(tag: &str, payload: i64) -> Value {
    let mut object = Map::new();
    object.insert(tag.to_string(), Value::Number(payload.into()));
    Value::Object(object)
}

/// Extended JSON 会把非有限浮点数写成文本，而不是 Rust 的 `inf` / `NaN` 调试形态。
fn double_text(number: f64) -> String {
    if number.is_nan() {
        "NaN".to_string()
    }
    else if number.is_infinite() {
        if number.is_sign_positive() { "Infinity".to_string() } else { "-Infinity".to_string() }
    }
    else {
        format!("{number:?}")
    }
}

fn parse_tagged(tag: &str, payload: &Value) -> Result<Option<BsonValue>, String> {
    match tag {
        "$oid" => {
            let text = payload.as_str().ok_or_else(|| "`$oid` must be a string".to_string())?;
            let bytes = hex_decode(text)?;
            let identifier: [u8; 12] = bytes.try_into().map_err(|_| "`$oid` must hold 12 bytes".to_string())?;
            Ok(Some(BsonValue::ObjectId(identifier)))
        }
        "$numberInt" => {
            let text = payload.as_str().ok_or_else(|| "`$numberInt` must be a string".to_string())?;
            let number: i32 = text.parse().map_err(|_| format!("invalid `$numberInt` value `{text}`"))?;
            Ok(Some(BsonValue::Int32(number)))
        }
        "$numberLong" => {
            let text = payload.as_str().ok_or_else(|| "`$numberLong` must be a string".to_string())?;
            let number: i64 = text.parse().map_err(|_| format!("invalid `$numberLong` value `{text}`"))?;
            Ok(Some(BsonValue::Int64(number)))
        }
        "$numberDouble" => {
            let text = payload.as_str().ok_or_else(|| "`$numberDouble` must be a string".to_string())?;
            let number: f64 = text.parse().map_err(|_| format!("invalid `$numberDouble` value `{text}`"))?;
            Ok(Some(BsonValue::Double(number)))
        }
        "$undefined" => {
            if payload.as_bool() != Some(true) {
                return Err("`$undefined` must be true".to_string());
            }
            Ok(Some(BsonValue::Undefined))
        }
        "$binary" => {
            let object = payload.as_object().ok_or_else(|| "`$binary` must be an object".to_string())?;
            let encoded = object.get("base64").and_then(Value::as_str).ok_or_else(|| "`$binary.base64` is required".to_string())?;
            let subtype = object.get("subType").and_then(Value::as_str).ok_or_else(|| "`$binary.subType` is required".to_string())?;
            let subtype = u8::from_str_radix(subtype, 16).map_err(|_| format!("invalid `$binary.subType` value `{subtype}`"))?;
            Ok(Some(BsonValue::Binary(BinaryData::new(subtype, base64_decode(encoded)?))))
        }
        "$date" => {
            // 规范形态是 `{"$numberLong": "<ms>"}`。宽松形态接受 ISO-8601 字符串
            // 或裸毫秒数。
            let milliseconds = match payload {
                Value::String(text) => parse_iso8601_millis(text)?,
                Value::Number(number) => number.as_i64().ok_or_else(|| "`$date` must hold whole milliseconds".to_string())?,
                Value::Object(object) => {
                    let text = object
                        .get("$numberLong")
                        .and_then(Value::as_str)
                        .ok_or_else(|| "`$date` object must hold `$numberLong`".to_string())?;
                    text.parse().map_err(|_| format!("invalid `$date` value `{text}`"))?
                }
                _ => return Err("`$date` must be an ISO-8601 string, a number or `$numberLong`".to_string()),
            };
            Ok(Some(BsonValue::DateTime(milliseconds)))
        }
        "$timestamp" => {
            let object = payload.as_object().ok_or_else(|| "`$timestamp` must be an object".to_string())?;
            let t = timestamp_part(object.get("t"), "t")?;
            let i = timestamp_part(object.get("i"), "i")?;
            Ok(Some(BsonValue::Timestamp { t, i }))
        }
        "$minKey" => {
            if payload.as_i64() != Some(1) {
                return Err("`$minKey` must be 1".to_string());
            }
            Ok(Some(BsonValue::MinKey))
        }
        "$maxKey" => {
            if payload.as_i64() != Some(1) {
                return Err("`$maxKey` must be 1".to_string());
            }
            Ok(Some(BsonValue::MaxKey))
        }
        "$numberDecimal" => {
            let text = payload.as_str().ok_or_else(|| "`$numberDecimal` must be a string".to_string())?;
            let number = Decimal128::from_str(text).ok_or_else(|| format!("invalid `$numberDecimal` value `{text}`"))?;
            Ok(Some(BsonValue::Decimal128(number)))
        }
        "$symbol" => {
            let text = payload.as_str().ok_or_else(|| "`$symbol` must be a string".to_string())?;
            Ok(Some(BsonValue::Symbol(text.to_string())))
        }
        "$regularExpression" => {
            let object = payload.as_object().ok_or_else(|| "`$regularExpression` must be an object".to_string())?;
            let pattern =
                object.get("pattern").and_then(Value::as_str).ok_or_else(|| "`$regularExpression.pattern` is required".to_string())?;
            let options =
                object.get("options").and_then(Value::as_str).ok_or_else(|| "`$regularExpression.options` is required".to_string())?;
            Ok(Some(BsonValue::Regex { pattern: pattern.to_string(), options: options.to_string() }))
        }
        "$code" => {
            let text = payload.as_str().ok_or_else(|| "`$code` must be a string".to_string())?;
            Ok(Some(BsonValue::Code(text.to_string())))
        }
        "$dbPointer" => {
            let object = payload.as_object().ok_or_else(|| "`$dbPointer` must be an object".to_string())?;
            let namespace = object.get("$ref").and_then(Value::as_str).ok_or_else(|| "`$dbPointer.$ref` is required".to_string())?;
            let identifier = object
                .get("$id")
                .and_then(Value::as_object)
                .and_then(|identifier| identifier.get("$oid"))
                .and_then(Value::as_str)
                .ok_or_else(|| "`$dbPointer.$id` must hold `$oid`".to_string())?;
            let bytes = hex_decode(identifier)?;
            let id: [u8; 12] = bytes.try_into().map_err(|_| "`$dbPointer.$id` must hold 12 bytes".to_string())?;
            Ok(Some(BsonValue::DbPointer { namespace: namespace.to_string(), id }))
        }
        _ => Ok(None),
    }
}

fn timestamp_part(value: Option<&Value>, name: &str) -> Result<u32, String> {
    let number = value.and_then(Value::as_u64).ok_or_else(|| format!("`$timestamp.{name}` must be a uint32"))?;
    u32::try_from(number).map_err(|_| format!("`$timestamp.{name}` must be a uint32"))
}

/// 把宽松写法的 `$date` ISO-8601 文本解析成纪元毫秒。
///
/// 接受形态：`YYYY-MM-DDTHH:MM:SS[.fff][Z|±HH:MM]`。缺省时区按 UTC 读取；
/// `mongod` 始终写 `Z` 或显式时区偏移。
fn parse_iso8601_millis(text: &str) -> Result<i64, String> {
    let bad = || format!("invalid ISO-8601 date `{text}`");
    let mut rest = text;
    let year: i64 = take_digits(&mut rest, 4, &bad)?.parse().map_err(|_| bad())?;
    expect(&mut rest, '-', &bad)?;
    let month: u32 = take_digits(&mut rest, 2, &bad)?.parse().map_err(|_| bad())?;
    expect(&mut rest, '-', &bad)?;
    let day: u32 = take_digits(&mut rest, 2, &bad)?.parse().map_err(|_| bad())?;
    expect(&mut rest, 'T', &bad)?;
    let hour: i64 = take_digits(&mut rest, 2, &bad)?.parse().map_err(|_| bad())?;
    expect(&mut rest, ':', &bad)?;
    let minute: i64 = take_digits(&mut rest, 2, &bad)?.parse().map_err(|_| bad())?;
    expect(&mut rest, ':', &bad)?;
    let second: i64 = take_digits(&mut rest, 2, &bad)?.parse().map_err(|_| bad())?;

    let mut milliseconds = 0_i64;
    if let Some(fraction) = rest.strip_prefix('.') {
        let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return Err(bad());
        }
        let padded = format!("{:0<3}", &fraction[..digits]);
        milliseconds = padded[..3].parse().map_err(|_| bad())?;
        rest = &fraction[digits..];
    }

    let offset_minutes = if let Some(tail) = rest.strip_prefix('Z').or_else(|| rest.strip_prefix('z')) {
        rest = tail;
        0
    }
    else if rest.is_empty() {
        0
    }
    else {
        let sign = match rest.as_bytes()[0] {
            b'+' => 1,
            b'-' => -1,
            _ => return Err(bad()),
        };
        rest = &rest[1..];
        let hours: i64 = take_digits(&mut rest, 2, &bad)?.parse().map_err(|_| bad())?;
        rest = rest.strip_prefix(':').unwrap_or(rest);
        let minutes: i64 = take_digits(&mut rest, 2, &bad)?.parse().map_err(|_| bad())?;
        sign * (hours * 60 + minutes)
    };
    if !rest.is_empty() {
        return Err(bad());
    }
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || minute > 59 || second > 60 {
        return Err(bad());
    }

    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offset_minutes * 60;
    Ok(seconds * 1_000 + milliseconds)
}

fn take_digits<'a>(rest: &mut &'a str, count: usize, bad: &dyn Fn() -> String) -> Result<&'a str, String> {
    if rest.len() < count {
        return Err(bad());
    }
    let (head, tail) = rest.split_at(count);
    if !head.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(bad());
    }
    *rest = tail;
    Ok(head)
}

fn expect(rest: &mut &str, character: char, bad: &dyn Fn() -> String) -> Result<(), String> {
    match rest.strip_prefix(character) {
        Some(tail) => {
            *rest = tail;
            Ok(())
        }
        None => Err(bad()),
    }
}

/// 前推格里高利历相对 1970-01-01 的天数（Howard Hinnant 的 `days_from_civil`，
/// 在对外暴露的整个 i64 范围内都是精确的）。
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_prime = if month > 2 { month - 3 } else { month + 9 } as i64;
    let day_of_year = (153 * month_prime + 2) / 5 + day as i64 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_decode(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err("hex string must have an even length".to_string());
    }
    let mut bytes = Vec::with_capacity(text.len() / 2);
    let characters: Vec<char> = text.chars().collect();
    for pair in characters.chunks(2) {
        let byte = u8::from_str_radix(&format!("{}{}", pair[0], pair[1]), 16).map_err(|_| "hex string is not valid".to_string())?;
        bytes.push(byte);
    }
    Ok(bytes)
}

const BASE64_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let triple = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let value = (u32::from(triple[0]) << 16) | (u32::from(triple[1]) << 8) | u32::from(triple[2]);
        for position in 0..4 {
            if position <= chunk.len() {
                out.push(BASE64_ALPHABET[((value >> (18 - 6 * position)) & 0x3f) as usize] as char);
            }
            else {
                out.push('=');
            }
        }
    }
    out
}

fn base64_decode(text: &str) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    let mut buffer: u32 = 0;
    let mut bits = 0;
    for character in text.chars() {
        if character == '=' {
            break;
        }
        let index =
            BASE64_ALPHABET.iter().position(|candidate| *candidate as char == character).ok_or_else(|| "invalid base64".to_string())? as u32;
        buffer = (buffer << 6) | index;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            bytes.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    Ok(bytes)
}
