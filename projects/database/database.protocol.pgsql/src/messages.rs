//! Byte level protocol-v3 framing.
//!
//! Every function is a pure transformation over bytes, so callers can be tested
//! without a socket.

use crate::encode_message;
use crate::error::{MessageField, Notice, PostgresError};

/// Protocol 3.0 as encoded in the startup packet.
pub const PROTOCOL_VERSION: u32 = 196_608;
/// Refuse payloads larger than this so a hostile server cannot exhaust memory.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

fn truncated(what: &str) -> PostgresError {
    PostgresError::Protocol(format!("truncated {what}"))
}

/// One startup packet carrying the user, database and application name.
pub fn startup_packet(user: &str, database: &str) -> Vec<u8> {
    let mut parameters = Vec::new();
    for (key, value) in [
        ("user", user),
        ("database", database),
        ("client_encoding", "UTF8"),
        ("application_name", "postgres-psql"),
    ] {
        parameters.extend_from_slice(key.as_bytes());
        parameters.push(0);
        parameters.extend_from_slice(value.as_bytes());
        parameters.push(0);
    }
    parameters.push(0);

    let mut packet = Vec::with_capacity(parameters.len() + 8);
    packet.extend_from_slice(&(parameters.len() as u32 + 8).to_be_bytes());
    packet.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    packet.extend_from_slice(&parameters);
    packet
}

/// `PasswordMessage` carrying a cleartext or MD5 password response.
///
/// The response is a null-terminated string, and PostgreSQL rejects a packet
/// whose length does not include that terminator.
pub fn password_packet(password: impl AsRef<[u8]>) -> Result<Vec<u8>, PostgresError> {
    let mut payload = password.as_ref().to_vec();
    payload.push(0);
    frame(b'p', payload)
}

/// `SASLInitialResponse` for the selected mechanism.
pub fn sasl_initial_packet(mechanism: &str, response: &[u8]) -> Result<Vec<u8>, PostgresError> {
    let mut payload = Vec::with_capacity(mechanism.len() + 5 + response.len());
    payload.extend_from_slice(mechanism.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&(response.len() as u32).to_be_bytes());
    payload.extend_from_slice(response);
    frame(b'p', payload)
}

/// `SASLResponse` carrying the client-final SCRAM message.
pub fn sasl_response_packet(response: &[u8]) -> Result<Vec<u8>, PostgresError> {
    frame(b'p', response.to_vec())
}

/// One `Query` message in the simple-query protocol.
pub fn query_packet(sql: &str) -> Result<Vec<u8>, PostgresError> {
    let payload = sql.len() + 1;
    if payload + 4 > MAX_MESSAGE_BYTES {
        return Err(PostgresError::Protocol("query exceeds the byte limit".into()));
    }
    let mut packet = Vec::with_capacity(payload + 5);
    packet.push(b'Q');
    packet.extend_from_slice(&(payload as u32 + 4).to_be_bytes());
    packet.extend_from_slice(sql.as_bytes());
    packet.push(0);
    Ok(packet)
}

/// Decodes the ordered fields of an `ErrorResponse` or `NoticeResponse` payload.
///
/// The field order is the wire order, and values are decoded byte for byte,
/// because callers compare them with the raw payload.
pub fn message_fields(payload: &[u8]) -> Result<Vec<MessageField>, PostgresError> {
    let mut fields = Vec::new();
    let mut offset = 0;
    while offset < payload.len() && payload[offset] != 0 {
        let kind = payload[offset];
        offset += 1;
        let Some(end) = payload[offset..].iter().position(|byte| *byte == 0).map(|index| index + offset) else {
            return Err(PostgresError::Protocol("malformed PostgreSQL error response".into()));
        };
        fields.push(MessageField::new(kind, String::from_utf8_lossy(&payload[offset..end]).into_owned()));
        offset = end + 1;
    }
    Ok(fields)
}

/// Decodes one `NoticeResponse` payload, keeping every field in wire order.
pub fn notice_response(payload: &[u8]) -> Result<Notice, PostgresError> {
    message_fields(payload).map(Notice::new)
}

/// Decodes an `ErrorResponse` payload into the server error shape.
pub fn error_response(payload: &[u8]) -> PostgresError {
    let fields = match message_fields(payload) {
        Ok(fields) => fields,
        Err(error) => return error,
    };
    let code = fields
        .iter()
        .find(|field| field.kind == b'C')
        .map(|field| field.value.clone())
        .unwrap_or_else(|| "unknown".to_owned());
    let message = fields
        .iter()
        .find(|field| field.kind == b'M')
        .map(|field| field.value.clone())
        .unwrap_or_else(|| "PostgreSQL error".to_owned());
    PostgresError::Server { code, message, fields }
}

/// One column of a `RowDescription`, including the type and format the server
/// actually applied to the following `DataRow` values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDescription {
    pub name: String,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
    /// `0` for text and `1` for binary.
    pub format: u16,
}

/// Decodes a full `RowDescription` in wire order.
pub fn row_description(payload: &[u8]) -> Result<Vec<ColumnDescription>, PostgresError> {
    if payload.len() < 2 {
        return Err(truncated("RowDescription"));
    }
    let count = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut columns = Vec::with_capacity(count);
    let mut offset = 2;
    for _ in 0..count {
        let Some(end) = payload[offset..].iter().position(|byte| *byte == 0).map(|index| index + offset) else {
            return Err(truncated("column description"));
        };
        if end + 19 > payload.len() {
            return Err(truncated("column description"));
        }
        // After the name and its NUL: tableOID(4), attribute(2), typeOID(4),
        // typeSize(2), typeModifier(4) and format(2).
        let base = end + 1;
        let type_oid =
            u32::from_be_bytes([payload[base + 6], payload[base + 7], payload[base + 8], payload[base + 9]]);
        let type_size = i16::from_be_bytes([payload[base + 10], payload[base + 11]]);
        let type_modifier =
            i32::from_be_bytes([payload[base + 12], payload[base + 13], payload[base + 14], payload[base + 15]]);
        let format = u16::from_be_bytes([payload[base + 16], payload[base + 17]]);
        columns.push(ColumnDescription {
            name: String::from_utf8_lossy(&payload[offset..end]).into_owned(),
            type_oid,
            type_size,
            type_modifier,
            format,
        });
        offset = base + 18;
    }
    if offset != payload.len() {
        return Err(PostgresError::Protocol("trailing RowDescription bytes".into()));
    }
    Ok(columns)
}

/// Column names carried by a `RowDescription`. Binary formats are rejected.
pub fn column_names(payload: &[u8]) -> Result<Vec<String>, PostgresError> {
    let columns = row_description(payload)?;
    let mut names = Vec::with_capacity(columns.len());
    for column in columns {
        if column.format != 0 {
            return Err(PostgresError::Protocol("binary PostgreSQL results are unsupported".into()));
        }
        names.push(column.name);
    }
    Ok(names)
}

/// Cell values carried by a `DataRow`. `-1` lengths become SQL `NULL`.
pub fn data_row(payload: &[u8], columns: usize) -> Result<Vec<Option<String>>, PostgresError> {
    if payload.len() < 2 || u16::from_be_bytes([payload[0], payload[1]]) as usize != columns {
        return Err(PostgresError::Protocol("invalid DataRow column count".into()));
    }
    let mut values = Vec::with_capacity(columns);
    let mut offset = 2;
    for _ in 0..columns {
        if offset + 4 > payload.len() {
            return Err(truncated("DataRow length"));
        }
        let length = i32::from_be_bytes([payload[offset], payload[offset + 1], payload[offset + 2], payload[offset + 3]]);
        offset += 4;
        if length == -1 {
            values.push(None);
            continue;
        }
        if length < 0 || offset + length as usize > payload.len() {
            return Err(PostgresError::Protocol("invalid DataRow length".into()));
        }
        let end = offset + length as usize;
        values.push(Some(String::from_utf8_lossy(&payload[offset..end]).into_owned()));
        offset = end;
    }
    if offset != payload.len() {
        return Err(PostgresError::Protocol("trailing DataRow bytes".into()));
    }
    Ok(values)
}

/// Cell values carried by a `DataRow`, decoded according to the type and format
/// each column declared in `RowDescription`.
pub fn data_row_with_formats(
    payload: &[u8],
    columns: &[ColumnDescription],
) -> Result<Vec<Option<String>>, PostgresError> {
    if payload.len() < 2 || u16::from_be_bytes([payload[0], payload[1]]) as usize != columns.len() {
        return Err(PostgresError::Protocol("invalid DataRow column count".into()));
    }
    let mut values = Vec::with_capacity(columns.len());
    let mut offset = 2;
    for column in columns {
        if offset + 4 > payload.len() {
            return Err(truncated("DataRow length"));
        }
        let length = i32::from_be_bytes([payload[offset], payload[offset + 1], payload[offset + 2], payload[offset + 3]]);
        offset += 4;
        if length == -1 {
            values.push(None);
            continue;
        }
        if length < 0 || offset + length as usize > payload.len() {
            return Err(PostgresError::Protocol("invalid DataRow length".into()));
        }
        let end = offset + length as usize;
        let bytes = &payload[offset..end];
        values.push(Some(if column.format == 0 {
            String::from_utf8_lossy(bytes).into_owned()
        } else {
            binary_value(column.type_oid, bytes)?
        }));
        offset = end;
    }
    if offset != payload.len() {
        return Err(PostgresError::Protocol("trailing DataRow bytes".into()));
    }
    Ok(values)
}

fn binary_value(oid: u32, bytes: &[u8]) -> Result<String, PostgresError> {
    match oid {
        16 => {
            if bytes.len() != 1 {
                return Err(binary_size_error(oid, 1, bytes.len()));
            }
            Ok(if bytes[0] == 0 { "f" } else { "t" }.to_owned())
        }
        17 => Ok(encode_bytea(bytes)),
        20 => {
            if bytes.len() != 8 {
                return Err(binary_size_error(oid, 8, bytes.len()));
            }
            Ok(i64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]).to_string())
        }
        21 => {
            if bytes.len() != 2 {
                return Err(binary_size_error(oid, 2, bytes.len()));
            }
            Ok(i16::from_be_bytes([bytes[0], bytes[1]]).to_string())
        }
        23 => {
            if bytes.len() != 4 {
                return Err(binary_size_error(oid, 4, bytes.len()));
            }
            Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string())
        }
        // text, varchar, bpchar, name and json all carry their text form.
        19 | 25 | 114 | 1042 | 1043 => Ok(String::from_utf8_lossy(bytes).into_owned()),
        1700 => numeric_value(bytes),
        1082 => date_value(bytes),
        1083 => time_value(bytes),
        1114 => timestamp_value(bytes),
        2950 => uuid_value(bytes),
        3802 => match bytes.split_first() {
            Some((1, json)) => Ok(String::from_utf8_lossy(json).into_owned()),
            _ => Err(PostgresError::Unsupported("binary-jsonb-version".into())),
        },
        _ => Err(PostgresError::Unsupported(format!("binary-result-type-{oid}"))),
    }
}

fn binary_size_error(oid: u32, expected: usize, actual: usize) -> PostgresError {
    PostgresError::Protocol(format!("binary type {oid} expects {expected} bytes but got {actual}"))
}

/// PostgreSQL stores `numeric` as base-10000 groups with an explicit scale.
fn numeric_value(bytes: &[u8]) -> Result<String, PostgresError> {
    if bytes.len() < 8 {
        return Err(truncated("binary numeric"));
    }
    let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]) as i32;
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]) as i32;
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
    let dscale = u16::from_be_bytes([bytes[6], bytes[7]]) as usize;
    if ndigits < 0 || bytes.len() != 8 + ndigits as usize * 2 {
        return Err(PostgresError::Protocol("invalid binary numeric digits".into()));
    }
    match sign {
        0xC000 => return Ok("NaN".into()),
        0xD000 => return Ok("Infinity".into()),
        0xF000 => return Ok("-Infinity".into()),
        0x0000 | 0x4000 => {}
        other => return Err(PostgresError::Protocol(format!("invalid binary numeric sign {other}"))),
    }
    let digits: Vec<u16> = (0..ndigits as usize)
        .map(|index| {
            let offset = 8 + index * 2;
            u16::from_be_bytes([bytes[offset], bytes[offset + 1]])
        })
        .collect();

    let mut integer = String::new();
    if weight >= 0 {
        for group in 0..=weight {
            let value = digits.get(group as usize).copied().unwrap_or(0);
            if integer.is_empty() {
                integer.push_str(&value.to_string());
            } else {
                integer.push_str(&format!("{value:04}"));
            }
        }
    } else {
        integer.push('0');
    }

    let mut fraction = String::new();
    let mut group = weight + 1;
    while fraction.len() < dscale {
        let value = if group >= 0 { digits.get(group as usize).copied().unwrap_or(0) } else { 0 };
        fraction.push_str(&format!("{value:04}"));
        group += 1;
    }
    fraction.truncate(dscale);

    let mut text = String::new();
    if sign == 0x4000 && ndigits > 0 {
        text.push('-');
    }
    text.push_str(&integer);
    if dscale > 0 {
        text.push('.');
        text.push_str(&fraction);
    }
    Ok(text)
}

/// Days between 1970-01-01 and 2000-01-01, the epoch PostgreSQL stores dates in.
const POSTGRES_EPOCH_DAYS: i64 = 10_957;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SECOND;

/// Howard Hinnant's civil date algorithm, over days since 1970-01-01.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn date_value(bytes: &[u8]) -> Result<String, PostgresError> {
    if bytes.len() != 4 {
        return Err(binary_size_error(1082, 4, bytes.len()));
    }
    let days = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64;
    let (year, month, day) = civil_from_days(days + POSTGRES_EPOCH_DAYS);
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn format_time_of_day(micros: i64) -> String {
    let total_hours = micros / (3_600 * MICROS_PER_SECOND);
    let minutes = (micros / (60 * MICROS_PER_SECOND)) % 60;
    let seconds = (micros / MICROS_PER_SECOND) % 60;
    let fraction = micros % MICROS_PER_SECOND;
    if fraction == 0 {
        format!("{total_hours:02}:{minutes:02}:{seconds:02}")
    } else {
        let mut digits = format!("{fraction:06}");
        while digits.ends_with('0') {
            digits.pop();
        }
        format!("{total_hours:02}:{minutes:02}:{seconds:02}.{digits}")
    }
}

fn time_value(bytes: &[u8]) -> Result<String, PostgresError> {
    if bytes.len() != 8 {
        return Err(binary_size_error(1083, 8, bytes.len()));
    }
    let micros = i64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]);
    if !(0..MICROS_PER_DAY).contains(&micros) {
        return Err(PostgresError::Protocol("binary time is outside a single day".into()));
    }
    Ok(format_time_of_day(micros))
}

fn timestamp_value(bytes: &[u8]) -> Result<String, PostgresError> {
    if bytes.len() != 8 {
        return Err(binary_size_error(1114, 8, bytes.len()));
    }
    let micros = i64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]);
    let days = micros.div_euclid(MICROS_PER_DAY) + POSTGRES_EPOCH_DAYS;
    let time = micros.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    Ok(format!("{year:04}-{month:02}-{day:02} {}", format_time_of_day(time)))
}

fn uuid_value(bytes: &[u8]) -> Result<String, PostgresError> {
    if bytes.len() != 16 {
        return Err(binary_size_error(2950, 16, bytes.len()));
    }
    use core::fmt::Write as _;
    let mut text = String::with_capacity(36);
    for (index, byte) in bytes.iter().enumerate() {
        if index == 4 || index == 6 || index == 8 || index == 10 {
            text.push('-');
        }
        write!(text, "{byte:02x}").expect("writing into a String cannot fail");
    }
    Ok(text)
}

/// Command tag carried by a `CommandComplete`, such as `SELECT 1`.
pub fn command_tag(payload: &[u8]) -> Result<String, PostgresError> {
    if payload.len() < 2 || payload[payload.len() - 1] != 0 {
        return Err(PostgresError::Protocol("invalid CommandComplete".into()));
    }
    Ok(String::from_utf8_lossy(&payload[..payload.len() - 1]).into_owned())
}

// -------------------------------------------------------------------------------
// Extended query protocol: Parse, Bind, Describe, Execute, Close and Sync.
// ---------------------------------------------------------------------------

/// The wire encodes parameter counts in 16 bits, so this is the hard ceiling for
/// every declared or bound parameter list.
pub const MAX_PARAMETERS: usize = 65_535;

/// OID of the named parameter type, as published by the PostgreSQL catalog.
/// `None` means the name is not part of the supported mapping.
pub fn parameter_type_oid(name: &str) -> Option<u32> {
    Some(match name {
        "bool" => 16,
        "bytea" => 17,
        "int8" | "bigint" => 20,
        "int2" | "smallint" => 21,
        "int4" | "int" | "integer" => 23,
        "text" => 25,
        "varchar" => 1043,
        "float4" | "real" => 700,
        "float8" | "double precision" => 701,
        "numeric" | "decimal" => 1700,
        "date" => 1082,
        "time" => 1083,
        "timestamp" => 1114,
        "timestamptz" => 1184,
        "uuid" => 2950,
        "json" => 114,
        "jsonb" => 3802,
        _ => return None,
    })
}

/// Text-format `bytea` encoding: `\x` followed by lowercase hexadecimal.
pub fn encode_bytea(bytes: &[u8]) -> String {
    use core::fmt::Write as _;
    let mut text = String::with_capacity(2 + bytes.len() * 2);
    text.push_str("\\x");
    for byte in bytes {
        write!(text, "{byte:02x}").expect("writing into a String cannot fail");
    }
    text
}

fn push_text(payload: &mut Vec<u8>, value: &str) {
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
}

fn frame(tag: u8, payload: Vec<u8>) -> Result<Vec<u8>, PostgresError> {
    encode_message(tag, &payload, MAX_MESSAGE_BYTES)
        .map_err(|error| PostgresError::Protocol(format!("extended message framing failed: {error:?}")))
}

/// One `Parse` message. Type OIDs may be empty so the server infers them from
/// the statement, and may be shorter than the highest `$n` the statement uses.
pub fn parse_packet(name: &str, sql: &str, param_types: &[u32]) -> Result<Vec<u8>, PostgresError> {
    if param_types.len() > MAX_PARAMETERS {
        return Err(PostgresError::Protocol(format!(
            "at most {MAX_PARAMETERS} parameter types can be declared"
        )));
    }
    let mut payload = Vec::with_capacity(name.len() + sql.len() + 4 + param_types.len() * 4);
    push_text(&mut payload, name);
    push_text(&mut payload, sql);
    payload.extend_from_slice(&(param_types.len() as u16).to_be_bytes());
    for oid in param_types {
        payload.extend_from_slice(&oid.to_be_bytes());
    }
    frame(b'P', payload)
}

/// One `Bind` message carrying text parameters. `None` is SQL `NULL`.
///
/// `result_format` is `0` for text result columns and `1` for binary ones, which
/// the wire expresses as one format code shared by every column.
pub fn bind_packet(
    portal: &str,
    statement: &str,
    params: &[Option<&str>],
    result_format: u16,
) -> Result<Vec<u8>, PostgresError> {
    if result_format > 1 {
        return Err(PostgresError::Protocol(format!("invalid result format code {result_format}")));
    }
    let values: Vec<Option<&[u8]>> = params.iter().map(|value| value.map(str::as_bytes)).collect();
    let result_formats: &[u16] = if result_format == 0 { &[] } else { core::slice::from_ref(&result_format) };
    bind_packet_with_formats(portal, statement, &values, &[], result_formats)
}

/// One `Bind` message with explicit parameter and result format codes.
///
/// `param_formats` and `result_formats` follow the wire shape: an empty slice
/// means every value uses text format, a single code applies to every value,
/// and a longer slice carries one code per value. `None` inside `params` is
/// SQL `NULL` and its format code is ignored by the server.
pub fn bind_packet_with_formats(
    portal: &str,
    statement: &str,
    params: &[Option<&[u8]>],
    param_formats: &[u16],
    result_formats: &[u16],
) -> Result<Vec<u8>, PostgresError> {
    if params.len() > MAX_PARAMETERS {
        return Err(PostgresError::Protocol(format!(
            "at most {MAX_PARAMETERS} parameters can be bound"
        )));
    }
    if !param_formats.is_empty() && param_formats.len() != 1 && param_formats.len() != params.len() {
        return Err(PostgresError::Protocol("parameter format count must be 0, 1 or the parameter count".into()));
    }
    if !result_formats.is_empty() && result_formats.len() > MAX_PARAMETERS {
        return Err(PostgresError::Protocol(format!(
            "at most {MAX_PARAMETERS} result formats can be declared"
        )));
    }
    if param_formats.iter().any(|code| *code > 1) || result_formats.iter().any(|code| *code > 1) {
        return Err(PostgresError::Protocol("format codes are 0 for text and 1 for binary".into()));
    }
    let mut payload = Vec::new();
    push_text(&mut payload, portal);
    push_text(&mut payload, statement);
    payload.extend_from_slice(&(param_formats.len() as u16).to_be_bytes());
    for code in param_formats {
        payload.extend_from_slice(&code.to_be_bytes());
    }
    payload.extend_from_slice(&(params.len() as u16).to_be_bytes());
    for param in params {
        match param {
            None => payload.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(value) => {
                let length = i32::try_from(value.len())
                    .map_err(|_| PostgresError::Protocol("parameter value is too long".into()))?;
                payload.extend_from_slice(&length.to_be_bytes());
                payload.extend_from_slice(value);
            }
        }
    }
    payload.extend_from_slice(&(result_formats.len() as u16).to_be_bytes());
    for code in result_formats {
        payload.extend_from_slice(&code.to_be_bytes());
    }
    frame(b'B', payload)
}

/// One `Describe` message for a prepared statement (`b'S'`) or a portal (`b'P'`).
pub fn describe_packet(target: u8, name: &str) -> Result<Vec<u8>, PostgresError> {
    if target != b'S' && target != b'P' {
        return Err(PostgresError::Protocol("describe target must be S or P".into()));
    }
    let mut payload = vec![target];
    push_text(&mut payload, name);
    frame(b'D', payload)
}

/// One `Execute` message. `max_rows` of `0` runs the whole portal, and a
/// positive limit asks the server to suspend the portal after that many rows.
pub fn execute_packet(portal: &str, max_rows: i32) -> Result<Vec<u8>, PostgresError> {
    if max_rows < 0 {
        return Err(PostgresError::Protocol("execute row limit cannot be negative".into()));
    }
    let mut payload = Vec::new();
    push_text(&mut payload, portal);
    payload.extend_from_slice(&max_rows.to_be_bytes());
    frame(b'E', payload)
}

/// One `Close` message for a prepared statement (`b'S'`) or a portal (`b'P'`).
pub fn close_packet(target: u8, name: &str) -> Result<Vec<u8>, PostgresError> {
    if target != b'S' && target != b'P' {
        return Err(PostgresError::Protocol("close target must be S or P".into()));
    }
    let mut payload = vec![target];
    push_text(&mut payload, name);
    frame(b'C', payload)
}

// ---------------------------------------------------------------------------
// COPY subprotocol: server `G`/`H`/`W`, client `d`/`c`/`f`.
// ---------------------------------------------------------------------------

/// Overall and per-column formats carried by `CopyInResponse`,
/// `CopyOutResponse` and `CopyBothResponse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyResponse {
    /// `0` for text and `1` for binary.
    pub format: u8,
    pub columns: u16,
    /// One format code per column.
    pub column_formats: Vec<u16>,
}

/// Decodes a `CopyInResponse`, `CopyOutResponse` or `CopyBothResponse` payload.
pub fn copy_response(payload: &[u8]) -> Result<CopyResponse, PostgresError> {
    if payload.len() < 3 {
        return Err(PostgresError::Protocol("truncated PostgreSQL copy response".into()));
    }
    let format = payload[0];
    if format > 1 {
        return Err(PostgresError::Protocol(format!("invalid PostgreSQL copy format {format}")));
    }
    let columns = u16::from_be_bytes([payload[1], payload[2]]);
    let expected = 3 + columns as usize * 2;
    if payload.len() != expected {
        return Err(PostgresError::Protocol("PostgreSQL copy response column count does not match its length".into()));
    }
    let mut column_formats = Vec::with_capacity(columns as usize);
    for index in 0..columns as usize {
        let offset = 3 + index * 2;
        let code = u16::from_be_bytes([payload[offset], payload[offset + 1]]);
        if code > 1 {
            return Err(PostgresError::Protocol(format!("invalid PostgreSQL copy column format {code}")));
        }
        column_formats.push(code);
    }
    Ok(CopyResponse { format, columns, column_formats })
}

/// One `CopyData` message carrying a chunk of the copy stream.
pub fn copy_data_packet(data: &[u8]) -> Result<Vec<u8>, PostgresError> {
    frame(b'd', data.to_vec())
}

/// One `CopyDone` message.
pub fn copy_done_packet() -> Vec<u8> {
    vec![b'c', 0, 0, 0, 4]
}

/// One `CopyFail` message carrying a human readable reason.
pub fn copy_fail_packet(reason: &str) -> Result<Vec<u8>, PostgresError> {
    let mut payload = Vec::with_capacity(reason.len() + 1);
    push_text(&mut payload, reason);
    frame(b'f', payload)
}

/// One `Sync` message, which ends every extended-protocol round trip.
pub fn sync_packet() -> Vec<u8> {
    let mut packet = Vec::with_capacity(5);
    packet.push(b'S');
    packet.extend_from_slice(&4_u32.to_be_bytes());
    packet
}

// -------------------------------------------------------------------------------
// `CancelRequest`: a startup-position packet sent on its own connection.
// ---------------------------------------------------------------------------

/// Code carried where the startup packet carries the protocol version.
pub const CANCEL_REQUEST_CODE: u32 = 808_771_02;
/// Every `CancelRequest` packet is exactly this long.
pub const CANCEL_REQUEST_BYTES: usize = 16;

/// One `CancelRequest`, carrying the `pid` and `secret key` from `BackendKeyData`.
///
/// The packet has the startup layout instead of a tagged message, so the caller
/// writes it on a fresh socket and closes it: the server answers nothing.
pub fn cancel_request_packet(process_id: i32, secret_key: i32) -> Vec<u8> {
    let mut packet = Vec::with_capacity(CANCEL_REQUEST_BYTES);
    packet.extend_from_slice(&(CANCEL_REQUEST_BYTES as u32).to_be_bytes());
    packet.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
    packet.extend_from_slice(&process_id.to_be_bytes());
    packet.extend_from_slice(&secret_key.to_be_bytes());
    packet
}

/// Parameter OIDs carried by a `ParameterDescription`, in declaration order.
/// The server sends OID `0` for parameters whose type it could not infer.
pub fn parameter_description(payload: &[u8]) -> Result<Vec<u32>, PostgresError> {
    if payload.len() < 2 {
        return Err(truncated("ParameterDescription"));
    }
    let count = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    if payload.len() != 2 + count * 4 {
        return Err(PostgresError::Protocol("invalid ParameterDescription length".into()));
    }
    let mut oids = Vec::with_capacity(count);
    for index in 0..count {
        let offset = 2 + index * 4;
        oids.push(u32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]));
    }
    Ok(oids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_packet_starts_with_version_and_ends_once() {
        let packet = startup_packet("postgres", "postgres");
        assert_eq!(&packet[4..8], &PROTOCOL_VERSION.to_be_bytes());
        assert_eq!(packet.len() as u32, u32::from_be_bytes([packet[0], packet[1], packet[2], packet[3]]));
        assert_eq!(packet[packet.len() - 1], 0);
    }

    #[test]
    fn password_packet_is_a_null_terminated_password_message() {
        let packet = password_packet("md5deadbeef").unwrap();
        assert_eq!(packet[0], b'p');
        assert_eq!(&packet[1..5], &((packet.len() - 1) as u32).to_be_bytes());
        assert_eq!(&packet[5..], b"md5deadbeef\0");
    }

    #[test]
    fn sasl_initial_packet_names_the_mechanism_then_the_response() {
        let response = b"n,,n=user,r=abc";
        let packet = sasl_initial_packet("SCRAM-SHA-256", response).unwrap();
        assert_eq!(packet[0], b'p');
        assert_eq!(&packet[1..5], &((packet.len() - 1) as u32).to_be_bytes());
        let body = &packet[5..];
        assert_eq!(&body[..13], b"SCRAM-SHA-256");
        assert_eq!(body[13], 0);
        assert_eq!(&body[14..18], &(response.len() as u32).to_be_bytes());
        assert_eq!(&body[18..], response);
    }

    #[test]
    fn error_response_keeps_code_and_message() {
        let error = error_response(b"SERROR\0C42P01\0Mrelation does not exist\0\0");
        assert_eq!(error.code(), Some("42P01"));
        assert_eq!(error.message(), "relation does not exist");
    }

    #[test]
    fn message_fields_keep_wire_order_and_every_selector() {
        let payload = b"SERROR\0VERROR\0C22P02\0Minvalid input syntax\0P8\0Ddetail text\0Hhint text\0\0";
        let fields = message_fields(payload).unwrap();
        let ordered: Vec<(u8, &str)> = fields.iter().map(|field| (field.kind, field.value.as_str())).collect();
        assert_eq!(
            ordered,
            vec![
                (b'S', "ERROR"),
                (b'V', "ERROR"),
                (b'C', "22P02"),
                (b'M', "invalid input syntax"),
                (b'P', "8"),
                (b'D', "detail text"),
                (b'H', "hint text"),
            ]
        );
    }

    #[test]
    fn error_response_exposes_position_detail_and_hint() {
        let error = error_response(b"SERROR\0VERROR\0C22P02\0Minvalid input\0P8\0Ddetail\0Hhint\0\0");
        assert_eq!(error.field(b'P'), Some("8"));
        assert_eq!(error.field(b'D'), Some("detail"));
        assert_eq!(error.field(b'H'), Some("hint"));
    }

    #[test]
    fn notice_response_keeps_severity_code_and_message() {
        let notice = notice_response(b"SNOTICE\0VNOTICE\0C00000\0Mtable \"x\" does not exist, skipping\0\0").unwrap();
        assert_eq!(notice.severity(), Some("NOTICE"));
        assert_eq!(notice.code(), Some("00000"));
        assert_eq!(notice.message(), Some("table \"x\" does not exist, skipping"));
    }

    #[test]
    fn data_row_maps_negative_length_to_null() {
        let mut payload = vec![0, 2, 0, 0, 0, 1, b'7', 0xff, 0xff, 0xff, 0xff];
        assert_eq!(data_row(&payload, 2).unwrap(), vec![Some("7".into()), None]);
        payload[1] = 3;
        assert!(data_row(&payload, 3).is_err());
    }

    fn body(packet: &[u8]) -> &[u8] {
        &packet[5..]
    }

    #[test]
    fn parse_packet_carries_names_and_type_oids() {
        let packet = parse_packet("", "SELECT $1::int4", &[23, 25]).unwrap();
        assert_eq!(packet[0], b'P');
        assert_eq!(packet.len() as u32 - 1, u32::from_be_bytes([packet[1], packet[2], packet[3], packet[4]]));
        let payload = body(&packet);
        assert_eq!(payload[0], 0);
        assert_eq!(&payload[1..17], b"SELECT $1::int4\0");
        assert_eq!(u16::from_be_bytes([payload[17], payload[18]]), 2);
        assert_eq!(u32::from_be_bytes([payload[19], payload[20], payload[21], payload[22]]), 23);
        assert_eq!(u32::from_be_bytes([payload[23], payload[24], payload[25], payload[26]]), 25);
    }

    #[test]
    fn parse_packet_refuses_more_types_than_the_wire_allows() {
        let types = vec![23_u32; MAX_PARAMETERS + 1];
        let error = parse_packet("", "SELECT 1", &types).unwrap_err();
        assert_eq!(error.message(), "at most 65535 parameter types can be declared");
    }

    #[test]
    fn bind_packet_marks_nulls_and_keeps_text_values() {
        let packet = bind_packet("", "", &[Some("42"), None, Some("héllo")], 0).unwrap();
        assert_eq!(packet[0], b'B');
        let payload = body(&packet);
        assert_eq!(&payload[..6], b"\0\0\0\0\0\x03"); // portal, statement, no formats, three parameters
        assert_eq!(i32::from_be_bytes([payload[6], payload[7], payload[8], payload[9]]), 2);
        assert_eq!(&payload[10..12], b"42");
        assert_eq!(i32::from_be_bytes([payload[12], payload[13], payload[14], payload[15]]), -1);
        assert_eq!(i32::from_be_bytes([payload[16], payload[17], payload[18], payload[19]]), "héllo".len() as i32);
        assert_eq!(payload[20], b'h');
        assert_eq!(&payload[26..], b"\0\0"); // text result formats
    }

    #[test]
    fn bind_packet_carries_a_single_binary_result_format() {
        let packet = bind_packet("", "", &[Some("42")], 1).unwrap();
        let payload = body(&packet);
        assert_eq!(&payload[..6], b"\0\0\0\0\0\x01"); // portal, statement, no parameter formats
        assert_eq!(i32::from_be_bytes([payload[6], payload[7], payload[8], payload[9]]), 2);
        assert_eq!(&payload[10..12], b"42");
        assert_eq!(&payload[12..], b"\0\x01\0\x01"); // one result format code, binary
        assert!(bind_packet("", "", &[], 2).is_err());
    }

    #[test]
    fn bind_packet_with_formats_mixes_text_and_binary_parameters() {
        let params: [Option<&[u8]>; 3] = [Some(b"42"), None, Some(&[0x2a, 0x00, 0x00, 0x00])];
        let packet = bind_packet_with_formats("p1", "s1", &params, &[0, 1, 1], &[1]).unwrap();
        let payload = body(&packet);
        assert_eq!(payload[0], b'p');
        assert_eq!(payload[1], b'1');
        assert_eq!(payload[2], 0);
        assert_eq!(payload[3], b's');
        assert_eq!(payload[4], b'1');
        assert_eq!(payload[5], 0);
        let formats = u16::from_be_bytes([payload[6], payload[7]]);
        assert_eq!(formats, 3);
        assert_eq!(&payload[8..14], b"\0\0\0\x01\0\x01");
        let count = u16::from_be_bytes([payload[14], payload[15]]);
        assert_eq!(count, 3);
        assert_eq!(&payload[16..22], &[0, 0, 0, 2, b'4', b'2']);
        assert_eq!(&payload[22..26], &[0xff, 0xff, 0xff, 0xff]);
        assert_eq!(&payload[26..30], &[0, 0, 0, 4]);
        assert_eq!(&payload[30..34], &[0x2a, 0x00, 0x00, 0x00]);
        assert_eq!(&payload[34..], &[0, 1, 0, 1]);
    }

    #[test]
    fn bind_packet_with_formats_refuses_mismatched_counts() {
        let params: [Option<&[u8]>; 2] = [Some(b"1"), Some(b"2")];
        let error = bind_packet_with_formats("", "", &params, &[0, 1, 1], &[]).unwrap_err();
        assert_eq!(error.message(), "parameter format count must be 0, 1 or the parameter count");
        assert!(bind_packet_with_formats("", "", &params, &[2], &[]).is_err());
    }

    #[test]
    fn bind_packet_refuses_more_parameters_than_the_wire_allows() {
        let params = vec![None; MAX_PARAMETERS + 1];
        let error = bind_packet("", "", &params, 0).unwrap_err();
        assert_eq!(error.message(), "at most 65535 parameters can be bound");
    }

    #[test]
    fn describe_execute_and_close_frames_are_well_formed() {
        let describe = describe_packet(b'S', "s1").unwrap();
        assert_eq!(describe, b"D\0\0\0\x08Ss1\0".to_vec());
        let execute = execute_packet("", 0).unwrap();
        assert_eq!(execute, b"E\0\0\0\x09\0\0\0\0\0".to_vec());
        let close = close_packet(b'S', "s1").unwrap();
        assert_eq!(close, b"C\0\0\0\x08Ss1\0".to_vec());
        assert_eq!(sync_packet(), b"S\0\0\0\x04".to_vec());
        assert!(describe_packet(b'X', "s1").is_err());
    }

    #[test]
    fn execute_packet_carries_a_row_limit() {
        let packet = execute_packet("", 5).unwrap();
        assert_eq!(packet.len(), 10);
        assert_eq!(u32::from_be_bytes([packet[1], packet[2], packet[3], packet[4]]), 9);
        assert_eq!(&packet[5..], &[0, 0, 0, 0, 5]);
        assert!(execute_packet("", -1).is_err());
    }

    #[test]
    fn parameter_description_reads_oids_in_order() {
        assert_eq!(
            parameter_description(&[0, 2, 0, 0, 0, 23, 0, 0, 6, 164]).unwrap(),
            vec![23, 1700]
        );
        assert!(parameter_description(&[0, 2, 0, 0, 0, 23]).is_err());
        assert!(parameter_description(&[]).is_err());
    }

    #[test]
    fn parameter_type_oid_matches_the_catalog() {
        assert_eq!(parameter_type_oid("int4"), Some(23));
        assert_eq!(parameter_type_oid("timestamptz"), Some(1184));
        assert_eq!(parameter_type_oid("nope"), None);
    }

    #[test]
    fn cancel_request_carries_pid_and_secret_in_sixteen_bytes() {
        let packet = cancel_request_packet(1234, 5678);
        assert_eq!(packet, b"\0\0\0\x10\x04\xd2\x16\x2e\0\0\x04\xd2\0\0\x16\x2e".to_vec());
        assert_eq!(packet.len(), CANCEL_REQUEST_BYTES);
        assert_eq!(u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]), CANCEL_REQUEST_CODE);
        assert_eq!(i32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]), 1234);
        assert_eq!(i32::from_be_bytes([packet[12], packet[13], packet[14], packet[15]]), 5678);
        assert_ne!(cancel_request_packet(0, 0), packet);
    }

    #[test]
    fn bytea_text_is_backslash_x_and_lowercase_hex() {
        assert_eq!(encode_bytea(&[0x00, 0x41, 0xff]), "\\x0041ff");
        assert_eq!(encode_bytea(&[]), "\\x");
    }

    fn description_payload(columns: &[(&str, u32, i16, i32, u16)]) -> Vec<u8> {
        let mut payload = Vec::with_capacity(2 + columns.len() * 30);
        payload.extend_from_slice(&(columns.len() as u16).to_be_bytes());
        for (name, oid, size, modifier, format) in columns {
            payload.extend_from_slice(name.as_bytes());
            payload.push(0);
            payload.extend_from_slice(&0_u32.to_be_bytes());
            payload.extend_from_slice(&0_u16.to_be_bytes());
            payload.extend_from_slice(&oid.to_be_bytes());
            payload.extend_from_slice(&size.to_be_bytes());
            payload.extend_from_slice(&modifier.to_be_bytes());
            payload.extend_from_slice(&format.to_be_bytes());
        }
        payload
    }

    fn data_row_payload(cells: &[Option<&[u8]>]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(cells.len() as u16).to_be_bytes());
        for cell in cells {
            match cell {
                None => payload.extend_from_slice(&(-1_i32).to_be_bytes()),
                Some(bytes) => {
                    payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    payload.extend_from_slice(bytes);
                }
            }
        }
        payload
    }

    #[test]
    fn row_description_reads_type_and_format_of_every_column() {
        let payload = description_payload(&[("a", 23, 4, -1, 1), ("b", 25, -1, -1, 0)]);
        let columns = row_description(&payload).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "a");
        assert_eq!(columns[0].type_oid, 23);
        assert_eq!(columns[0].type_size, 4);
        assert_eq!(columns[0].format, 1);
        assert_eq!(columns[1].name, "b");
        assert_eq!(columns[1].type_oid, 25);
        assert!(column_names(&payload).is_err());
    }

    #[test]
    fn data_row_with_formats_decodes_the_pglite_binary_payloads() {
        let binaries: &[(u32, &[u8], &str)] = &[
            (23, &[0, 0, 0, 42], "42"),
            (21, &[0, 7], "7"),
            (20, &[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], "9223372036854775807"),
            (16, &[1], "t"),
            (25, &[0x68, 0xc3, 0xa9, 0x6c, 0x6c, 0x6f], "héllo"),
            (17, &[0x00, 0x41, 0xff], "\\x0041ff"),
            (1700, &[0, 6, 0, 0, 0, 0, 0, 0x12, 0, 1, 9, 0x29, 0x1a, 0x85, 0, 0x7b, 0x11, 0xd7, 0x22, 0xc4], "1.234567890123456789"),
            (1082, &[0, 0, 0x22, 0x79], "2024-02-29"),
            (1083, &[0, 0, 0, 0x0a, 0x8b, 0xe6, 0x26, 0x08], "12:34:56.789"),
            (1114, &[0, 2, 0xb5, 0x83, 0x41, 0x72, 0x86, 0x08], "2024-02-29 12:34:56.789"),
            (2950, &[0x0f, 0x8f, 0xad, 0x5b, 0xd9, 0xcb, 0x46, 0x9f, 0xa1, 0x65, 0x70, 0x86, 0x77, 0x28, 0x95, 0x0e], "0f8fad5b-d9cb-469f-a165-70867728950e"),
            (3802, &[1, b'{', b'"', b'a', b'"', b':', b' ', b'1', b'}'], "{\"a\": 1}"),
        ];
        for (oid, bytes, expected) in binaries {
            let columns = vec![ColumnDescription {
                name: "c".into(),
                type_oid: *oid,
                type_size: 0,
                type_modifier: -1,
                format: 1,
            }];
            let row = data_row_with_formats(&data_row_payload(&[Some(*bytes)]), &columns).unwrap();
            assert_eq!(row, vec![Some((*expected).to_owned())], "oid {oid}");
        }
    }

    #[test]
    fn data_row_with_formats_mixes_text_binary_and_null() {
        let columns = vec![
            ColumnDescription { name: "a".into(), type_oid: 23, type_size: 4, type_modifier: -1, format: 1 },
            ColumnDescription { name: "b".into(), type_oid: 25, type_size: -1, type_modifier: -1, format: 0 },
            ColumnDescription { name: "c".into(), type_oid: 23, type_size: 4, type_modifier: -1, format: 1 },
        ];
        let payload = data_row_payload(&[Some(&[0, 0, 0, 42]), Some(b"plain"), None]);
        assert_eq!(
            data_row_with_formats(&payload, &columns).unwrap(),
            vec![Some("42".into()), Some("plain".into()), None]
        );
    }

    #[test]
    fn data_row_with_formats_refuses_unsupported_binary_types() {
        let columns = vec![
            ColumnDescription { name: "f".into(), type_oid: 701, type_size: 8, type_modifier: -1, format: 1 },
        ];
        let error = data_row_with_formats(&data_row_payload(&[Some(&[0, 0, 0, 0, 0, 0, 0, 0])]), &columns)
            .unwrap_err();
        assert_eq!(error.feature(), Some("binary-result-type-701"));
        let unknown = vec![
            ColumnDescription { name: "u".into(), type_oid: 999999, type_size: 4, type_modifier: -1, format: 1 },
        ];
        let error = data_row_with_formats(&data_row_payload(&[Some(&[0, 0, 0, 1])]), &unknown).unwrap_err();
        assert_eq!(error.feature(), Some("binary-result-type-999999"));
    }
}
