//! YYDB 与 YYDS 共享的线协议：帧布局、消息体与 `0000` 版本前缀。
//!
//! 嵌入式与分布式宿主共用同一套帧布局与消息集合；产品身份只体现在 4 字节 magic
//! （`YYDB` 与 `YYDS`，也可视为不同 URI 头）。本 crate 不打开数据库，也不分派请求。

#![deny(missing_docs)]

use std::io::{Read, Write};

use database_types::{Error, Result, SchemaVersion, Value};
use database_udf::{HostFunctionHandle, HostMicroDefinition, UdfType, UdfValue};

/// 嵌入式宿主自声明的 magic（与分布式宿主同一线协议）。
pub const MAGIC_YYDB: &[u8; 4] = b"YYDB";

/// 分布式宿主自声明的 magic（与嵌入式宿主同一线协议）。
pub const MAGIC_YYDS: &[u8; 4] = b"YYDS";

/// 当前线协议版本数字（`0000`）。
pub const WIRE_VERSION: &[u8; 4] = b"0000";

/// 下一版计划中的版本数字（当前不接受）。
pub const WIRE_VERSION_NEXT: &[u8; 4] = b"0001";

/// 绝对头长度（字节）：`magic` + `version` + 其余字段。
pub const HEADER_LEN: usize = 20;

/// 参考服务端 / 客户端的 body 软上限。
pub const MAX_BODY_LEN: u32 = 16 * 1024 * 1024;

/// 线消息类型码。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    /// 客户端握手。
    Hello,
    /// 服务端握手回复（版本字符串）。
    HelloOk,
    /// 数据库信息请求。
    Info,
    /// 数据库信息回复。
    InfoOk,
    /// 拉取 schema。
    SchemaGet,
    /// schema 载荷。
    SchemaGetOk,
    /// 确保 schema。
    SchemaEnsure,
    /// 确保 schema 确认。
    SchemaEnsureOk,
    /// KV 读取。
    KvGet,
    /// KV 读取回复。
    KvGetOk,
    /// KV 写入。
    KvPut,
    /// KV 写入确认。
    KvPutOk,
    /// 注册会话级宿主微函数（仅元数据）。
    MicroRegister,
    /// 宿主微函数注册确认。
    MicroRegisterOk,
    /// 服务端请求对端执行宿主微函数（S→C）。
    MicroHostInvoke,
    /// 宿主微函数结果（C→S）。
    MicroHostInvokeOk,
    /// 调用已注册的标量 UDF（C→S）。
    ScalarCall,
    /// 标量 UDF 结果（S→C）。
    ScalarCallOk,
    /// Hello 之后的客户端认证（令牌或挑战应答）。
    Authenticate,
    /// 认证回复（body 含可用于重连的不透明会话令牌）。
    AuthenticateOk,
    /// 客户端把查询源编译成可复用计划。
    Prepare,
    /// Prepare 回复（body：statement_id 字符串）。
    PrepareOk,
    /// 执行已准备语句，可带绑定参数。
    ExecutePrepared,
    /// ExecutePrepared 回复（body：JSON 行，形状同 KvGetOk 值）。
    ExecutePreparedOk,
    /// 错误回复。
    Error,
    /// 未识别类型（仍按帧发出，便于对端回 Error）。
    Unknown(u16),
}

impl MessageType {
    /// 解析原始 `u16` 类型码。
    pub fn from_u16(value: u16) -> Self {
        match value {
            1 => Self::Hello,
            2 => Self::HelloOk,
            3 => Self::Info,
            4 => Self::InfoOk,
            5 => Self::SchemaGet,
            6 => Self::SchemaGetOk,
            7 => Self::SchemaEnsure,
            8 => Self::SchemaEnsureOk,
            9 => Self::KvGet,
            10 => Self::KvGetOk,
            11 => Self::KvPut,
            12 => Self::KvPutOk,
            13 => Self::MicroRegister,
            14 => Self::MicroRegisterOk,
            15 => Self::MicroHostInvoke,
            16 => Self::MicroHostInvokeOk,
            17 => Self::ScalarCall,
            18 => Self::ScalarCallOk,
            19 => Self::Authenticate,
            20 => Self::AuthenticateOk,
            21 => Self::Prepare,
            22 => Self::PrepareOk,
            23 => Self::ExecutePrepared,
            24 => Self::ExecutePreparedOk,
            255 => Self::Error,
            other => Self::Unknown(other),
        }
    }

    /// 原始类型码。
    pub fn as_u16(self) -> u16 {
        match self {
            Self::Hello => 1,
            Self::HelloOk => 2,
            Self::Info => 3,
            Self::InfoOk => 4,
            Self::SchemaGet => 5,
            Self::SchemaGetOk => 6,
            Self::SchemaEnsure => 7,
            Self::SchemaEnsureOk => 8,
            Self::KvGet => 9,
            Self::KvGetOk => 10,
            Self::KvPut => 11,
            Self::KvPutOk => 12,
            Self::MicroRegister => 13,
            Self::MicroRegisterOk => 14,
            Self::MicroHostInvoke => 15,
            Self::MicroHostInvokeOk => 16,
            Self::ScalarCall => 17,
            Self::ScalarCallOk => 18,
            Self::Authenticate => 19,
            Self::AuthenticateOk => 20,
            Self::Prepare => 21,
            Self::PrepareOk => 22,
            Self::ExecutePrepared => 23,
            Self::ExecutePreparedOk => 24,
            Self::Error => 255,
            Self::Unknown(code) => code,
        }
    }
}

/// 一条带长度前缀的二进制帧。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// 消息类型。
    pub msg_type: MessageType,
    /// 保留标志（v0 必须为 0）。
    pub flags: u16,
    /// 客户端选定的请求 id（回复原样回显）。
    pub request_id: u32,
    /// 载荷字节。
    pub body: Vec<u8>,
}

impl Frame {
    /// 构造 flags 为空的帧。
    pub fn new(msg_type: MessageType, request_id: u32, body: impl Into<Vec<u8>>) -> Self {
        Self { msg_type, flags: 0, request_id, body: body.into() }
    }

    /// 编码为线上字节布局（默认 magic `YYDB`）。
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.encode_as(MAGIC_YYDB)
    }

    /// 使用显式宿主 magic（`YYDB` 或 `YYDS`，同一协议）编码。
    pub fn encode_as(&self, product: &[u8; 4]) -> Result<Vec<u8>> {
        if product != MAGIC_YYDB && product != MAGIC_YYDS {
            return Err(Error::Unsupported("wire product magic must be YYDB or YYDS"));
        }
        if self.body.len() > MAX_BODY_LEN as usize {
            return Err(Error::Unsupported("wire body too large"));
        }
        let mut out = Vec::with_capacity(HEADER_LEN + self.body.len());
        out.extend_from_slice(product);
        out.extend_from_slice(WIRE_VERSION);
        out.extend_from_slice(&self.msg_type.as_u16().to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.request_id.to_le_bytes());
        out.extend_from_slice(&(self.body.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.body);
        Ok(out)
    }

    /// 从完整缓冲区（头 + body）解码一帧。
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(Error::Corrupt("wire frame truncated header"));
        }
        check_prefix(&bytes[..8])?;
        let msg_raw = u16::from_le_bytes([bytes[8], bytes[9]]);
        let msg_type = MessageType::from_u16(msg_raw);
        let flags = u16::from_le_bytes([bytes[10], bytes[11]]);
        let request_id = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
        let body_len = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
        if body_len > MAX_BODY_LEN {
            return Err(Error::Unsupported("wire body too large"));
        }
        let need = HEADER_LEN + body_len as usize;
        if bytes.len() < need {
            return Err(Error::Corrupt("wire frame truncated body"));
        }
        Ok(Self { msg_type, flags, request_id, body: bytes[HEADER_LEN..need].to_vec() })
    }
}

fn check_prefix(prefix: &[u8]) -> Result<()> {
    if prefix.len() < 8 {
        return Err(Error::Corrupt("wire frame truncated magic/version"));
    }
    let magic = &prefix[0..4];
    let version = &prefix[4..8];
    if magic != MAGIC_YYDB && magic != MAGIC_YYDS {
        return Err(Error::Corrupt("wire frame bad product magic (want YYDB|YYDS)"));
    }
    if version != WIRE_VERSION {
        return Err(Error::Unsupported("wire version not supported (want 0000)"));
    }
    Ok(())
}

/// 从阻塞 reader 精确读取一帧。
pub fn read_frame<R: Read>(reader: &mut R) -> Result<Frame> {
    let mut header = [0_u8; HEADER_LEN];
    reader.read_exact(&mut header)?;
    check_prefix(&header)?;
    let body_len = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
    if body_len > MAX_BODY_LEN {
        return Err(Error::Unsupported("wire body too large"));
    }
    let mut body = vec![0_u8; body_len as usize];
    if body_len > 0 {
        reader.read_exact(&mut body)?;
    }
    let mut full = Vec::with_capacity(HEADER_LEN + body.len());
    full.extend_from_slice(&header);
    full.extend_from_slice(&body);
    Frame::decode(&full)
}

/// 向阻塞 writer 写入一帧。
pub fn write_frame<W: Write>(writer: &mut W, frame: &Frame) -> Result<()> {
    let bytes = frame.encode()?;
    writer.write_all(&bytes)?;
    writer.flush()?;
    Ok(())
}

fn read_u32(buf: &[u8], offset: &mut usize) -> Result<u32> {
    if *offset + 4 > buf.len() {
        return Err(Error::Corrupt("wire body truncated u32"));
    }
    let value = u32::from_le_bytes([buf[*offset], buf[*offset + 1], buf[*offset + 2], buf[*offset + 3]]);
    *offset += 4;
    Ok(value)
}

fn read_bytes<'a>(buf: &'a [u8], offset: &mut usize, len: u32) -> Result<&'a [u8]> {
    let len = len as usize;
    if *offset + len > buf.len() {
        return Err(Error::Corrupt("wire body truncated bytes"));
    }
    let slice = &buf[*offset..*offset + len];
    *offset += len;
    Ok(slice)
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

/// 构造一条 `Error` 回复帧。
pub fn error_frame(request_id: u32, error: &Error) -> Frame {
    Frame::new(MessageType::Error, request_id, encode_error_body(error))
}

/// 编码 `SchemaGetOk` body。
pub fn encode_schema_ok(schema: Option<SchemaVersion>) -> Vec<u8> {
    match schema {
        None => vec![0],
        Some(schema) => {
            let mut body = vec![1];
            push_u32(&mut body, schema.version);
            push_bytes(&mut body, schema.document.as_bytes());
            body
        }
    }
}

/// 解码 `SchemaEnsure` body。
pub fn decode_schema_ensure(body: &[u8]) -> Result<(u32, String)> {
    let mut offset = 0;
    let version = read_u32(body, &mut offset)?;
    let doc_len = read_u32(body, &mut offset)?;
    let doc = read_bytes(body, &mut offset, doc_len)?;
    let document = std::str::from_utf8(doc).map_err(|_| Error::Corrupt("schema document is not utf-8"))?.to_owned();
    Ok((version, document))
}

/// 解码 `KvGet` body。
pub fn decode_kv_get(body: &[u8]) -> Result<String> {
    let mut offset = 0;
    let key_len = read_u32(body, &mut offset)?;
    let key = read_bytes(body, &mut offset, key_len)?;
    Ok(std::str::from_utf8(key).map_err(|_| Error::Corrupt("kv key is not utf-8"))?.to_owned())
}

/// 解码 `KvPut` body。
pub fn decode_kv_put(body: &[u8]) -> Result<(String, Vec<u8>)> {
    let mut offset = 0;
    let key_len = read_u32(body, &mut offset)?;
    let key = read_bytes(body, &mut offset, key_len)?;
    let key = std::str::from_utf8(key).map_err(|_| Error::Corrupt("kv key is not utf-8"))?.to_owned();
    let val_len = read_u32(body, &mut offset)?;
    let value = read_bytes(body, &mut offset, val_len)?.to_vec();
    Ok((key, value))
}

/// 编码 `KvGetOk` body。
pub fn encode_kv_get_ok(value: Option<Vec<u8>>) -> Vec<u8> {
    match value {
        None => vec![0],
        Some(bytes) => {
            let mut body = vec![1];
            push_bytes(&mut body, &bytes);
            body
        }
    }
}

/// 编码 `SchemaEnsure` body。
pub fn encode_schema_ensure(version: u32, document: &str) -> Vec<u8> {
    let mut body = Vec::new();
    push_u32(&mut body, version);
    push_bytes(&mut body, document.as_bytes());
    body
}

/// 编码 `KvGet` body。
pub fn encode_kv_get(key: &str) -> Vec<u8> {
    let mut body = Vec::new();
    push_bytes(&mut body, key.as_bytes());
    body
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(buf: &[u8], offset: &mut usize) -> Result<u64> {
    if *offset + 8 > buf.len() {
        return Err(Error::Corrupt("wire body truncated u64"));
    }
    let value = u64::from_le_bytes([
        buf[*offset],
        buf[*offset + 1],
        buf[*offset + 2],
        buf[*offset + 3],
        buf[*offset + 4],
        buf[*offset + 5],
        buf[*offset + 6],
        buf[*offset + 7],
    ]);
    *offset += 8;
    Ok(value)
}

fn encode_udf_type_tag(ty: UdfType) -> u8 {
    match ty {
        UdfType::Null => 0,
        UdfType::Bool => 1,
        UdfType::I64 => 2,
        UdfType::Text => 3,
        UdfType::Any => {
            unreachable!("UdfType::Any is not encodable on the wire")
        }
    }
}

fn decode_udf_type_tag(tag: u8) -> Result<UdfType> {
    match tag {
        0 => Ok(UdfType::Null),
        1 => Ok(UdfType::Bool),
        2 => Ok(UdfType::I64),
        3 => Ok(UdfType::Text),
        _ => Err(Error::Corrupt("wire scalar value has invalid type tag")),
    }
}

fn encode_udf_value(out: &mut Vec<u8>, value: &UdfValue) {
    match value {
        UdfValue::Null => out.push(0),
        UdfValue::Bool(value) => {
            out.push(1);
            out.push(if *value { 1 } else { 0 });
        }
        UdfValue::I64(value) => {
            out.push(2);
            out.extend_from_slice(&value.to_le_bytes());
        }
        UdfValue::Text(value) => {
            out.push(3);
            push_bytes(out, value.as_bytes());
        }
    }
}

fn decode_udf_value(body: &[u8], offset: &mut usize) -> Result<UdfValue> {
    if *offset >= body.len() {
        return Err(Error::Corrupt("wire scalar value missing tag"));
    }
    let tag = body[*offset];
    *offset += 1;
    match tag {
        0 => Ok(UdfValue::Null),
        1 => {
            if *offset >= body.len() {
                return Err(Error::Corrupt("wire bool value truncated"));
            }
            let value = body[*offset] != 0;
            *offset += 1;
            Ok(UdfValue::Bool(value))
        }
        2 => {
            if *offset + 8 > body.len() {
                return Err(Error::Corrupt("wire i64 value truncated"));
            }
            let value = i64::from_le_bytes([
                body[*offset],
                body[*offset + 1],
                body[*offset + 2],
                body[*offset + 3],
                body[*offset + 4],
                body[*offset + 5],
                body[*offset + 6],
                body[*offset + 7],
            ]);
            *offset += 8;
            Ok(UdfValue::I64(value))
        }
        3 => {
            let len = read_u32(body, offset)?;
            let bytes = read_bytes(body, offset, len)?;
            let text = std::str::from_utf8(bytes).map_err(|_| Error::Corrupt("wire text value is not utf-8"))?.to_owned();
            Ok(UdfValue::Text(text))
        }
        _ => Err(Error::Corrupt("wire scalar value has invalid type tag")),
    }
}

fn encode_udf_values(values: &[UdfValue]) -> Vec<u8> {
    let mut body = Vec::new();
    assert!(values.len() <= 255, "wire scalar arg count exceeds u8");
    body.push(values.len() as u8);
    for value in values {
        encode_udf_value(&mut body, value);
    }
    body
}

fn decode_udf_values(body: &[u8], offset: &mut usize) -> Result<Vec<UdfValue>> {
    if *offset >= body.len() {
        return Err(Error::Corrupt("wire scalar args missing count"));
    }
    let count = body[*offset] as usize;
    *offset += 1;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(decode_udf_value(body, offset)?);
    }
    Ok(values)
}

fn udf_value_to_runtime(value: UdfValue) -> Result<Value> {
    match value {
        UdfValue::Null => Ok(Value::Null),
        UdfValue::Bool(value) => Ok(Value::Bool(value)),
        UdfValue::I64(value) => Ok(Value::I64(value)),
        UdfValue::Text(value) => Ok(Value::Text(value)),
    }
}

fn runtime_value_to_udf(value: &Value) -> Result<UdfValue> {
    match value {
        Value::Null => Ok(UdfValue::Null),
        Value::Bool(value) => Ok(UdfValue::Bool(*value)),
        Value::I64(value) => Ok(UdfValue::I64(*value)),
        Value::Text(value) => Ok(UdfValue::Text(value.clone())),
        _ => Err(Error::Unsupported("scalar call value type is outside the Phase-1 wire subset")),
    }
}

/// 编码 `MicroHostInvoke` body。
pub fn encode_micro_host_invoke(handle: &HostFunctionHandle, args: &[UdfValue]) -> Vec<u8> {
    let mut body = Vec::new();
    push_u64(&mut body, handle.host_id);
    push_u32(&mut body, handle.version);
    push_bytes(&mut body, handle.function_id.as_bytes());
    body.extend_from_slice(&encode_udf_values(args));
    body
}

/// 解码 `MicroHostInvoke` body。
pub fn decode_micro_host_invoke(body: &[u8]) -> Result<(HostFunctionHandle, Vec<UdfValue>)> {
    let mut offset = 0;
    let host_id = read_u64(body, &mut offset)?;
    let handle_version = read_u32(body, &mut offset)?;
    let function_id_len = read_u32(body, &mut offset)?;
    let function_id = read_bytes(body, &mut offset, function_id_len)?;
    let function_id = std::str::from_utf8(function_id).map_err(|_| Error::Corrupt("micro host invoke function_id is not utf-8"))?.to_owned();
    let args = decode_udf_values(body, &mut offset)?;
    Ok((HostFunctionHandle { host_id, function_id, version: handle_version }, args))
}

/// 编码 `MicroHostInvokeOk` body。
pub fn encode_micro_host_invoke_ok(value: UdfValue) -> Vec<u8> {
    let mut body = Vec::new();
    encode_udf_value(&mut body, &value);
    body
}

/// 解码 `MicroHostInvokeOk` body。
pub fn decode_micro_host_invoke_ok(body: &[u8]) -> Result<UdfValue> {
    let mut offset = 0;
    decode_udf_value(body, &mut offset)
}

/// 编码 `ScalarCall` body。
pub fn encode_scalar_call(name: &str, version: u32, args: &[Value]) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    push_u32(&mut body, version);
    push_bytes(&mut body, name.as_bytes());
    let mut udf_args = Vec::with_capacity(args.len());
    for value in args {
        udf_args.push(runtime_value_to_udf(value)?);
    }
    body.extend_from_slice(&encode_udf_values(&udf_args));
    Ok(body)
}

/// 解码 `ScalarCall` body。
pub fn decode_scalar_call(body: &[u8]) -> Result<(String, u32, Vec<Value>)> {
    let mut offset = 0;
    let version = read_u32(body, &mut offset)?;
    let name_len = read_u32(body, &mut offset)?;
    let name = read_bytes(body, &mut offset, name_len)?;
    let name = std::str::from_utf8(name).map_err(|_| Error::Corrupt("scalar call name is not utf-8"))?.to_owned();
    let udf_args = decode_udf_values(body, &mut offset)?;
    let mut args = Vec::with_capacity(udf_args.len());
    for value in udf_args {
        args.push(udf_value_to_runtime(value)?);
    }
    Ok((name, version, args))
}

/// 编码 `ScalarCallOk` body。
pub fn encode_scalar_call_ok(value: Value) -> Vec<u8> {
    let udf_value = runtime_value_to_udf(&value).expect("scalar call ok value must be wire-safe");
    encode_micro_host_invoke_ok(udf_value)
}

/// 解码 `ScalarCallOk` body。
pub fn decode_scalar_call_ok(body: &[u8]) -> Result<Value> {
    udf_value_to_runtime(decode_micro_host_invoke_ok(body)?)
}

/// 编码 `MicroRegister` body。
pub fn encode_micro_register(definition: &HostMicroDefinition) -> Vec<u8> {
    let mut body = Vec::new();
    push_u64(&mut body, definition.handle.host_id);
    push_u32(&mut body, definition.handle.version);
    push_u32(&mut body, definition.identity.version);
    push_bytes(&mut body, definition.identity.name().as_bytes());
    push_bytes(&mut body, definition.handle.function_id.as_bytes());
    let arg_count = definition.signature.args.len();
    body.push(arg_count as u8);
    for ty in &definition.signature.args {
        body.push(encode_udf_type_tag(*ty));
    }
    body.push(encode_udf_type_tag(definition.signature.returns));
    body.extend_from_slice(&definition.fingerprint);
    body
}

/// 解码 `MicroRegister` body。
pub fn decode_micro_register(body: &[u8]) -> Result<HostMicroDefinition> {
    let mut offset = 0;
    let host_id = read_u64(body, &mut offset)?;
    let handle_version = read_u32(body, &mut offset)?;
    let udf_version = read_u32(body, &mut offset)?;
    let name_len = read_u32(body, &mut offset)?;
    let name = read_bytes(body, &mut offset, name_len)?;
    let name = std::str::from_utf8(name).map_err(|_| Error::Corrupt("micro register name is not utf-8"))?;
    let function_id_len = read_u32(body, &mut offset)?;
    let function_id = read_bytes(body, &mut offset, function_id_len)?;
    let function_id = std::str::from_utf8(function_id).map_err(|_| Error::Corrupt("micro register function_id is not utf-8"))?.to_owned();
    if offset >= body.len() {
        return Err(Error::Corrupt("micro register missing arg count"));
    }
    let arg_count = body[offset] as usize;
    offset += 1;
    let mut args = Vec::with_capacity(arg_count);
    for _ in 0..arg_count {
        if offset >= body.len() {
            return Err(Error::Corrupt("micro register truncated arg types"));
        }
        args.push(decode_udf_type_tag(body[offset])?);
        offset += 1;
    }
    if offset >= body.len() {
        return Err(Error::Corrupt("micro register missing return type"));
    }
    let returns = decode_udf_type_tag(body[offset])?;
    offset += 1;
    if offset + 32 > body.len() {
        return Err(Error::Corrupt("micro register missing fingerprint"));
    }
    let mut fingerprint = [0_u8; 32];
    fingerprint.copy_from_slice(&body[offset..offset + 32]);
    HostMicroDefinition::from_scalar(name, udf_version, args, returns, host_id, function_id, handle_version, fingerprint)
        .map_err(|error| Error::Udf { name: name.to_owned(), message: error.to_string() })
}

/// 编码 `KvPut` body。
pub fn encode_kv_put(key: &str, value: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    push_bytes(&mut body, key.as_bytes());
    push_bytes(&mut body, value);
    body
}

/// 解码 `SchemaGetOk` body。
pub fn decode_schema_get_ok(body: &[u8]) -> Result<Option<SchemaVersion>> {
    if body.is_empty() {
        return Err(Error::Corrupt("schema get ok empty"));
    }
    if body[0] == 0 {
        return Ok(None);
    }
    let mut offset = 1;
    let version = read_u32(body, &mut offset)?;
    let doc_len = read_u32(body, &mut offset)?;
    let doc = read_bytes(body, &mut offset, doc_len)?;
    let document = std::str::from_utf8(doc).map_err(|_| Error::Corrupt("schema document is not utf-8"))?.to_owned();
    Ok(Some(SchemaVersion { version, document }))
}

/// 解码 `KvGetOk` body。
pub fn decode_kv_get_ok(body: &[u8]) -> Result<Option<Vec<u8>>> {
    if body.is_empty() {
        return Err(Error::Corrupt("kv get ok empty"));
    }
    if body[0] == 0 {
        return Ok(None);
    }
    let mut offset = 1;
    let val_len = read_u32(body, &mut offset)?;
    Ok(Some(read_bytes(body, &mut offset, val_len)?.to_vec()))
}

// ─── Authenticate ─────────────────────────────────────────────────────────

/// 编码 `Authenticate` 请求 body：原始不透明 bearer 令牌。
pub fn encode_authenticate(token: &str) -> Vec<u8> {
    let mut body = Vec::new();
    push_bytes(&mut body, token.as_bytes());
    body
}

/// 解码 `Authenticate` 请求 body。
pub fn decode_authenticate(body: &[u8]) -> Result<String> {
    let mut offset = 0;
    let len = read_u32(body, &mut offset)?;
    let bytes = read_bytes(body, &mut offset, len)?;
    std::str::from_utf8(bytes).map_err(|_| Error::Corrupt("authenticate token is not utf-8")).map(|s| s.to_owned())
}

/// 编码 `AuthenticateOk` 回复 body：可供后续连接使用的会话令牌。
pub fn encode_authenticate_ok(session_token: &str) -> Vec<u8> {
    let mut body = Vec::new();
    push_bytes(&mut body, session_token.as_bytes());
    body
}

// ─── Prepare / ExecutePrepared ────────────────────────────────────────────

/// 编码 `Prepare` 请求 body：原始查询源文本。
pub fn encode_prepare(source: &str) -> Vec<u8> {
    let mut body = Vec::new();
    push_bytes(&mut body, source.as_bytes());
    body
}

/// 解码 `Prepare` 请求 body。
pub fn decode_prepare(body: &[u8]) -> Result<String> {
    let mut offset = 0;
    let len = read_u32(body, &mut offset)?;
    let bytes = read_bytes(body, &mut offset, len)?;
    std::str::from_utf8(bytes).map_err(|_| Error::Corrupt("prepare source is not utf-8")).map(|s| s.to_owned())
}

/// 编码 `PrepareOk` 回复 body：statement_id 字符串。
pub fn encode_prepare_ok(statement_id: &str) -> Vec<u8> {
    let mut body = Vec::new();
    push_bytes(&mut body, statement_id.as_bytes());
    body
}

/// 解码 `PrepareOk` 回复 body。
pub fn decode_prepare_ok(body: &[u8]) -> Result<String> {
    let mut offset = 0;
    let len = read_u32(body, &mut offset)?;
    let bytes = read_bytes(body, &mut offset, len)?;
    std::str::from_utf8(bytes).map_err(|_| Error::Corrupt("prepare ok statement_id is not utf-8")).map(|s| s.to_owned())
}

/// 编码 `ExecutePrepared` 请求 body：statement_id + 可选 JSON 参数。
///
/// 语句无参数时 `params_json` 可为 `None` 或空串。
pub fn encode_execute_prepared(statement_id: &str, params_json: Option<&str>) -> Vec<u8> {
    let mut body = Vec::new();
    push_bytes(&mut body, statement_id.as_bytes());
    match params_json {
        Some(p) if !p.is_empty() => push_bytes(&mut body, p.as_bytes()),
        _ => push_u32(&mut body, 0),
    }
    body
}

/// 解码 `ExecutePrepared` 请求 body → (statement_id, 可选 JSON 参数)。
pub fn decode_execute_prepared(body: &[u8]) -> Result<(String, Option<String>)> {
    let mut offset = 0;
    let id_len = read_u32(body, &mut offset)?;
    let id_bytes = read_bytes(body, &mut offset, id_len)?;
    let statement_id = std::str::from_utf8(id_bytes).map_err(|_| Error::Corrupt("execute prepared statement_id is not utf-8"))?.to_owned();
    let params_len = read_u32(body, &mut offset)?;
    let params = if params_len > 0 {
        let bytes = read_bytes(body, &mut offset, params_len)?;
        let text = std::str::from_utf8(bytes).map_err(|_| Error::Corrupt("execute prepared params is not utf-8"))?.to_owned();
        Some(text)
    }
    else {
        None
    };
    Ok((statement_id, params))
}

/// 编码 `ExecutePreparedOk` 回复 body —— 原始 JSON 行。
pub fn encode_execute_prepared_ok(rows_json: &str) -> Vec<u8> {
    let mut body = Vec::new();
    push_bytes(&mut body, rows_json.as_bytes());
    body
}

/// 解码 `ExecutePreparedOk` 回复 body。
pub fn decode_execute_prepared_ok(body: &[u8]) -> Result<String> {
    let mut offset = 0;
    let len = read_u32(body, &mut offset)?;
    let bytes = read_bytes(body, &mut offset, len)?;
    std::str::from_utf8(bytes).map_err(|_| Error::Corrupt("execute prepared ok rows is not utf-8")).map(|s| s.to_owned())
}

/// 结构化的 `Error` 帧 body：错误码加可读消息。
///
/// 完整诊断合同见 [`Error::to_json`]（code、operation、retryable、params、message）。
/// [`encode_error_body`] / [`decode_error`] 负责该紧凑 JSON 对象的线上编解码。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError {
    /// 稳定错误码（如 `yy.schema.invalid`）；见 [`Error::code`]。
    pub code: String,
    /// 对端给出的可读消息。宿主不得按此文本分支，应按 [`Self::code`] 分支。
    pub message: String,
    /// 高层操作类别（`"schema"`、`"udf"` 等）。
    pub operation: String,
    /// 退避后是否可重试。
    pub retryable: bool,
    /// 可选的结构化键值参数，供目录化模板使用。
    pub params: Option<std::collections::BTreeMap<String, String>>,
}

/// 把 [`Error`] 编码为 `Error` 帧 body（紧凑诊断 JSON）。
pub fn encode_error_body(error: &Error) -> Vec<u8> {
    let json = serde_json::to_vec(&error.to_json()).expect("yy diagnostic JSON must never fail to encode");
    json
}

/// 从诊断 JSON 解码 `Error` 帧 body。
pub fn decode_error(body: &[u8]) -> Result<WireError> {
    let value: serde_json::Value = serde_json::from_slice(body).map_err(|_| Error::Corrupt("wire error body is not valid JSON"))?;
    let code = value["code"].as_str().ok_or_else(|| Error::Corrupt("wire error body missing code"))?.to_owned();
    let message = value["message"].as_str().ok_or_else(|| Error::Corrupt("wire error body missing message"))?.to_owned();
    let operation = value["operation"].as_str().ok_or_else(|| Error::Corrupt("wire error body missing operation"))?.to_owned();
    let retryable = value["retryable"].as_bool().ok_or_else(|| Error::Corrupt("wire error body missing retryable"))?;
    let params = value["params"].as_object().map(|obj| obj.iter().filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned()))).collect());
    Ok(WireError { code, message, operation, retryable, params })
}

/// 把 `host:port` / `[::1]:port` 拆成主机与端口。
pub fn split_bind(bind: &str) -> Result<(String, u16)> {
    let bind = bind.trim();
    if let Some(rest) = bind.strip_prefix('[') {
        let (host, port) = rest.split_once("]:").ok_or(Error::Unsupported("bind must look like [host]:port"))?;
        let port: u16 = port.parse().map_err(|_| Error::Unsupported("bind port is not a u16"))?;
        return Ok((host.to_owned(), port));
    }
    let (host, port) = bind.rsplit_once(':').ok_or(Error::Unsupported("bind must look like host:port"))?;
    let port: u16 = port.parse().map_err(|_| Error::Unsupported("bind port is not a u16"))?;
    Ok((host.to_owned(), port))
}
