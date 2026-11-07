//! MongoDB 线协议伪装网关。
//!
//! 接受 MongoDB 的 OP_MSG 帧（握手期也接受 OP_QUERY），用 YYDS KV 执行器承载文档，
//! 返回 MongoDB 形态的应答。不是完整的 MongoDB 服务端，命令子集如下：
//!
//! - 握手：`hello` / `isMaster` / `ismaster` / `ping`
//! - 写入：`insert` / `delete`
//! - 读取：`find` / `count` / `aggregate`
//! - 游标：`getMore` / `killCursors`
//!
//! 命令语义全部由上游 `mongo` 门面的命令层提供，本模块只做 OP_MSG / OP_QUERY
//! 编解码并注入 `MongoStorage` 后端（`storage::ExecutorStorage`）。固定语料对账
//! 基线在 `tests/mongo/reconcile.rs`：38 条语料中 35 条逐字节一致，3 条为已记录
//! 在案的能力差异，当前不补实现：
//!
//! - 不实现 `update`，该命令与替换式更新都只回通用的 `59 unsupported`。
//! - 不实现索引（`createIndexes`），因此唯一索引与 `{a: []}` 触发的 `undefined`
//!   键路径无法在本网关触发。
#![warn(missing_docs)]

use std::{
    sync::{Mutex, MutexGuard, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use database_execution::cluster::KeyValueExecutor;
use mongo::{
    commands::{self, CommandError},
    cursor::CursorStore,
    storage::MongoStorage,
};

pub mod connection;
pub mod service;
mod storage;

/// BSON 编解码（来自 `database-protocol-mongo`）。
pub use database_protocol_mongo::bson;
/// OP_MSG / OP_QUERY 组帧（来自 `database-protocol-mongo`）。
pub use database_protocol_mongo::wire;

use bson::{BsonDocument, BsonValue, encode_bson, parse_bson};

/// Gateway kind implemented by this crate.
pub const GATEWAY_KIND: database_protocol::GatewayKind = database_protocol::GatewayKind::Mongo;

/// Gateway identifier used in logs and diagnostics.
pub const GATEWAY_ID: &str = GATEWAY_KIND.id();

/// Default MongoDB disguise listen port.
pub const DEFAULT_PORT: u16 = GATEWAY_KIND.default_port();

/// Extracts the command name from an inbound frame — useful for wire-level
/// tests that only need dispatch routing.
pub fn command_name_from_frame(frame: &wire::Frame) -> Option<&'static str> {
    if frame.header.opcode == wire::OP_QUERY {
        return decode_op_query_command(&frame.body).ok()?.command_name().map(command_kind);
    }
    // Skip OP_MSG flags (4) + section kind (1)
    let mut cursor = frame.body.get(5..)?;
    // Read BSON document length
    let doc_len = u32::from_le_bytes(cursor.get(..4)?.try_into().ok()?) as usize;
    cursor = cursor.get(4..)?;
    if doc_len < 5 || doc_len.saturating_sub(4) > cursor.len() {
        return None;
    }
    // Skip the first element's type byte.
    if cursor.is_empty() {
        return None;
    }
    cursor = &cursor[1..];
    let end = cursor.iter().position(|&b| b == 0)?;
    let name = std::str::from_utf8(&cursor[..end]).ok()?;
    Some(command_kind(name))
}

/// Normalizes the command name a driver used into the routing key this gateway
/// dispatches on. `isMaster` reaches us spelled three ways depending on the
/// driver and the framing it chose.
fn command_kind(name: &str) -> &'static str {
    match name {
        "hello" => "hello",
        "isMaster" | "ismaster" => "isMaster",
        "ping" => "ping",
        "insert" => "insert",
        "find" => "find",
        "count" => "count",
        "aggregate" => "aggregate",
        "delete" => "delete",
        "getMore" => "getMore",
        "killCursors" => "killCursors",
        _ => "unknown",
    }
}

/// Handles a fully-parsed OP_MSG command payload and returns the response BSON
/// body (ready to be wrapped into an OP_MSG reply by the connection layer).
///
/// `op_msg_body` is the bytes after the 16-byte header: flags (u32) + the body
/// section (kind `0x00`) + optional document-sequence sections (kind `0x01`) +
/// optional checksum. Errors are always turned into a generic
/// `{ ok: 0, errmsg: ... }` BSON response so the connection layer never has to carry
/// IO-result-like fallbacks.
pub fn handle_op_msg(op_msg_body: &[u8], executor: Option<&dyn KeyValueExecutor>) -> Vec<u8> {
    let bson_doc = match decode_op_msg_command(op_msg_body) {
        Ok(doc) => doc,
        Err(message) => return generic_error_body(&message),
    };
    dispatch_command(&bson_doc, executor)
}

/// Handles a legacy OP_QUERY payload and returns the response BSON document,
/// ready to be wrapped into an OP_REPLY by the connection layer.
///
/// Drivers send their first `isMaster` / `hello` probe as OP_QUERY because they
/// cannot assume the server speaks OP_MSG yet. The command is the query document
/// itself (optionally under `$query`).
pub fn handle_op_query(op_query_body: &[u8], executor: Option<&dyn KeyValueExecutor>) -> Vec<u8> {
    let bson_doc = match decode_op_query_command(op_query_body) {
        Ok(doc) => doc,
        Err(message) => return generic_error_body(&message),
    };
    dispatch_command(&bson_doc, executor)
}

/// 全网关共享的游标表，游标编号由门面的命令层分配。
static CURSORS: OnceLock<Mutex<CursorStore>> = OnceLock::new();

fn cursors() -> MutexGuard<'static, CursorStore> {
    CURSORS.get_or_init(|| Mutex::new(CursorStore::default())).lock().unwrap_or_else(|poison| poison.into_inner())
}

/// 把门面返回的命令文档或错误文档编码成应答帧。
fn render(result: Result<BsonDocument, CommandError>) -> Vec<u8> {
    match result {
        Ok(document) => encode_bson(&document),
        Err(error) => encode_bson(&commands::error_document(&error)),
    }
}

/// 在 `KeyValueExecutor` 适配器上执行一条门面命令。
fn run(
    executor: Option<&dyn KeyValueExecutor>,
    command: &str,
    apply: impl FnOnce(&dyn MongoStorage) -> Result<BsonDocument, CommandError>,
) -> Vec<u8> {
    let Some(executor) = executor
    else {
        return build_error_bson(2, &format!("{command} requires an executor"));
    };
    let storage = storage::ExecutorStorage::new(executor);
    render(apply(&storage))
}

/// Routes one decoded command document to its handler.
fn dispatch_command(bson_doc: &BsonDocument, executor: Option<&dyn KeyValueExecutor>) -> Vec<u8> {
    let Some(command_name) = bson_doc.command_name()
    else {
        return generic_error_body("empty command");
    };

    match command_name {
        "hello" | "isMaster" | "ismaster" => build_hello_bson(),
        "ping" => build_ok_bson(1),
        "insert" => run(executor, "insert", |storage| commands::insert(bson_doc, storage)),
        "find" => {
            let Some(executor) = executor
            else {
                return build_error_bson(2, "find requires an executor");
            };
            let storage = storage::ExecutorStorage::new(executor);
            let mut cursor_store = cursors();
            render(commands::find(bson_doc, &storage, &mut cursor_store))
        }
        "count" => run(executor, "count", |storage| commands::count(bson_doc, storage)),
        "aggregate" => run(executor, "aggregate", |storage| commands::aggregate(bson_doc, storage)),
        "delete" => run(executor, "delete", |storage| commands::delete(bson_doc, storage)),
        "getMore" => render(commands::get_more(bson_doc, &mut cursors())),
        "killCursors" => render(commands::kill_cursors(bson_doc, &mut cursors())),
        other => generic_error_body(other),
    }
}

/// Decodes a legacy OP_QUERY body into its command document.
///
/// Layout: flags (u32), full collection name (cstring), skip (i32),
/// number to return (i32), then the query document.
fn decode_op_query_command(op_query_body: &[u8]) -> Result<BsonDocument, String> {
    let rest = op_query_body.get(4..).ok_or_else(|| "OP_QUERY body too short".to_string())?;
    let name_end = rest.iter().position(|byte| *byte == 0).ok_or_else(|| "OP_QUERY namespace has no terminator".to_string())?;
    // Skip the namespace terminator, the skip field and the number to return.
    let query = rest.get(name_end + 1 + 8..).ok_or_else(|| "OP_QUERY carries no query document".to_string())?;
    let mut document = parse_bson(query).map_err(|error| format!("BSON parse error: {error:?}"))?;
    if let Some(BsonValue::Document(inner)) = document.get("$query").cloned() {
        document = inner;
    }
    Ok(document)
}

/// OP_MSG flag bit 0: the frame ends with a CRC-32C checksum.
const OP_MSG_CHECKSUM_PRESENT: u32 = 0x0000_0001;

/// Decodes the OP_MSG body into one command document.
///
/// The body section (`0x00`) carries the command itself. Document-sequence
/// sections (`0x01`) carry the bulk payload of write commands: real drivers put
/// `insert.documents` and `delete.deletes` there instead of inlining them, so each
/// sequence is folded back into the command as the array named by its identifier.
fn decode_op_msg_command(op_msg_body: &[u8]) -> Result<BsonDocument, String> {
    let flags = op_msg_body
        .get(..4)
        .and_then(|head| <[u8; 4]>::try_from(head).ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| "OP_MSG body too short".to_string())?;

    let mut sections = op_msg_body.get(4..).ok_or_else(|| "OP_MSG body too short".to_string())?;
    if flags & OP_MSG_CHECKSUM_PRESENT != 0 {
        sections =
            sections.len().checked_sub(4).and_then(|end| sections.get(..end)).ok_or_else(|| "OP_MSG checksum section truncated".to_string())?;
    }

    let mut body: Option<BsonDocument> = None;
    let mut sequences: Vec<(String, Vec<BsonValue>)> = Vec::new();

    while let Some((kind, rest)) = sections.split_first() {
        match kind {
            0x00 => {
                let document = parse_bson(rest).map_err(|error| format!("BSON parse error: {error:?}"))?;
                let length = bson_document_length(rest)?;
                sections = rest.get(length..).ok_or_else(|| "OP_MSG body section truncated".to_string())?;
                if body.replace(document).is_some() {
                    return Err("OP_MSG carries more than one body section".to_string());
                }
            }
            0x01 => {
                let size = rest
                    .get(..4)
                    .and_then(|head| <[u8; 4]>::try_from(head).ok())
                    .map(|head| i32::from_le_bytes(head))
                    .filter(|size| *size > 4)
                    .ok_or_else(|| "document sequence section missing its size".to_string())? as usize;
                let section = rest.get(..size).ok_or_else(|| "document sequence section truncated".to_string())?;
                let header = &section[4..];
                let name_end =
                    header.iter().position(|byte| *byte == 0).ok_or_else(|| "document sequence identifier has no terminator".to_string())?;
                let identifier =
                    std::str::from_utf8(&header[..name_end]).map_err(|_| "document sequence identifier is not UTF-8".to_string())?.to_string();
                let mut documents = &header[name_end + 1..];
                let mut values = Vec::new();
                while !documents.is_empty() {
                    let length = bson_document_length(documents)?;
                    let document = parse_bson(documents).map_err(|error| format!("BSON parse error: {error:?}"))?;
                    values.push(BsonValue::Document(document));
                    documents = &documents[length..];
                }
                sequences.push((identifier, values));
                sections = &rest[size..];
            }
            other => return Err(format!("unsupported OP_MSG section kind {other}")),
        }
    }

    let mut command = body.ok_or_else(|| "OP_MSG carries no body section".to_string())?;
    for (identifier, values) in sequences {
        if command.get(&identifier).is_none() {
            command.push(identifier, BsonValue::Array(values));
        }
    }
    Ok(command)
}

fn bson_document_length(bytes: &[u8]) -> Result<usize, String> {
    let length = bytes
        .get(..4)
        .and_then(|head| <[u8; 4]>::try_from(head).ok())
        .map(|head| i32::from_le_bytes(head))
        .filter(|length| *length >= 5)
        .ok_or_else(|| "BSON document length missing".to_string())? as usize;
    if length > bytes.len() {
        return Err("BSON document length exceeds the section".to_string());
    }
    Ok(length)
}

// ---------------------------------------------------------------------------
// Handshake helpers
// ---------------------------------------------------------------------------

fn build_hello_bson() -> Vec<u8> {
    let mut doc = BsonDocument::new();
    doc.push("isWritablePrimary".to_string(), BsonValue::Bool(true));
    doc.push("maxWireVersion".to_string(), BsonValue::Int32(17));
    doc.push("minWireVersion".to_string(), BsonValue::Int32(0));
    doc.push("ok".to_string(), BsonValue::Int32(1));
    encode_bson(&doc)
}

fn build_ok_bson(ok: i32) -> Vec<u8> {
    let mut doc = BsonDocument::new();
    doc.push("ok".to_string(), BsonValue::Int32(if ok >= 1 { 1 } else { 0 }));
    encode_bson(&doc)
}

fn build_error_bson(code: i32, errmsg: &str) -> Vec<u8> {
    let mut doc = BsonDocument::new();
    doc.push("ok".to_string(), BsonValue::Int32(0));
    doc.push("errmsg".to_string(), BsonValue::String(errmsg.to_string()));
    doc.push("code".to_string(), BsonValue::Int32(code));
    if let Some(name) = code_name(code) {
        doc.push("codeName".to_string(), BsonValue::String(name.to_string()));
    }
    encode_bson(&doc)
}

/// `mongod` 在错误文档里附带的代码符号名。只列网关会产出的代码，其余保持
/// 三字段形态，与 `mongod` 对未知符号名的处理一致。
fn code_name(code: i32) -> Option<&'static str> {
    match code {
        2 => Some("BadValue"),
        73 => Some("InvalidNamespace"),
        _ => None,
    }
}

/// A minimal BSON error document returned for commands we deliberately reject.
/// Kept minimal (`errmsg: "unsupported"`) so existing handshake tests still pass.
pub fn generic_error_body(_command: &str) -> Vec<u8> {
    build_error_bson(59, "unsupported")
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Generates a 12-byte Mongo ObjectId using the classic 4+5+3 layout: unix
/// timestamp + random bytes + monotonic counter. Good enough for a gateway
/// disguise — the wire format matches what Mongo drivers expect.
pub fn generate_object_id() -> [u8; 12] {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as u32).unwrap_or(0);
    let random = rand_bytes();
    let counter = (COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst) & 0x00ffffff) as u32;
    let mut out = [0u8; 12];
    out[0..4].copy_from_slice(&secs.to_be_bytes());
    out[4..9].copy_from_slice(&random);
    out[9..12].copy_from_slice(&counter.to_be_bytes()[1..4]);
    out
}

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn rand_bytes() -> [u8; 5] {
    let mut out = [0u8; 5];
    let mut seed = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
    for byte in out.iter_mut() {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *byte = (seed >> 56) as u8;
    }
    out
}
