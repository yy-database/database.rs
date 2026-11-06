//! MongoDB wire disguise gateway for YYDS.
//!
//! Accepts the MongoDB OP_MSG binary frame protocol and answers with YYDS-backed
//! document responses. Not a full MongoDB server: it presents a minimal CRUD surface
//! (`insert`, `find`, `delete`) on top of the YYDS KV executor plus the standard
//! `hello` / `ping` handshake probes.
#![warn(missing_docs)]

use std::{
    cmp::Ordering,
    collections::HashMap,
    sync::{Mutex, MutexGuard, OnceLock, atomic::Ordering as AtomicOrdering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use database_execution::cluster::KeyValueExecutor;

pub mod connection;
pub mod service;

/// BSON 编解码（来自 `database-protocol-mongo`）。
pub use database_protocol_mongo::bson;
/// OP_MSG / OP_QUERY 组帧（来自 `database-protocol-mongo`）。
pub use database_protocol_mongo::wire;

use bson::{BsonDocument, BsonValue, encode_bson, parse_bson, value_to_key};

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

/// Routes one decoded command document to its handler.
fn dispatch_command(bson_doc: &BsonDocument, executor: Option<&dyn KeyValueExecutor>) -> Vec<u8> {
    let Some(command_name) = bson_doc.command_name()
    else {
        return generic_error_body("empty command");
    };

    match command_name {
        "hello" | "isMaster" | "ismaster" => build_hello_bson(),
        "ping" => build_ok_bson(1),
        "insert" => handle_insert(bson_doc, executor),
        "find" => handle_find(bson_doc, executor),
        "count" => handle_count(bson_doc, executor),
        "aggregate" => handle_aggregate(bson_doc, executor),
        "delete" => handle_delete(bson_doc, executor),
        "getMore" => handle_get_more(bson_doc),
        "killCursors" => handle_kill_cursors(bson_doc),
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

/// 写入应答的成功标记是 `ok: 1.0`（double），不是整数。
fn write_ok() -> BsonValue {
    BsonValue::Double(1.0)
}

/// A minimal BSON error document returned for commands we deliberately reject.
/// Kept minimal (`errmsg: "unsupported"`) so existing handshake tests still pass.
pub fn generic_error_body(_command: &str) -> Vec<u8> {
    build_error_bson(59, "unsupported")
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

/// Renders an `_id` value the way MongoDB's duplicate-key message does.
fn describe_id(value: &BsonValue) -> String {
    match value {
        BsonValue::String(text) => format!("\"{text}\""),
        BsonValue::Int32(number) => number.to_string(),
        BsonValue::Int64(number) => number.to_string(),
        BsonValue::ObjectId(bytes) => {
            let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            format!("ObjectId('{hex}')")
        }
        other => format!("{other:?}"),
    }
}

fn handle_insert(doc: &BsonDocument, executor: Option<&dyn KeyValueExecutor>) -> Vec<u8> {
    let Some(executor) = executor
    else {
        return build_error_bson(2, "insert requires an executor");
    };

    let Some(ns) = namespace_from_command(doc, "insert")
    else {
        return build_error_bson(73, "insert command missing collection name");
    };

    let Some(BsonValue::Array(docs)) = doc.get("documents")
    else {
        return build_error_bson(2, "insert requires a 'documents' array");
    };

    // MongoDB defaults to an ordered insert and only continues past a failed
    // document when the client explicitly asks for `ordered: false`.
    let ordered = match doc.get("ordered") {
        Some(BsonValue::Bool(ordered)) => *ordered,
        _ => true,
    };

    let mut inserted: i64 = 0;
    let mut write_errors = BsonDocument::new();

    for (idx, item) in docs.iter().enumerate() {
        match item {
            BsonValue::Document(sub) => {
                let mut doc_with_id = sub.clone();
                let has_id = doc_with_id.fields.iter().any(|(k, _)| k == "_id");
                if !has_id {
                    doc_with_id.push("_id".to_string(), BsonValue::ObjectId(generate_object_id()));
                }
                let Some(id_val) = doc_with_id.fields.iter().find(|(k, _)| k == "_id").map(|(_, v)| v.clone())
                else {
                    continue;
                };
                let key = value_to_key(&id_val);

                // MongoDB's `_id` is a unique key: a repeated value is a duplicate-key
                // error, not an overwrite. Emulated writes go through a plain Put, so the
                // check has to be explicit before writing.
                match executor.execute(
                    None,
                    database_query::cluster_kv::KeyValueCommand {
                        namespace: ns.clone(),
                        key: key.clone(),
                        action: database_query::cluster_kv::KeyValueAction::Get,
                    },
                ) {
                    Ok(database_query::cluster_kv::KeyValueResult::Get(Some(_))) => {
                        let mut err = BsonDocument::new();
                        err.push("index".to_string(), BsonValue::Int32(idx as i32));
                        err.push("code".to_string(), BsonValue::Int32(11000));
                        err.push(
                            "errmsg".to_string(),
                            BsonValue::String(format!(
                                "E11000 duplicate key error collection: {} index: _id_ dup key: {{ _id: {} }}",
                                namespace_string(doc, "insert"),
                                describe_id(&id_val)
                            )),
                        );
                        let mut key_pattern = BsonDocument::new();
                        key_pattern.push("_id".to_string(), BsonValue::Int32(1));
                        err.push("keyPattern".to_string(), BsonValue::Document(key_pattern));
                        let mut key_value = BsonDocument::new();
                        key_value.push("_id".to_string(), id_val);
                        err.push("keyValue".to_string(), BsonValue::Document(key_value));
                        write_errors.push(format!("err{idx}"), BsonValue::Document(err));
                        if ordered {
                            break;
                        }
                        continue;
                    }
                    _ => {}
                }

                let payload = encode_bson(&doc_with_id);
                let result = executor.execute(
                    None,
                    database_query::cluster_kv::KeyValueCommand {
                        namespace: ns.clone(),
                        key,
                        action: database_query::cluster_kv::KeyValueAction::Put(payload),
                    },
                );
                match result {
                    Ok(database_query::cluster_kv::KeyValueResult::Put { .. }) => inserted += 1,
                    _ => {
                        let mut err = BsonDocument::new();
                        err.push("index".to_string(), BsonValue::Int32(idx as i32));
                        err.push("errmsg".to_string(), BsonValue::String("insert failed".to_string()));
                        write_errors.push(format!("err{idx}"), BsonValue::Document(err));
                        if ordered {
                            break;
                        }
                    }
                }
            }
            _ => {
                let mut err = BsonDocument::new();
                err.push("index".to_string(), BsonValue::Int32(idx as i32));
                err.push("errmsg".to_string(), BsonValue::String("documents must be objects".to_string()));
                write_errors.push(format!("err{idx}"), BsonValue::Document(err));
                if ordered {
                    break;
                }
            }
        }
    }

    let mut resp = BsonDocument::new();
    resp.push("n".to_string(), count_value(inserted));
    if !write_errors.fields.is_empty() {
        let err_list: Vec<BsonValue> = write_errors.fields.into_iter().map(|(_, v)| v).collect();
        resp.push("writeErrors".to_string(), BsonValue::Array(err_list));
    }
    resp.push("ok".to_string(), write_ok());
    encode_bson(&resp)
}

// ---------------------------------------------------------------------------
// find
// ---------------------------------------------------------------------------

fn handle_find(doc: &BsonDocument, executor: Option<&dyn KeyValueExecutor>) -> Vec<u8> {
    let Some(executor) = executor
    else {
        return build_error_bson(2, "find requires an executor");
    };

    let Some(ns) = namespace_from_command(doc, "find")
    else {
        return build_error_bson(73, "find command missing collection name");
    };

    let filter = match doc.get("filter") {
        Some(BsonValue::Document(filter)) => filter.clone(),
        _ => BsonDocument::new(),
    };
    let sort = parse_sort(doc.get("sort"));
    let skip = parse_count(doc.get("skip"));
    let limit = parse_count(doc.get("limit"));
    let batch_size = parse_count(doc.get("batchSize"));

    let mut documents = collect_matching(executor, &ns, &filter);
    sort_documents(&mut documents, &sort);

    let start = skip.min(documents.len());
    let end = if limit > 0 { (start + limit).min(documents.len()) } else { documents.len() };
    let mut window: Vec<BsonValue> = documents.into_iter().skip(start).take(end - start).map(BsonValue::Document).collect();

    let namespace = namespace_string(doc, "find");
    let cursor_id = if batch_size > 0 && window.len() > batch_size {
        let remaining = window.split_off(batch_size);
        let id = NEXT_CURSOR_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let mut cursors = lock_cursors();
        cursors.retain(|_, state| state.last_used.elapsed() < CURSOR_IDLE_TIMEOUT);
        cursors.insert(id, CursorState { namespace: namespace.clone(), remaining, last_used: Instant::now() });
        id
    }
    else {
        0
    };

    encode_cursor_reply(&namespace, "firstBatch", window, cursor_id)
}

/// Answers `count`, which `estimated_document_count` and direct `db.command`
/// calls use. `query` reuses the same matcher as `find`.
fn handle_count(doc: &BsonDocument, executor: Option<&dyn KeyValueExecutor>) -> Vec<u8> {
    let Some(executor) = executor
    else {
        return build_error_bson(2, "count requires an executor");
    };

    let Some(ns) = namespace_from_command(doc, "count")
    else {
        return build_error_bson(73, "count command missing collection name");
    };

    let filter = match doc.get("query") {
        Some(BsonValue::Document(filter)) => filter.clone(),
        _ => BsonDocument::new(),
    };
    let skip = parse_count(doc.get("skip"));
    let limit = parse_count(doc.get("limit"));

    let matched = collect_matching(executor, &ns, &filter).len();
    let start = skip.min(matched);
    let end = if limit > 0 { (start + limit).min(matched) } else { matched };

    let mut resp = BsonDocument::new();
    resp.push("n".to_string(), count_value((end - start) as i64));
    resp.push("ok".to_string(), write_ok());
    encode_bson(&resp)
}

/// Runs the aggregation stages this gateway serves: `$match`, `$group` with a
/// constant `_id` and a `$sum: 1` accumulator, and `$project` field selection.
///
/// Every other stage answers `59` on purpose, so a caller can tell "this stage
/// is not implemented" from "the pipeline matched nothing".
fn handle_aggregate(doc: &BsonDocument, executor: Option<&dyn KeyValueExecutor>) -> Vec<u8> {
    let Some(executor) = executor
    else {
        return build_error_bson(2, "aggregate requires an executor");
    };

    let Some(ns) = namespace_from_command(doc, "aggregate")
    else {
        return build_error_bson(73, "aggregate command missing collection name");
    };

    let stages = match doc.get("pipeline") {
        Some(BsonValue::Array(stages)) => stages,
        _ => return build_error_bson(2, "aggregate requires a pipeline array"),
    };

    let mut documents = collect_matching(executor, &ns, &BsonDocument::new());
    for stage in stages {
        let stage_doc = match stage {
            BsonValue::Document(stage_doc) => stage_doc,
            _ => return build_error_bson(59, "aggregate stage must be a document"),
        };
        let Some((name, body)) = stage_doc.fields.first()
        else {
            return build_error_bson(59, "aggregate stage is empty");
        };
        match name.as_str() {
            "$match" => {
                let BsonValue::Document(filter) = body
                else {
                    return build_error_bson(59, "$match requires a document");
                };
                documents.retain(|document| document_matches(document, filter));
            }
            "$group" => {
                let BsonValue::Document(spec) = body
                else {
                    return build_error_bson(59, "$group requires a document");
                };
                match group_documents(&documents, spec) {
                    Ok(grouped) => documents = grouped,
                    Err(message) => return build_error_bson(59, &message),
                }
            }
            "$project" => {
                let BsonValue::Document(spec) = body
                else {
                    return build_error_bson(59, "$project requires a document");
                };
                documents = documents.iter().map(|document| project_document(document, spec)).collect();
            }
            other => return build_error_bson(59, other),
        }
    }

    let namespace = namespace_string(doc, "aggregate");
    encode_cursor_reply(&namespace, "firstBatch", documents.into_iter().map(BsonValue::Document).collect(), 0)
}

/// Returns the batch requested by a following `getMore`, draining the stored cursor.
fn handle_get_more(doc: &BsonDocument) -> Vec<u8> {
    let Some(cursor_id) = doc.get("getMore").and_then(int_value)
    else {
        return build_error_bson(2, "getMore requires a cursor id");
    };
    let batch_size = parse_count(doc.get("batchSize"));

    let mut cursors = lock_cursors();
    cursors.retain(|_, state| state.last_used.elapsed() < CURSOR_IDLE_TIMEOUT);
    let Some(state) = cursors.get_mut(&cursor_id)
    else {
        return build_error_bson(43, &format!("cursor id {cursor_id} not found"));
    };
    state.last_used = Instant::now();
    let take = if batch_size > 0 { batch_size.min(state.remaining.len()) } else { state.remaining.len() };
    let namespace = state.namespace.clone();
    let batch: Vec<BsonValue> = state.remaining.drain(..take).collect();
    let drained = state.remaining.is_empty();
    if drained {
        cursors.remove(&cursor_id);
    }
    drop(cursors);

    encode_cursor_reply(&namespace, "nextBatch", batch, if drained { 0 } else { cursor_id })
}

/// Drops the cursors a driver closed. The gateway does not need the collection
/// name because cursor ids are globally unique.
fn handle_kill_cursors(doc: &BsonDocument) -> Vec<u8> {
    let ids = match doc.get("cursors") {
        Some(BsonValue::Array(ids)) => ids.clone(),
        _ => Vec::new(),
    };
    let mut killed = Vec::new();
    let mut not_found = Vec::new();
    let mut cursors = lock_cursors();
    for id in ids.iter().filter_map(int_value) {
        if cursors.remove(&id).is_some() {
            killed.push(BsonValue::Int64(id));
        }
        else {
            not_found.push(BsonValue::Int64(id));
        }
    }

    let mut resp = BsonDocument::new();
    resp.push("cursorsKilled".to_string(), BsonValue::Array(killed));
    resp.push("cursorsNotFound".to_string(), BsonValue::Array(not_found));
    resp.push("cursorsAlive".to_string(), BsonValue::Array(Vec::new()));
    resp.push("cursorsUnknown".to_string(), BsonValue::Array(Vec::new()));
    resp.push("ok".to_string(), BsonValue::Int32(1));
    encode_bson(&resp)
}

/// Builds the `cursor` sub-document shared by `find` (`firstBatch`) and
/// `getMore` (`nextBatch`).
fn encode_cursor_reply(namespace: &str, batch_key: &str, batch: Vec<BsonValue>, cursor_id: i64) -> Vec<u8> {
    let mut cursor = BsonDocument::new();
    cursor.push(batch_key.to_string(), BsonValue::Array(batch));
    cursor.push("id".to_string(), BsonValue::Int64(cursor_id));
    cursor.push("ns".to_string(), BsonValue::String(namespace.to_string()));

    let mut resp = BsonDocument::new();
    resp.push("ok".to_string(), BsonValue::Int32(1));
    resp.push("cursor".to_string(), BsonValue::Document(cursor));
    encode_bson(&resp)
}

/// Reads every document the filter accepts, using the `_id` key directly when
/// the filter is a plain `_id` equality.
fn collect_matching(executor: &dyn KeyValueExecutor, ns: &database_types::cluster::Namespace, filter: &BsonDocument) -> Vec<BsonDocument> {
    if let Some(id_value) = single_id_lookup(filter) {
        let key = value_to_key(id_value);
        return match executor.execute(
            None,
            database_query::cluster_kv::KeyValueCommand { namespace: ns.clone(), key, action: database_query::cluster_kv::KeyValueAction::Get },
        ) {
            Ok(database_query::cluster_kv::KeyValueResult::Get(Some(bytes))) => match parse_bson(&bytes) {
                Ok(document) if document_matches(&document, filter) => vec![document],
                _ => Vec::new(),
            },
            _ => Vec::new(),
        };
    }

    let results = executor.execute_scan(None, ns, b"", None, None, 0).unwrap_or_default();
    let mut documents = Vec::new();
    for (_key, value) in results {
        if let Ok(candidate) = parse_bson(&value) {
            if document_matches(&candidate, filter) {
                documents.push(candidate);
            }
        }
    }
    documents
}

/// Returns the `_id` value when the filter is exactly one `_id` equality
/// without query operators.
fn single_id_lookup(filter: &BsonDocument) -> Option<&BsonValue> {
    if filter.fields.len() != 1 {
        return None;
    }
    let (key, value) = filter.fields.first()?;
    if key != "_id" {
        return None;
    }
    match value {
        BsonValue::Document(operators) if operators.fields.iter().any(|(op, _)| op.starts_with('$')) => None,
        _ => Some(value),
    }
}

// ---------------------------------------------------------------------------
// filter, sort and window helpers
// ---------------------------------------------------------------------------

/// Applies a `$group` stage with a constant `_id` and `$sum: 1` accumulators.
/// An empty input produces an empty result, exactly like a real server.
fn group_documents(documents: &[BsonDocument], spec: &BsonDocument) -> Result<Vec<BsonDocument>, String> {
    let Some(id) = spec.get("_id")
    else {
        return Err("$group requires an `_id` expression".to_string());
    };
    if matches!(id, BsonValue::Document(_) | BsonValue::Array(_)) {
        return Err("$group only supports a constant `_id`".to_string());
    }

    let mut counters: Vec<String> = Vec::new();
    for (field, accumulator) in spec.fields.iter().filter(|(field, _)| field != "_id") {
        let BsonValue::Document(accumulator) = accumulator
        else {
            return Err(format!("$group field `{field}` is not a `$sum` accumulator"));
        };
        match accumulator.get("$sum") {
            Some(BsonValue::Int32(1) | BsonValue::Int64(1)) => counters.push(field.clone()),
            Some(_) => return Err(format!("$group field `{field}` only supports `$sum: 1`")),
            None => return Err(format!("$group field `{field}` is not a `$sum` accumulator")),
        }
    }
    if counters.is_empty() {
        return Err("$group requires at least one `$sum` accumulator".to_string());
    }
    if documents.is_empty() {
        return Ok(Vec::new());
    }

    let mut grouped = BsonDocument::new();
    grouped.push("_id".to_string(), id.clone());
    for field in counters {
        let count = documents.len() as i64;
        grouped.push(field, count_value(count));
    }
    Ok(vec![grouped])
}

fn count_value(count: i64) -> BsonValue {
    i32::try_from(count).map(BsonValue::Int32).unwrap_or(BsonValue::Int64(count))
}

/// Applies a `$project` stage: inclusion (`field: 1`) keeps only the listed
/// fields plus `_id`, exclusion (`field: 0`) drops the listed fields.
fn project_document(document: &BsonDocument, spec: &BsonDocument) -> BsonDocument {
    let inclusion_only = spec.fields.iter().any(|(field, rule)| field != "_id" && rule_is_inclusion(rule));
    let id_excluded = spec.get("_id").map(rule_is_exclusion).unwrap_or(false);
    let mut projected = BsonDocument::new();
    for (field, value) in document.fields.iter() {
        let keep = match spec.get(field) {
            Some(rule) if rule_is_inclusion(rule) => true,
            Some(_) => false,
            None if field == "_id" => !id_excluded,
            None => !inclusion_only,
        };
        if keep {
            projected.push(field.clone(), value.clone());
        }
    }
    projected
}

fn rule_is_inclusion(value: &BsonValue) -> bool {
    matches!(value, BsonValue::Int32(1) | BsonValue::Int64(1) | BsonValue::Bool(true))
        || matches!(value, BsonValue::Double(number) if *number == 1.0)
}

fn rule_is_exclusion(value: &BsonValue) -> bool {
    matches!(value, BsonValue::Int32(0) | BsonValue::Int64(0) | BsonValue::Bool(false))
        || matches!(value, BsonValue::Double(number) if *number == 0.0)
}

/// Parses the `sort` document into `(field, direction)` pairs, ignoring
/// anything that is not a numeric direction.
fn parse_sort(sort: Option<&BsonValue>) -> Vec<(String, i32)> {
    match sort {
        Some(BsonValue::Document(document)) => document
            .fields
            .iter()
            .filter_map(|(field, direction)| int_value(direction).filter(|value| *value != 0).map(|value| (field.clone(), value as i32)))
            .collect(),
        _ => Vec::new(),
    }
}

fn sort_documents(documents: &mut [BsonDocument], sort: &[(String, i32)]) {
    if sort.is_empty() {
        return;
    }
    documents.sort_by(|left, right| compare_documents(left, right, sort));
}

fn compare_documents(left: &BsonDocument, right: &BsonDocument, sort: &[(String, i32)]) -> Ordering {
    for (field, direction) in sort {
        let ordering = match (lookup_field(left, field), lookup_field(right, field)) {
            (Some(left), Some(right)) => compare_values(left, right),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        };
        if ordering != Ordering::Equal {
            return if *direction < 0 { ordering.reverse() } else { ordering };
        }
    }
    Ordering::Equal
}

/// Returns whether a document satisfies every equality / comparison predicate.
/// One level of dotted nesting (`a.b`) is supported, which covers the document
/// shapes this gateway stores.
fn document_matches(document: &BsonDocument, filter: &BsonDocument) -> bool {
    for (field, expected) in &filter.fields {
        let (path, entry) = match expected {
            BsonValue::Document(operators) if operators.fields.iter().any(|(op, _)| op.starts_with('$')) => (field.as_str(), Some(operators)),
            _ => (field.as_str(), None),
        };
        let actual = lookup_field(document, path);
        match entry {
            Some(operators) => {
                for (operator, operand) in &operators.fields {
                    let matched = match operator.as_str() {
                        "$eq" => values_equal(actual, operand),
                        "$ne" => !values_equal(actual, operand),
                        "$gt" => actual.map(|value| compare_values(value, operand)) == Some(Ordering::Greater),
                        "$gte" => matches!(actual.map(|value| compare_values(value, operand)), Some(Ordering::Greater | Ordering::Equal)),
                        "$lt" => actual.map(|value| compare_values(value, operand)) == Some(Ordering::Less),
                        "$lte" => matches!(actual.map(|value| compare_values(value, operand)), Some(Ordering::Less | Ordering::Equal)),
                        _ => false,
                    };
                    if !matched {
                        return false;
                    }
                }
            }
            None => {
                if !values_equal(actual, expected) {
                    return false;
                }
            }
        }
    }
    true
}

fn lookup_field<'a>(document: &'a BsonDocument, path: &str) -> Option<&'a BsonValue> {
    if let Some(value) = document.get(path) {
        return Some(value);
    }
    let (head, tail) = path.split_once('.')?;
    match document.get(head)? {
        BsonValue::Document(inner) => inner.get(tail),
        _ => None,
    }
}

fn values_equal(actual: Option<&BsonValue>, expected: &BsonValue) -> bool {
    match (actual, expected) {
        (None, BsonValue::Null) => true,
        (None, _) => false,
        (Some(left), right) => match (left, right) {
            (BsonValue::Document(left), BsonValue::Document(right)) => encode_bson(left) == encode_bson(right),
            (BsonValue::Array(left), BsonValue::Array(right)) => {
                left.len() == right.len() && left.iter().zip(right.iter()).all(|(left, right)| values_equal(Some(left), right))
            }
            (BsonValue::Null, BsonValue::Null) => true,
            _ => numeric_equal(left, right).unwrap_or_else(|| scalar_key(left) == scalar_key(right)),
        },
    }
}

fn numeric_equal(left: &BsonValue, right: &BsonValue) -> Option<bool> {
    Some(number_of(left)? == number_of(right)?)
}

fn scalar_key(value: &BsonValue) -> String {
    match value {
        BsonValue::String(text) => format!("s:{text}"),
        BsonValue::Bool(flag) => format!("b:{flag}"),
        BsonValue::ObjectId(bytes) => format!("o:{bytes:?}"),
        BsonValue::Binary(data) => format!("y:{data:?}"),
        other => format!("x:{other:?}"),
    }
}

fn number_of(value: &BsonValue) -> Option<f64> {
    match value {
        BsonValue::Int32(number) => Some(*number as f64),
        BsonValue::Int64(number) => Some(*number as f64),
        BsonValue::Double(number) => Some(*number),
        _ => None,
    }
}

/// Total order used by `sort`: numbers compare numerically, strings
/// lexicographically, and cross-type pairs follow MongoDB's canonical order
/// (null, numbers, strings, documents, arrays, binary, object ids, booleans).
fn compare_values(left: &BsonValue, right: &BsonValue) -> Ordering {
    let rank = |value: &BsonValue| match value {
        // Undefined 已废弃，真机也不再产出，这里只给它一个稳定且低于 Null 的槽位。
        BsonValue::Undefined => 0,
        BsonValue::Null => 1,
        BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_) => 2,
        BsonValue::String(_) => 3,
        BsonValue::Document(_) => 4,
        BsonValue::Array(_) => 5,
        BsonValue::Binary(_) => 6,
        BsonValue::ObjectId(_) => 7,
        BsonValue::Bool(_) => 8,
    };
    if rank(left) != rank(right) {
        return rank(left).cmp(&rank(right));
    }
    match (left, right) {
        (BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_), _) => {
            let left = number_of(left).unwrap_or(0.0);
            let right = number_of(right).unwrap_or(0.0);
            left.partial_cmp(&right).unwrap_or(Ordering::Equal)
        }
        (BsonValue::String(left), BsonValue::String(right)) => left.cmp(right),
        (BsonValue::Bool(left), BsonValue::Bool(right)) => left.cmp(right),
        (BsonValue::ObjectId(left), BsonValue::ObjectId(right)) => left.cmp(right),
        (BsonValue::Binary(left), BsonValue::Binary(right)) => (left.subtype, &left.bytes).cmp(&(right.subtype, &right.bytes)),
        (BsonValue::Document(left), BsonValue::Document(right)) => encode_bson(left).cmp(&encode_bson(right)),
        (BsonValue::Array(left), BsonValue::Array(right)) => {
            for (left, right) in left.iter().zip(right.iter()) {
                let ordering = compare_values(left, right);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            left.len().cmp(&right.len())
        }
        _ => Ordering::Equal,
    }
}

fn parse_count(value: Option<&BsonValue>) -> usize {
    value.and_then(int_value).filter(|count| *count > 0).map(|count| count as usize).unwrap_or(0)
}

fn int_value(value: &BsonValue) -> Option<i64> {
    match value {
        BsonValue::Int32(number) => Some(*number as i64),
        BsonValue::Int64(number) => Some(*number),
        BsonValue::Double(number) => Some(*number as i64),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// cursors
// ---------------------------------------------------------------------------

/// One open `find` cursor holding the documents a later `getMore` will drain.
#[derive(Debug)]
struct CursorState {
    namespace: String,
    remaining: Vec<BsonValue>,
    last_used: Instant,
}

/// Cursors are reclaimed after this much idle time, mirroring the timeout a
/// driver expects from a real server.
const CURSOR_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

static NEXT_CURSOR_ID: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1);

fn lock_cursors() -> MutexGuard<'static, HashMap<i64, CursorState>> {
    match cursor_registry().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn cursor_registry() -> &'static Mutex<HashMap<i64, CursorState>> {
    static CURSORS: OnceLock<Mutex<HashMap<i64, CursorState>>> = OnceLock::new();
    CURSORS.get_or_init(|| Mutex::new(HashMap::new()))
}

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

fn handle_delete(doc: &BsonDocument, executor: Option<&dyn KeyValueExecutor>) -> Vec<u8> {
    let Some(executor) = executor
    else {
        return build_error_bson(2, "delete requires an executor");
    };

    let Some(ns) = namespace_from_command(doc, "delete")
    else {
        return build_error_bson(73, "delete command missing collection name");
    };

    let delete_specs = match doc.get("deletes") {
        Some(BsonValue::Array(arr)) => arr.clone(),
        _ => {
            let mut single = BsonDocument::new();
            single.push("q".to_string(), BsonValue::Document(BsonDocument::new()));
            single.push("limit".to_string(), BsonValue::Int32(0));
            vec![BsonValue::Document(single)]
        }
    };

    let mut deleted: i64 = 0;

    for spec in &delete_specs {
        let Some(spec_doc) = (match spec {
            BsonValue::Document(d) => Some(d),
            _ => None,
        })
        else {
            continue;
        };
        let filter = match spec_doc.get("q") {
            Some(BsonValue::Document(f)) => f,
            _ => &BsonDocument::new(),
        };
        let limit = match spec_doc.get("limit") {
            Some(BsonValue::Int32(n)) if *n >= 0 => *n as usize,
            _ => 0,
        };

        if let Some(id_value) = single_id_lookup(filter) {
            let key = value_to_key(id_value);
            if let Ok(database_query::cluster_kv::KeyValueResult::Delete { removed: true }) = executor.execute(
                None,
                database_query::cluster_kv::KeyValueCommand {
                    namespace: ns.clone(),
                    key,
                    action: database_query::cluster_kv::KeyValueAction::Delete,
                },
            ) {
                deleted += 1;
            }
        }
        else {
            let results = executor.execute_scan(None, &ns, b"", None, None, 0).unwrap_or_default();
            let mut removed_so_far = 0;
            for (k, value) in results {
                if limit > 0 && removed_so_far >= limit {
                    break;
                }
                let Ok(candidate) = parse_bson(&value)
                else {
                    continue;
                };
                if !document_matches(&candidate, filter) {
                    continue;
                }
                if let Ok(database_query::cluster_kv::KeyValueResult::Delete { removed: true }) = executor.execute(
                    None,
                    database_query::cluster_kv::KeyValueCommand {
                        namespace: ns.clone(),
                        key: k,
                        action: database_query::cluster_kv::KeyValueAction::Delete,
                    },
                ) {
                    deleted += 1;
                    removed_so_far += 1;
                }
            }
        }
    }

    let mut resp = BsonDocument::new();
    resp.push("n".to_string(), count_value(deleted));
    resp.push("ok".to_string(), write_ok());
    encode_bson(&resp)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn namespace_from_command(doc: &BsonDocument, command_key: &str) -> Option<database_types::cluster::Namespace> {
    let db = doc.database();
    let collection = match doc.get(command_key) {
        Some(BsonValue::String(collection)) if valid_collection_name(collection) => collection.clone(),
        _ => return None,
    };
    Some(database_types::cluster::Namespace(format!("{db}.{collection}")))
}

/// Applies MongoDB's namespace rules to a collection name: it cannot be empty,
/// cannot contain `$` or a null byte, cannot start or end with `.`, and cannot
/// carry an empty segment.
fn valid_collection_name(name: &str) -> bool {
    if name.is_empty() || name.contains('$') || name.contains('\0') {
        return false;
    }
    if name.starts_with('.') || name.ends_with('.') {
        return false;
    }
    !name.contains("..")
}

fn namespace_string(doc: &BsonDocument, command_key: &str) -> String {
    let db = doc.database();
    let collection = match doc.get(command_key) {
        Some(BsonValue::String(c)) => c.clone(),
        _ => "unknown".to_string(),
    };
    format!("{db}.{collection}")
}

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
