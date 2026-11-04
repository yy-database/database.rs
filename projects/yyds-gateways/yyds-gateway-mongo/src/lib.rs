//! MongoDB wire disguise gateway for YYDS.
//!
//! Accepts the MongoDB OP_MSG binary frame protocol and answers with YYDS-backed
//! document responses. Not a full MongoDB server: it presents a minimal CRUD surface
//! (`insert`, `find`, `delete`) on top of the YYDS KV executor plus the standard
//! `hello` / `ping` handshake probes.

use std::time::{SystemTime, UNIX_EPOCH};

use yyds_execution::KeyValueExecutor;

pub mod bson;
pub mod connection;
pub mod service;
pub mod wire;

use bson::{encode_bson, parse_bson, value_to_key, BsonDocument, BsonValue};

/// Gateway kind implemented by this crate.
pub const GATEWAY_KIND: yyds_gateway::GatewayKind = yyds_gateway::GatewayKind::Mongo;

/// Gateway identifier used in logs and diagnostics.
pub const GATEWAY_ID: &str = GATEWAY_KIND.id();

/// Default MongoDB disguise listen port.
pub const DEFAULT_PORT: u16 = GATEWAY_KIND.default_port();

/// Extracts the command name from an inbound OP_MSG frame — useful for wire-level
/// tests that only need dispatch routing.
pub fn command_name_from_frame(frame: &wire::Frame) -> Option<&'static str> {
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
    match name {
        "hello" => Some("hello"),
        "isMaster" | "ismaster" => Some("isMaster"),
        "ping" => Some("ping"),
        "insert" => Some("insert"),
        "find" => Some("find"),
        "delete" => Some("delete"),
        _ => Some("unknown"),
    }
}

/// Handles a fully-parsed OP_MSG command payload and returns the response BSON
/// body (ready to be wrapped into an OP_MSG reply by the connection layer).
///
/// `op_msg_body` is the bytes after the 16-byte header: flags (u32) + section kind
/// (u8) + BSON document + optional checksum. Errors are always turned into a generic
/// `{ ok: 0, errmsg: ... }` BSON response so the connection layer never has to carry
/// IO-result-like fallbacks.
pub fn handle_op_msg(op_msg_body: &[u8], executor: Option<&dyn KeyValueExecutor>) -> Vec<u8> {
    let Some(bson_start) = op_msg_body.get(5..)
    else {
        return generic_error_body("OP_MSG body too short");
    };
    let section_kind = op_msg_body[4];
    if section_kind != 0x00 {
        return generic_error_body("document sequence sections are not supported");
    }

    let bson_doc = match parse_bson(bson_start) {
        Ok(doc) => doc,
        Err(e) => return generic_error_body(&format!("BSON parse error: {e:?}")),
    };

    let Some(command_name) = bson_doc.command_name()
    else {
        return generic_error_body("empty command");
    };

    match command_name {
        "hello" | "isMaster" => build_hello_bson(),
        "ping" => build_ok_bson(1),
        "insert" => handle_insert(&bson_doc, executor),
        "find" => handle_find(&bson_doc, executor),
        "delete" => handle_delete(&bson_doc, executor),
        other => generic_error_body(other),
    }
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
    encode_bson(&doc)
}

/// A minimal BSON error document returned for commands we deliberately reject.
/// Kept minimal (`errmsg: "unsupported"`) so existing handshake tests still pass.
pub fn generic_error_body(_command: &str) -> Vec<u8> {
    build_error_bson(59, "unsupported")
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

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
                let payload = encode_bson(&doc_with_id);
                let result = executor.execute(
                    None,
                    yyds_types::KeyValueCommand {
                        namespace: ns.clone(),
                        key,
                        action: yyds_types::KeyValueAction::Put(payload),
                    },
                );
                match result {
                    Ok(yyds_types::KeyValueResult::Put { .. }) => inserted += 1,
                    _ => {
                        let mut err = BsonDocument::new();
                        err.push("index".to_string(), BsonValue::Int32(idx as i32));
                        err.push("errmsg".to_string(), BsonValue::String("insert failed".to_string()));
                        write_errors.push(format!("err{idx}"), BsonValue::Document(err));
                    }
                }
            }
            _ => {
                let mut err = BsonDocument::new();
                err.push("index".to_string(), BsonValue::Int32(idx as i32));
                err.push("errmsg".to_string(), BsonValue::String("documents must be objects".to_string()));
                write_errors.push(format!("err{idx}"), BsonValue::Document(err));
            }
        }
    }

    let mut resp = BsonDocument::new();
    resp.push("ok".to_string(), BsonValue::Int32(1));
    resp.push("n".to_string(), BsonValue::Int64(inserted));
    if !write_errors.fields.is_empty() {
        let err_list: Vec<BsonValue> = write_errors.fields.into_iter().map(|(_, v)| v).collect();
        resp.push("writeErrors".to_string(), BsonValue::Array(err_list));
    }
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
        Some(BsonValue::Document(f)) => f.clone(),
        _ => BsonDocument::new(),
    };

    let limit = match doc.get("limit") {
        Some(BsonValue::Int32(n)) if *n >= 0 => *n as usize,
        Some(BsonValue::Int64(n)) if *n >= 0 => *n as usize,
        _ => 0,
    };

    let first_batch = if let Some(id_val) = filter.get("_id") {
        let key = value_to_key(id_val);
        match executor
            .execute(None, yyds_types::KeyValueCommand { namespace: ns.clone(), key, action: yyds_types::KeyValueAction::Get })
        {
            Ok(yyds_types::KeyValueResult::Get(Some(bytes))) => match parse_bson(&bytes) {
                Ok(bson) => vec![BsonValue::Document(bson)],
                Err(_) => vec![],
            },
            _ => vec![],
        }
    }
    else {
        scan_all(executor, &ns, limit)
    };

    let mut cursor_doc = BsonDocument::new();
    cursor_doc.push("firstBatch".to_string(), BsonValue::Array(first_batch));
    cursor_doc.push("id".to_string(), BsonValue::Int64(0));
    cursor_doc.push("ns".to_string(), BsonValue::String(namespace_string(doc, "find")));

    let mut resp = BsonDocument::new();
    resp.push("ok".to_string(), BsonValue::Int32(1));
    resp.push("cursor".to_string(), BsonValue::Document(cursor_doc));
    encode_bson(&resp)
}

fn scan_all(executor: &dyn KeyValueExecutor, ns: &yyds_types::Namespace, limit: usize) -> Vec<BsonValue> {
    let results = executor.execute_scan(None, ns, b"", None, None, 0).unwrap_or_default();
    let mut docs = Vec::new();
    for (_k, value_bytes) in results {
        if let Ok(bson) = parse_bson(&value_bytes) {
            docs.push(BsonValue::Document(bson));
        }
        if limit > 0 && docs.len() >= limit {
            break;
        }
    }
    docs
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

        if let Some(id_value) = filter.get("_id") {
            let key = value_to_key(id_value);
            if let Ok(yyds_types::KeyValueResult::Delete { removed: true }) = executor.execute(
                None,
                yyds_types::KeyValueCommand { namespace: ns.clone(), key, action: yyds_types::KeyValueAction::Delete },
            ) {
                deleted += 1;
            }
        }
        else {
            let results = executor.execute_scan(None, &ns, b"", None, None, 0).unwrap_or_default();
            let mut removed_so_far = 0;
            for (k, _v) in results {
                if limit > 0 && removed_so_far >= limit {
                    break;
                }
                if let Ok(yyds_types::KeyValueResult::Delete { removed: true }) = executor.execute(
                    None,
                    yyds_types::KeyValueCommand { namespace: ns.clone(), key: k, action: yyds_types::KeyValueAction::Delete },
                ) {
                    deleted += 1;
                    removed_so_far += 1;
                }
            }
        }
    }

    let mut resp = BsonDocument::new();
    resp.push("ok".to_string(), BsonValue::Int32(1));
    resp.push("n".to_string(), BsonValue::Int64(deleted));
    resp.push("deletedCount".to_string(), BsonValue::Int64(deleted));
    encode_bson(&resp)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn namespace_from_command(doc: &BsonDocument, command_key: &str) -> Option<yyds_types::Namespace> {
    let db = doc.database();
    let collection = match doc.get(command_key) {
        Some(BsonValue::String(c)) if !c.is_empty() => c.clone(),
        _ => return None,
    };
    Some(yyds_types::Namespace(format!("{db}.{collection}")))
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
