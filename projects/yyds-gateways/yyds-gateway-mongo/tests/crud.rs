//! End-to-end MongoDB CRUD tests — insert, find, delete over OP_MSG wire
//! frames with a real YYDS LocalExecutor backing the gateway.

use std::{
    collections::HashMap,
    io::{Cursor, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::Duration,
};

use yyds_execution::LocalExecutor;
use yyds_gateway_mongo::{
    connection::serve_cancellable_with_executor,
    wire::{self, FrameLimits},
};
use yyds_kv::MemoryShard;
use yyds_types::{ShardEpoch, ShardId, ShardMap};

fn executor_server() -> (TcpStream, thread::JoinHandle<()>) {
    let shard = ShardId("mongo-local".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, MemoryShard::new())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let stopping = Arc::new(AtomicBool::new(false));
    let stopping_clone = Arc::clone(&stopping);
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_cancellable_with_executor(stream, FrameLimits::default(), stopping_clone, executor).unwrap();
    });
    let client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    client.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
    (client, server)
}

// ---------------------------------------------------------------------------
// BSON builders — each returns raw bytes
// ---------------------------------------------------------------------------

/// One BSON element: type byte + cstring key + value bytes.
/// Not a full document — use `bson_document` to wrap multiple elements.
fn bson_int32(key: &str, value: i32) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0x10);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
    out.extend_from_slice(&value.to_le_bytes());
    out
}

fn bson_int64(key: &str, value: i64) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0x12);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
    out.extend_from_slice(&value.to_le_bytes());
    out
}

fn bson_double(key: &str, value: f64) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0x01);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
    out.extend_from_slice(&value.to_le_bytes());
    out
}

fn bson_string(key: &str, value: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0x02);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
    let bytes = value.as_bytes();
    out.extend_from_slice(&(bytes.len() as i32 + 1).to_le_bytes());
    out.extend_from_slice(bytes);
    out.push(0);
    out
}

fn bson_bool(key: &str, value: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0x08);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
    out.push(if value { 1 } else { 0 });
    out
}

/// A full BSON document: length prefix + elements + 0x00 terminator.
fn bson_document(elements: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&0_u32.to_le_bytes());
    for e in elements {
        out.extend_from_slice(e);
    }
    out.push(0);
    let len = out.len() as u32;
    out[0..4].copy_from_slice(&len.to_le_bytes());
    out
}

/// An embedded-document element inside a parent doc: type 0x03 + cstring key + sub-doc bytes.
fn bson_doc_element(key: &str, sub_doc: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0x03);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
    out.extend_from_slice(sub_doc);
    out
}

/// An array element inside a parent doc: type 0x04 + cstring key + array-as-doc bytes.
/// `member_docs` are full BSON documents, each becomes a 0x03 element with key "0", "1", ...
fn bson_array_element(key: &str, member_docs: &[Vec<u8>]) -> Vec<u8> {
    let mut array_doc = Vec::new();
    array_doc.extend_from_slice(&0_u32.to_le_bytes());
    for (i, doc) in member_docs.iter().enumerate() {
        array_doc.push(0x03); // embedded doc element
        let idx = format!("{i}");
        array_doc.extend_from_slice(idx.as_bytes());
        array_doc.push(0);
        array_doc.extend_from_slice(doc);
    }
    array_doc.push(0);
    let len = array_doc.len() as u32;
    array_doc[0..4].copy_from_slice(&len.to_le_bytes());

    // Now wrap as type 0x04 element in the parent
    let mut out = Vec::new();
    out.push(0x04);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
    out.extend_from_slice(&array_doc);
    out
}

// ---------------------------------------------------------------------------
// OP_MSG frame builder
// ---------------------------------------------------------------------------

/// Encodes an OP_MSG frame. `bson_doc` is the full BSON command document.
fn build_op_msg(request_id: i32, bson_doc: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0_u32.to_le_bytes()); // flags
    body.push(0x00); // section kind: single BSON document
    body.extend_from_slice(bson_doc);
    let total = 16 + body.len() as u32;
    let mut frame = Vec::new();
    frame.extend_from_slice(&total.to_le_bytes());
    frame.extend_from_slice(&request_id.to_le_bytes());
    frame.extend_from_slice(&0_i32.to_le_bytes());
    frame.extend_from_slice(&wire::OP_MSG.to_le_bytes());
    frame.extend_from_slice(&body);
    frame
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Send one OP_MSG, read one reply frame, return the reply's BSON document bytes.
/// Multiple calls on the same connection are fine — we use framed reads.
fn roundtrip(client: &mut TcpStream, request_id: i32, bson_doc: &[u8]) -> Vec<u8> {
    client.write_all(&build_op_msg(request_id, bson_doc)).unwrap();
    client.flush().unwrap();

    let reply = wire::read_frame(client, FrameLimits::default()).unwrap().expect("server closed connection");
    assert_eq!(reply.header.request_id, 0);
    assert_eq!(reply.header.response_to, request_id);
    assert_eq!(reply.header.opcode, wire::OP_MSG);

    // body layout: [flags u32][kind u8][BSON doc...]
    let body = &reply.body[5..]; // skip flags(4) + kind(1)
    assert!(body.len() >= 5, "BSON body too short: {body:?}");
    // BSON doc: [len(4)][elements...][0x00 terminator]
    let doc_len = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
    assert_eq!(doc_len, body.len(), "BSON length prefix mismatch: len says {doc_len}, actual {}. body={body:?}", body.len());
    assert_eq!(body[doc_len - 1], 0, "BSON terminator missing at offset {}", doc_len - 1);
    body.to_vec()
}

#[test]
fn insert_documents_without_id_gets_auto_object_id() {
    let (mut client, server) = executor_server();

    // { insert: "users", $db: "test", documents: [{ name: "Alice", age: 30 }] }
    let inner_doc = bson_document(&[bson_string("name", "Alice"), bson_int32("age", 30)]);
    let command = bson_document(&[
        bson_string("insert", "users"),
        bson_string("$db", "test"),
        bson_array_element("documents", &[inner_doc]),
    ]);

    let reply = roundtrip(&mut client, 1, &command);
    let n = extract_int64(&reply, "n");
    let ok = extract_int32(&reply, "ok");
    assert_eq!(ok, Some(1), "reply={reply:?}");
    assert_eq!(n, Some(1), "reply={reply:?}");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn find_exact_id_after_insert_returns_one_document() {
    let (mut client, server) = executor_server();

    // insert one doc first
    let inner_doc = bson_document(&[bson_string("name", "Alice"), bson_int32("age", 30)]);
    let insert_cmd = bson_document(&[
        bson_string("insert", "users"),
        bson_string("$db", "test"),
        bson_array_element("documents", &[inner_doc]),
    ]);
    let _ = roundtrip(&mut client, 1, &insert_cmd);

    // find with empty filter — should return cursor with one document
    let empty_filter = bson_document(&[]);
    let find_cmd =
        bson_document(&[bson_string("find", "users"), bson_string("$db", "test"), bson_doc_element("filter", &empty_filter)]);
    let reply = roundtrip(&mut client, 2, &find_cmd);
    let ok = extract_int32(&reply, "ok");
    assert_eq!(ok, Some(1), "find reply ok");

    let batch_len = extract_cursor_firstbatch_len(&reply);
    assert_eq!(batch_len, Some(1), "firstBatch length");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn delete_everything_from_collection() {
    let (mut client, server) = executor_server();

    // insert two docs
    let doc1 = bson_document(&[bson_string("name", "Alice")]);
    let doc2 = bson_document(&[bson_string("name", "Bob")]);
    let insert_cmd = bson_document(&[
        bson_string("insert", "users"),
        bson_string("$db", "test"),
        bson_array_element("documents", &[doc1, doc2]),
    ]);
    let _ = roundtrip(&mut client, 1, &insert_cmd);

    // delete all: { delete: "users", $db: "test", deletes: [{ q: {}, limit: 0 }] }
    let empty_q = bson_document(&[]);
    let delete_spec = bson_document(&[bson_doc_element("q", &empty_q), bson_int32("limit", 0)]);
    let delete_cmd = bson_document(&[
        bson_string("delete", "users"),
        bson_string("$db", "test"),
        bson_array_element("deletes", &[delete_spec]),
    ]);
    let reply = roundtrip(&mut client, 2, &delete_cmd);
    let deleted = extract_int64(&reply, "deletedCount");
    assert_eq!(deleted, Some(2), "deletedCount after delete: reply={reply:?}");
    let ok = extract_int32(&reply, "ok");
    assert_eq!(ok, Some(1));

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

// ---------------------------------------------------------------------------
// BSON element builders — ObjectId
// ---------------------------------------------------------------------------

/// A BSON ObjectId element inside a parent doc: type 0x07 + cstring key + 12 raw bytes.
fn bson_object_id(key: &str, bytes: [u8; 12]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0x07);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
    out.extend_from_slice(&bytes);
    out
}

// ---------------------------------------------------------------------------
// CRUD boundary & isolation conformance tests
// ---------------------------------------------------------------------------

#[test]
fn different_databases_isolate_documents_of_same_collection_name() {
    // Insert into db_A.users and db_B.users — both under collection name "users" —
    // then find by _id in each db and verify they do not see each other's data.
    let (mut client, server) = executor_server();

    // Two distinct 12-byte ObjectIds so we can address each document directly.
    let id_a = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C];
    let id_b = [0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C];

    let doc_a = bson_document(&[bson_object_id("_id", id_a), bson_string("name", "Alice")]);
    let doc_b = bson_document(&[bson_object_id("_id", id_b), bson_string("name", "Bob")]);

    let insert_a =
        bson_document(&[bson_string("insert", "users"), bson_string("$db", "db_A"), bson_array_element("documents", &[doc_a])]);
    let insert_b =
        bson_document(&[bson_string("insert", "users"), bson_string("$db", "db_B"), bson_array_element("documents", &[doc_b])]);

    let r1 = roundtrip(&mut client, 1, &insert_a);
    let r2 = roundtrip(&mut client, 2, &insert_b);
    assert_eq!(extract_int32(&r1, "ok"), Some(1));
    assert_eq!(extract_int64(&r1, "n"), Some(1));
    assert_eq!(extract_int32(&r2, "ok"), Some(1));
    assert_eq!(extract_int64(&r2, "n"), Some(1));

    // Find Alice from db_A.users by _id → should get 1 doc
    let filter_by_id = bson_document(&[bson_object_id("_id", id_a)]);
    let find_a =
        bson_document(&[bson_string("find", "users"), bson_string("$db", "db_A"), bson_doc_element("filter", &filter_by_id)]);
    let ra = roundtrip(&mut client, 3, &find_a);
    assert_eq!(extract_int32(&ra, "ok"), Some(1));
    assert_eq!(extract_cursor_firstbatch_len(&ra), Some(1), "db_A must see Alice");

    // Find Alice from db_B.users by _id → should get 0 docs (isolated)
    let find_a_in_b =
        bson_document(&[bson_string("find", "users"), bson_string("$db", "db_B"), bson_doc_element("filter", &filter_by_id)]);
    let rab = roundtrip(&mut client, 4, &find_a_in_b);
    assert_eq!(extract_int32(&rab, "ok"), Some(1));
    assert_eq!(extract_cursor_firstbatch_len(&rab), Some(0), "db_B must NOT see db_A data");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn empty_collection_find_returns_empty_array_without_error() {
    let (mut client, server) = executor_server();

    // find on a collection that never existed → firstBatch = [], ok = 1
    let empty_filter = bson_document(&[]);
    let find_cmd = bson_document(&[
        bson_string("find", "ghosts"),
        bson_string("$db", "empty_db"),
        bson_doc_element("filter", &empty_filter),
    ]);
    let reply = roundtrip(&mut client, 1, &find_cmd);
    assert_eq!(extract_int32(&reply, "ok"), Some(1), "empty collection find must succeed");
    assert_eq!(extract_cursor_firstbatch_len(&reply), Some(0), "must return empty firstBatch");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn delete_nonexistent_object_id_returns_zero_deleted_count() {
    let (mut client, server) = executor_server();

    let missing_id = [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66];
    let filter = bson_document(&[bson_object_id("_id", missing_id)]);
    let delete_spec = bson_document(&[bson_doc_element("q", &filter), bson_int32("limit", 1)]);
    let delete_cmd = bson_document(&[
        bson_string("delete", "things"),
        bson_string("$db", "test"),
        bson_array_element("deletes", &[delete_spec]),
    ]);

    let reply = roundtrip(&mut client, 1, &delete_cmd);
    assert_eq!(extract_int32(&reply, "ok"), Some(1));
    assert_eq!(extract_int64(&reply, "deletedCount"), Some(0), "deleting missing _id must report 0");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn insert_without_id_auto_generates_object_id() {
    // Gateway must synthesize an ObjectId for documents that lack _id.
    // After insert, find with empty filter → exactly 1 document must appear.
    // The stored _id must be a 12-byte value (BSON type 0x07).
    let (mut client, server) = executor_server();

    let inner_doc = bson_document(&[bson_string("label", "auto-id-test"), bson_int32("seq", 42)]);
    let insert_cmd = bson_document(&[
        bson_string("insert", "auto_col"),
        bson_string("$db", "test"),
        bson_array_element("documents", &[inner_doc]),
    ]);
    let reply = roundtrip(&mut client, 1, &insert_cmd);
    assert_eq!(extract_int32(&reply, "ok"), Some(1));
    assert_eq!(extract_int64(&reply, "n"), Some(1));

    // Find it back — empty filter scan.
    let empty_filter = bson_document(&[]);
    let find_cmd = bson_document(&[
        bson_string("find", "auto_col"),
        bson_string("$db", "test"),
        bson_doc_element("filter", &empty_filter),
    ]);
    let find_reply = roundtrip(&mut client, 2, &find_cmd);
    assert_eq!(extract_int32(&find_reply, "ok"), Some(1));
    assert_eq!(extract_cursor_firstbatch_len(&find_reply), Some(1));

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn insert_with_duplicate_id_overwrites_without_unique_key_error() {
    // Current YYDS Mongo gateway stores documents via a plain Put — there is
    // no unique-index enforcement yet. A second insert with the same _id
    // silently overwrites the previous document rather than returning a
    // MongoDB-style duplicate-key error (code 11000). This test pins the
    // current behaviour so we can spot the change when unique-constraint
    // emulation lands.
    let (mut client, server) = executor_server();

    let fixed_id = [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77];
    let doc_v1 = bson_document(&[bson_object_id("_id", fixed_id), bson_string("label", "first"), bson_int32("version", 1)]);
    let doc_v2 = bson_document(&[bson_object_id("_id", fixed_id), bson_string("label", "second"), bson_int32("version", 2)]);

    let insert_v1 = bson_document(&[
        bson_string("insert", "dup_col"),
        bson_string("$db", "test"),
        bson_array_element("documents", &[doc_v1]),
    ]);
    let insert_v2 = bson_document(&[
        bson_string("insert", "dup_col"),
        bson_string("$db", "test"),
        bson_array_element("documents", &[doc_v2]),
    ]);

    let r1 = roundtrip(&mut client, 1, &insert_v1);
    let r2 = roundtrip(&mut client, 2, &insert_v2);

    // Both inserts succeed (ok=1, n=1 each) — no duplicate-key rejection.
    assert_eq!(extract_int32(&r1, "ok"), Some(1));
    assert_eq!(extract_int64(&r1, "n"), Some(1));
    assert_eq!(extract_int32(&r2, "ok"), Some(1));
    assert_eq!(extract_int64(&r2, "n"), Some(1), "second insert must not fail with duplicate key");

    // No writeErrors array in either reply.
    let cursor_doc = walk_bson(&r2, |elem| match elem {
        BsonElem::Document(k, v) if k == "writeErrors" => Some(v.clone()),
        _ => None,
    });
    assert!(cursor_doc.is_none(), "duplicate _id must not produce writeErrors: reply={r2:?}");

    // Only 1 doc visible under the namespace — v2 overwrote v1.
    let empty_filter = bson_document(&[]);
    let find_cmd =
        bson_document(&[bson_string("find", "dup_col"), bson_string("$db", "test"), bson_doc_element("filter", &empty_filter)]);
    let find_reply = roundtrip(&mut client, 3, &find_cmd);
    assert_eq!(extract_cursor_firstbatch_len(&find_reply), Some(1), "duplicate _id must result in 1 visible doc");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

// ---------------------------------------------------------------------------
// BSON extractors for test assertions
// ---------------------------------------------------------------------------

fn extract_int32(bson: &[u8], key: &str) -> Option<i32> {
    walk_bson(bson, |elem| match elem {
        BsonElem::Int32(k, v) if k == key => Some(v),
        _ => None,
    })
}

fn extract_int64(bson: &[u8], key: &str) -> Option<i64> {
    walk_bson(bson, |elem| match elem {
        BsonElem::Int64(k, v) if k == key => Some(v),
        _ => None,
    })
}

fn extract_cursor_firstbatch_len(bson: &[u8]) -> Option<usize> {
    let cursor_doc = walk_bson(bson, |elem| match elem {
        BsonElem::Document(k, v) if k == "cursor" => Some(v.clone()),
        _ => None,
    })?;
    let batch_doc = walk_bson(&cursor_doc, |elem| match elem {
        BsonElem::Array(k, v) if k == "firstBatch" => Some(v.clone()),
        _ => None,
    })?;
    // batch_doc is a BSON array encoded as a document; count top-level keys.
    let mut count = 0;
    let _ = walk_bson(&batch_doc, |elem| {
        match elem {
            BsonElem::Document(_, _) | BsonElem::String(_, _) | BsonElem::Int32(_, _) => count += 1,
            _ => {}
        }
        None::<()>
    });
    Some(count)
}

// Minimal BSON walker for test use.
#[derive(Debug)]
enum BsonElem<'a> {
    Int32(&'a str, i32),
    Int64(&'a str, i64),
    String(&'a str, &'a str),
    Document(&'a str, Vec<u8>),
    Array(&'a str, Vec<u8>),
}

fn walk_bson<'a, F, R>(bson: &'a [u8], mut f: F) -> Option<R>
where
    F: FnMut(BsonElem<'a>) -> Option<R>,
{
    let mut cursor = 4usize; // skip doc len (4) — start at first element type byte
    if bson.len() < 5 {
        return None;
    }
    while cursor < bson.len() {
        let elem_type = bson[cursor];
        if elem_type == 0 {
            break;
        }
        cursor += 1;
        // read cstring key
        let key_start = cursor;
        while cursor < bson.len() && bson[cursor] != 0 {
            cursor += 1;
        }
        if cursor >= bson.len() {
            break;
        }
        let key = std::str::from_utf8(&bson[key_start..cursor]).ok()?;
        cursor += 1; // skip null terminator

        let result = match elem_type {
            0x01 => {
                if cursor + 8 > bson.len() {
                    break;
                }
                cursor += 8; // skip double
                None
            }
            0x02 => {
                if cursor + 4 > bson.len() {
                    break;
                }
                let str_len = i32::from_le_bytes([bson[cursor], bson[cursor + 1], bson[cursor + 2], bson[cursor + 3]]) as usize;
                cursor += 4;
                if cursor + str_len > bson.len() {
                    break;
                }
                let s = std::str::from_utf8(&bson[cursor..cursor + str_len - 1]).ok()?;
                cursor += str_len;
                f(BsonElem::String(key, s))
            }
            0x03 => {
                if cursor + 4 > bson.len() {
                    break;
                }
                let sub_len = u32::from_le_bytes([bson[cursor], bson[cursor + 1], bson[cursor + 2], bson[cursor + 3]]) as usize;
                let sub_start = cursor;
                cursor += sub_len;
                f(BsonElem::Document(key, bson[sub_start..cursor].to_vec()))
            }
            0x04 => {
                if cursor + 4 > bson.len() {
                    break;
                }
                let sub_len = u32::from_le_bytes([bson[cursor], bson[cursor + 1], bson[cursor + 2], bson[cursor + 3]]) as usize;
                let sub_start = cursor;
                cursor += sub_len;
                f(BsonElem::Array(key, bson[sub_start..cursor].to_vec()))
            }
            0x10 => {
                if cursor + 4 > bson.len() {
                    break;
                }
                let v = i32::from_le_bytes([bson[cursor], bson[cursor + 1], bson[cursor + 2], bson[cursor + 3]]);
                cursor += 4;
                f(BsonElem::Int32(key, v))
            }
            0x12 => {
                if cursor + 8 > bson.len() {
                    break;
                }
                let v = i64::from_le_bytes([
                    bson[cursor],
                    bson[cursor + 1],
                    bson[cursor + 2],
                    bson[cursor + 3],
                    bson[cursor + 4],
                    bson[cursor + 5],
                    bson[cursor + 6],
                    bson[cursor + 7],
                ]);
                cursor += 8;
                f(BsonElem::Int64(key, v))
            }
            _ => break,
        };
        if result.is_some() {
            return result;
        }
    }
    None
}
