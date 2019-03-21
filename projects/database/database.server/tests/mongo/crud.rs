//! End-to-end MongoDB CRUD tests — insert, find, delete over OP_MSG wire
//! frames with a real YYDS LocalExecutor backing the gateway.

use std::{
    collections::HashMap,
    io::{Cursor, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex, atomic::AtomicBool},
    thread,
    time::Duration,
};

use database_cluster::topology::ShardMap;
use database_execution::cluster::LocalExecutor;
use database_server::yyds::mongo::{
    connection::serve_cancellable_with_executor,
    wire::{self, FrameLimits},
};
use database_storage::cluster_kv::MemoryShard;
use database_types::cluster::{ShardEpoch, ShardId};
use mongo::cursor::CursorStore;

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
    let cursors = Arc::new(Mutex::new(CursorStore::default()));
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_cancellable_with_executor(stream, FrameLimits::default(), stopping_clone, executor, cursors).unwrap();
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

/// Encodes an OP_MSG frame whose bulk payload travels in a document-sequence
/// section (kind `0x01`) instead of inline in the command document — the shape
/// PyMongo sends for `insert.documents`.
fn build_op_msg_with_document_sequence(request_id: i32, command: &[u8], identifier: &str, documents: &[Vec<u8>]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0_u32.to_le_bytes()); // flags
    body.push(0x00); // section kind: single BSON document
    body.extend_from_slice(command);

    let mut section = vec![0_u8; 4]; // size placeholder, filled below
    section.extend_from_slice(identifier.as_bytes());
    section.push(0);
    for document in documents {
        section.extend_from_slice(document);
    }
    let size = section.len() as u32;
    section[0..4].copy_from_slice(&size.to_le_bytes());
    body.push(0x01); // section kind: document sequence
    body.extend_from_slice(&section);

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
    roundtrip_frame(client, request_id, build_op_msg(request_id, bson_doc))
}

fn roundtrip_frame(client: &mut TcpStream, request_id: i32, frame: Vec<u8>) -> Vec<u8> {
    client.write_all(&frame).unwrap();
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
    let command = bson_document(&[bson_string("insert", "users"), bson_string("$db", "test"), bson_array_element("documents", &[inner_doc])]);

    let reply = roundtrip(&mut client, 1, &command);
    let n = extract_int32(&reply, "n");
    let ok = extract_double(&reply, "ok");
    assert_eq!(ok, Some(1.0), "reply={reply:?}");
    assert_eq!(n, Some(1), "reply={reply:?}");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn find_exact_id_after_insert_returns_one_document() {
    let (mut client, server) = executor_server();

    // insert one doc first
    let inner_doc = bson_document(&[bson_string("name", "Alice"), bson_int32("age", 30)]);
    let insert_cmd =
        bson_document(&[bson_string("insert", "users"), bson_string("$db", "test"), bson_array_element("documents", &[inner_doc])]);
    let _ = roundtrip(&mut client, 1, &insert_cmd);

    // find with empty filter — should return cursor with one document
    let empty_filter = bson_document(&[]);
    let find_cmd = bson_document(&[bson_string("find", "users"), bson_string("$db", "test"), bson_doc_element("filter", &empty_filter)]);
    let reply = roundtrip(&mut client, 2, &find_cmd);
    let ok = extract_double(&reply, "ok");
    assert_eq!(ok, Some(1.0), "find reply ok");

    let batch_len = extract_cursor_firstbatch_len(&reply);
    assert_eq!(batch_len, Some(1), "firstBatch length");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn get_more_refuses_a_namespace_that_did_not_open_the_cursor() {
    let (mut client, server) = executor_server();

    let documents: Vec<Vec<u8>> = (0..4).map(|index| bson_document(&[bson_int32("_id", index)])).collect();
    let insert_cmd =
        bson_document(&[bson_string("insert", "private"), bson_string("$db", "tenant_a"), bson_array_element("documents", &documents)]);
    let _ = roundtrip(&mut client, 1, &insert_cmd);

    let find_cmd = bson_document(&[
        bson_string("find", "private"),
        bson_string("$db", "tenant_a"),
        bson_doc_element("filter", &bson_document(&[])),
        bson_int32("batchSize", 2),
    ]);
    let find_reply = roundtrip(&mut client, 2, &find_cmd);
    let id = extract_cursor_id(&find_reply).expect("find leaves a cursor");
    assert_ne!(id, 0, "find must keep the cursor alive");
    assert_eq!(extract_cursor_firstbatch_len(&find_reply), Some(2));

    // The id alone is not an authorization to read another namespace.
    let foreign = bson_document(&[bson_int64("getMore", id), bson_string("collection", "other"), bson_string("$db", "tenant_b")]);
    let refused = roundtrip(&mut client, 3, &foreign);
    assert_eq!(extract_int32(&refused, "code"), Some(13), "cross-namespace getMore reply={refused:?}");
    assert_eq!(extract_string(&refused, "codeName"), Some("Unauthorized".to_string()));
    assert_eq!(
        extract_string(&refused, "errmsg"),
        Some("Requested getMore on namespace 'tenant_b.other', but cursor belongs to a different namespace tenant_a.private".to_string())
    );

    // The refusal does not consume the cursor for the namespace that owns it.
    let own = bson_document(&[bson_int64("getMore", id), bson_string("collection", "private"), bson_string("$db", "tenant_a")]);
    let next = roundtrip(&mut client, 4, &own);
    assert_eq!(extract_cursor_batch_len(&next, "nextBatch"), Some(2), "reply={next:?}");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn cursor_tables_of_two_services_do_not_cross() {
    let (mut first, first_server) = executor_server();
    let (mut second, second_server) = executor_server();

    let documents: Vec<Vec<u8>> = (0..4).map(|index| bson_document(&[bson_int32("_id", index)])).collect();
    let insert_cmd =
        bson_document(&[bson_string("insert", "private"), bson_string("$db", "tenant_a"), bson_array_element("documents", &documents)]);
    let _ = roundtrip(&mut first, 1, &insert_cmd);

    let find_cmd = bson_document(&[
        bson_string("find", "private"),
        bson_string("$db", "tenant_a"),
        bson_doc_element("filter", &bson_document(&[])),
        bson_int32("batchSize", 2),
    ]);
    let find_reply = roundtrip(&mut first, 2, &find_cmd);
    let id = extract_cursor_id(&find_reply).expect("find leaves a cursor");

    // The same numeric id on a second service instance must not resolve.
    let crossing = bson_document(&[bson_int64("getMore", id), bson_string("collection", "private"), bson_string("$db", "tenant_a")]);
    let stranger = roundtrip(&mut second, 1, &crossing);
    assert_eq!(extract_int32(&stranger, "code"), Some(43), "a second instance must not see the cursor, reply={stranger:?}");

    let own = roundtrip(&mut first, 3, &crossing);
    assert_eq!(extract_cursor_batch_len(&own, "nextBatch"), Some(2), "reply={own:?}");

    let _ = first.shutdown(Shutdown::Write);
    let _ = second.shutdown(Shutdown::Write);
    let _ = first_server.join();
    let _ = second_server.join();
}

#[test]
fn delete_everything_from_collection() {
    let (mut client, server) = executor_server();

    // insert two docs
    let doc1 = bson_document(&[bson_string("name", "Alice")]);
    let doc2 = bson_document(&[bson_string("name", "Bob")]);
    let insert_cmd =
        bson_document(&[bson_string("insert", "users"), bson_string("$db", "test"), bson_array_element("documents", &[doc1, doc2])]);
    let _ = roundtrip(&mut client, 1, &insert_cmd);

    // delete all: { delete: "users", $db: "test", deletes: [{ q: {}, limit: 0 }] }
    let empty_q = bson_document(&[]);
    let delete_spec = bson_document(&[bson_doc_element("q", &empty_q), bson_int32("limit", 0)]);
    let delete_cmd =
        bson_document(&[bson_string("delete", "users"), bson_string("$db", "test"), bson_array_element("deletes", &[delete_spec])]);
    let reply = roundtrip(&mut client, 2, &delete_cmd);
    let deleted = extract_int32(&reply, "n");
    assert_eq!(deleted, Some(2), "n after delete: reply={reply:?}");
    let ok = extract_double(&reply, "ok");
    assert_eq!(ok, Some(1.0));

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

/// A BSON undefined element: type 0x06 + cstring key, no payload.
fn bson_undefined_element(key: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0x06);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
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

    let insert_a = bson_document(&[bson_string("insert", "users"), bson_string("$db", "db_A"), bson_array_element("documents", &[doc_a])]);
    let insert_b = bson_document(&[bson_string("insert", "users"), bson_string("$db", "db_B"), bson_array_element("documents", &[doc_b])]);

    let r1 = roundtrip(&mut client, 1, &insert_a);
    let r2 = roundtrip(&mut client, 2, &insert_b);
    assert_eq!(extract_double(&r1, "ok"), Some(1.0));
    assert_eq!(extract_int32(&r1, "n"), Some(1));
    assert_eq!(extract_double(&r2, "ok"), Some(1.0));
    assert_eq!(extract_int32(&r2, "n"), Some(1));

    // Find Alice from db_A.users by _id → should get 1 doc
    let filter_by_id = bson_document(&[bson_object_id("_id", id_a)]);
    let find_a = bson_document(&[bson_string("find", "users"), bson_string("$db", "db_A"), bson_doc_element("filter", &filter_by_id)]);
    let ra = roundtrip(&mut client, 3, &find_a);
    assert_eq!(extract_double(&ra, "ok"), Some(1.0));
    assert_eq!(extract_cursor_firstbatch_len(&ra), Some(1), "db_A must see Alice");

    // Find Alice from db_B.users by _id → should get 0 docs (isolated)
    let find_a_in_b = bson_document(&[bson_string("find", "users"), bson_string("$db", "db_B"), bson_doc_element("filter", &filter_by_id)]);
    let rab = roundtrip(&mut client, 4, &find_a_in_b);
    assert_eq!(extract_double(&rab, "ok"), Some(1.0));
    assert_eq!(extract_cursor_firstbatch_len(&rab), Some(0), "db_B must NOT see db_A data");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn empty_collection_find_returns_empty_array_without_error() {
    let (mut client, server) = executor_server();

    // find on a collection that never existed → firstBatch = [], ok = 1
    let empty_filter = bson_document(&[]);
    let find_cmd = bson_document(&[bson_string("find", "ghosts"), bson_string("$db", "empty_db"), bson_doc_element("filter", &empty_filter)]);
    let reply = roundtrip(&mut client, 1, &find_cmd);
    assert_eq!(extract_double(&reply, "ok"), Some(1.0), "empty collection find must succeed");
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
    let delete_cmd =
        bson_document(&[bson_string("delete", "things"), bson_string("$db", "test"), bson_array_element("deletes", &[delete_spec])]);

    let reply = roundtrip(&mut client, 1, &delete_cmd);
    assert_eq!(extract_double(&reply, "ok"), Some(1.0));
    assert_eq!(extract_int32(&reply, "n"), Some(0), "deleting missing _id must report 0");

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
    let insert_cmd =
        bson_document(&[bson_string("insert", "auto_col"), bson_string("$db", "test"), bson_array_element("documents", &[inner_doc])]);
    let reply = roundtrip(&mut client, 1, &insert_cmd);
    assert_eq!(extract_double(&reply, "ok"), Some(1.0));
    assert_eq!(extract_int32(&reply, "n"), Some(1));

    // Find it back — empty filter scan.
    let empty_filter = bson_document(&[]);
    let find_cmd = bson_document(&[bson_string("find", "auto_col"), bson_string("$db", "test"), bson_doc_element("filter", &empty_filter)]);
    let find_reply = roundtrip(&mut client, 2, &find_cmd);
    assert_eq!(extract_double(&find_reply, "ok"), Some(1.0));
    assert_eq!(extract_cursor_firstbatch_len(&find_reply), Some(1));

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn write_replies_use_the_reference_field_order_and_types() {
    // Byte-for-byte contract: `n` (int32) comes first and `ok` is `1.0` (double).
    // A different order or an int32 `ok` is a wire-level mismatch for drivers.
    let (mut client, server) = executor_server();

    let insert = bson_document(&[
        bson_string("insert", "shape_col"),
        bson_string("$db", "test"),
        bson_array_element("documents", &[bson_document(&[bson_int32("_id", 1), bson_int32("v", 1)])]),
    ]);
    let insert_reply = roundtrip(&mut client, 1, &insert);
    assert_eq!(hex(&insert_reply), "18000000106e0001000000016f6b00000000000000f03f00");

    let count = bson_document(&[bson_string("count", "shape_col"), bson_string("$db", "test"), bson_doc_element("query", &bson_document(&[]))]);
    let count_reply = roundtrip(&mut client, 2, &count);
    assert_eq!(hex(&count_reply), "18000000106e0001000000016f6b00000000000000f03f00");

    let delete = bson_document(&[
        bson_string("delete", "shape_col"),
        bson_string("$db", "test"),
        bson_array_element("deletes", &[bson_document(&[bson_doc_element("q", &bson_document(&[])), bson_int32("limit", 0)])]),
    ]);
    let delete_reply = roundtrip(&mut client, 3, &delete);
    assert_eq!(hex(&delete_reply), "18000000106e0001000000016f6b00000000000000f03f00");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn insert_with_undefined_id_reports_undefined_key_value() {
    // `mongod` reports the offending `_id` verbatim in `keyValue`; an undefined
    // `_id` must therefore come back as BSON undefined (0x06), not as a string.
    let (mut client, server) = executor_server();

    let first = bson_document(&[bson_undefined_element("_id"), bson_int32("b", 1)]);
    let second = bson_document(&[bson_undefined_element("_id"), bson_int32("b", 2)]);
    let insert_first =
        bson_document(&[bson_string("insert", "undef_col"), bson_string("$db", "test"), bson_array_element("documents", &[first])]);
    let insert_second =
        bson_document(&[bson_string("insert", "undef_col"), bson_string("$db", "test"), bson_array_element("documents", &[second])]);

    let r1 = roundtrip(&mut client, 1, &insert_first);
    let r2 = roundtrip(&mut client, 2, &insert_second);
    assert_eq!(extract_int32(&r1, "n"), Some(1), "reply={r1:?}");
    assert_eq!(extract_double(&r1, "ok"), Some(1.0), "reply={r1:?}");
    assert_eq!(extract_int32(&r2, "n"), Some(0), "reply={r2:?}");
    assert_eq!(extract_double(&r2, "ok"), Some(1.0), "reply={r2:?}");

    let write_errors = walk_bson(&r2, |elem| match elem {
        BsonElem::Array(k, v) if k == "writeErrors" => Some(v.clone()),
        _ => None,
    })
    .expect("a duplicate undefined _id must produce writeErrors");
    let first_error = walk_bson(&write_errors, |elem| match elem {
        BsonElem::Document(_, v) => Some(v.clone()),
        _ => None,
    })
    .expect("writeErrors must carry one document");
    let key_value = extract_document(&first_error, "keyValue").expect("writeErrors carry keyValue");
    let undefined = walk_bson(&key_value, |elem| match elem {
        BsonElem::Undefined(k) if k == "_id" => Some(()),
        _ => None,
    });
    assert!(undefined.is_some(), "keyValue must carry BSON undefined: {key_value:?}");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn insert_with_duplicate_id_reports_11000_and_keeps_the_first_document() {
    // MongoDB's `_id` is a unique key, so a repeated value is a duplicate-key
    // error (code 11000) and the first document survives. The gateway writes
    // through a plain Put, so the check has to happen before the write.
    let (mut client, server) = executor_server();

    let fixed_id = [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77];
    let doc_v1 = bson_document(&[bson_object_id("_id", fixed_id), bson_string("label", "first"), bson_int32("version", 1)]);
    let doc_v2 = bson_document(&[bson_object_id("_id", fixed_id), bson_string("label", "second"), bson_int32("version", 2)]);

    let insert_v1 = bson_document(&[bson_string("insert", "dup_col"), bson_string("$db", "test"), bson_array_element("documents", &[doc_v1])]);
    let insert_v2 = bson_document(&[bson_string("insert", "dup_col"), bson_string("$db", "test"), bson_array_element("documents", &[doc_v2])]);

    let r1 = roundtrip(&mut client, 1, &insert_v1);
    let r2 = roundtrip(&mut client, 2, &insert_v2);

    assert_eq!(extract_double(&r1, "ok"), Some(1.0));
    assert_eq!(extract_int32(&r1, "n"), Some(1), "the first insert must be stored");
    assert_eq!(extract_double(&r2, "ok"), Some(1.0), "a write error is still reported with ok=1");
    assert_eq!(extract_int32(&r2, "n"), Some(0), "the duplicate must not be counted as inserted");

    let write_errors = walk_bson(&r2, |elem| match elem {
        BsonElem::Array(k, v) if k == "writeErrors" => Some(v.clone()),
        _ => None,
    })
    .expect("a duplicate _id must produce writeErrors");
    let first_error = walk_bson(&write_errors, |elem| match elem {
        BsonElem::Document(_, v) => Some(v.clone()),
        _ => None,
    })
    .expect("writeErrors must carry one document");
    assert_eq!(extract_int32(&first_error, "index"), Some(0), "reply={r2:?}");
    assert_eq!(extract_int32(&first_error, "code"), Some(11000), "reply={r2:?}");
    let key_pattern = extract_document(&first_error, "keyPattern").expect("writeErrors carry keyPattern");
    assert_eq!(extract_int32(&key_pattern, "_id"), Some(1), "keyPattern={key_pattern:?}");
    let key_value = extract_document(&first_error, "keyValue").expect("writeErrors carry keyValue");
    assert_eq!(extract_object_id(&key_value, "_id"), Some(fixed_id), "keyValue={key_value:?}");

    // The overwritten document must not be visible: `first` stays, `second` never landed.
    let empty_filter = bson_document(&[]);
    let find_cmd = bson_document(&[bson_string("find", "dup_col"), bson_string("$db", "test"), bson_doc_element("filter", &empty_filter)]);
    let find_reply = roundtrip(&mut client, 3, &find_cmd);
    assert_eq!(extract_cursor_firstbatch_len(&find_reply), Some(1), "the duplicate must not add a document");
    let text = String::from_utf8_lossy(&find_reply);
    assert!(text.contains("first"), "the first document must survive: reply={find_reply:?}");
    assert!(!text.contains("second"), "the rejected document must not be stored: reply={find_reply:?}");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

// ---------------------------------------------------------------------------
// BSON extractors for test assertions
// ---------------------------------------------------------------------------

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn extract_int32(bson: &[u8], key: &str) -> Option<i32> {
    walk_bson(bson, |elem| match elem {
        BsonElem::Int32(k, v) if k == key => Some(v),
        _ => None,
    })
}

fn extract_double(bson: &[u8], key: &str) -> Option<f64> {
    walk_bson(bson, |elem| match elem {
        BsonElem::Double(k, v) if k == key => Some(v),
        _ => None,
    })
}

fn extract_object_id(bson: &[u8], key: &str) -> Option<[u8; 12]> {
    walk_bson(bson, |elem| match elem {
        BsonElem::ObjectId(k, v) if k == key => Some(v),
        _ => None,
    })
}

fn extract_document(bson: &[u8], key: &str) -> Option<Vec<u8>> {
    walk_bson(bson, |elem| match elem {
        BsonElem::Document(k, v) if k == key => Some(v),
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
    extract_cursor_batch_len(bson, "firstBatch")
}

fn extract_cursor_batch_len(bson: &[u8], key: &str) -> Option<usize> {
    let cursor_doc = extract_document(bson, "cursor")?;
    let batch_doc = walk_bson(&cursor_doc, |elem| match elem {
        BsonElem::Array(k, v) if k == key => Some(v.clone()),
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

fn extract_cursor_id(bson: &[u8]) -> Option<i64> {
    extract_int64(&extract_document(bson, "cursor")?, "id")
}

fn extract_string(bson: &[u8], key: &str) -> Option<String> {
    walk_bson(bson, |elem| match elem {
        BsonElem::String(k, v) if k == key => Some(v.to_string()),
        _ => None,
    })
}

// Minimal BSON walker for test use.
#[derive(Debug)]
enum BsonElem<'a> {
    Double(&'a str, f64),
    Int32(&'a str, i32),
    Int64(&'a str, i64),
    String(&'a str, &'a str),
    ObjectId(&'a str, [u8; 12]),
    Undefined(&'a str),
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
                let value = f64::from_le_bytes([
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
                f(BsonElem::Double(key, value))
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
            0x07 => {
                if cursor + 12 > bson.len() {
                    break;
                }
                let mut bytes = [0_u8; 12];
                bytes.copy_from_slice(&bson[cursor..cursor + 12]);
                cursor += 12;
                f(BsonElem::ObjectId(key, bytes))
            }
            0x06 => f(BsonElem::Undefined(key)),
            0x08 => {
                if cursor + 1 > bson.len() {
                    break;
                }
                cursor += 1;
                None
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

#[test]
fn insert_reads_documents_from_a_document_sequence_section() {
    let (mut client, server) = executor_server();

    // { insert: "users", $db: "test" } with a kind-1 section carrying `documents`.
    let document = bson_document(&[bson_string("name", "Alice")]);
    let command = bson_document(&[bson_string("insert", "users"), bson_string("$db", "test")]);
    let reply = roundtrip_frame(&mut client, 1, build_op_msg_with_document_sequence(1, &command, "documents", &[document]));

    assert_eq!(extract_double(&reply, "ok"), Some(1.0), "reply={reply:?}");
    assert_eq!(extract_int32(&reply, "n"), Some(1), "the kind-1 section must be folded into `documents`: reply={reply:?}");

    // Reading the document back proves it was stored, not just counted.
    let empty_filter = bson_document(&[]);
    let find_cmd = bson_document(&[bson_string("find", "users"), bson_string("$db", "test"), bson_doc_element("filter", &empty_filter)]);
    let find_reply = roundtrip(&mut client, 2, &find_cmd);
    assert_eq!(extract_cursor_firstbatch_len(&find_reply), Some(1), "reply={find_reply:?}");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

fn expect_ok(reply: &[u8], command: &str) {
    assert_eq!(extract_double(reply, "ok"), Some(1.0), "{command} failed: reply={reply:?}");
}

#[test]
fn catalog_and_admin_commands_are_routed_over_op_msg() {
    let (mut client, server) = executor_server();
    let empty = bson_document(&[]);

    let create = bson_document(&[bson_string("create", "ledger"), bson_string("$db", "app")]);
    expect_ok(&roundtrip(&mut client, 1, &create), "create");

    let first = bson_document(&[bson_int32("_id", 1), bson_string("value", "a")]);
    let second = bson_document(&[bson_int32("_id", 2), bson_string("value", "a")]);
    let insert =
        bson_document(&[bson_string("insert", "ledger"), bson_string("$db", "app"), bson_array_element("documents", &[first, second])]);
    expect_ok(&roundtrip(&mut client, 2, &insert), "insert");

    let list_collections = bson_document(&[bson_int32("listCollections", 1), bson_string("$db", "app")]);
    let collections = roundtrip(&mut client, 3, &list_collections);
    assert_eq!(extract_cursor_firstbatch_len(&collections), Some(1), "reply={collections:?}");

    let coll_stats = bson_document(&[bson_string("collStats", "ledger"), bson_string("$db", "app")]);
    expect_ok(&roundtrip(&mut client, 4, &coll_stats), "collStats");

    let db_stats = bson_document(&[bson_int32("dbStats", 1), bson_string("$db", "app")]);
    expect_ok(&roundtrip(&mut client, 5, &db_stats), "dbStats");

    let list_databases = bson_document(&[bson_int32("listDatabases", 1), bson_string("$db", "admin")]);
    expect_ok(&roundtrip(&mut client, 6, &list_databases), "listDatabases");

    let distinct = bson_document(&[
        bson_string("distinct", "ledger"),
        bson_string("key", "value"),
        bson_string("$db", "app"),
        bson_doc_element("query", &empty),
    ]);
    expect_ok(&roundtrip(&mut client, 7, &distinct), "distinct");

    let find_and_modify = bson_document(&[
        bson_string("findAndModify", "ledger"),
        bson_string("$db", "app"),
        bson_doc_element("query", &bson_document(&[bson_int32("_id", 1)])),
        bson_doc_element("update", &bson_document(&[bson_doc_element("$set", &bson_document(&[bson_string("value", "b")]))])),
    ]);
    expect_ok(&roundtrip(&mut client, 8, &find_and_modify), "findAndModify");

    let operation = bson_document(&[
        bson_int32("insert", 0),
        bson_doc_element("document", &bson_document(&[bson_int32("_id", 3), bson_string("value", "c")])),
    ]);
    let namespace = bson_document(&[bson_string("ns", "app.ledger")]);
    let bulk_write = bson_document(&[
        bson_int32("bulkWrite", 1),
        bson_string("$db", "admin"),
        bson_array_element("ops", &[operation]),
        bson_array_element("nsInfo", &[namespace]),
    ]);
    expect_ok(&roundtrip(&mut client, 9, &bulk_write), "bulkWrite");

    let explain = bson_document(&[
        bson_doc_element("explain", &bson_document(&[bson_string("find", "ledger"), bson_doc_element("filter", &empty)])),
        bson_string("$db", "app"),
    ]);
    expect_ok(&roundtrip(&mut client, 10, &explain), "explain");

    let index = bson_document(&[bson_doc_element("key", &bson_document(&[bson_int32("value", 1)])), bson_string("name", "value_1")]);
    let create_indexes =
        bson_document(&[bson_string("createIndexes", "ledger"), bson_string("$db", "app"), bson_array_element("indexes", &[index])]);
    expect_ok(&roundtrip(&mut client, 11, &create_indexes), "createIndexes");

    let list_indexes = bson_document(&[bson_string("listIndexes", "ledger"), bson_string("$db", "app")]);
    let indexes = roundtrip(&mut client, 12, &list_indexes);
    assert_eq!(extract_cursor_firstbatch_len(&indexes), Some(2), "reply={indexes:?}");

    let drop_indexes = bson_document(&[bson_string("dropIndexes", "ledger"), bson_string("index", "value_1"), bson_string("$db", "app")]);
    expect_ok(&roundtrip(&mut client, 13, &drop_indexes), "dropIndexes");

    let coll_mod = bson_document(&[bson_string("collMod", "ledger"), bson_string("$db", "app"), bson_doc_element("validator", &empty)]);
    expect_ok(&roundtrip(&mut client, 14, &coll_mod), "collMod");

    let drop = bson_document(&[bson_string("drop", "ledger"), bson_string("$db", "app")]);
    expect_ok(&roundtrip(&mut client, 15, &drop), "drop");

    let drop_database = bson_document(&[bson_int32("dropDatabase", 1), bson_string("$db", "app")]);
    expect_ok(&roundtrip(&mut client, 16, &drop_database), "dropDatabase");

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}

#[test]
fn transaction_commands_reach_facade_semantics_instead_of_unknown_command() {
    let (mut client, server) = executor_server();
    let lsid = bson_document(&[bson_string("id", "no-uuid")]);

    // 入口已接，待定的逻辑会话裁定只需补状态管道。在此之前门面返回真实的
    // `NoSuchTransaction` 错误码，而不是网关的 `unknown` 兜底。
    for (request_id, name) in [(1, "commitTransaction"), (2, "abortTransaction")] {
        let command = bson_document(&[bson_int32(name, 1), bson_string("$db", "admin"), bson_doc_element("lsid", &lsid)]);
        let reply = roundtrip(&mut client, request_id, &command);
        assert_eq!(extract_double(&reply, "ok"), Some(0.0), "{name} reply={reply:?}");
        assert!(extract_int32(&reply, "code").is_some(), "{name} must carry a facade error code: reply={reply:?}");
    }

    let _ = client.shutdown(Shutdown::Write);
    let _ = server.join();
}
