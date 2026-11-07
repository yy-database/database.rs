//! Command-primitive tests: no server loop, no global state.

use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use mongo::{
    bson::{BsonDocument, BsonValue, encode_bson, parse_bson},
    commands::{self, CommandError},
    cursor::CursorStore,
    json,
    store::Store,
    wire::{decode_op_msg_command, encode_op_msg_body},
};
use serde_json::json;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn temporary_dir() -> std::path::PathBuf {
    let unique = SEQUENCE.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!("mongo-core-test-{}-{unique}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn store() -> Store {
    Store::open(temporary_dir().join("store.yykv")).expect("open shard")
}

fn document(fields: &[(&str, BsonValue)]) -> BsonDocument {
    let mut document = BsonDocument::new();
    for (key, value) in fields {
        document.push(*key, value.clone());
    }
    document
}

fn text(value: &str) -> BsonValue {
    BsonValue::String(value.to_string())
}

fn command(name: &str, collection: &str, database: &str) -> BsonDocument {
    document(&[(name, text(collection)), ("$db", text(database))])
}

fn int(value: &BsonDocument, key: &str) -> i64 {
    match value.get(key) {
        Some(BsonValue::Int32(number)) => *number as i64,
        Some(BsonValue::Int64(number)) => *number,
        Some(BsonValue::Double(number)) => *number as i64,
        _ => 0,
    }
}

fn array(value: &BsonDocument, key: &str) -> Vec<BsonDocument> {
    match value.get(key) {
        Some(BsonValue::Array(items)) => items
            .iter()
            .filter_map(|item| match item {
                BsonValue::Document(document) => Some(document.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn array_len(value: &BsonDocument, key: &str) -> usize {
    match value.get(key) {
        Some(BsonValue::Array(items)) => items.len(),
        _ => 0,
    }
}

fn batch(document: &BsonDocument, key: &str) -> Vec<BsonDocument> {
    match document.get("cursor") {
        Some(BsonValue::Document(cursor)) => array(cursor, key),
        _ => Vec::new(),
    }
}

fn cursor_id(document: &BsonDocument) -> i64 {
    match document.get("cursor") {
        Some(BsonValue::Document(cursor)) => int(cursor, "id"),
        _ => 0,
    }
}

fn insert_documents(store: &Store, collection: &str, documents: Vec<BsonDocument>) -> BsonDocument {
    let mut insert = command("insert", collection, "app");
    insert.push("documents", BsonValue::Array(documents.into_iter().map(BsonValue::Document).collect()));
    commands::insert(&insert, store).expect("insert")
}

fn find_all(store: &Store, collection: &str, cursors: &mut CursorStore) -> Vec<BsonDocument> {
    let mut find = command("find", collection, "app");
    find.push("filter", BsonValue::Document(BsonDocument::new()));
    batch(&commands::find(&find, store, cursors).expect("find"), "firstBatch")
}

#[test]
fn hello_answers_a_primary() {
    let response = commands::hello();
    assert_eq!(response.get("isWritablePrimary"), Some(&BsonValue::Bool(true)));
    assert_eq!(int(&response, "ok"), 1);
    assert_eq!(int(&commands::ping(), "ok"), 1);
}

#[test]
fn insert_assigns_object_id_and_reports_one_document() {
    let store = store();
    let response = insert_documents(&store, "items", vec![document(&[("n", BsonValue::Int32(1))])]);
    assert_eq!(int(&response, "n"), 1);
    assert!(response.get("writeErrors").is_none());

    let mut cursors = CursorStore::new();
    let found = find_all(&store, "items", &mut cursors);
    assert_eq!(found.len(), 1);
    assert!(matches!(found[0].get("_id"), Some(BsonValue::ObjectId(_))));
}

#[test]
fn duplicate_id_maps_to_11000_and_keeps_the_first_document() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("value", text("first"))])]);

    let response = insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("value", text("second"))])]);
    assert_eq!(int(&response, "n"), 0);

    let errors = array(&response, "writeErrors");
    assert_eq!(errors.len(), 1);
    assert_eq!(int(&errors[0], "code"), 11000);

    let mut cursors = CursorStore::new();
    let found = find_all(&store, "items", &mut cursors);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].get("value"), Some(&text("first")));
}

#[test]
fn unordered_insert_continues_past_duplicates() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1))])]);

    let mut insert = command("insert", "items", "app");
    insert.push("ordered", BsonValue::Bool(false));
    insert.push(
        "documents",
        BsonValue::Array(vec![
            BsonValue::Document(document(&[("_id", BsonValue::Int32(1))])),
            BsonValue::Document(document(&[("_id", BsonValue::Int32(2))])),
        ]),
    );
    let response = commands::insert(&insert, &store).expect("insert");

    assert_eq!(int(&response, "n"), 1);
    let errors = array(&response, "writeErrors");
    assert_eq!(errors.len(), 1);
    assert_eq!(int(&errors[0], "code"), 11000);
    assert_eq!(int(&errors[0], "index"), 0);
}

#[test]
fn find_filters_with_comparison_operators() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![
            document(&[("_id", BsonValue::Int32(1)), ("score", BsonValue::Int32(10))]),
            document(&[("_id", BsonValue::Int32(2)), ("score", BsonValue::Int32(20))]),
            document(&[("_id", BsonValue::Int32(3)), ("score", BsonValue::Int32(30))]),
        ],
    );

    let filter = document(&[("score", BsonValue::Document(document(&[("$gte", BsonValue::Int32(20))])))]);
    let mut find = command("find", "items", "app");
    find.push("filter", BsonValue::Document(filter));
    find.push("sort", BsonValue::Document(document(&[("score", BsonValue::Int32(-1))])));
    let mut cursors = CursorStore::new();
    let found = batch(&commands::find(&find, &store, &mut cursors).expect("find"), "firstBatch");

    assert_eq!(found.len(), 2);
    assert_eq!(found[0].get("score"), Some(&BsonValue::Int32(30)));
}

#[test]
fn count_respects_the_query_filter() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![
            document(&[("_id", BsonValue::Int32(1)), ("kind", text("a"))]),
            document(&[("_id", BsonValue::Int32(2)), ("kind", text("b"))]),
            document(&[("_id", BsonValue::Int32(3)), ("kind", text("b"))]),
        ],
    );

    let mut count = command("count", "items", "app");
    count.push("query", BsonValue::Document(document(&[("kind", text("b"))])));
    assert_eq!(int(&commands::count(&count, &store).expect("count"), "n"), 2);
}

#[test]
fn aggregate_matches_groups_and_projects() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![
            document(&[("_id", BsonValue::Int32(1)), ("kind", text("b"))]),
            document(&[("_id", BsonValue::Int32(2)), ("kind", text("b"))]),
            document(&[("_id", BsonValue::Int32(3)), ("kind", text("a"))]),
        ],
    );

    let mut aggregate = command("aggregate", "items", "app");
    aggregate.push(
        "pipeline",
        BsonValue::Array(vec![
            BsonValue::Document(document(&[("$match", BsonValue::Document(document(&[("kind", text("b"))])))])),
            BsonValue::Document(document(&[(
                "$group",
                BsonValue::Document(document(&[
                    ("_id", BsonValue::Null),
                    ("total", BsonValue::Document(document(&[("$sum", BsonValue::Int32(1))]))),
                ])),
            )])),
            BsonValue::Document(document(&[(
                "$project",
                BsonValue::Document(document(&[("_id", BsonValue::Int32(0)), ("total", BsonValue::Int32(1))])),
            )])),
        ]),
    );

    let grouped = batch(&commands::aggregate(&aggregate, &store).expect("aggregate"), "firstBatch");
    assert_eq!(grouped.len(), 1);
    assert_eq!(grouped[0].get("total"), Some(&BsonValue::Int32(2)));
    assert!(grouped[0].get("_id").is_none());
}

#[test]
fn aggregate_rejects_unsupported_stages_even_after_match() {
    let store = store();
    let search = BsonValue::Document(document(&[("text", text("x"))]));
    let mut aggregate = command("aggregate", "items", "app");
    aggregate.push(
        "pipeline",
        BsonValue::Array(vec![
            BsonValue::Document(document(&[("$match", BsonValue::Document(BsonDocument::new()))])),
            BsonValue::Document(document(&[("$search", search)])),
        ]),
    );

    let error = commands::aggregate(&aggregate, &store).expect_err("stage must be rejected");
    assert_eq!(error.code, 59);
    assert_eq!(
        error.message,
        "unknown aggregation stage: $search (supported: $match, $sort, $limit, $skip, $count, $unwind, $lookup, $graphLookup, $facet, $bucket, $bucketAuto, $setWindowFields, $sortByCount, $group, $addFields, $set, $project, $replaceWith, $merge, $out, $unionWith, $sample, $densify, $fill, $redact)"
    );
}

#[test]
fn delete_honours_limit() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![document(&[("_id", BsonValue::Int32(1)), ("kind", text("b"))]), document(&[("_id", BsonValue::Int32(2)), ("kind", text("b"))])],
    );

    let mut delete = command("delete", "items", "app");
    delete.push(
        "deletes",
        BsonValue::Array(vec![BsonValue::Document(document(&[
            ("q", BsonValue::Document(document(&[("kind", text("b"))]))),
            ("limit", BsonValue::Int32(1)),
        ]))]),
    );
    assert_eq!(int(&commands::delete(&delete, &store).expect("delete"), "n"), 1);

    let mut count = command("count", "items", "app");
    count.push("query", BsonValue::Document(BsonDocument::new()));
    assert_eq!(int(&commands::count(&count, &store).expect("count"), "n"), 1);
}

#[test]
fn find_batch_leaves_a_get_more_cursor() {
    let store = store();
    insert_documents(&store, "items", (0..6).map(|index| document(&[("_id", BsonValue::Int32(index))])).collect());

    let mut find = command("find", "items", "app");
    find.push("filter", BsonValue::Document(BsonDocument::new()));
    find.push("batchSize", BsonValue::Int32(2));
    let mut cursors = CursorStore::new();
    let response = commands::find(&find, &store, &mut cursors).expect("find");
    assert_eq!(batch(&response, "firstBatch").len(), 2);
    let id = cursor_id(&response);
    assert!(id > 0);

    let mut get_more = document(&[("getMore", BsonValue::Int64(id)), ("collection", text("items")), ("$db", text("app"))]);
    get_more.push("batchSize", BsonValue::Int32(2));
    let next = commands::get_more(&get_more, &mut cursors).expect("getMore");
    assert_eq!(batch(&next, "nextBatch").len(), 2);
    assert_eq!(cursor_id(&next), id);

    // The third batch drains what is left, so the reply must close the cursor.
    let last = commands::get_more(&get_more, &mut cursors).expect("getMore");
    assert_eq!(batch(&last, "nextBatch").len(), 2);
    assert_eq!(cursor_id(&last), 0);
}

#[test]
fn kill_cursors_reports_killed_and_unknown_ids() {
    let mut cursors = CursorStore::new();
    let (id, first) = cursors.open("app.items", (0..4).map(BsonValue::Int32).collect(), 2);
    assert!(id > 0);
    assert_eq!(first.len(), 2);

    let mut kill = document(&[("killCursors", text("items")), ("$db", text("app"))]);
    kill.push("cursors", BsonValue::Array(vec![BsonValue::Int64(id), BsonValue::Int64(id + 100)]));
    let response = commands::kill_cursors(&kill, &mut cursors).expect("killCursors");
    assert_eq!(array_len(&response, "cursorsKilled"), 1);
    assert_eq!(array_len(&response, "cursorsNotFound"), 1);
}

#[test]
fn idle_cursors_are_reclaimed_lazily() {
    let mut cursors = CursorStore::with_idle_timeout(Duration::from_millis(1));
    let (id, _) = cursors.open("app.items", (0..4).map(BsonValue::Int32).collect(), 2);
    assert_eq!(cursors.len(), 1);
    std::thread::sleep(Duration::from_millis(20));
    assert!(cursors.get_more(id, 2).is_err());
    assert!(cursors.is_empty());
    assert_eq!(CursorStore::new().idle_timeout(), Duration::from_secs(600));
}

#[test]
fn unknown_command_is_rejected_with_59() {
    let error = CommandError::unsupported("findAndModify");
    assert_eq!(error.code, 59);
    assert_eq!(error.message, "findAndModify");
}

#[test]
fn invalid_collection_name_is_rejected_with_73() {
    let store = store();
    let mut find = command("find", "bad$name", "app");
    find.push("filter", BsonValue::Document(BsonDocument::new()));
    let mut cursors = CursorStore::new();
    let error = commands::find(&find, &store, &mut cursors).expect_err("namespace must be rejected");
    assert_eq!(error.code, 73);
}

#[test]
fn documents_survive_a_reopen() {
    let directory = temporary_dir();
    {
        let store = Store::open(directory.join("store.yykv")).expect("open shard");
        insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1))])]);
    }
    let store = Store::open(directory.join("store.yykv")).expect("reopen shard");
    assert_eq!(store.scan("app.items").len(), 1);
    assert_eq!(store.namespaces(), vec!["app.items".to_string()]);
}

#[test]
fn mongodump_stream_is_imported() {
    let store = store();
    let mut stream = Vec::new();
    stream.extend_from_slice(&encode_bson(&document(&[("_id", BsonValue::Int32(1)), ("name", text("one"))])));
    stream.extend_from_slice(&encode_bson(&document(&[("_id", BsonValue::Int32(2)), ("name", text("two"))])));
    let imported = mongo::import::import_bson_documents(&store, "app.items", &stream).expect("import");
    assert_eq!(imported, 2);
    assert_eq!(store.scan("app.items").len(), 2);
}

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

/// The two relaxed `$date` spellings must land on the same millisecond value
/// as the canonical `$numberLong` form.
#[test]
fn relaxed_date_input_matches_the_canonical_form() {
    assert_eq!(json::from_json(&json!({"$date": {"$numberLong": "0"}})).expect("canonical"), BsonValue::DateTime(0));
    assert_eq!(json::from_json(&json!({"$date": "1970-01-01T00:00:00Z"})).expect("iso"), BsonValue::DateTime(0));
    assert_eq!(json::from_json(&json!({"$date": "2017-01-01T00:00:00.123+08:00"})).expect("offset"), BsonValue::DateTime(1_483_200_000_123));
    assert_eq!(json::from_json(&json!({"$date": 1})).expect("milliseconds"), BsonValue::DateTime(1));
}

#[test]
fn library_smoke_decodes_op_msg_bytes_and_returns_a_reply() {
    let store = store();
    let mut insert = command("insert", "items", "app");
    insert.push("documents", BsonValue::Array(vec![BsonValue::Document(document(&[("n", BsonValue::Int32(7))]))]));
    let frame = encode_op_msg_body(&insert);
    let decoded = decode_op_msg_command(&frame).expect("decode");
    let response = commands::insert(&decoded, &store).expect("insert");
    assert_eq!(int(&response, "n"), 1);

    let mut find = command("find", "items", "app");
    find.push("filter", BsonValue::Document(BsonDocument::new()));
    let mut cursors = CursorStore::new();
    let reply = commands::find(&find, &store, &mut cursors).expect("find");
    assert_eq!(batch(&reply, "firstBatch").len(), 1);

    let _ = parse_bson(&encode_bson(&reply)).expect("reply is valid BSON");
    let _ = SocketAddr::from(([127, 0, 0, 1], 0));
}

fn index_command(collection: &str, direction: i32, name: &str, unique: bool) -> BsonDocument {
    let mut key = BsonDocument::new();
    key.push("a", BsonValue::Int32(direction));
    let mut specification = BsonDocument::new();
    specification.push("key", BsonValue::Document(key));
    specification.push("name", text(name));
    if unique {
        specification.push("unique", BsonValue::Bool(true));
    }
    let mut create = command("createIndexes", collection, "app");
    create.push("indexes", BsonValue::Array(vec![BsonValue::Document(specification)]));
    create
}

#[test]
fn indexes_persist_across_reopen_and_drop_by_name() {
    let path = temporary_dir().join("store.yykv");
    let store = Store::open(&path).expect("open shard");
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1))])]);

    let created = commands::create_indexes(&index_command("items", 1, "a_1", true), &store).expect("createIndexes");
    assert_eq!(int(&created, "numIndexesBefore"), 1);
    assert_eq!(int(&created, "numIndexesAfter"), 2);
    assert_eq!(created.get("createdCollectionAutomatically"), Some(&BsonValue::Bool(false)));

    let listed = commands::list_indexes(&command("listIndexes", "items", "app"), &store).expect("listIndexes");
    let indexes = batch(&listed, "firstBatch");
    assert_eq!(indexes.len(), 2);
    assert_eq!(indexes[0].get("name"), Some(&text("_id_")));
    assert_eq!(indexes[1].get("name"), Some(&text("a_1")));
    assert_eq!(indexes[1].get("unique"), Some(&BsonValue::Bool(true)));

    let mut drop = command("dropIndexes", "items", "app");
    drop.push("index", text("a_1"));
    let dropped = commands::drop_indexes(&drop, &store).expect("dropIndexes");
    assert_eq!(int(&dropped, "nIndexesWas"), 2);

    let reopened = Store::open(&path).expect("reopen shard");
    let relisted = commands::list_indexes(&command("listIndexes", "items", "app"), &reopened).expect("listIndexes");
    assert_eq!(batch(&relisted, "firstBatch").len(), 1);

    let mut drop_id = command("dropIndexes", "items", "app");
    drop_id.push("index", text("_id_"));
    let error = commands::drop_indexes(&drop_id, &reopened).expect_err("_id_ is not droppable");
    assert_eq!(error.code, 72);
}

#[test]
fn unique_index_rejects_duplicate_keys_on_insert_and_update() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![
            document(&[("_id", BsonValue::Int32(1)), ("a", BsonValue::Int32(7))]),
            document(&[("_id", BsonValue::Int32(2)), ("a", BsonValue::Int32(8))]),
        ],
    );
    commands::create_indexes(&index_command("items", 1, "a_1", true), &store).expect("createIndexes");

    let duplicate = insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(3)), ("a", BsonValue::Int32(7))])]);
    assert_eq!(int(&duplicate, "n"), 0);
    let errors = array(&duplicate, "writeErrors");
    assert_eq!(int(&errors[0], "code"), 11000);
    let key_value = match errors[0].get("keyValue") {
        Some(BsonValue::Document(document)) => document.clone(),
        _ => BsonDocument::new(),
    };
    assert_eq!(key_value.get("a"), Some(&BsonValue::Int32(7)));

    let mut update = command("update", "items", "app");
    let mut specification = BsonDocument::new();
    specification.push("q", BsonValue::Document(document(&[("_id", BsonValue::Int32(2))])));
    specification.push("u", BsonValue::Document(document(&[("$set", BsonValue::Document(document(&[("a", BsonValue::Int32(7))])))])));
    update.push("updates", BsonValue::Array(vec![BsonValue::Document(specification)]));
    let reply = commands::update(&update, &store).expect("update");
    assert_eq!(int(&reply, "n"), 0);
    assert_eq!(int(&reply, "nModified"), 0);
    let errors = array(&reply, "writeErrors");
    assert_eq!(int(&errors[0], "code"), 11000);
}

#[test]
fn create_indexes_rejects_conflicting_definitions() {
    let store = store();
    commands::create_indexes(&index_command("items", 1, "a_1", false), &store).expect("createIndexes");

    let same_name = commands::create_indexes(&index_command("items", -1, "a_1", false), &store).expect_err("name conflict");
    assert_eq!(same_name.code, 86);

    let equivalent = commands::create_indexes(&index_command("items", 1, "a_second", false), &store).expect_err("equivalent key conflict");
    assert_eq!(equivalent.code, 85);
}

fn ttl_index_command(collection: &str, field: &str, seconds: i32) -> BsonDocument {
    let mut key = BsonDocument::new();
    key.push(field, BsonValue::Int32(1));
    let mut specification = BsonDocument::new();
    specification.push("key", BsonValue::Document(key));
    specification.push("name", BsonValue::String(format!("{field}_1")));
    specification.push("expireAfterSeconds", BsonValue::Int32(seconds));
    let mut create = command("createIndexes", collection, "app");
    create.push("indexes", BsonValue::Array(vec![BsonValue::Document(specification)]));
    create
}

/// `expireAfterSeconds` removal is periodic in `mongod`: the monitor wakes up
/// every `ttlMonitorSleepSecs` (60 by default) and only then removes whatever
/// expired. This test pins that window: a document whose deadline passes right
/// after a pass survives until the next pass, and the pass that runs one full
/// window later removes it.
#[test]
fn ttl_monitor_removes_expired_documents_only_on_scan_ticks() {
    let store = store();
    commands::create_indexes(&ttl_index_command("ttl", "expireAt", 60), &store).expect("createIndexes");
    assert_eq!(store.ttl_monitor_state().sleep_seconds, 60);

    let now = 1_700_000_000_000_i64;
    insert_documents(
        &store,
        "ttl",
        vec![
            document(&[("_id", BsonValue::Int32(1)), ("expireAt", BsonValue::DateTime(now - 61_000))]),
            document(&[("_id", BsonValue::Int32(2)), ("expireAt", BsonValue::DateTime(now + 60_000))]),
            document(&[("_id", BsonValue::Int32(3)), ("expireAt", BsonValue::String("not a date".to_string()))]),
        ],
    );

    // The first pass has no previous wake-up, so it runs right away and removes
    // the document that was already past its 60-second deadline.
    let first = commands::ttl_monitor(&store, now).expect("first pass");
    assert!(first.scanned);
    assert_eq!(first.deleted, 1);
    assert_eq!(first.collections, vec![("app.ttl".to_string(), 1)]);

    let mut cursors = CursorStore::new();
    assert_eq!(find_all(&store, "ttl", &mut cursors).len(), 2);

    // `now - 59_999` plus the 60-second TTL expires one millisecond after the
    // first pass, so the deadline has passed long before the next wake-up.
    insert_documents(&store, "ttl", vec![document(&[("_id", BsonValue::Int32(4)), ("expireAt", BsonValue::DateTime(now - 59_999))])]);

    let mut cursors = CursorStore::new();
    let early = commands::ttl_monitor(&store, now + 2_000).expect("early pass");
    assert!(!early.scanned, "the 60-second window has not elapsed yet");
    assert_eq!(early.deleted, 0);
    assert_eq!(find_all(&store, "ttl", &mut cursors).len(), 3, "an expired document survives until the next scan tick");

    let late = commands::ttl_monitor(&store, now + 61_000).expect("late pass");
    assert!(late.scanned);
    assert_eq!(late.deleted, 1);
    let mut cursors = CursorStore::new();
    assert_eq!(find_all(&store, "ttl", &mut cursors).len(), 2);

    // The window is configurable, but a configured window still is a window.
    store.set_ttl_monitor_sleep_seconds(0);
    assert_eq!(store.ttl_monitor_state().sleep_seconds, 1);
}

/// A `findAndModify` that fails the unique check must fail before anything is
/// staged: the matched document keeps every original field.
#[test]
fn find_and_modify_duplicate_key_failure_leaves_the_document_untouched() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![
            document(&[("_id", BsonValue::Int32(1)), ("a", BsonValue::Int32(7)), ("note", BsonValue::String("original".to_string()))]),
            document(&[("_id", BsonValue::Int32(2)), ("a", BsonValue::Int32(8))]),
        ],
    );
    commands::create_indexes(&index_command("items", 1, "a_1", true), &store).expect("createIndexes");

    let mut find_and_modify = command("findAndModify", "items", "app");
    find_and_modify.push("query", BsonValue::Document(document(&[("_id", BsonValue::Int32(1))])));
    let mut set = BsonDocument::new();
    set.push("a", BsonValue::Int32(8));
    set.push("note", BsonValue::String("rewritten".to_string()));
    find_and_modify.push("update", BsonValue::Document(document(&[("$set", BsonValue::Document(set))])));
    find_and_modify.push("new", BsonValue::Bool(true));

    let error = commands::find_and_modify(&find_and_modify, &store).expect_err("duplicate key");
    assert_eq!(error.code, 11000);
    assert!(error.message.starts_with("Plan executor error during findAndModify :: caused by ::"));

    let mut cursors = CursorStore::new();
    let documents = find_all(&store, "items", &mut cursors);
    assert_eq!(documents.len(), 2);
    let matched = documents.iter().find(|document| document.get("_id") == Some(&BsonValue::Int32(1))).expect("_id 1");
    assert_eq!(matched.get("a"), Some(&BsonValue::Int32(7)), "the conflicting field must not be staged");
    assert_eq!(matched.get("note"), Some(&BsonValue::String("original".to_string())), "no field may be staged");
}

fn aggregate_pipeline(collection: &str, stages: Vec<BsonValue>) -> BsonDocument {
    let mut aggregate = command("aggregate", collection, "app");
    aggregate.push("pipeline", BsonValue::Array(stages));
    aggregate
}

fn stage(name: &str, body: BsonValue) -> BsonValue {
    BsonValue::Document(document(&[(name, body)]))
}

#[test]
fn union_with_appends_documents_from_the_other_collection() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1))])]);
    insert_documents(
        &store,
        "other",
        vec![document(&[("_id", BsonValue::Int32(2)), ("kind", text("b"))]), document(&[("_id", BsonValue::Int32(3)), ("kind", text("a"))])],
    );

    let mut union = BsonDocument::new();
    union.push("coll", text("other"));
    union.push("pipeline", BsonValue::Array(vec![stage("$match", BsonValue::Document(document(&[("kind", text("b"))])))]));
    let reply =
        commands::aggregate(&aggregate_pipeline("items", vec![stage("$unionWith", BsonValue::Document(union))]), &store).expect("aggregate");
    let documents = batch(&reply, "firstBatch");

    assert_eq!(documents.len(), 2);
    assert_eq!(documents[0].get("_id"), Some(&BsonValue::Int32(1)));
    assert_eq!(documents[1].get("_id"), Some(&BsonValue::Int32(2)));
}

#[test]
fn sample_truncates_a_fractional_size() {
    let store = store();
    insert_documents(&store, "items", (0..4).map(|index| document(&[("_id", BsonValue::Int32(index))])).collect());

    let reply = commands::aggregate(
        &aggregate_pipeline("items", vec![stage("$sample", BsonValue::Document(document(&[("size", BsonValue::Double(1.5))])))]),
        &store,
    )
    .expect("aggregate");

    assert_eq!(batch(&reply, "firstBatch").len(), 1);
}

#[test]
fn densify_fills_numeric_gaps_and_leaves_existing_documents_alone() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![
            document(&[("_id", BsonValue::Int32(1)), ("x", BsonValue::Int32(1))]),
            document(&[("_id", BsonValue::Int32(2)), ("x", BsonValue::Int32(4))]),
        ],
    );

    let mut range = BsonDocument::new();
    range.push("step", BsonValue::Int32(1));
    range.push("bounds", text("full"));
    let mut body = BsonDocument::new();
    body.push("field", text("x"));
    body.push("range", BsonValue::Document(range));

    let reply =
        commands::aggregate(&aggregate_pipeline("items", vec![stage("$densify", BsonValue::Document(body))]), &store).expect("aggregate");
    let documents = batch(&reply, "firstBatch");

    let values: Vec<i64> = documents.iter().map(|document| int(document, "x")).collect();
    assert_eq!(values, vec![1, 2, 3, 4]);
    assert_eq!(documents[0].get("_id"), Some(&BsonValue::Int32(1)));
    assert!(documents[1].get("_id").is_none(), "generated documents only carry the densified field");
}

#[test]
fn fill_locf_carries_the_previous_value_forward() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![
            document(&[("_id", BsonValue::Int32(1)), ("t", BsonValue::Int32(1)), ("v", BsonValue::Int32(10))]),
            document(&[("_id", BsonValue::Int32(2)), ("t", BsonValue::Int32(2)), ("v", BsonValue::Null)]),
            document(&[("_id", BsonValue::Int32(3)), ("t", BsonValue::Int32(3)), ("v", BsonValue::Int32(30))]),
        ],
    );

    let mut body = BsonDocument::new();
    body.push("sortBy", BsonValue::Document(document(&[("t", BsonValue::Int32(1))])));
    let mut output = BsonDocument::new();
    output.push("v", BsonValue::Document(document(&[("method", text("locf"))])));
    body.push("output", BsonValue::Document(output));

    let reply = commands::aggregate(&aggregate_pipeline("items", vec![stage("$fill", BsonValue::Document(body))]), &store).expect("aggregate");
    let documents = batch(&reply, "firstBatch");

    assert_eq!(documents.len(), 3);
    assert_eq!(int(&documents[0], "v"), 10);
    assert_eq!(int(&documents[1], "v"), 10, "the null value is filled from the previous document");
    assert_eq!(int(&documents[2], "v"), 30);
}

#[test]
fn redact_prunes_matching_documents_and_descends_into_subdocuments() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![
            document(&[("_id", BsonValue::Int32(1)), ("secret", BsonValue::Bool(true)), ("v", text("hidden"))]),
            document(&[("_id", BsonValue::Int32(2)), ("v", text("shown"))]),
            document(&[("_id", BsonValue::Int32(3)), ("child", BsonValue::Document(document(&[("secret", BsonValue::Bool(true))])))]),
        ],
    );

    let condition = BsonValue::Array(vec![
        BsonValue::Document(document(&[("$eq", BsonValue::Array(vec![BsonValue::String("$secret".to_string()), BsonValue::Bool(true)]))])),
        BsonValue::String("$$PRUNE".to_string()),
        BsonValue::String("$$DESCEND".to_string()),
    ]);
    let reply = commands::aggregate(
        &aggregate_pipeline("items", vec![stage("$redact", BsonValue::Document(document(&[("$cond", condition)])))]),
        &store,
    )
    .expect("aggregate");
    let documents = batch(&reply, "firstBatch");

    assert_eq!(documents.len(), 2);
    assert_eq!(documents[0].get("_id"), Some(&BsonValue::Int32(2)));
    let descended = documents.iter().find(|document| document.get("_id") == Some(&BsonValue::Int32(3))).expect("_id 3");
    assert!(descended.get("child").is_none(), "$$DESCEND prunes the offending subdocument, not the parent");
}

#[test]
fn out_replaces_the_target_only_when_the_whole_output_is_valid() {
    let store = store();
    insert_documents(&store, "results", vec![document(&[("_id", BsonValue::Int32(1)), ("k", text("a"))])]);
    let mut create = command("createIndexes", "results", "app");
    let mut specification = BsonDocument::new();
    specification.push("key", BsonValue::Document(document(&[("k", BsonValue::Int32(1))])));
    specification.push("name", text("k_1"));
    specification.push("unique", BsonValue::Bool(true));
    create.push("indexes", BsonValue::Array(vec![BsonValue::Document(specification)]));
    commands::create_indexes(&create, &store).expect("createIndexes");

    insert_documents(
        &store,
        "rows",
        vec![document(&[("_id", BsonValue::Int32(2)), ("k", text("a"))]), document(&[("_id", BsonValue::Int32(3)), ("k", text("a"))])],
    );

    let error = commands::aggregate(&aggregate_pipeline("rows", vec![stage("$out", text("results"))]), &store)
        .expect_err("duplicate keys must abort the $out");
    assert_eq!(error.code, 11000);

    let mut cursors = CursorStore::new();
    let kept = find_all(&store, "results", &mut cursors);
    assert_eq!(kept.len(), 1, "the old target collection survives a failed $out");
    assert_eq!(kept[0].get("k"), Some(&text("a")));

    let listed = commands::list_indexes(&command("listIndexes", "results", "app"), &store).expect("listIndexes");
    assert_eq!(batch(&listed, "firstBatch").len(), 2, "the target keeps its indexes");
}

#[test]
fn merge_applies_writes_one_by_one_and_keeps_earlier_ones_on_failure() {
    let store = store();
    insert_documents(&store, "target", vec![document(&[("_id", BsonValue::Int32(1)), ("k", text("a"))])]);
    let mut create = command("createIndexes", "target", "app");
    let mut specification = BsonDocument::new();
    specification.push("key", BsonValue::Document(document(&[("k", BsonValue::Int32(1))])));
    specification.push("name", text("k_1"));
    specification.push("unique", BsonValue::Bool(true));
    create.push("indexes", BsonValue::Array(vec![BsonValue::Document(specification)]));
    commands::create_indexes(&create, &store).expect("createIndexes");
    insert_documents(
        &store,
        "source",
        vec![
            document(&[("_id", BsonValue::Int32(1)), ("k", text("a")), ("extra", BsonValue::Bool(true))]),
            document(&[("_id", BsonValue::Int32(3)), ("k", text("c"))]),
        ],
    );

    let mut merge = BsonDocument::new();
    merge.push("into", text("target"));
    merge.push("on", text("k"));
    merge.push("whenNotMatched", text("fail"));
    let error = commands::aggregate(&aggregate_pipeline("source", vec![stage("$merge", BsonValue::Document(merge))]), &store)
        .expect_err("the unmatched document must abort");
    assert_eq!(error.code, 13113);

    let mut cursors = CursorStore::new();
    let target_documents = find_all(&store, "target", &mut cursors);
    assert_eq!(target_documents.len(), 1);
    let merged = &target_documents[0];
    assert_eq!(merged.get("_id"), Some(&BsonValue::Int32(1)));
    assert_eq!(merged.get("extra"), Some(&BsonValue::Bool(true)), "the write before the failing document is not rolled back");
}

#[test]
fn wildcard_and_hashed_indexes_follow_the_real_server_rules() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("a", BsonValue::Int32(1))])]);

    let mut wildcard_key = BsonDocument::new();
    wildcard_key.push("$**", BsonValue::Int32(1));
    let mut wildcard = BsonDocument::new();
    wildcard.push("key", BsonValue::Document(wildcard_key));
    wildcard.push("name", text("wild"));
    wildcard.push("wildcardProjection", BsonValue::Document(document(&[("a", BsonValue::Int32(1))])));
    let mut create = command("createIndexes", "items", "app");
    create.push("indexes", BsonValue::Array(vec![BsonValue::Document(wildcard.clone())]));
    commands::create_indexes(&create, &store).expect("wildcard index");

    let listed = commands::list_indexes(&command("listIndexes", "items", "app"), &store).expect("listIndexes");
    let indexes = batch(&listed, "firstBatch");
    assert_eq!(indexes[1].get("name"), Some(&text("wild")));
    assert!(matches!(indexes[1].get("wildcardProjection"), Some(BsonValue::Document(_))));

    let mut unique_wildcard = wildcard;
    unique_wildcard.push("unique", BsonValue::Bool(true));
    let mut create_unique = command("createIndexes", "items", "app");
    create_unique.push("indexes", BsonValue::Array(vec![BsonValue::Document(unique_wildcard)]));
    assert_eq!(commands::create_indexes(&create_unique, &store).expect_err("unique wildcard").code, 67);

    let mut hashed_key = BsonDocument::new();
    hashed_key.push("h", BsonValue::String("hashed".to_string()));
    let mut hashed = BsonDocument::new();
    hashed.push("key", BsonValue::Document(hashed_key));
    hashed.push("name", text("h_hashed"));
    let mut create_hashed = command("createIndexes", "items", "app");
    create_hashed.push("indexes", BsonValue::Array(vec![BsonValue::Document(hashed.clone())]));
    commands::create_indexes(&create_hashed, &store).expect("hashed index");

    let mut second_hashed = BsonDocument::new();
    second_hashed.push("h", BsonValue::String("hashed".to_string()));
    second_hashed.push("i", BsonValue::String("hashed".to_string()));
    let mut hashed_pair = BsonDocument::new();
    hashed_pair.push("key", BsonValue::Document(second_hashed));
    hashed_pair.push("name", text("h_i_hashed"));
    let mut create_pair = command("createIndexes", "items", "app");
    create_pair.push("indexes", BsonValue::Array(vec![BsonValue::Document(hashed_pair)]));
    assert_eq!(commands::create_indexes(&create_pair, &store).expect_err("two hashed fields").code, 31303);

    let mut unique_hashed = hashed;
    unique_hashed.push("unique", BsonValue::Bool(true));
    let mut create_unique_hashed = command("createIndexes", "items", "app");
    create_unique_hashed.push("indexes", BsonValue::Array(vec![BsonValue::Document(unique_hashed)]));
    assert_eq!(commands::create_indexes(&create_unique_hashed, &store).expect_err("unique hashed").code, 16764);

    let mut text_key = BsonDocument::new();
    text_key.push("body", BsonValue::String("text".to_string()));
    let mut text_index = BsonDocument::new();
    text_index.push("key", BsonValue::Document(text_key));
    text_index.push("name", text("body_text"));
    let mut create_text = command("createIndexes", "items", "app");
    create_text.push("indexes", BsonValue::Array(vec![BsonValue::Document(text_index)]));
    assert_eq!(
        commands::create_indexes(&create_text, &store).expect_err("text index").code,
        59,
        "text indexes are rejected explicitly instead of being served incorrectly"
    );
}
