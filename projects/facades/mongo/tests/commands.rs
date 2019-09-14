//! Command-primitive tests: no server loop, no global state.

use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use mongo::{
    bson::{BinaryData, BsonDocument, BsonValue, encode_bson, parse_bson},
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

fn count_with(store: &Store, field: &str, operators: &[(&str, BsonValue)]) -> Result<i64, CommandError> {
    let mut count = command("count", "items", "app");
    count.push("query", BsonValue::Document(document(&[(field, BsonValue::Document(document(operators)))])));
    commands::count(&count, store).map(|reply| int(&reply, "n"))
}

#[test]
fn count_supports_mod_and_bitwise_operators() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![9, 10, 11, 12]
            .into_iter()
            .enumerate()
            .map(|(index, value)| document(&[("_id", BsonValue::Int32(index as i32)), ("value", BsonValue::Int32(value))]))
            .collect(),
    );

    assert_eq!(count_with(&store, "value", &[("$mod", BsonValue::Array(vec![BsonValue::Int32(4), BsonValue::Int32(1)]))]).unwrap(), 1);
    assert_eq!(count_with(&store, "value", &[("$mod", BsonValue::Array(vec![BsonValue::Int32(4), BsonValue::Int32(2)]))]).unwrap(), 1);
    assert_eq!(count_with(&store, "value", &[("$mod", BsonValue::Array(vec![BsonValue::Int32(4), BsonValue::Int32(0)]))]).unwrap(), 1);

    let positions = |values: &[i32]| BsonValue::Array(values.iter().copied().map(BsonValue::Int32).collect());
    assert_eq!(count_with(&store, "value", &[("$bitsAllSet", positions(&[1, 3]))]).unwrap(), 2);
    assert_eq!(count_with(&store, "value", &[("$bitsAnySet", positions(&[0, 2]))]).unwrap(), 3);
    assert_eq!(count_with(&store, "value", &[("$bitsAllClear", positions(&[0, 2]))]).unwrap(), 1);
    assert_eq!(count_with(&store, "value", &[("$bitsAnyClear", BsonValue::Int32(10))]).unwrap(), 2);
}

#[test]
fn malformed_mod_and_bit_operands_are_client_errors() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("value", BsonValue::Int32(9))])]);

    let zero_divisor = count_with(&store, "value", &[("$mod", BsonValue::Array(vec![BsonValue::Int32(0), BsonValue::Int32(1)]))]);
    assert_eq!(zero_divisor.expect_err("zero divisor must fail").code, 2);

    let short_mod = count_with(&store, "value", &[("$mod", BsonValue::Array(vec![BsonValue::Int32(4)]))]);
    assert_eq!(short_mod.expect_err("short operand must fail").code, 2);

    let out_of_range = count_with(&store, "value", &[("$bitsAllSet", BsonValue::Array(vec![BsonValue::Int32(64)]))]);
    assert_eq!(out_of_range.expect_err("bit 64 must fail").code, 2);

    let not_a_mask = count_with(&store, "value", &[("$bitsAnyClear", text("nope"))]);
    assert_eq!(not_a_mask.expect_err("string mask must fail").code, 2);
}

fn session_document() -> BsonValue {
    BsonValue::Document(document(&[("id", BsonValue::Binary(BinaryData::new(0x04, vec![0x2a; 16])))]))
}

fn transaction_insert(store: &Store, collection: &str, session: &BsonValue, txn_number: i64, id: i32, value: &str) -> BsonDocument {
    let mut insert = command("insert", collection, "app");
    insert.push("lsid", session.clone());
    insert.push("txnNumber", BsonValue::Int64(txn_number));
    insert.push("startTransaction", BsonValue::Bool(true));
    insert.push("autocommit", BsonValue::Bool(false));
    insert.push("documents", BsonValue::Array(vec![BsonValue::Document(document(&[("_id", BsonValue::Int32(id)), ("value", text(value))]))]));
    commands::insert(&insert, store).expect("transactional insert")
}

fn find_in_session(store: &Store, collection: &str, session: &BsonValue, txn_number: i64) -> usize {
    let mut find = command("find", collection, "app");
    find.push("filter", BsonValue::Document(BsonDocument::new()));
    find.push("lsid", session.clone());
    find.push("txnNumber", BsonValue::Int64(txn_number));
    find.push("autocommit", BsonValue::Bool(false));
    let mut cursors = CursorStore::new();
    batch(&commands::find(&find, store, &mut cursors).expect("find"), "firstBatch").len()
}

fn end_transaction(store: &Store, name: &str, session: &BsonValue, txn_number: i64) -> Result<BsonDocument, CommandError> {
    let mut command = command(name, "ledger", "admin");
    command.push("lsid", session.clone());
    command.push("txnNumber", BsonValue::Int64(txn_number));
    if name == "commitTransaction" { commands::commit_transaction(&command, store) } else { commands::abort_transaction(&command, store) }
}

#[test]
fn transactions_hide_staged_writes_until_commit_and_discard_them_on_abort() {
    let store = store();
    let session = session_document();

    let inserted = transaction_insert(&store, "ledger", &session, 1, 1, "kept");
    assert_eq!(int(&inserted, "n"), 1);

    let mut cursors = CursorStore::new();
    assert!(find_all(&store, "ledger", &mut cursors).is_empty(), "staged writes must stay invisible outside the session");
    assert_eq!(find_in_session(&store, "ledger", &session, 1), 1, "the owning session reads its own write");

    assert_eq!(int(&end_transaction(&store, "commitTransaction", &session, 1).expect("commit"), "ok"), 1);
    assert_eq!(find_all(&store, "ledger", &mut cursors).len(), 1, "commit publishes the staged write");

    let staged = transaction_insert(&store, "ledger", &session, 2, 2, "dropped");
    assert_eq!(int(&staged, "n"), 1);
    assert_eq!(int(&end_transaction(&store, "abortTransaction", &session, 2).expect("abort"), "ok"), 1);
    assert_eq!(find_all(&store, "ledger", &mut cursors).len(), 1, "abort discards the staged write");
}

#[test]
fn transaction_commands_reject_a_command_without_a_session() {
    let store = store();
    let mut commit = command("commitTransaction", "ledger", "admin");
    commit.push("txnNumber", BsonValue::Int64(1));
    assert_eq!(commands::commit_transaction(&commit, &store).expect_err("no session").code, 40414);

    let mut abort = command("abortTransaction", "ledger", "admin");
    abort.push("txnNumber", BsonValue::Int64(1));
    assert_eq!(commands::abort_transaction(&abort, &store).expect_err("no session").code, 40414);

    let session = session_document();
    let mut short_txn = command("commitTransaction", "ledger", "admin");
    short_txn.push("lsid", session);
    short_txn.push("txnNumber", BsonValue::Int32(1));
    assert_eq!(commands::commit_transaction(&short_txn, &store).expect_err("txnNumber must be Int64").code, 14);
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
fn arithmetic_expressions_follow_numeric_promotion() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("n", BsonValue::Int32(7))])]);

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("sum", operator("$add", BsonValue::Array(vec![BsonValue::Int32(2), BsonValue::Int32(3)]))),
            ("wide", operator("$add", BsonValue::Array(vec![BsonValue::Int32(2), BsonValue::Int64(3)]))),
            ("mixed", operator("$add", BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Double(2.5)]))),
            ("from_field", operator("$add", BsonValue::Array(vec![text("$n"), BsonValue::Int32(1)]))),
            ("difference", operator("$subtract", BsonValue::Array(vec![BsonValue::Int32(10), BsonValue::Int32(4)]))),
            ("product", operator("$multiply", BsonValue::Array(vec![BsonValue::Int32(3), BsonValue::Int32(4)]))),
            ("quotient", operator("$divide", BsonValue::Array(vec![BsonValue::Int32(7), BsonValue::Int32(2)]))),
            ("remainder", operator("$mod", BsonValue::Array(vec![BsonValue::Int32(7), BsonValue::Int32(4)]))),
            ("power", operator("$pow", BsonValue::Array(vec![BsonValue::Int32(2), BsonValue::Int32(10)]))),
            ("fractional_power", operator("$pow", BsonValue::Array(vec![BsonValue::Int32(2), BsonValue::Int32(-1)]))),
            ("absolute", operator("$abs", BsonValue::Int32(-3))),
            ("ceiling", operator("$ceil", BsonValue::Double(2.1))),
            ("flooring", operator("$floor", BsonValue::Double(2.9))),
            ("rounded", operator("$round", BsonValue::Array(vec![BsonValue::Double(2.5)]))),
            ("rounded_away", operator("$round", BsonValue::Array(vec![BsonValue::Double(3.5)]))),
            ("truncated", operator("$trunc", BsonValue::Array(vec![BsonValue::Double(2.9)]))),
            ("negative_place", operator("$trunc", BsonValue::Array(vec![BsonValue::Int32(1234), BsonValue::Int32(-2)]))),
            ("root", operator("$sqrt", BsonValue::Int32(9))),
        ]),
    );

    assert_eq!(row.get("sum"), Some(&BsonValue::Int32(5)));
    assert_eq!(row.get("wide"), Some(&BsonValue::Int64(5)));
    assert_eq!(row.get("mixed"), Some(&BsonValue::Double(3.5)));
    assert_eq!(row.get("from_field"), Some(&BsonValue::Int32(8)));
    assert_eq!(row.get("difference"), Some(&BsonValue::Int32(6)));
    assert_eq!(row.get("product"), Some(&BsonValue::Int32(12)));
    assert_eq!(row.get("quotient"), Some(&BsonValue::Double(3.5)));
    assert_eq!(row.get("remainder"), Some(&BsonValue::Int32(3)));
    assert_eq!(row.get("power"), Some(&BsonValue::Int32(1024)));
    assert_eq!(row.get("fractional_power"), Some(&BsonValue::Double(0.5)));
    assert_eq!(row.get("absolute"), Some(&BsonValue::Int32(3)));
    assert_eq!(row.get("ceiling"), Some(&BsonValue::Double(3.0)));
    assert_eq!(row.get("flooring"), Some(&BsonValue::Double(2.0)));
    assert_eq!(row.get("rounded"), Some(&BsonValue::Double(2.0)));
    assert_eq!(row.get("rounded_away"), Some(&BsonValue::Double(4.0)));
    assert_eq!(row.get("truncated"), Some(&BsonValue::Double(2.0)));
    assert_eq!(row.get("negative_place"), Some(&BsonValue::Int32(1200)));
    assert_eq!(row.get("root"), Some(&BsonValue::Double(3.0)));
}

#[test]
fn arithmetic_expressions_handle_dates() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1))])]);

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("delta", operator("$subtract", BsonValue::Array(vec![BsonValue::DateTime(1000), BsonValue::DateTime(400)]))),
            ("shifted", operator("$add", BsonValue::Array(vec![BsonValue::DateTime(1000), BsonValue::Int32(500)]))),
            ("pulled", operator("$subtract", BsonValue::Array(vec![BsonValue::DateTime(1000), BsonValue::Int32(500)]))),
        ]),
    );

    assert_eq!(row.get("delta"), Some(&BsonValue::Int64(600)));
    assert_eq!(row.get("shifted"), Some(&BsonValue::DateTime(1500)));
    assert_eq!(row.get("pulled"), Some(&BsonValue::DateTime(500)));
}

#[test]
fn arithmetic_expressions_propagate_null_and_reject_bad_operands() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("word", text("nope"))])]);

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("null_sum", operator("$add", BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Null]))),
            ("null_division", operator("$divide", BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Null]))),
            ("null_root", operator("$sqrt", BsonValue::Null)),
        ]),
    );
    assert_eq!(row.get("null_sum"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_division"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_root"), Some(&BsonValue::Null));

    let failure = |projection: BsonDocument| -> CommandError {
        let aggregate = aggregate_pipeline("items", vec![stage("$project", BsonValue::Document(projection))]);
        commands::aggregate(&aggregate, &store).expect_err("expression must fail")
    };

    assert_eq!(failure(document(&[("x", operator("$add", BsonValue::Array(vec![text("$word"), BsonValue::Int32(1)])))])).code, 16554);
    assert_eq!(failure(document(&[("x", operator("$subtract", BsonValue::Array(vec![BsonValue::Int32(1)])))])).code, 2);
    assert_eq!(failure(document(&[("x", operator("$divide", BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(0)])))])).code, 2);
    assert_eq!(failure(document(&[("x", operator("$sqrt", BsonValue::Int32(-1)))])).code, 28714);
    assert_eq!(failure(document(&[("x", operator("$round", BsonValue::Array(vec![BsonValue::Double(1.5), BsonValue::Int32(101)])),)])).code, 2);
}

#[test]
fn string_expressions_follow_the_manual() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("word", text("  Hello World  "))])]);

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("lower", operator("$toLower", text("$word"))),
            ("upper", operator("$toUpper", text("Hello"))),
            ("code_points", operator("$strLenCP", text("héllo"))),
            ("bytes", operator("$strLenBytes", text("héllo"))),
            ("head", operator("$substrCP", BsonValue::Array(vec![text("héllo"), BsonValue::Int32(1), BsonValue::Int32(3)]))),
            ("rest", operator("$substrBytes", BsonValue::Array(vec![text("abcdef"), BsonValue::Int32(2), BsonValue::Int32(-1)]))),
            ("legacy", operator("$substr", BsonValue::Array(vec![text("abcdef"), BsonValue::Int32(2), BsonValue::Int32(2)]))),
            ("negative", operator("$substrCP", BsonValue::Array(vec![text("abc"), BsonValue::Int32(-1), BsonValue::Int32(2)]))),
        ]),
    );

    assert_eq!(row.get("lower"), Some(&text("  hello world  ")));
    assert_eq!(row.get("upper"), Some(&text("HELLO")));
    assert_eq!(row.get("code_points"), Some(&BsonValue::Int32(5)));
    assert_eq!(row.get("bytes"), Some(&BsonValue::Int32(6)));
    assert_eq!(row.get("head"), Some(&text("éll")));
    assert_eq!(row.get("rest"), Some(&text("cdef")));
    assert_eq!(row.get("legacy"), Some(&text("cd")));
    assert_eq!(row.get("negative"), Some(&text("")));
}

#[test]
fn string_search_and_replacement_expressions_follow_the_manual() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("csv", text("a,b,,c"))])]);

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("parts", operator("$split", BsonValue::Array(vec![text("$csv"), text(",")]))),
            ("trimmed", operator("$trim", BsonValue::Document(document(&[("input", text("  padded  "))])))),
            ("left", operator("$ltrim", BsonValue::Document(document(&[("input", text("xxhixx")), ("chars", text("x"))])))),
            ("right", operator("$rtrim", BsonValue::Document(document(&[("input", text("xxhixx")), ("chars", text("x"))])))),
            ("found", operator("$indexOfCP", BsonValue::Array(vec![text("héllo"), text("llo")]))),
            ("found_bytes", operator("$indexOfBytes", BsonValue::Array(vec![text("héllo"), text("llo")]))),
            ("bounded", operator("$indexOfCP", BsonValue::Array(vec![text("abcabc"), text("abc"), BsonValue::Int32(1)]))),
            ("missing", operator("$indexOfCP", BsonValue::Array(vec![text("abc"), text("z")]))),
            ("one", operator("$replaceOne", BsonValue::Array(vec![text("a-a-a"), text("a"), text("b")]))),
            ("all", operator("$replaceAll", BsonValue::Array(vec![text("a-a-a"), text("a"), text("b")]))),
            ("same", operator("$strcasecmp", BsonValue::Array(vec![text("Apple"), text("apple")]))),
            ("less", operator("$strcasecmp", BsonValue::Array(vec![text("apple"), text("Banana")]))),
        ]),
    );

    assert_eq!(row.get("parts"), Some(&BsonValue::Array(vec![text("a"), text("b"), text(""), text("c")])));
    assert_eq!(row.get("trimmed"), Some(&text("padded")));
    assert_eq!(row.get("left"), Some(&text("hixx")));
    assert_eq!(row.get("right"), Some(&text("xxhi")));
    assert_eq!(row.get("found"), Some(&BsonValue::Int32(2)));
    assert_eq!(row.get("found_bytes"), Some(&BsonValue::Int32(3)));
    assert_eq!(row.get("bounded"), Some(&BsonValue::Int32(3)));
    assert_eq!(row.get("missing"), Some(&BsonValue::Int32(-1)));
    assert_eq!(row.get("one"), Some(&text("b-a-a")));
    assert_eq!(row.get("all"), Some(&text("b-b-b")));
    assert_eq!(row.get("same"), Some(&BsonValue::Int32(0)));
    assert_eq!(row.get("less"), Some(&BsonValue::Int32(-1)));
}

#[test]
fn string_expressions_propagate_null_and_reject_bad_operands() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("n", BsonValue::Int32(7))])]);

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("null_lower", operator("$toLower", BsonValue::Null)),
            ("null_length", operator("$strLenCP", BsonValue::Null)),
            ("null_split", operator("$split", BsonValue::Array(vec![BsonValue::Null, text(",")]))),
            ("null_trim", operator("$trim", BsonValue::Null)),
            ("null_index", operator("$indexOfCP", BsonValue::Array(vec![BsonValue::Null, text("a")]))),
            ("null_replace", operator("$replaceOne", BsonValue::Array(vec![text("abc"), BsonValue::Null, text("x")]))),
            ("null_cmp", operator("$strcasecmp", BsonValue::Array(vec![BsonValue::Null, BsonValue::Null]))),
        ]),
    );

    assert_eq!(row.get("null_lower"), Some(&text("")));
    assert_eq!(row.get("null_length"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_split"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_trim"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_index"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_replace"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_cmp"), Some(&BsonValue::Int32(0)));

    let failure = |projection: BsonDocument| -> CommandError {
        let aggregate = aggregate_pipeline("items", vec![stage("$project", BsonValue::Document(projection))]);
        commands::aggregate(&aggregate, &store).expect_err("expression must fail")
    };

    assert_eq!(failure(document(&[("x", operator("$strLenCP", text("$n")))])).code, 34471);
    assert_eq!(failure(document(&[("x", operator("$split", BsonValue::Array(vec![text("a"), text("")])))])).code, 40086);
    assert_eq!(failure(document(&[("x", operator("$substrCP", BsonValue::Array(vec![text("abc"), BsonValue::Int32(1)])))])).code, 2);
    assert_eq!(
        failure(document(&[("x", operator("$indexOfCP", BsonValue::Array(vec![text("abc"), text("a"), BsonValue::Int32(-1)])))])).code,
        40093
    );
}

#[test]
fn array_expressions_follow_the_manual() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![document(&[("_id", BsonValue::Int32(1)), ("tags", BsonValue::Array(vec![text("a"), text("b"), text("c")]))])],
    );

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("is_array", operator("$isArray", text("$tags"))),
            ("is_not_array", operator("$isArray", text("$missing"))),
            ("second", operator("$arrayElemAt", BsonValue::Array(vec![text("$tags"), BsonValue::Int32(1)]))),
            ("last_by_negative", operator("$arrayElemAt", BsonValue::Array(vec![text("$tags"), BsonValue::Int32(-1)]))),
            ("first", operator("$first", text("$tags"))),
            ("last", operator("$last", text("$tags"))),
            ("joined", operator("$concatArrays", BsonValue::Array(vec![text("$tags"), BsonValue::Array(vec![text("d")])]))),
            ("reversed", operator("$reverseArray", text("$tags"))),
            ("position", operator("$indexOfArray", BsonValue::Array(vec![text("$tags"), text("b")]))),
            ("bounded", operator("$indexOfArray", BsonValue::Array(vec![text("$tags"), text("a"), BsonValue::Int32(1)]))),
            ("series", operator("$range", BsonValue::Array(vec![BsonValue::Int32(0), BsonValue::Int32(5), BsonValue::Int32(2)]))),
            ("counting_down", operator("$range", BsonValue::Array(vec![BsonValue::Int32(3), BsonValue::Int32(0), BsonValue::Int32(-1)]))),
            ("head", operator("$slice", BsonValue::Array(vec![text("$tags"), BsonValue::Int32(2)]))),
            ("tail", operator("$slice", BsonValue::Array(vec![text("$tags"), BsonValue::Int32(-2)]))),
            ("window", operator("$slice", BsonValue::Array(vec![text("$tags"), BsonValue::Int32(1), BsonValue::Int32(2)]))),
            ("from_end", operator("$slice", BsonValue::Array(vec![text("$tags"), BsonValue::Int32(-2), BsonValue::Int32(1)]))),
            ("out_of_range", operator("$arrayElemAt", BsonValue::Array(vec![text("$tags"), BsonValue::Int32(9)]))),
        ]),
    );

    assert_eq!(row.get("is_array"), Some(&BsonValue::Bool(true)));
    assert_eq!(row.get("is_not_array"), Some(&BsonValue::Bool(false)));
    assert_eq!(row.get("second"), Some(&text("b")));
    assert_eq!(row.get("last_by_negative"), Some(&text("c")));
    assert_eq!(row.get("first"), Some(&text("a")));
    assert_eq!(row.get("last"), Some(&text("c")));
    assert_eq!(row.get("joined"), Some(&BsonValue::Array(vec![text("a"), text("b"), text("c"), text("d")])));
    assert_eq!(row.get("reversed"), Some(&BsonValue::Array(vec![text("c"), text("b"), text("a")])));
    assert_eq!(row.get("position"), Some(&BsonValue::Int32(1)));
    assert_eq!(row.get("bounded"), Some(&BsonValue::Int32(-1)));
    assert_eq!(row.get("series"), Some(&BsonValue::Array(vec![BsonValue::Int32(0), BsonValue::Int32(2), BsonValue::Int32(4)])));
    assert_eq!(row.get("counting_down"), Some(&BsonValue::Array(vec![BsonValue::Int32(3), BsonValue::Int32(2), BsonValue::Int32(1)])));
    assert_eq!(row.get("head"), Some(&BsonValue::Array(vec![text("a"), text("b")])));
    assert_eq!(row.get("tail"), Some(&BsonValue::Array(vec![text("b"), text("c")])));
    assert_eq!(row.get("window"), Some(&BsonValue::Array(vec![text("b"), text("c")])));
    assert_eq!(row.get("from_end"), Some(&BsonValue::Array(vec![text("b")])));
    assert_eq!(row.get("out_of_range"), Some(&BsonValue::Null));
}

#[test]
fn array_expressions_propagate_null_and_reject_bad_operands() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("value", text("nope"))])]);

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("null_elem", operator("$arrayElemAt", BsonValue::Array(vec![BsonValue::Null, BsonValue::Int32(0)]))),
            ("null_first", operator("$first", BsonValue::Null)),
            ("null_concat", operator("$concatArrays", BsonValue::Array(vec![BsonValue::Array(Vec::new()), BsonValue::Null]))),
            ("null_reverse", operator("$reverseArray", BsonValue::Null)),
            ("null_index", operator("$indexOfArray", BsonValue::Array(vec![BsonValue::Null, BsonValue::Int32(1)]))),
            ("null_slice", operator("$slice", BsonValue::Array(vec![BsonValue::Null, BsonValue::Int32(1)]))),
        ]),
    );

    assert_eq!(row.get("null_elem"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_first"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_concat"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_reverse"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_index"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_slice"), Some(&BsonValue::Null));

    let failure = |projection: BsonDocument| -> CommandError {
        let aggregate = aggregate_pipeline("items", vec![stage("$project", BsonValue::Document(projection))]);
        commands::aggregate(&aggregate, &store).expect_err("expression must fail")
    };

    assert_eq!(failure(document(&[("x", operator("$arrayElemAt", BsonValue::Array(vec![text("$value"), BsonValue::Int32(0)])))])).code, 28689);
    assert_eq!(
        failure(document(&[("x", operator("$arrayElemAt", BsonValue::Array(vec![BsonValue::Array(Vec::new()), text("x")])))])).code,
        28690
    );
    assert_eq!(failure(document(&[("x", operator("$concatArrays", BsonValue::Array(vec![text("$value")])))])).code, 28664);
    assert_eq!(
        failure(document(&[("x", operator("$range", BsonValue::Array(vec![BsonValue::Int32(0), BsonValue::Int32(3), BsonValue::Int32(0)])))]))
            .code,
        34449
    );
    assert_eq!(
        failure(document(&[(
            "x",
            operator("$slice", BsonValue::Array(vec![BsonValue::Array(vec![BsonValue::Int32(1)]), BsonValue::Int32(0), BsonValue::Int32(0)]))
        )]))
        .code,
        28729
    );
}

#[test]
fn date_field_expressions_follow_the_manual() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("when", BsonValue::DateTime(1_614_834_367_008))])]);

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("year", operator("$year", text("$when"))),
            ("month", operator("$month", text("$when"))),
            ("day", operator("$dayOfMonth", text("$when"))),
            ("hour", operator("$hour", text("$when"))),
            ("minute", operator("$minute", text("$when"))),
            ("second", operator("$second", text("$when"))),
            ("millisecond", operator("$millisecond", text("$when"))),
            ("day_of_week", operator("$dayOfWeek", text("$when"))),
            ("day_of_year", operator("$dayOfYear", text("$when"))),
            ("week", operator("$week", text("$when"))),
            ("iso_week", operator("$isoWeek", text("$when"))),
            ("iso_week_year", operator("$isoWeekYear", text("$when"))),
            ("iso_day_of_week", operator("$isoDayOfWeek", text("$when"))),
            ("local_hour", operator("$hour", BsonValue::Document(document(&[("date", text("$when")), ("timezone", text("+08:00"))])))),
        ]),
    );

    assert_eq!(row.get("year"), Some(&BsonValue::Int32(2021)));
    assert_eq!(row.get("month"), Some(&BsonValue::Int32(3)));
    assert_eq!(row.get("day"), Some(&BsonValue::Int32(4)));
    assert_eq!(row.get("hour"), Some(&BsonValue::Int32(5)));
    assert_eq!(row.get("minute"), Some(&BsonValue::Int32(6)));
    assert_eq!(row.get("second"), Some(&BsonValue::Int32(7)));
    assert_eq!(row.get("millisecond"), Some(&BsonValue::Int32(8)));
    assert_eq!(row.get("day_of_week"), Some(&BsonValue::Int32(5)));
    assert_eq!(row.get("day_of_year"), Some(&BsonValue::Int32(63)));
    assert_eq!(row.get("week"), Some(&BsonValue::Int32(9)));
    assert_eq!(row.get("iso_week"), Some(&BsonValue::Int32(9)));
    assert_eq!(row.get("iso_week_year"), Some(&BsonValue::Int32(2021)));
    assert_eq!(row.get("iso_day_of_week"), Some(&BsonValue::Int32(4)));
    assert_eq!(row.get("local_hour"), Some(&BsonValue::Int32(13)));
}

#[test]
fn date_conversion_expressions_follow_the_manual() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("when", BsonValue::DateTime(1_614_834_367_008))])]);

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("rendered", operator("$dateToString", BsonValue::Document(document(&[("date", text("$when"))])))),
            (
                "formatted",
                operator("$dateToString", BsonValue::Document(document(&[("date", text("$when")), ("format", text("%Y-%m-%d %H:%M:%S.%L"))]))),
            ),
            (
                "formatted_local",
                operator(
                    "$dateToString",
                    BsonValue::Document(document(&[("date", text("$when")), ("format", text("%H:%M %z")), ("timezone", text("+08:00"))])),
                ),
            ),
            ("parsed", operator("$dateFromString", BsonValue::Document(document(&[("dateString", text("2021-03-04T05:06:07.008Z"))])))),
            (
                "parsed_offset",
                operator("$dateFromString", BsonValue::Document(document(&[("dateString", text("2021-03-04 13:06:07.008+08:00"))]))),
            ),
            (
                "parsed_format",
                operator(
                    "$dateFromString",
                    BsonValue::Document(document(&[
                        ("dateString", text("2021-03-04 05:06:07.008")),
                        ("format", text("%Y-%m-%d %H:%M:%S.%L")),
                        ("timezone", text("UTC")),
                    ])),
                ),
            ),
            ("parts", operator("$dateToParts", BsonValue::Document(document(&[("date", text("$when"))])))),
            (
                "iso_parts",
                operator("$dateToParts", BsonValue::Document(document(&[("date", text("$when")), ("iso8601", BsonValue::Bool(true))]))),
            ),
            (
                "built",
                operator(
                    "$dateFromParts",
                    BsonValue::Document(document(&[
                        ("year", BsonValue::Int32(2021)),
                        ("month", BsonValue::Int32(2)),
                        ("day", BsonValue::Int32(31)),
                    ])),
                ),
            ),
            (
                "built_iso",
                operator(
                    "$dateFromParts",
                    BsonValue::Document(document(&[
                        ("isoWeekYear", BsonValue::Int32(2021)),
                        ("isoWeek", BsonValue::Int32(9)),
                        ("isoDayOfWeek", BsonValue::Int32(4)),
                    ])),
                ),
            ),
        ]),
    );

    assert_eq!(row.get("rendered"), Some(&text("2021-03-04T05:06:07.008Z")));
    assert_eq!(row.get("formatted"), Some(&text("2021-03-04 05:06:07.008")));
    assert_eq!(row.get("formatted_local"), Some(&text("13:06 +0800")));
    assert_eq!(row.get("parsed"), Some(&BsonValue::DateTime(1_614_834_367_008)));
    assert_eq!(row.get("parsed_offset"), Some(&BsonValue::DateTime(1_614_834_367_008)));
    assert_eq!(row.get("parsed_format"), Some(&BsonValue::DateTime(1_614_834_367_008)));
    assert_eq!(
        row.get("parts"),
        Some(&BsonValue::Document(document(&[
            ("year", BsonValue::Int32(2021)),
            ("month", BsonValue::Int32(3)),
            ("day", BsonValue::Int32(4)),
            ("hour", BsonValue::Int32(5)),
            ("minute", BsonValue::Int32(6)),
            ("second", BsonValue::Int32(7)),
            ("millisecond", BsonValue::Int32(8)),
        ])))
    );
    assert_eq!(
        row.get("iso_parts"),
        Some(&BsonValue::Document(document(&[
            ("isoWeekYear", BsonValue::Int32(2021)),
            ("isoWeek", BsonValue::Int32(9)),
            ("isoDayOfWeek", BsonValue::Int32(4)),
            ("hour", BsonValue::Int32(5)),
            ("minute", BsonValue::Int32(6)),
            ("second", BsonValue::Int32(7)),
            ("millisecond", BsonValue::Int32(8)),
        ])))
    );
    assert_eq!(row.get("built"), Some(&BsonValue::DateTime(1_614_729_600_000)));
    assert_eq!(row.get("built_iso"), Some(&BsonValue::DateTime(1_614_816_000_000)));
}

#[test]
fn date_arithmetic_expressions_follow_the_manual() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![document(&[
            ("_id", BsonValue::Int32(1)),
            ("january", BsonValue::DateTime(1_612_051_200_000)),
            ("when", BsonValue::DateTime(1_614_834_367_008)),
            ("before", BsonValue::DateTime(1_609_459_199_000)),
            ("after", BsonValue::DateTime(1_609_459_200_000)),
        ])],
    );

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            (
                "next_month",
                operator(
                    "$dateAdd",
                    BsonValue::Document(document(&[("startDate", text("$january")), ("unit", text("month")), ("amount", BsonValue::Int32(1))])),
                ),
            ),
            (
                "previous_day",
                operator(
                    "$dateSubtract",
                    BsonValue::Document(document(&[("startDate", text("$when")), ("unit", text("day")), ("amount", BsonValue::Int32(1))])),
                ),
            ),
            (
                "years",
                operator(
                    "$dateDiff",
                    BsonValue::Document(document(&[("startDate", text("$before")), ("endDate", text("$after")), ("unit", text("year"))])),
                ),
            ),
            (
                "hours",
                operator(
                    "$dateDiff",
                    BsonValue::Document(document(&[("startDate", text("$before")), ("endDate", text("$after")), ("unit", text("hour"))])),
                ),
            ),
            ("truncated_month", operator("$dateTrunc", BsonValue::Document(document(&[("date", text("$when")), ("unit", text("month"))])))),
            ("truncated_day", operator("$dateTrunc", BsonValue::Document(document(&[("date", text("$when")), ("unit", text("day"))])))),
            (
                "truncated_week",
                operator(
                    "$dateTrunc",
                    BsonValue::Document(document(&[("date", text("$when")), ("unit", text("week")), ("startOfWeek", text("monday"))])),
                ),
            ),
        ]),
    );

    assert_eq!(row.get("next_month"), Some(&BsonValue::DateTime(1_614_470_400_000)));
    assert_eq!(row.get("previous_day"), Some(&BsonValue::DateTime(1_614_747_967_008)));
    assert_eq!(row.get("years"), Some(&BsonValue::Int32(1)));
    assert_eq!(row.get("hours"), Some(&BsonValue::Int32(1)));
    assert_eq!(row.get("truncated_month"), Some(&BsonValue::DateTime(1_614_556_800_000)));
    assert_eq!(row.get("truncated_day"), Some(&BsonValue::DateTime(1_614_816_000_000)));
    assert_eq!(row.get("truncated_week"), Some(&BsonValue::DateTime(1_614_556_800_000)));
}

#[test]
fn date_expressions_propagate_null_and_reject_bad_operands() {
    let store = store();
    insert_documents(&store, "items", vec![document(&[("_id", BsonValue::Int32(1)), ("n", BsonValue::Int32(7))])]);

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("null_year", operator("$year", BsonValue::Null)),
            ("null_rendered", operator("$dateToString", BsonValue::Document(document(&[("date", BsonValue::Null), ("onNull", text("none"))])))),
            (
                "bad_parsed",
                operator("$dateFromString", BsonValue::Document(document(&[("dateString", text("not a date")), ("onError", text("bad"))]))),
            ),
            (
                "null_parsed",
                operator("$dateFromString", BsonValue::Document(document(&[("dateString", BsonValue::Null), ("onNull", text("none"))]))),
            ),
        ]),
    );

    assert_eq!(row.get("null_year"), Some(&BsonValue::Null));
    assert_eq!(row.get("null_rendered"), Some(&text("none")));
    assert_eq!(row.get("bad_parsed"), Some(&text("bad")));
    assert_eq!(row.get("null_parsed"), Some(&text("none")));

    let failure = |projection: BsonDocument| -> CommandError {
        let aggregate = aggregate_pipeline("items", vec![stage("$project", BsonValue::Document(projection))]);
        commands::aggregate(&aggregate, &store).expect_err("expression must fail")
    };

    assert_eq!(failure(document(&[("x", operator("$year", text("$n")))])).code, 16006);
    assert_eq!(
        failure(document(&[(
            "x",
            operator(
                "$dateAdd",
                BsonValue::Document(document(&[
                    ("startDate", BsonValue::DateTime(0)),
                    ("unit", text("fortnight")),
                    ("amount", BsonValue::Int32(1)),
                ]))
            )
        )]))
        .code,
        2
    );
    assert_eq!(
        failure(document(&[(
            "x",
            operator("$hour", BsonValue::Document(document(&[("date", BsonValue::DateTime(0)), ("timezone", text("Asia/Shanghai"))])))
        )]))
        .code,
        40485
    );
    assert_eq!(
        failure(document(&[("x", operator("$dateFromString", BsonValue::Document(document(&[("dateString", text("not a date"))]))))])).code,
        40516
    );
    assert_eq!(
        failure(document(&[(
            "x",
            operator(
                "$dateTrunc",
                BsonValue::Document(document(&[("date", BsonValue::DateTime(0)), ("unit", text("day")), ("binSize", BsonValue::Int32(0))]))
            )
        )]))
        .code,
        2
    );
}

#[test]
fn conversion_expressions_follow_the_manual() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![document(&[
            ("_id", BsonValue::Int32(1)),
            ("number", BsonValue::Int32(42)),
            ("fraction", BsonValue::Double(2.9)),
            ("negative", BsonValue::Double(-2.9)),
            ("flag", BsonValue::Bool(true)),
            ("id", BsonValue::ObjectId([0x01; 12])),
            ("when", BsonValue::DateTime(1_614_834_367_008)),
            ("digits", text("15")),
        ])],
    );

    let row = expression_projection(
        &store,
        document(&[
            ("_id", BsonValue::Int32(0)),
            ("number_text", operator("$toString", text("$number"))),
            ("flag_text", operator("$toString", text("$flag"))),
            ("date_text", operator("$toString", text("$when"))),
            ("id_text", operator("$toString", text("$id"))),
            ("int_from_string", operator("$toInt", text("$digits"))),
            ("int_from_double", operator("$toInt", text("$fraction"))),
            ("int_from_negative", operator("$toInt", text("$negative"))),
            ("long_from_bool", operator("$toLong", text("$flag"))),
            ("double_from_int", operator("$toDouble", text("$number"))),
            ("bool_from_zero", operator("$toBool", BsonValue::Int32(0))),
            ("bool_from_string", operator("$toBool", text(""))),
            ("id_from_string", operator("$toObjectId", text("010101010101010101010101"))),
            ("date_from_string", operator("$toDate", text("2021-03-04T05:06:07.008Z"))),
            ("date_from_id", operator("$toDate", text("$id"))),
            ("decimal_type", operator("$type", operator("$toDecimal", text("$digits")))),
            ("decimal_is_number", operator("$isNumber", operator("$toDecimal", text("$digits")))),
            ("int_type", operator("$type", text("$number"))),
            ("missing_type", operator("$type", text("$absent"))),
            ("array_type", operator("$type", BsonValue::Array(vec![BsonValue::Int32(1)]))),
            ("convert_text", operator("$convert", BsonValue::Document(document(&[("input", text("$number")), ("to", text("string"))])))),
            ("convert_code", operator("$convert", BsonValue::Document(document(&[("input", text("$digits")), ("to", BsonValue::Int32(19))])))),
            (
                "convert_null",
                operator(
                    "$convert",
                    BsonValue::Document(document(&[("input", BsonValue::Null), ("to", text("int")), ("onNull", text("none"))])),
                ),
            ),
            (
                "convert_error",
                operator(
                    "$convert",
                    BsonValue::Document(document(&[("input", text("not-a-number")), ("to", text("int")), ("onError", text("bad"))])),
                ),
            ),
            ("not_a_number", operator("$isNumber", text("$digits"))),
            ("null_to_int", operator("$toInt", BsonValue::Null)),
        ]),
    );

    assert_eq!(row.get("number_text"), Some(&text("42")));
    assert_eq!(row.get("flag_text"), Some(&text("true")));
    assert_eq!(row.get("date_text"), Some(&text("2021-03-04T05:06:07.008Z")));
    assert_eq!(row.get("id_text"), Some(&text("010101010101010101010101")));
    assert_eq!(row.get("int_from_string"), Some(&BsonValue::Int32(15)));
    assert_eq!(row.get("int_from_double"), Some(&BsonValue::Int32(2)), "取整必须向零截断");
    assert_eq!(row.get("int_from_negative"), Some(&BsonValue::Int32(-2)), "负数同样向零截断");
    assert_eq!(row.get("long_from_bool"), Some(&BsonValue::Int64(1)));
    assert_eq!(row.get("double_from_int"), Some(&BsonValue::Double(42.0)));
    assert_eq!(row.get("bool_from_zero"), Some(&BsonValue::Bool(false)));
    assert_eq!(row.get("bool_from_string"), Some(&BsonValue::Bool(true)), "空串也转成 true");
    assert_eq!(row.get("id_from_string"), Some(&BsonValue::ObjectId([0x01; 12])));
    assert_eq!(row.get("date_from_string"), Some(&BsonValue::DateTime(1_614_834_367_008)));
    assert_eq!(row.get("date_from_id"), Some(&BsonValue::DateTime(16_843_009_000)), "ObjectId 取其时间戳部分");
    assert_eq!(row.get("decimal_type"), Some(&text("decimal")));
    assert_eq!(row.get("decimal_is_number"), Some(&BsonValue::Bool(true)));
    assert_eq!(row.get("int_type"), Some(&text("int")));
    assert_eq!(row.get("missing_type"), Some(&text("missing")), "缺失字段不能折叠成 null 类型");
    assert_eq!(row.get("array_type"), Some(&text("array")));
    assert_eq!(row.get("convert_text"), Some(&text("42")));
    assert!(matches!(row.get("convert_code"), Some(BsonValue::Decimal128(_))), "数值类型码 19 表示 decimal");
    assert_eq!(row.get("convert_null"), Some(&text("none")));
    assert_eq!(row.get("convert_error"), Some(&text("bad")));
    assert_eq!(row.get("not_a_number"), Some(&BsonValue::Bool(false)));
    assert_eq!(row.get("null_to_int"), Some(&BsonValue::Null));
}

#[test]
fn conversion_expressions_reject_impossible_conversions() {
    let store = store();
    insert_documents(
        &store,
        "items",
        vec![document(&[("_id", BsonValue::Int32(1)), ("big", BsonValue::Int64(9_007_199_254_740_993)), ("nan", BsonValue::Double(f64::NAN))])],
    );

    let failure = |projection: BsonDocument| -> CommandError {
        let aggregate = aggregate_pipeline("items", vec![stage("$project", BsonValue::Document(projection))]);
        commands::aggregate(&aggregate, &store).expect_err("expression must fail")
    };

    assert_eq!(failure(document(&[("x", operator("$toInt", text("$big")))])).code, 241, "超出 int32 范围必须失败");
    assert_eq!(failure(document(&[("x", operator("$toObjectId", text("0123")))])).code, 241);
    assert_eq!(failure(document(&[("x", operator("$toInt", text("not-a-number")))])).code, 241);
    assert_eq!(failure(document(&[("x", operator("$toString", BsonValue::Array(vec![BsonValue::Int32(1)])))])).code, 241);
    assert_eq!(failure(document(&[("x", operator("$toLong", text("$nan")))])).code, 241);
    assert_eq!(
        failure(document(&[("x", operator("$convert", BsonValue::Document(document(&[("input", text("$big")), ("to", text("nope"))]))))])).code,
        2
    );
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
fn get_more_refuses_another_namespace() {
    let store = store();
    insert_documents(&store, "items", (0..4).map(|index| document(&[("_id", BsonValue::Int32(index))])).collect());

    let mut find = command("find", "items", "app");
    find.push("filter", BsonValue::Document(BsonDocument::new()));
    find.push("batchSize", BsonValue::Int32(2));
    let mut cursors = CursorStore::new();
    let response = commands::find(&find, &store, &mut cursors).expect("find");
    let id = cursor_id(&response);

    let foreign = document(&[("getMore", BsonValue::Int64(id)), ("collection", text("items")), ("$db", text("tenant_b"))]);
    let error = commands::get_more(&foreign, &mut cursors).expect_err("a foreign namespace must be refused");
    assert_eq!(error.code, 13);
    assert_eq!(error.message, "Requested getMore on namespace 'tenant_b.items', but cursor belongs to a different namespace app.items");

    // The cursor itself stays intact for the namespace that opened it.
    let own = document(&[("getMore", BsonValue::Int64(id)), ("collection", text("items")), ("$db", text("app"))]);
    let next = commands::get_more(&own, &mut cursors).expect("getMore on its own namespace");
    assert_eq!(batch(&next, "nextBatch").len(), 2);
}

#[test]
fn get_more_requires_a_collection() {
    let mut cursors = CursorStore::new();
    let (id, _) = cursors.open("app.items", (0..4).map(BsonValue::Int32).collect(), Some(2));
    let request = document(&[("getMore", BsonValue::Int64(id)), ("$db", text("app"))]);
    let error = commands::get_more(&request, &mut cursors).expect_err("collection is required");
    assert_eq!(error.code, 40415);
}

#[test]
fn kill_cursors_reports_killed_and_unknown_ids() {
    let mut cursors = CursorStore::new();
    let (id, first) = cursors.open("app.items", (0..4).map(BsonValue::Int32).collect(), Some(2));
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
    let (id, _) = cursors.open("app.items", (0..4).map(BsonValue::Int32).collect(), Some(2));
    assert_eq!(cursors.len(), 1);
    std::thread::sleep(Duration::from_millis(20));
    assert!(cursors.get_more(id, "app.items", None).is_err());
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
    assert_eq!(store.scan("app.items").expect("scan lands").len(), 1);
    assert_eq!(store.namespaces().expect("catalog is readable"), vec!["app.items".to_string()]);
}

#[test]
fn mongodump_stream_is_imported() {
    let store = store();
    let mut stream = Vec::new();
    stream.extend_from_slice(&encode_bson(&document(&[("_id", BsonValue::Int32(1)), ("name", text("one"))])));
    stream.extend_from_slice(&encode_bson(&document(&[("_id", BsonValue::Int32(2)), ("name", text("two"))])));
    let imported = mongo::import::import_bson_documents(&store, "app.items", &stream).expect("import");
    assert_eq!(imported, 2);
    assert_eq!(store.scan("app.items").expect("scan lands").len(), 2);
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

fn operator(name: &str, argument: BsonValue) -> BsonValue {
    BsonValue::Document(document(&[(name, argument)]))
}

/// 用一条 `$project` 把表达式结果投影出来，供算术算子断言使用。
fn expression_projection(store: &Store, projection: BsonDocument) -> BsonDocument {
    let aggregate = aggregate_pipeline("items", vec![stage("$project", BsonValue::Document(projection))]);
    batch(&commands::aggregate(&aggregate, store).expect("aggregate"), "firstBatch").into_iter().next().expect("one projected document")
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
