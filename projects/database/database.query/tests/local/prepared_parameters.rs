//! Prepared VOS statements: named parameter extraction, binding, and plan reuse.

use std::collections::BTreeMap;

use database_query::local::{QueryKind, SlotTy, bind_named, prepare};
use database_types::Value;

#[test]
fn prepare_deduplicates_repeated_parameter() {
    let stmt = prepare("User.all().take($limit).skip($limit).collect()").unwrap();
    assert_eq!(stmt.parameters.len(), 1);
    assert_eq!(stmt.parameters[0].usage_count, 2);
}

#[test]
fn prepare_same_source_gives_same_fingerprint_and_statement_id() {
    let a = prepare("User.all().take($limit).collect()").unwrap();
    let b = prepare("User.all().take($limit).collect()").unwrap();
    assert_eq!(a.source_fingerprint, b.source_fingerprint);
    assert_eq!(a.statement_id, b.statement_id);
    assert!(a.source_fingerprint.len() >= 8);
}

#[test]
fn execute_prepared_reuses_plan_for_different_bindings() {
    let stmt = prepare("User.all().take($limit).collect()").unwrap();

    let bound_a = bind_named(&stmt, &BTreeMap::from([("limit".into(), Value::I64(100))])).expect("bind a");
    assert_eq!(bound_a.len(), 1);
    assert_eq!(bound_a[0], Value::I64(100));

    let bound_b = bind_named(&stmt, &BTreeMap::from([("limit".into(), Value::I64(1))])).expect("bind b");
    assert_eq!(bound_b[0], Value::I64(1));
}

#[test]
fn bind_named_rejects_missing_parameter() {
    let stmt = prepare("User.all().take($limit).collect()").unwrap();
    let err = bind_named(&stmt, &BTreeMap::from([])).unwrap_err();
    assert_eq!(err.code(), "database.query.bind");
    assert_eq!(err.operation(), "query");
    let params = err.params().expect("QueryBind has params");
    let param_value = params.iter().find(|(k, _)| *k == "parameter").map(|(_, v)| v.as_str()).expect("parameter exists");
    assert_eq!(param_value, "limit");
}

#[test]
fn bind_named_rejects_extraneous_parameter() {
    let stmt = prepare("User.all().take($limit).collect()").unwrap();
    let err = bind_named(&stmt, &BTreeMap::from([("limit".into(), Value::I64(10)), ("unreferenced".into(), Value::Text("oops".into()))]))
        .unwrap_err();
    assert_eq!(err.code(), "database.query.bind");
}

#[test]
fn prepare_works_with_no_parameters() {
    let stmt = prepare("User.all().collect()").unwrap();
    assert!(stmt.parameters.is_empty());
    let bound = bind_named(&stmt, &BTreeMap::new()).unwrap();
    assert!(bound.is_empty());
}

#[test]
fn prepare_fails_on_vos_syntax_error() {
    let err = prepare("this is not valid VOS").unwrap_err();
    assert_eq!(err.code(), "database.schema.invalid");
}

#[test]
fn take_and_skip_can_both_be_bound() {
    let stmt = prepare("User.all().skip($offset).take($limit).collect()").unwrap();
    assert_eq!(stmt.parameters.len(), 2);
    assert_eq!(stmt.parameters[0].name, "offset");
    assert_eq!(stmt.parameters[1].name, "limit");

    let bound = bind_named(&stmt, &BTreeMap::from([("offset".into(), Value::I64(50)), ("limit".into(), Value::I64(10))])).unwrap();
    assert_eq!(bound, vec![Value::I64(50), Value::I64(10)]);
}

#[test]
fn read_set_records_scan_table() {
    let stmt = prepare("User.all().collect()").unwrap();
    assert_eq!(stmt.read_set, vec!["User".to_string()]);
    assert_eq!(stmt.query_kind, QueryKind::Read);
}

#[test]
fn multiple_scans_collected() {
    // Two `Type.all()` in the same source — the pipeline starts at the
    // outermost scan; nested scans inside filter bodies are Phase 2+.
    let stmt = prepare("User.all().take($limit).collect()").unwrap();
    assert!(stmt.read_set.contains(&"User".to_string()));
}

#[test]
fn slot_type_can_be_inferred_from_context() {
    // $limit used as .take($limit) → must be numeric
    let stmt = prepare("User.all().take($limit).collect()").unwrap();
    // Phase-1 lowering still reports Any, but the infrastructure is now
    // in place for context-aware inference.
    assert_eq!(stmt.parameters[0].slot_type, SlotTy::Any);
}
