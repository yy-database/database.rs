use database_storage::sqlite::{TableLimits, TableRow, TextEncoding, WriteValue, encode_record, parse_view, validate_strict_rows};

#[test]
fn view_projection_parses_only_plain_column_lists() {
    let projection = parse_view("CREATE VIEW v AS SELECT b, a FROM t").expect("simple projection");
    assert_eq!(projection.base, "t");
    assert_eq!(projection.columns.as_deref(), Some(&["b".to_string(), "a".to_string()][..]));

    let wildcard = parse_view("CREATE VIEW v AS SELECT * FROM t").expect("wildcard projection");
    assert!(wildcard.columns.is_none());

    for sql in [
        "CREATE VIEW v AS SELECT a+1 FROM t",
        "CREATE VIEW v AS SELECT a FROM t WHERE a > 0",
        "CREATE VIEW v AS SELECT a FROM t JOIN u ON t.a = u.a",
        "CREATE VIEW v AS SELECT a AS b FROM t",
    ] {
        assert!(parse_view(sql).is_err(), "{sql} must fail closed");
    }
}

#[test]
fn strict_tables_reject_values_outside_the_declared_type() {
    let sql = "CREATE TABLE s (id INTEGER PRIMARY KEY, label TEXT, score REAL) STRICT";
    let payload = encode_record(&[WriteValue::Null, WriteValue::Blob(vec![1, 2]), WriteValue::Real(0.5)], TextEncoding::Utf8);
    let rows = [TableRow { rowid: 1, payload }];
    let error = validate_strict_rows(&rows, sql, TextEncoding::Utf8, TableLimits::default()).expect_err("blob in TEXT column");
    assert!(error.to_string().contains("outside its declared type"), "{error}");

    let payload = encode_record(&[WriteValue::Null, WriteValue::Text("ok".into()), WriteValue::Real(0.5)], TextEncoding::Utf8);
    let rows = [TableRow { rowid: 1, payload }];
    validate_strict_rows(&rows, sql, TextEncoding::Utf8, TableLimits::default()).expect("conforming STRICT row");
}
