//! MySQL 真机冒烟：只在 `MYSQL_TEST_URL` / `MYSQL_TEST_NATIVE_URL` 存在时运行。
//!
//! 期望值来自本机 MySQL 8.4.6 服务器本身（官方实现），不以本仓组件当期望值。

use std::time::Duration;

use database_client::mysql::{Cell, Param, QueryResult, Session, mysql_type};
use database_types::Error;

fn url(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn cells(result: &QueryResult) -> Vec<&[Cell]> {
    result.rows.iter().map(|row| row.as_slice()).collect()
}

#[test]
fn live_mysql_caching_sha2_query_roundtrip() {
    let Some(url) = url("MYSQL_TEST_URL")
    else {
        eprintln!("MYSQL_TEST_URL is not set, skipping the live MySQL smoke");
        return;
    };
    let mut session = Session::connect(&url, Duration::from_secs(5)).expect("connect with caching_sha2_password");
    session.ping().expect("ping");

    session.execute("DROP TABLE IF EXISTS smoke").expect("drop");
    session.execute("CREATE TABLE smoke (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, note VARCHAR(64) NULL)").expect("create");
    let inserted = session.execute("INSERT INTO smoke VALUES (1, '中文', 'first'), (2, 'second', NULL)").expect("insert");
    assert_eq!(inserted, 2);

    let result = session.query("SELECT id, name, note FROM smoke ORDER BY id").expect("select");
    assert_eq!(result.columns, vec!["id", "name", "note"]);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        cells(&result),
        vec![
            &[Cell::Bytes(b"1".to_vec()), Cell::Bytes("中文".as_bytes().to_vec()), Cell::Bytes(b"first".to_vec())][..],
            &[Cell::Bytes(b"2".to_vec()), Cell::Bytes(b"second".to_vec()), Cell::Null][..],
        ]
    );

    let updated = session.execute("UPDATE smoke SET note = 'changed' WHERE id = 2").expect("update");
    assert_eq!(updated, 1);
    let selected = session.query("SELECT note FROM smoke WHERE id = 2").expect("read back");
    assert_eq!(selected.rows, vec![vec![Cell::Bytes(b"changed".to_vec())]]);

    let missing = session.query("SELECT * FROM absent_table").expect_err("missing table");
    let Error::Protocol { message } = missing
    else {
        panic!("expected a protocol error")
    };
    assert!(message.contains("1146"), "unexpected error message: {message}");

    // 第二次连接命中服务器的认证缓存，走 fast-auth（0x03）分支。
    let mut again = Session::connect(&url, Duration::from_secs(5)).expect("reconnect after auth cache warm-up");
    again.ping().expect("ping again");

    // The adapter drift test treats extra tables as drift, so restore the
    // shared database after the smoke.
    session.execute("DROP TABLE IF EXISTS smoke").expect("cleanup");
}

#[test]
fn live_mysql_prepared_statement_roundtrip() {
    let Some(url) = url("MYSQL_TEST_URL")
    else {
        eprintln!("MYSQL_TEST_URL is not set, skipping the live prepared-statement smoke");
        return;
    };
    let mut session = Session::connect(&url, Duration::from_secs(5)).expect("connect");

    session.execute("DROP TABLE IF EXISTS stmt").expect("drop");
    session.execute("CREATE TABLE stmt (id INT PRIMARY KEY, name VARCHAR(64), active TINYINT(1), payload VARBINARY(16) NULL)").expect("create");

    let inserted = session
        .query_with_params(
            "INSERT INTO stmt (id, name, active, payload) VALUES (?, ?, ?, ?)",
            &[Param::text(Some("1")), Param::text(Some("中文")), Param::text(Some("1")), Param::binary(&[0xab; 16])],
        )
        .expect("insert with parameters");
    assert_eq!(inserted.affected_rows, 1);

    // A NULL parameter travels through the null bitmap, not through SQL text.
    let inserted_null = session
        .query_with_params(
            "INSERT INTO stmt (id, name, active, payload) VALUES (?, ?, ?, ?)",
            &[Param::text(Some("2")), Param::text(Some("null")), Param::text(Some("0")), Param::text(None)],
        )
        .expect("insert null parameter");
    assert_eq!(inserted_null.affected_rows, 1);

    let result = session
        .query_with_params("SELECT id, name, active, payload FROM stmt WHERE id = ?", &[Param::text(Some("1"))])
        .expect("select with parameters");
    assert_eq!(result.columns, vec!["id", "name", "active", "payload"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Cell::Bytes(b"1".to_vec()));
    assert_eq!(result.rows[0][1], Cell::Bytes("中文".as_bytes().to_vec()));
    assert_eq!(result.rows[0][2], Cell::Bytes(b"1".to_vec()));
    assert_eq!(result.rows[0][3], Cell::Bytes(vec![0xab; 16]));

    // Binary rows carry declared types, so `TINYINT(1)` and `VARBINARY` stay
    // distinguishable from `VARCHAR`.
    assert_eq!(result.column_info[0].type_code, mysql_type::LONG);
    assert_eq!(result.column_info[2].type_code, mysql_type::TINY);
    assert_eq!(result.column_info[2].column_length, 1);
    assert!(result.column_info[3].binary());
    assert!(!result.column_info[1].binary());

    let nulls = session.query_with_params("SELECT payload FROM stmt WHERE id = ?", &[Param::text(Some("2"))]).expect("select null payload");
    assert_eq!(nulls.rows, vec![vec![Cell::Null]]);

    // Leave the shared test database as it was found; the adapter drift test
    // treats extra tables as drift.
    session.execute("DROP TABLE IF EXISTS stmt").expect("cleanup");
}

#[test]
fn live_mysql_native_password_query_roundtrip() {
    let Some(url) = url("MYSQL_TEST_NATIVE_URL")
    else {
        eprintln!("MYSQL_TEST_NATIVE_URL is not set, skipping the native-password smoke");
        return;
    };
    let mut session = Session::connect(&url, Duration::from_secs(5)).expect("connect with mysql_native_password");
    let result = session.query("SELECT 'native', 1 + 1").expect("select");
    assert_eq!(result.columns, vec!["native", "1 + 1"]);
    assert_eq!(result.rows, vec![vec![Cell::Bytes(b"native".to_vec()), Cell::Bytes(b"2".to_vec())]]);
}
