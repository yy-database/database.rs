//! MySQL 真机冒烟：只在 `MYSQL_TEST_URL` / `MYSQL_TEST_NATIVE_URL` 存在时运行。
//!
//! 期望值来自本机 MySQL 8.4.6 服务器本身（官方实现），不以本仓组件当期望值。

use std::time::Duration;

use database_client::mysql::{Cell, QueryResult, Session};
use database_types::Error;

fn url(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn cells(result: &QueryResult) -> Vec<&[Cell]> {
    result.rows.iter().map(|row| row.as_slice()).collect()
}

#[test]
fn live_mysql_caching_sha2_query_roundtrip() {
    let Some(url) = url("MYSQL_TEST_URL") else {
        eprintln!("MYSQL_TEST_URL is not set, skipping the live MySQL smoke");
        return;
    };
    let mut session = Session::connect(&url, Duration::from_secs(5)).expect("connect with caching_sha2_password");
    session.ping().expect("ping");

    session.execute("DROP TABLE IF EXISTS smoke").expect("drop");
    session
        .execute("CREATE TABLE smoke (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, note VARCHAR(64) NULL)")
        .expect("create");
    let inserted = session
        .execute("INSERT INTO smoke VALUES (1, '中文', 'first'), (2, 'second', NULL)")
        .expect("insert");
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
    let Error::Protocol { message } = missing else {
        panic!("expected a protocol error")
    };
    assert!(message.contains("1146"), "unexpected error message: {message}");

    // 第二次连接命中服务器的认证缓存，走 fast-auth（0x03）分支。
    let mut again = Session::connect(&url, Duration::from_secs(5)).expect("reconnect after auth cache warm-up");
    again.ping().expect("ping again");
}

#[test]
fn live_mysql_native_password_query_roundtrip() {
    let Some(url) = url("MYSQL_TEST_NATIVE_URL") else {
        eprintln!("MYSQL_TEST_NATIVE_URL is not set, skipping the native-password smoke");
        return;
    };
    let mut session = Session::connect(&url, Duration::from_secs(5)).expect("connect with mysql_native_password");
    let result = session.query("SELECT 'native', 1 + 1").expect("select");
    assert_eq!(result.columns, vec!["native", "1 + 1"]);
    assert_eq!(result.rows, vec![vec![Cell::Bytes(b"native".to_vec()), Cell::Bytes(b"2".to_vec())]]);
}
