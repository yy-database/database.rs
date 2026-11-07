//! Smallest real round trip against a live PostgreSQL server.
//!
//! `POSTGRES_TEST_HOST` / `_PORT` / `_USER` / `_DATABASE` / `_PASSWORD` point at
//! the server. Following the other live tests in this crate, a missing
//! `POSTGRES_TEST_PORT` skips instead of failing: the workspace test run has no
//! database service, while the dedicated CI job sets every variable.

use database_client::pgsql::client::{ClientConfig, Connection};

fn config() -> Option<ClientConfig> {
    let Ok(port) = std::env::var("POSTGRES_TEST_PORT")
    else {
        eprintln!("POSTGRES_TEST_PORT is not set, skipping the live PostgreSQL round trip");
        return None;
    };
    let port = port.parse().expect("POSTGRES_TEST_PORT must be a number");
    let config = ClientConfig::new(
        std::env::var("POSTGRES_TEST_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        port,
        std::env::var("POSTGRES_TEST_USER").unwrap_or_else(|_| "postgres".into()),
        std::env::var("POSTGRES_TEST_DATABASE").unwrap_or_else(|_| "postgres".into()),
    );
    Some(match std::env::var("POSTGRES_TEST_PASSWORD") {
        Ok(password) => config.with_password(password),
        Err(_) => config,
    })
}

#[test]
fn selects_one_row_from_a_live_server() {
    let Some(config) = config()
    else {
        return;
    };
    let mut connection = Connection::open(&config).expect("a live server must accept the startup packet");
    let results = connection.execute("SELECT 1 AS one").expect("SELECT 1 must succeed");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].tag, "SELECT 1");
    assert_eq!(results[0].column_names().collect::<Vec<_>>(), vec!["one"]);
    assert_eq!(results[0].cell(0, 0), Some(Some("1")));
    connection.close().expect("closing must succeed");
}

#[test]
fn reports_server_error_code_and_message() {
    let Some(config) = config()
    else {
        return;
    };
    let mut connection = Connection::open(&config).expect("a live server must accept the startup packet");
    let error = connection.execute("SELECT * FROM missing_table").expect_err("a missing table must fail");
    assert_eq!(error.code(), Some("42P01"));
    assert!(error.message().contains("missing_table"));
}
