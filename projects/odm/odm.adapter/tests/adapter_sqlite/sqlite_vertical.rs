//! SQLite adapter runtime — in-memory execution through the `sqlite` facade.
//!
//! These tests pin two things at once: the adapter executes real statements via
//! the product facade (no `rusqlite`), and the capabilities the facade still
//! lacks stay fail-closed with the gap named in the error text.

use odm_adapter::adapter_sqlite::{BACKEND_ID, SqliteProvider, SqliteSource, SqliteValue};

fn integer(value: i64) -> SqliteValue {
    SqliteValue::Integer(value)
}

fn text(value: &str) -> SqliteValue {
    SqliteValue::Text(value.as_bytes().to_vec())
}

#[test]
fn in_memory_open_backs_onto_the_facade() {
    let source = SqliteSource::open_in_memory().expect("in-memory runtime");
    assert_eq!(source.path(), None);
    assert!(!source.sqlite_version().unwrap().is_empty());
    assert!(source.sqlite_source_id().unwrap().starts_with("sqlite-product:"));
}

#[test]
fn crud_round_trip_executes_through_the_facade() {
    let provider = SqliteProvider::open_in_memory().unwrap();
    provider
        .execute_batch(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER,
                blob BLOB
            );",
        )
        .unwrap();

    // 门面的 `INSERT ... VALUES` 只接受字面量，占位参数尚未支持（见下方缺口用例）。
    let inserted = provider.execute_one("INSERT INTO users(id, name, age, blob) VALUES (1, 'ada', 36, x'010203')", &[]).unwrap();
    assert_eq!(inserted.changes, 1);

    let found = provider.execute_one("SELECT id, name, age, blob FROM users WHERE id = ?1", &[integer(1)]).unwrap();
    assert_eq!(found.columns, ["id", "name", "age", "blob"]);
    assert_eq!(found.rows, [vec![integer(1), text("ada"), integer(36), SqliteValue::Blob(vec![1, 2, 3])]]);

    let updated = provider.execute_one("UPDATE users SET age = ?1 WHERE id = ?2", &[integer(37), integer(1)]).unwrap();
    assert_eq!(updated.changes, 1);

    let deleted = provider.execute_one("DELETE FROM users WHERE id = ?1", &[integer(1)]).unwrap();
    assert_eq!(deleted.changes, 1);

    let empty = provider.execute_one("SELECT id FROM users WHERE id = ?1", &[integer(1)]).unwrap();
    assert!(empty.rows.is_empty());
}

#[test]
fn returning_and_scalar_functions_flow_through_the_facade() {
    let provider = SqliteProvider::open_in_memory().unwrap();
    provider.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);").unwrap();

    let returned = provider.execute_one("INSERT INTO users(name) VALUES ('grace') RETURNING id, name", &[]).unwrap();
    assert_eq!(returned.columns, ["id", "name"]);
    assert_eq!(returned.rows, [vec![integer(1), text("grace")]]);

    let matched = provider.execute_one("SELECT length(name), substr(name, ?1, ?2) FROM users", &[integer(2), integer(3)]).unwrap();
    assert_eq!(matched.rows, [vec![integer(5), text("rac")]]);
}

#[test]
fn instr_returns_the_match_offset() {
    let provider = SqliteProvider::open_in_memory().unwrap();
    provider.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);").unwrap();
    provider.execute_one("INSERT INTO users(id, name) VALUES (1, 'grace')", &[]).unwrap();

    let matched = provider.execute_one("SELECT instr(name, ?1) FROM users", &[text("a")]).unwrap();
    assert_eq!(matched.rows, [vec![integer(3)]]);
}

#[test]
fn unique_violation_surfaces_the_facade_message() {
    let provider = SqliteProvider::open_in_memory().unwrap();
    provider.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);").unwrap();
    provider.execute_one("INSERT INTO users(id, name) VALUES (1, 'ada')", &[]).unwrap();

    let failure = provider.execute_one("INSERT INTO users(id, name) VALUES (1, 'dup')", &[]).expect_err("duplicate primary key must fail");
    assert!(failure.to_string().contains("UNIQUE constraint failed"), "{failure}");
}

#[test]
fn parameterized_insert_binds_values() {
    let provider = SqliteProvider::open_in_memory().unwrap();
    provider.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);").unwrap();

    provider.execute_one("INSERT INTO users(id, name) VALUES (?1, ?2)", &[integer(7), text("grace")]).unwrap();
    let inserted = provider.execute_one("SELECT id, name FROM users", &[]).unwrap();
    assert_eq!(inserted.rows, [vec![integer(7), text("grace")]]);
}

#[test]
fn catalog_reports_columns_types_nullability_and_primary_key() {
    let provider = SqliteProvider::open_in_memory().unwrap();
    provider
        .execute_batch(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER
            );",
        )
        .unwrap();

    let catalog = provider.inspect_catalog().unwrap();
    let table = catalog.table("users").expect("users table");
    assert_eq!(table.columns.len(), 3);
    assert_eq!(table.columns[0].name, "id");
    assert_eq!(table.columns[0].type_name, "INTEGER");
    assert!(table.columns[0].primary_key);
    assert_eq!(table.columns[1].name, "name");
    assert!(!table.columns[1].nullable);
    assert_eq!(table.columns[2].type_name, "INTEGER");
}

#[test]
fn file_backed_open_persists_across_reopen() {
    let path = std::env::temp_dir()
        .join(format!("iris-sqlite-file-open-{}.db", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    {
        let provider = SqliteProvider::open(&path).unwrap();
        provider.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);").unwrap();
        provider.execute_one("INSERT INTO users(id, name) VALUES (1, 'ada')", &[]).unwrap();
    }
    assert!(path.exists(), "the first mutation must write the image back");
    let reopened = SqliteProvider::open(&path).unwrap();
    let found = reopened.execute_one("SELECT id, name FROM users WHERE id = ?1", &[integer(1)]).unwrap();
    assert_eq!(found.rows, [vec![integer(1), text("ada")]]);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn transactions_commit_and_roll_back_through_the_facade() {
    let provider = SqliteProvider::open_in_memory().unwrap();
    provider.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);").unwrap();

    provider.begin_immediate().unwrap();
    provider.execute_one("INSERT INTO users(id, name) VALUES (1, 'ada')", &[]).unwrap();
    provider.rollback().unwrap();
    let after_rollback = provider.execute_one("SELECT id FROM users", &[]).unwrap();
    assert!(after_rollback.rows.is_empty(), "rollback must discard the insert");

    provider.begin_immediate().unwrap();
    provider.execute_one("INSERT INTO users(id, name) VALUES (2, 'grace')", &[]).unwrap();
    provider.commit().unwrap();
    let after_commit = provider.execute_one("SELECT id FROM users", &[]).unwrap();
    assert_eq!(after_commit.rows, [vec![integer(2)]]);
}

#[test]
fn distinct_collapses_duplicate_rows_through_the_facade() {
    let provider = SqliteProvider::open_in_memory().unwrap();
    provider.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);").unwrap();
    provider.execute_one("INSERT INTO users(id, name, age) VALUES (1, 'ada', 36)", &[]).unwrap();
    provider.execute_one("INSERT INTO users(id, name, age) VALUES (2, 'ada', 36)", &[]).unwrap();
    provider.execute_one("INSERT INTO users(id, name, age) VALUES (3, 'grace', 41)", &[]).unwrap();

    let distinct = provider.execute_one("SELECT DISTINCT name, age FROM users", &[]).unwrap();
    assert_eq!(distinct.columns, ["name", "age"]);
    assert_eq!(distinct.rows.len(), 2, "duplicate projections must collapse");
}

#[test]
fn insert_reports_last_insert_rowid() {
    let provider = SqliteProvider::open_in_memory().unwrap();
    provider.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);").unwrap();
    let inserted = provider.execute_one("INSERT INTO users(id, name) VALUES (7, 'ada')", &[]).unwrap();
    assert_eq!(inserted.last_insert_rowid, 7);
}

#[test]
fn execute_one_rejects_scripts() {
    let provider = SqliteProvider::open_in_memory().unwrap();
    let error = provider.execute_one("SELECT 1; SELECT 2;", &[]).expect_err("execute_one takes one statement");
    assert!(error.to_string().contains("exactly one statement"), "{error}");
}

#[test]
fn adapter_surface_does_not_depend_on_rusqlite() {
    assert_eq!(BACKEND_ID, "sqlite");
    let manifest = include_str!("../../Cargo.toml");
    assert!(!manifest.contains("rusqlite"));
    assert!(!manifest.contains("yydb"));
    assert!(manifest.contains("database-formats"));
    assert!(manifest.contains("sqlite"));
}

#[test]
fn capabilities_still_advertise_sqlite_backend() {
    let caps = SqliteSource::capabilities();
    assert_eq!(caps.backend_id, BACKEND_ID);

    // Declarative capability flags are not evidence of execution: the runtime test above is.
    assert!(caps.query.filter_cmp && caps.query.project);
    assert!(caps.write.insert && caps.write.update && caps.write.delete);
}
