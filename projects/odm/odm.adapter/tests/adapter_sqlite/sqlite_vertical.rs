//! SQLite adapter policy: fail-closed until product L3 continuation lands.
//!
//! L1 foreign adapters elsewhere may simulate services. This adapter must not
//! revive `rusqlite` / official SQLite to fake L3 continuation.

use odm_adapter::adapter_sqlite::{BACKEND_ID, SqliteProvider, SqliteSource};

#[test]
fn open_paths_fail_closed_pending_l3() {
    let memory = SqliteSource::open_in_memory().expect_err("in-memory open waits on L3");
    assert!(memory.to_string().contains("L3 continuation"), "{memory}");

    let path = std::env::temp_dir()
        .join(format!("iris-sqlite-l3-pending-{}.db", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let file = SqliteSource::open(&path).expect_err("file open waits on L3");
    assert!(file.to_string().contains("L3 continuation"), "{file}");
    assert!(!path.exists(), "fail-closed open must not create an engine file");
}

#[test]
fn provider_open_paths_fail_closed() {
    let memory = SqliteProvider::open_in_memory().expect_err("provider memory open waits on L3");
    assert!(memory.to_string().contains("no rusqlite"), "{memory}");
    let path = std::env::temp_dir().join("iris-sqlite-provider-l3-pending.db");
    let file = SqliteProvider::open(&path).expect_err("provider file open waits on L3");
    assert!(file.to_string().contains("L3 continuation"), "{file}");
}

#[test]
fn adapter_surface_does_not_depend_on_rusqlite() {
    assert_eq!(BACKEND_ID, "sqlite");
    let manifest = include_str!("../Cargo.toml");
    assert!(!manifest.contains("rusqlite"));
    assert!(!manifest.contains("yydb"));
    assert!(manifest.contains("sqlite ="));
}

#[test]
fn capabilities_still_advertise_sqlite_backend() {
    let caps = SqliteSource::capabilities();
    assert_eq!(caps.backend_id, BACKEND_ID);
}

/// Reverse assertion for the documented degradation: the declarative capability
/// set must never be read as evidence that an execution surface exists.
#[test]
fn advertised_capabilities_are_not_execution_evidence() {
    let caps = SqliteSource::capabilities();
    assert!(caps.query.filter_cmp && caps.query.project, "query metadata stays declarative");
    assert!(caps.write.insert && caps.write.update && caps.write.delete, "write metadata stays declarative");

    let memory = SqliteSource::open_in_memory().expect_err("no execution surface backs the declared capabilities");
    assert!(memory.to_string().contains("L3 continuation"), "{memory}");

    let path = std::env::temp_dir().join(format!(
        "iris-sqlite-capability-claims-{}.db",
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    ));
    let file = SqliteSource::open(&path).expect_err("no execution surface backs the declared capabilities");
    assert!(file.to_string().contains("L3 continuation"), "{file}");
    assert!(!path.exists(), "a declared capability must not create an engine file");
}
