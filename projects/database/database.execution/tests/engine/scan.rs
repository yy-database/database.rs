//! Whole-table scans terminate and never expose the reserved namespace.

use std::{
    fs,
    path::{Path, PathBuf},
};

use database_execution::engine::{Connection, Result};

fn temp_yydx(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("database-execution-scan-{}-{}.yydx", std::process::id(), label))
}

fn cleanup(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}-wal", path.display()));
    let _ = fs::remove_file(format!("{}-shm", path.display()));
    let stem = path.file_stem().and_then(|name| name.to_str()).unwrap_or("app");
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let _ = fs::remove_dir_all(parent.join(format!("{stem}-objects")));
}

fn keys(page: &[(String, Vec<u8>)]) -> Vec<String> {
    page.iter().map(|(key, _)| key.clone()).collect()
}

#[test]
fn empty_database_scans_terminate() -> Result<()> {
    let path = temp_yydx("empty");
    cleanup(&path);
    let root = Connection::open(&path)?;
    let session = root.session();
    for limit in [3usize, 4, 1000] {
        assert!(session.scan_prefix("", None, limit)?.is_empty());
    }
    drop(session);
    drop(root);
    cleanup(&path);
    Ok(())
}

#[test]
fn whole_table_scan_terminates_when_visible_rows_are_below_limit() -> Result<()> {
    let path = temp_yydx("short");
    cleanup(&path);
    let root = Connection::open(&path)?;
    let session = root.session();
    session.put("alpha", b"1")?;
    session.put("omega", b"2")?;
    for limit in [3usize, 4, 1000] {
        let page = session.scan_prefix("", None, limit)?;
        assert_eq!(keys(&page), vec!["alpha".to_owned(), "omega".to_owned()]);
    }
    drop(session);
    drop(root);
    cleanup(&path);
    Ok(())
}

#[test]
fn whole_table_scan_hides_reserved_namespace() -> Result<()> {
    let path = temp_yydx("reserved");
    cleanup(&path);
    let root = Connection::open(&path)?;
    let session = root.session();
    session.put("visible", b"1")?;
    session.put("__yydb/lru/probe", b"2")?;
    let page = session.scan_prefix("", None, 1000)?;
    assert_eq!(keys(&page), vec!["visible".to_owned()]);
    drop(session);
    drop(root);
    cleanup(&path);
    Ok(())
}

#[test]
fn in_memory_whole_table_scan_hides_reserved_namespace() -> Result<()> {
    let root = Connection::open_in_memory()?;
    let session = root.session();
    session.put("visible", b"1")?;
    session.put("__yydb/meta/probe", b"2")?;
    assert_eq!(keys(&session.scan_prefix("", None, 1000)?), vec!["visible".to_owned()]);
    Ok(())
}
