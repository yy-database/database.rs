//! 点查与区间读取(`row_by_rowid` / `table_range` / `read_table_range`)对
//! 全表扫描的一致性,以及 `set_synchronous` 的落盘语义。
//!
//! 数据库用 `SqliteWriter` 构造并 `save` 成真实文件,再用只读快照打开,
//! 覆盖多页分裂、overflow 载荷与 schema 树。

use std::{
    env, fs,
    path::PathBuf,
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use database_formats::sqlite::WriteValue;
use database_storage::sqlite::{ColumnSpec, SqliteDatabase, SqliteWriter, TableLimits};

fn temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock before epoch").as_nanos();
    env::temp_dir().join(format!("sqlite-point-range-{}-{nanos}-{name}", process::id()))
}

fn column(name: &str, declared: &str) -> ColumnSpec {
    ColumnSpec {
        name: name.to_string(),
        declared_type: declared.to_string(),
        not_null: false,
        default_value: None,
        default_text: None,
        primary_key: false,
        unique: false,
        autoincrement: false,
        collate: None,
    }
}

fn text(value: &str) -> Option<WriteValue> {
    Some(WriteValue::Text(value.to_string()))
}

fn integer(value: i64) -> Option<WriteValue> {
    Some(WriteValue::Integer(value))
}

/// 512 字节页强制多页分裂;第 100 行带 4 KB 文本制造 overflow 链。
fn saved_database(name: &str, synchronous: bool) -> PathBuf {
    let path = temp_path(name);
    let mut writer = SqliteWriter::create(512).expect("create database");
    writer.create_table("bulk", &[column("id", "INTEGER"), column("label", "TEXT")]).expect("create table");
    for index in 1..=200i64 {
        let label = if index == 100 { format!("overflow-{}", "x".repeat(4096)) } else { format!("row-{index:03}") };
        writer.insert("bulk", &[integer(index), text(&label)]).unwrap_or_else(|error| panic!("insert {index}: {error}"));
    }
    writer.set_synchronous(synchronous);
    writer.save(&path).expect("save database");
    path
}

#[test]
fn row_by_rowid_matches_the_full_scan_and_misses_absent_rowids() {
    let path = saved_database("point", false);
    let database = SqliteDatabase::open(&path).expect("open snapshot");
    let rows = database.table("bulk", TableLimits::default()).expect("full scan");
    assert_eq!(rows.len(), 200);
    for row in &rows {
        let found = database.row_by_rowid("bulk", row.rowid).expect("point read");
        let found = found.unwrap_or_else(|| panic!("point miss rowid {}", row.rowid));
        assert_eq!(found.payload, row.payload, "point payload must equal scan payload for rowid {}", row.rowid);
    }
    // overflow 行必须被完整组装(4 KB 文本远超一页本地载荷)。
    let overflow = database.row_by_rowid("bulk", 100).expect("point overflow").expect("overflow row present");
    assert!(overflow.payload.len() > 4096, "overflow payload must be assembled");
    for absent in [0i64, 201, 1_000, -5] {
        assert!(database.row_by_rowid("bulk", absent).expect("absent read").is_none(), "rowid {absent} must miss");
    }
    assert!(database.row_by_rowid("absent_table", 1).is_err(), "unknown table must error");
    fs::remove_file(&path).expect("cleanup");
}

#[test]
fn table_range_prunes_to_the_requested_window_in_order() {
    let path = saved_database("range", false);
    let database = SqliteDatabase::open(&path).expect("open snapshot");
    let window = database.table_range("bulk", 50, 60, TableLimits::default()).expect("range read");
    let expected: Vec<i64> = (50..=60).collect();
    assert_eq!(window.iter().map(|row| row.rowid).collect::<Vec<_>>(), expected);
    // 跨分裂边界的宽区间与全表一致;两端开区间只缺首尾行。
    let all = database.table("bulk", TableLimits::default()).expect("full scan");
    let wide = database.table_range("bulk", 1, 200, TableLimits::default()).expect("wide range");
    assert_eq!(wide.len(), all.len());
    let trimmed = database.table_range("bulk", 2, 199, TableLimits::default()).expect("trimmed range");
    assert_eq!(trimmed.len(), 198);
    assert_eq!(trimmed[0].rowid, 2);
    assert_eq!(trimmed.last().expect("last").rowid, 199);
    // lower > upper 是空区间,不是错误。
    assert!(database.table_range("bulk", 9, 8, TableLimits::default()).expect("empty window").is_empty());
    fs::remove_file(&path).expect("cleanup");
}

#[test]
fn range_reads_the_schema_tree_by_rowid() {
    let path = saved_database("schema-range", false);
    let database = SqliteDatabase::open(&path).expect("open snapshot");
    let schema = database.table("sqlite_schema", TableLimits::default()).expect("schema scan");
    let first = schema.first().expect("schema has rows");
    let point = database.row_by_rowid("sqlite_master", first.rowid).expect("schema point").expect("schema row found");
    assert_eq!(point.payload, first.payload);
    let window = database.table_range("sqlite_schema", first.rowid, first.rowid, TableLimits::default()).expect("schema window");
    assert_eq!(window.len(), 1);
    fs::remove_file(&path).expect("cleanup");
}

#[test]
fn synchronous_save_writes_the_same_image() {
    let plain = saved_database("sync-plain", false);
    let synced = saved_database("sync-on", true);
    let plain_bytes = fs::read(&plain).expect("read plain");
    let synced_bytes = fs::read(&synced).expect("read synced");
    assert_eq!(plain_bytes, synced_bytes, "synchronous must not change the image");
    let database = SqliteDatabase::open(&synced).expect("open synced snapshot");
    assert_eq!(database.table("bulk", TableLimits::default()).expect("read synced table").len(), 200);
    fs::remove_file(&plain).expect("cleanup plain");
    fs::remove_file(&synced).expect("cleanup synced");
}
