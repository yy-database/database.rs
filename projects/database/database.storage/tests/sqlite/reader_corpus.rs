//! Reader corpus for the `sqlite-storage` snapshot and b-tree readers.
//!
//! Every fixture is built from the blank database image, so the coverage runs
//! by default without a `sqlite3` CLI, an upstream server, or Python. The
//! assertions target structured error codes and exact rowid / record values.

use std::{
    env, fs,
    path::PathBuf,
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use database_formats::sqlite::{RecordLimits, RecordValue, TextEncoding, blank_database, decode_record};

use database_storage::sqlite::{SchemaObjectKind, SqliteDatabase, TableLimits, read_existing, read_named_table, read_schema, read_table};

const PAGE: usize = 4096;

fn temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock before epoch").as_nanos();
    env::temp_dir().join(format!("sqlite-unit-{}-{nanos}-{name}", process::id()))
}

fn push_varint(out: &mut Vec<u8>, value: u64) {
    if value > 0x00ff_ffff_ffff_ffff {
        let mut bytes = [0u8; 9];
        for (index, byte) in bytes.iter_mut().take(8).enumerate() {
            *byte = (((value >> (57 - 7 * index)) & 0x7f) as u8) | 0x80;
        }
        bytes[8] = (value & 0xff) as u8;
        out.extend_from_slice(&bytes);
        return;
    }
    let mut bytes = [0u8; 8];
    let mut index = 7;
    bytes[index] = (value & 0x7f) as u8;
    let mut rest = value >> 7;
    while rest > 0 {
        index -= 1;
        bytes[index] = ((rest & 0x7f) as u8) | 0x80;
        rest >>= 7;
    }
    out.extend_from_slice(&bytes[index..]);
}

fn record(serials: &[u64], body: &[u8]) -> Vec<u8> {
    let mut header = Vec::new();
    for serial in serials {
        push_varint(&mut header, *serial);
    }
    let mut payload = Vec::new();
    push_varint(&mut payload, (header.len() + 1) as u64);
    payload.extend_from_slice(&header);
    payload.extend_from_slice(body);
    payload
}

fn cell(payload: &[u8], rowid: i64) -> Vec<u8> {
    let mut bytes = Vec::new();
    push_varint(&mut bytes, payload.len() as u64);
    push_varint(&mut bytes, rowid as u64);
    bytes.extend_from_slice(payload);
    bytes
}

fn write_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn write_leaf(page: &mut [u8], header: usize, cells: &[Vec<u8>]) {
    page[header..].fill(0);
    page[header] = 13;
    write_u16(page, header + 3, cells.len() as u16);
    let mut content = page.len();
    let mut pointer = header + 8;
    for cell in cells {
        content -= cell.len();
        page[content..content + cell.len()].copy_from_slice(cell);
        write_u16(page, pointer, content as u16);
        pointer += 2;
    }
    write_u16(page, header + 5, content as u16);
}

/// Builds a format-3 file whose page 1 is a table leaf root holding `cells`.
fn database(cells: &[Vec<u8>]) -> Vec<u8> {
    let mut pages = blank_database().expect("blank database");
    write_leaf(&mut pages[..PAGE], 100, cells);
    pages
}

fn set_encoding(pages: &mut [u8], encoding: u32) {
    write_u32(pages, 56, encoding);
}

fn schema_record(kind: &str, name: &str, table_name: &str, root_page: Option<u32>, sql: Option<&str>) -> Vec<u8> {
    let mut serials: Vec<u64> = Vec::new();
    let mut body = Vec::new();
    for text in [kind, name, table_name] {
        serials.push((13 + 2 * text.len()) as u64);
        body.extend_from_slice(text.as_bytes());
    }
    match root_page {
        Some(page) => {
            serials.push(1);
            body.push(page as u8);
        }
        None => serials.push(0),
    }
    match sql {
        Some(sql) => {
            serials.push((13 + 2 * sql.len()) as u64);
            body.extend_from_slice(sql.as_bytes());
        }
        None => serials.push(0),
    }
    record(&serials, &body)
}

fn schema_database(rows: &[Vec<u8>]) -> Vec<u8> {
    let cells: Vec<Vec<u8>> = rows.iter().enumerate().map(|(index, row)| cell(row, index as i64 + 1)).collect();
    let mut pages = database(&cells);
    set_encoding(&mut pages, 1);
    pages
}

#[test]
fn rowid_bounds_survive_the_scan() {
    let rowids = [i64::MIN, -1, 0, 1, i64::MAX];
    let cells: Vec<Vec<u8>> = rowids.iter().map(|rowid| cell(&record(&[1], &[0x2a]), *rowid)).collect();
    let pages = database(&cells);

    let rows = read_table(&pages, 1, TableLimits::default()).expect("scan rowid bounds");
    assert_eq!(rows.iter().map(|row| row.rowid).collect::<Vec<_>>(), rowids);
    for row in &rows {
        let values = decode_record(&row.payload, TextEncoding::Utf8, RecordLimits::default()).expect("decode row");
        assert_eq!(values, [RecordValue::Integer(42)]);
    }
}

#[test]
fn malformed_btree_pages_are_structured_errors() {
    let pages = database(&[cell(&record(&[1], &[0x2a]), 1)]);

    let out_of_bounds = read_table(&pages, 2, TableLimits::default()).expect_err("root beyond page count");
    assert_eq!(out_of_bounds.code(), "database.corrupt");

    let mut index_page = pages.clone();
    index_page[100] = 2;
    let index = read_table(&index_page, 1, TableLimits::default()).expect_err("index page");
    assert_eq!(index.code(), "database.unsupported");
    assert!(index.to_string().contains("index b-tree scan"), "{index}");

    let mut bad_kind = pages.clone();
    bad_kind[100] = 14;
    let kind = read_table(&bad_kind, 1, TableLimits::default()).expect_err("page kind");
    assert_eq!(kind.code(), "database.corrupt");
    assert!(kind.to_string().contains("page type invalid"), "{kind}");

    let mut fragmented = pages.clone();
    fragmented[107] = 61;
    assert!(read_table(&fragmented, 1, TableLimits::default()).expect_err("fragmented").to_string().contains("header invalid"));

    let mut bad_pointer = pages.clone();
    write_u16(&mut bad_pointer, 108, 2);
    assert!(read_table(&bad_pointer, 1, TableLimits::default()).expect_err("pointer").to_string().contains("cell pointer out of bounds"));

    let mut truncated_cell = pages.clone();
    write_u16(&mut truncated_cell, 108, 4090);
    truncated_cell[4090] = 0x64;
    truncated_cell[4091] = 0x01;
    let truncated = read_table(&truncated_cell, 1, TableLimits::default()).expect_err("truncated cell");
    assert_eq!(truncated.code(), "database.corrupt");

    let duplicate_rowid = database(&[cell(&record(&[1], &[0x2a]), 5), cell(&record(&[1], &[0x2a]), 5)]);
    let order = read_table(&duplicate_rowid, 1, TableLimits::default()).expect_err("duplicate rowid");
    assert_eq!(order.code(), "database.corrupt");
    assert!(order.to_string().contains("ordered range"), "{order}");
}

#[test]
fn scan_limits_are_enforced() {
    let pages = database(&[cell(&record(&[1], &[0x2a]), 1), cell(&record(&[1], &[0x2a]), 2)]);

    let rows = read_table(&pages, 1, TableLimits { max_rows: 1, ..TableLimits::default() }).expect_err("row limit");
    assert_eq!(rows.code(), "database.unsupported");
    assert!(rows.to_string().contains("row limit"), "{rows}");

    let payload = read_table(&pages, 1, TableLimits { max_payload_bytes: 1, ..TableLimits::default() }).expect_err("payload limit");
    assert_eq!(payload.code(), "database.unsupported");

    let total = read_table(&pages, 1, TableLimits { max_total_payload_bytes: 1, ..TableLimits::default() }).expect_err("total payload limit");
    assert_eq!(total.code(), "database.unsupported");

    let page_limit = read_table(&pages, 1, TableLimits { max_pages: 0, ..TableLimits::default() }).expect_err("page limit");
    assert_eq!(page_limit.code(), "database.unsupported");
}

#[test]
fn truncated_and_corrupt_files_are_structured_errors() {
    let mut pages = database(&[cell(&record(&[1], &[0x2a]), 1)]);
    pages.resize(2 * PAGE, 0);
    write_u32(&mut pages, 28, 2);
    write_leaf(&mut pages[PAGE..], 0, &[]);

    let truncated = temp_path("truncated.sqlite");
    fs::write(&truncated, &pages[..PAGE]).expect("write truncated fixture");
    let error = SqliteDatabase::open(&truncated).err().expect("declared pages beyond file size");
    assert_eq!(error.code(), "database.corrupt");
    assert!(error.to_string().contains("declared page count"), "{error}");
    assert_eq!(read_existing(&truncated).expect_err("truncated read").code(), "database.corrupt");
    fs::remove_file(&truncated).expect("remove truncated fixture");

    let mut misaligned = pages.clone();
    misaligned.truncate(PAGE + 100);
    let aligned = temp_path("misaligned.sqlite");
    fs::write(&aligned, &misaligned).expect("write misaligned fixture");
    assert!(SqliteDatabase::open(&aligned).err().expect("misaligned file").to_string().contains("page-aligned"));
    fs::remove_file(&aligned).expect("remove misaligned fixture");

    pages[0] = b'X';
    let corrupt = temp_path("magic.sqlite");
    fs::write(&corrupt, &pages).expect("write corrupt fixture");
    let magic = SqliteDatabase::open(&corrupt).err().expect("wrong magic");
    assert_eq!(magic.code(), "database.corrupt");
    assert!(magic.to_string().contains("magic mismatch"), "{magic}");
    fs::remove_file(&corrupt).expect("remove corrupt fixture");

    let missing = temp_path("missing.sqlite");
    assert_eq!(read_existing(&missing).expect_err("missing file").code(), "database.io");
}

#[test]
fn schema_corpus_returns_structured_errors() {
    let four_columns = record(&[15, 15, 15, 1], &[b't', b'a', b'b', 0x02]);
    let pages = schema_database(&[four_columns]);
    let width = read_schema(&pages, TableLimits::default()).expect_err("schema width");
    assert_eq!(width.code(), "database.corrupt");
    assert!(width.to_string().contains("column count"), "{width}");

    let cases: [(&str, Vec<u8>, &str); 5] = [
        ("kind", schema_record("bogus", "t", "t", Some(2), None), "kind invalid"),
        ("empty name", schema_record("table", "", "t", Some(2), None), "name empty"),
        ("root text", record(&[23, 15, 15, 15, 0], b"tablett2"), "root page"),
        ("sql integer", record(&[23, 15, 15, 1, 1], &[b't', b'a', b'b', b'l', b'e', b't', b't', 0x02, 0x05]), "SQL"),
        ("view root", schema_record("view", "v", "v", Some(2), None), "non-b-tree object"),
    ];
    for (label, row, needle) in cases {
        let error = read_schema(&schema_database(&[row]), TableLimits::default()).expect_err(label);
        assert_eq!(error.code(), "database.corrupt", "{label}");
        assert!(error.to_string().contains(needle), "{label}: {error}");
    }

    let ambiguous = schema_database(&[
        schema_record("table", "dup", "dup", Some(2), Some("CREATE TABLE dup(x)")),
        schema_record("table", "dup", "dup", Some(2), Some("CREATE TABLE dup(x)")),
    ]);
    let duplicate = read_named_table(&ambiguous, "dup", TableLimits::default()).expect_err("ambiguous table");
    assert_eq!(duplicate.code(), "database.corrupt");
    assert!(duplicate.to_string().contains("ambiguous"), "{duplicate}");

    let objects = read_schema(&ambiguous, TableLimits::default()).expect("schema rows");
    assert!(objects.iter().all(|object| object.kind == SchemaObjectKind::Table));

    let missing = read_named_table(&ambiguous, "absent", TableLimits::default()).expect_err("missing table");
    assert!(missing.to_string().contains("not found"), "{missing}");

    let view = schema_database(&[schema_record("view", "v", "v", None, Some("CREATE VIEW v AS SELECT 1"))]);
    let view_error = read_named_table(&view, "v", TableLimits::default()).expect_err("view");
    assert_eq!(view_error.code(), "database.unsupported");
    assert!(view_error.to_string().contains("FROM clause"), "{view_error}");

    let virtual_table = schema_database(&[schema_record("table", "virt", "virt", None, Some("CREATE VIRTUAL TABLE virt USING x"))]);
    let virtual_error = read_named_table(&virtual_table, "virt", TableLimits::default()).expect_err("virtual table");
    assert_eq!(virtual_error.code(), "database.unsupported");
    assert!(virtual_error.to_string().contains("virtual table"), "{virtual_error}");

    let aliasing = schema_database(&[schema_record("table", "s", "s", Some(1), Some("CREATE TABLE s(x)"))]);
    let alias = read_named_table(&aliasing, "s", TableLimits::default()).expect_err("schema root alias");
    assert_eq!(alias.code(), "database.corrupt");
    assert!(alias.to_string().contains("aliases schema root"), "{alias}");

    let unnamed = schema_database(&[schema_record("table", "t", "t", Some(2), Some("CREATE TABLE t(x)"))]);
    let schema_name = read_named_table(&unnamed, "t", TableLimits::default());
    assert_eq!(schema_name.expect_err("page 2 is absent").code(), "database.corrupt");
}

#[test]
fn blank_database_files_round_trip_and_reject_writes() {
    let path = temp_path("blank.sqlite");
    let created = SqliteDatabase::open(&path).expect("create blank file");
    assert_eq!(created.pages().len(), PAGE);
    assert_eq!(created.path(), Some(path.as_path()));
    assert_eq!(created.ping().expect("ping"), 1);
    assert_eq!(created.sqlite_version().expect("version"), database_formats::sqlite::ENGINE_LIBRARY_VERSION);
    assert!(created.table("sqlite_schema", TableLimits::default()).expect("blank schema").is_empty());
    assert_eq!(created.flush().expect_err("snapshot writes").code(), "database.unsupported");
    assert_eq!(fs::read(&path).expect("read created file"), blank_database().expect("blank bytes"));

    let reopened = SqliteDatabase::open(&path).expect("reopen blank file");
    assert_eq!(reopened.pages(), created.pages());
    fs::remove_file(&path).expect("remove blank fixture");
}
