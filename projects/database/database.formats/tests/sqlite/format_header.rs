//! 从 `src/format.rs` 迁出的测试。

use database_formats::sqlite::{
    DEFAULT_PAGE_SIZE, ENGINE_LIBRARY_VERSION, ENGINE_LIBRARY_VERSION_NUMBER, MAGIC, blank_database, decode_library_version,
    format_library_version, stamp_library_version, validate_database, validate_header,
};

#[test]
fn blank_database_is_one_validated_page() {
    let pages = blank_database().expect("blank database");
    assert_eq!(pages.len(), DEFAULT_PAGE_SIZE);
    assert_eq!(validate_database(&pages).expect("page size"), DEFAULT_PAGE_SIZE);
    assert_eq!(decode_library_version(&pages).expect("library version"), ENGINE_LIBRARY_VERSION);
    assert_eq!(&pages[..MAGIC.len()], MAGIC);
    assert_eq!(u16::from_be_bytes([pages[16], pages[17]]), DEFAULT_PAGE_SIZE as u16);
    assert_eq!(u32::from_be_bytes(pages[28..32].try_into().unwrap()), 1);
    assert_eq!(pages[100], 13, "page 1 must be a table leaf");
    assert_eq!(u16::from_be_bytes([pages[105], pages[106]]), DEFAULT_PAGE_SIZE as u16);
}

#[test]
fn truncated_headers_are_rejected() {
    let short = decode_library_version(&blank_database().expect("blank")[..99]).expect_err("short header");
    assert_eq!(short.code(), "database.corrupt");
    assert!(short.to_string().contains("header too short"), "{short}");
    assert_eq!(validate_header(&[]).expect_err("empty header").code(), "database.corrupt");
}

#[test]
fn header_field_corpus_returns_structured_errors() {
    let mut wrong_magic = blank_database().expect("blank");
    wrong_magic[0] = b'X';
    let magic = validate_header(&wrong_magic).expect_err("magic");
    assert_eq!(magic.code(), "database.corrupt");
    assert!(magic.to_string().contains("magic mismatch"), "{magic}");

    let mut bad_page_size = blank_database().expect("blank");
    bad_page_size[16..18].copy_from_slice(&1000u16.to_be_bytes());
    let size = validate_header(&bad_page_size).expect_err("page size");
    assert_eq!(size.code(), "database.corrupt");
    assert!(size.to_string().contains("page size invalid"), "{size}");

    let mut bad_read_version = blank_database().expect("blank");
    bad_read_version[18] = 3;
    let version = validate_header(&bad_read_version).expect_err("read version");
    assert_eq!(version.code(), "database.unsupported");
    assert!(version.to_string().contains("format version"), "{version}");

    let mut bad_write_version = blank_database().expect("blank");
    bad_write_version[19] = 0;
    assert_eq!(validate_header(&bad_write_version).expect_err("write version").code(), "database.unsupported");

    let mut tiny_usable = blank_database().expect("blank");
    tiny_usable[16..18].copy_from_slice(&512u16.to_be_bytes());
    tiny_usable[20] = 33;
    let usable = validate_header(&tiny_usable).expect_err("usable size");
    assert_eq!(usable.code(), "database.corrupt");
    assert!(usable.to_string().contains("usable page size"), "{usable}");

    let mut bad_fractions = blank_database().expect("blank");
    bad_fractions[21] = 63;
    let fractions = validate_header(&bad_fractions).expect_err("payload fractions");
    assert_eq!(fractions.code(), "database.corrupt");
    assert!(fractions.to_string().contains("payload fractions"), "{fractions}");

    let mut bad_schema_version = blank_database().expect("blank");
    bad_schema_version[44..48].copy_from_slice(&5u32.to_be_bytes());
    let schema_version = validate_header(&bad_schema_version).expect_err("schema version");
    assert_eq!(schema_version.code(), "database.unsupported");

    let mut bad_encoding = blank_database().expect("blank");
    bad_encoding[56..60].copy_from_slice(&4u32.to_be_bytes());
    let encoding = validate_header(&bad_encoding).expect_err("encoding");
    assert_eq!(encoding.code(), "database.corrupt");
    assert!(encoding.to_string().contains("text encoding"), "{encoding}");

    let mut reserved = blank_database().expect("blank");
    reserved[72] = 1;
    let reserved_error = validate_header(&reserved).expect_err("reserved bytes");
    assert_eq!(reserved_error.code(), "database.corrupt");
    assert!(reserved_error.to_string().contains("reserved header"), "{reserved_error}");
}

#[test]
fn database_shape_corpus_returns_structured_errors() {
    let misaligned = validate_database(&blank_database().expect("blank")[..DEFAULT_PAGE_SIZE - 1]).expect_err("misaligned");
    assert_eq!(misaligned.code(), "database.corrupt");
    assert!(misaligned.to_string().contains("page-aligned"), "{misaligned}");

    let mut declared = blank_database().expect("blank");
    declared[28..32].copy_from_slice(&2u32.to_be_bytes());
    let declared_error = validate_database(&declared).expect_err("declared pages");
    assert_eq!(declared_error.code(), "database.corrupt");
    assert!(declared_error.to_string().contains("declared page count"), "{declared_error}");

    assert_eq!(validate_database(&[]).expect("empty database is valid"), DEFAULT_PAGE_SIZE);
}

#[test]
fn library_version_round_trips() {
    assert_eq!(format_library_version(ENGINE_LIBRARY_VERSION_NUMBER), ENGINE_LIBRARY_VERSION);
    let mut pages = blank_database().expect("blank");
    stamp_library_version(&mut pages, 3_042_001);
    assert_eq!(decode_library_version(&pages).expect("decode stamped version"), "3.42.1");

    let mut too_short = [0u8; 99];
    stamp_library_version(&mut too_short, ENGINE_LIBRARY_VERSION_NUMBER);
    assert_eq!(too_short[96..], [0u8; 3], "short buffers must not be written");
}
