//! SQLite format 3 header and blank-database materialization (pure Rust).

use database_types::{Error, Result};

/// SQLite format 3 magic prefix.
pub const MAGIC: &[u8] = b"SQLite format 3\0";

/// 新建 format-3 空白库的默认页大小。
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// 写入空白库头的库版本戳。
pub const ENGINE_LIBRARY_VERSION: &str = "3.45.0";

/// Packed SQLite library version integer written to the database header.
pub const ENGINE_LIBRARY_VERSION_NUMBER: u32 = 3_045_000;

/// Canonical blank database bytes (one 4096-byte page, validated by tests).
const BLANK_DATABASE: &[u8] = include_bytes!("assets/blank.sqlite");

/// 返回已校验、并盖上引擎版本戳的空白库字节。
pub fn blank_database() -> Result<Vec<u8>> {
    if BLANK_DATABASE.len() != DEFAULT_PAGE_SIZE {
        return Err(Error::Corrupt("blank sqlite asset has unexpected size"));
    }
    validate_header(BLANK_DATABASE)?;

    let mut pages = BLANK_DATABASE.to_vec();
    stamp_library_version(&mut pages, ENGINE_LIBRARY_VERSION_NUMBER);
    // The bundled asset predates first use, so its text-encoding field is
    // still zero; a real SQLite database stamps UTF-8 at creation.
    if pages[56..60] == [0, 0, 0, 0] {
        pages[56..60].copy_from_slice(&1u32.to_be_bytes());
    }
    Ok(pages)
}

/// Validates the SQLite file header in `bytes`.
pub fn validate_header(bytes: &[u8]) -> Result<()> {
    if bytes.len() < 100 {
        return Err(Error::Corrupt("sqlite header too short"));
    }
    if bytes.get(..MAGIC.len()) != Some(MAGIC) {
        return Err(Error::Corrupt("sqlite magic mismatch"));
    }

    let page_size = decode_page_size(bytes)?;
    if page_size < 512 || !page_size.is_power_of_two() {
        return Err(Error::Corrupt("sqlite page size invalid"));
    }

    if !matches!(bytes[18], 1 | 2) || !matches!(bytes[19], 1 | 2) {
        return Err(Error::Unsupported("sqlite file read/write format version"));
    }
    if page_size - usize::from(bytes[20]) < 480 {
        return Err(Error::Corrupt("sqlite usable page size too small"));
    }
    if bytes[21..24] != [64, 32, 32] {
        return Err(Error::Corrupt("sqlite payload fractions invalid"));
    }
    if read_u32(bytes, 44) > 4 {
        return Err(Error::Unsupported("sqlite schema format version"));
    }
    if read_u32(bytes, 56) > 3 {
        return Err(Error::Corrupt("sqlite text encoding invalid"));
    }
    if bytes[72..92].iter().any(|byte| *byte != 0) {
        return Err(Error::Corrupt("sqlite reserved header bytes are nonzero"));
    }

    Ok(())
}

/// Validates an on-disk SQLite payload and returns its page size.
pub fn validate_database(bytes: &[u8]) -> Result<usize> {
    if bytes.is_empty() {
        return Ok(DEFAULT_PAGE_SIZE);
    }
    validate_header(bytes)?;
    let page_size = decode_page_size(bytes)?;
    if !bytes.len().is_multiple_of(page_size) {
        return Err(Error::Corrupt("sqlite payload is not page-aligned"));
    }
    let declared_pages = read_u32(bytes, 28);
    if declared_pages != 0 && read_u32(bytes, 24) == read_u32(bytes, 92) && u64::from(declared_pages) > (bytes.len() / page_size) as u64 {
        return Err(Error::Corrupt("sqlite declared page count exceeds file size"));
    }
    Ok(page_size)
}

/// Decodes the SQLite library version string from a database header.
pub fn decode_library_version(bytes: &[u8]) -> Result<String> {
    validate_header(bytes)?;
    Ok(format_library_version(read_library_version_number(bytes)))
}

/// Writes the SQLite library version integer into `bytes` (header must exist).
pub fn stamp_library_version(bytes: &mut [u8], version_number: u32) {
    if bytes.len() >= 100 {
        bytes[96..100].copy_from_slice(&version_number.to_be_bytes());
    }
}

/// Reads the file change counter (header offset 24).
pub fn read_change_counter(bytes: &[u8]) -> u32 {
    read_u32(bytes, 24)
}

/// Reads the database size in pages (header offset 28).
pub fn read_page_count(bytes: &[u8]) -> u32 {
    read_u32(bytes, 28)
}

/// Reads the first freelist trunk page (header offset 32).
pub fn read_freelist_trunk(bytes: &[u8]) -> u32 {
    read_u32(bytes, 32)
}

/// Reads the total number of freelist pages (header offset 36).
pub fn read_freelist_count(bytes: &[u8]) -> u32 {
    read_u32(bytes, 36)
}

/// Reads the schema cookie (header offset 40).
pub fn read_schema_cookie(bytes: &[u8]) -> u32 {
    read_u32(bytes, 40)
}

/// Writes the database size in pages (header offset 28).
pub fn set_page_count(bytes: &mut [u8], count: u32) {
    bytes[28..32].copy_from_slice(&count.to_be_bytes());
}

/// Writes the first freelist trunk page (header offset 32).
pub fn set_freelist_trunk(bytes: &mut [u8], page: u32) {
    bytes[32..36].copy_from_slice(&page.to_be_bytes());
}

/// Writes the total number of freelist pages (header offset 36).
pub fn set_freelist_count(bytes: &mut [u8], count: u32) {
    bytes[36..40].copy_from_slice(&count.to_be_bytes());
}

/// Writes the schema cookie (header offset 40).
pub fn set_schema_cookie(bytes: &mut [u8], cookie: u32) {
    bytes[40..44].copy_from_slice(&cookie.to_be_bytes());
}

/// Bumps the file change counter and mirrors it into the version-valid-for
/// field so readers accept the counter without re-deriving it.
pub fn bump_change_counter(bytes: &mut [u8]) {
    let next = read_change_counter(bytes).wrapping_add(1);
    bytes[24..28].copy_from_slice(&next.to_be_bytes());
    bytes[92..96].copy_from_slice(&next.to_be_bytes());
}

/// Materializes a validated blank database with the given page size.
///
/// `page_size` must be a power of two between 512 and 65536. The result is a
/// single-page database whose page 1 is an empty `sqlite_master` table leaf.
pub fn blank_with_page_size(page_size: usize) -> Result<Vec<u8>> {
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
        return Err(Error::Corrupt("sqlite page size invalid"));
    }
    if page_size == DEFAULT_PAGE_SIZE {
        return blank_database();
    }
    let mut pages = vec![0u8; page_size];
    pages[..MAGIC.len()].copy_from_slice(MAGIC);
    pages[16..18].copy_from_slice(&(if page_size == 65_536 { 1u16 } else { page_size as u16 }).to_be_bytes());
    pages[18] = 1; // file format read version (legacy rollback journal)
    pages[19] = 1; // file format write version
    pages[20] = 0; // reserved space per page
    pages[21..24].copy_from_slice(&[64, 32, 32]);
    pages[24..28].copy_from_slice(&1u32.to_be_bytes()); // file change counter
    pages[28..32].copy_from_slice(&1u32.to_be_bytes()); // database size in pages
    pages[40..44].copy_from_slice(&1u32.to_be_bytes()); // schema cookie
    pages[44..48].copy_from_slice(&4u32.to_be_bytes()); // schema format number
    pages[56..60].copy_from_slice(&1u32.to_be_bytes()); // text encoding: UTF-8
    pages[92..96].copy_from_slice(&1u32.to_be_bytes()); // version-valid-for
    stamp_library_version(&mut pages, ENGINE_LIBRARY_VERSION_NUMBER);

    // Page 1 doubles as the database header and an empty sqlite_master leaf.
    let leaf = 100usize;
    pages[leaf] = 13; // table leaf page
    pages[leaf + 3..leaf + 5].copy_from_slice(&0u16.to_be_bytes()); // zero cells
    let content_start = if page_size == 65_536 { 0u16 } else { page_size as u16 };
    pages[leaf + 5..leaf + 7].copy_from_slice(&content_start.to_be_bytes());
    validate_header(&pages)?;
    Ok(pages)
}

fn decode_page_size(bytes: &[u8]) -> Result<usize> {
    let raw = u16::from_be_bytes([bytes[16], bytes[17]]);
    let page_size = if raw == 1 { 65_536 } else { raw as usize };
    Ok(page_size)
}

fn read_library_version_number(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[96], bytes[97], bytes[98], bytes[99]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("validated header offset"))
}

/// 将 SQLite 库版本号编码为 `major.minor.patch` 文本。
pub fn format_library_version(raw: u32) -> String {
    let major = raw / 1_000_000;
    let minor = (raw % 1_000_000) / 1_000;
    let patch = raw % 1_000;
    format!("{major}.{minor}.{patch}")
}
