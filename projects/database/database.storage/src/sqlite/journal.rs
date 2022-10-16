//! Rollback-journal replay and writer.
//!
//! A process that dies mid-transaction leaves a hot journal that still holds
//! the original page images. Reading only the main file would expose partially
//! written pages, so [`apply`] rolls those pages back in memory. The writer
//! side publishes the same format before it replaces the main file, so both
//! this crate and an external SQLite engine can roll a crashed commit back.

use std::{
    fs,
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use database_types::{Error, Result};

/// Rollback-journal magic, written big-endian at the start of the header.
const JOURNAL_MAGIC: [u8; 8] = [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];
/// Header bytes before the zero padding that fills the first sector.
const JOURNAL_HEADER_BYTES: usize = 28;
/// Journal headers start on a 512-byte sector boundary, as SQLite writes them.
const JOURNAL_SECTOR_BYTES: usize = 512;

/// Returns the `<database>-journal` companion path.
pub fn sidecar(database: &Path) -> PathBuf {
    let mut path = database.as_os_str().to_os_string();
    path.push("-journal");
    PathBuf::from(path)
}

/// Writes a rollback journal that restores `image` over the main file.
///
/// The header and every page record use SQLite's rollback-journal encoding:
/// magic, record count, checksum seed, original page count, sector size and
/// page size, followed by `(page number, page image, checksum)` records. The
/// records are written before the magic is stamped, so a crash during the
/// write leaves a journal that both this crate and SQLite treat as empty.
/// `synchronous` adds `fsync` calls between the two phases and afterwards.
///
/// `image` is the page image currently stored in the main file, which is the
/// image a crash must restore.
pub fn write(database: &Path, image: &[u8], synchronous: bool) -> Result<()> {
    let page_size = page_size_of(image).ok_or(Error::Corrupt("rollback journal needs a format-3 page image"))?;
    if image.is_empty() || image.len() % page_size != 0 {
        return Err(Error::Corrupt("rollback journal image is not a whole number of pages"));
    }
    let pages = image.len() / page_size;
    let pages_u32 = u32::try_from(pages).map_err(|_| Error::Corrupt("rollback journal image has too many pages"))?;
    let seed = checksum_seed();
    let mut buffer = Vec::with_capacity(JOURNAL_SECTOR_BYTES + pages * (page_size + 8));
    buffer.resize(JOURNAL_SECTOR_BYTES, 0);
    for (index, page) in image.chunks_exact(page_size).enumerate() {
        let number = u32::try_from(index + 1).map_err(|_| Error::Corrupt("rollback journal page number overflow"))?;
        buffer.extend_from_slice(&number.to_be_bytes());
        buffer.extend_from_slice(page);
        buffer.extend_from_slice(&record_checksum(page, seed).to_be_bytes());
    }

    let path = sidecar(database);
    let mut file = fs::File::create(&path).map_err(Error::Io)?;
    file.write_all(&buffer).map_err(Error::Io)?;
    if synchronous {
        file.sync_all().map_err(Error::Io)?;
    }
    file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
    let mut header = [0u8; JOURNAL_HEADER_BYTES];
    header[..8].copy_from_slice(&JOURNAL_MAGIC);
    header[8..12].copy_from_slice(&pages_u32.to_be_bytes());
    header[12..16].copy_from_slice(&seed.to_be_bytes());
    header[16..20].copy_from_slice(&pages_u32.to_be_bytes());
    header[20..24].copy_from_slice(&(JOURNAL_SECTOR_BYTES as u32).to_be_bytes());
    header[24..28].copy_from_slice(&(page_size as u32).to_be_bytes());
    file.write_all(&header).map_err(Error::Io)?;
    if synchronous {
        file.sync_all().map_err(Error::Io)?;
    }
    Ok(())
}

/// Clears the journal after a successful publish.
///
/// `truncate` keeps SQLite's `journal_mode=truncate` steady state by leaving a
/// zero-byte sidecar; otherwise the sidecar is removed, as `delete` does.
/// A missing sidecar is not an error.
pub fn finish(database: &Path, truncate: bool) -> Result<()> {
    let path = sidecar(database);
    if truncate {
        match fs::OpenOptions::new().write(true).create(true).truncate(false).open(&path) {
            Ok(file) => file.set_len(0).map_err(Error::Io),
            Err(error) => Err(Error::Io(error)),
        }
    }
    else {
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::Io(error)),
        }
    }
}

/// Checksum seed for one journal. SQLite uses random bytes; a clock/PID mix is
/// enough to keep adjacent journals distinct.
fn checksum_seed() -> u32 {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).map(|elapsed| elapsed.as_nanos() as u64).unwrap_or(0);
    ((stamp as u32) ^ std::process::id().rotate_left(16)) | 1
}

fn page_size_of(main: &[u8]) -> Option<usize> {
    if main.len() < 100 {
        return None;
    }
    let raw = u16::from_be_bytes([main[16], main[17]]) as usize;
    Some(match raw {
        1 => 65_536,
        value if value >= 512 && value.is_power_of_two() => value,
        _ => return None,
    })
}

fn record_checksum(page: &[u8], seed: u32) -> u32 {
    let mut checksum = seed;
    let mut index = page.len() as isize - 200;
    while index > 0 {
        checksum = checksum.wrapping_add(u32::from(page[index as usize]));
        index -= 200;
    }
    checksum
}

/// Rolls back a hot journal over a main-file snapshot.
///
/// Returns `None` when the journal is missing, too small, or not a rollback
/// journal for this page size. Unreadable trailing records stop the replay and
/// keep the pages replayed so far.
pub fn apply(main: &[u8], journal: &[u8]) -> Option<Vec<u8>> {
    if journal.len() < 512 || journal.len() < JOURNAL_HEADER_BYTES || journal[..8] != JOURNAL_MAGIC {
        return None;
    }
    let page_size = page_size_of(main)?;
    let declared = u32::from_be_bytes([journal[24], journal[25], journal[26], journal[27]]) as usize;
    if declared != page_size {
        return None;
    }
    let sector = u32::from_be_bytes([journal[20], journal[21], journal[22], journal[23]]) as usize;
    if sector < JOURNAL_HEADER_BYTES || !sector.is_power_of_two() {
        return None;
    }
    let original_pages = u32::from_be_bytes([journal[16], journal[17], journal[18], journal[19]]) as usize;
    let seed = u32::from_be_bytes([journal[12], journal[13], journal[14], journal[15]]);
    let record_bytes = 4 + page_size + 4;

    let mut output = main.to_vec();
    let mut cursor = sector;
    while cursor + record_bytes <= journal.len() {
        let page_number = u32::from_be_bytes([journal[cursor], journal[cursor + 1], journal[cursor + 2], journal[cursor + 3]]) as usize;
        if page_number == 0 {
            break;
        }
        let page = &journal[cursor + 4..cursor + 4 + page_size];
        let stored = u32::from_be_bytes([
            journal[cursor + 4 + page_size],
            journal[cursor + 5 + page_size],
            journal[cursor + 6 + page_size],
            journal[cursor + 7 + page_size],
        ]);
        if record_checksum(page, seed) != stored {
            break;
        }
        let offset = (page_number - 1).checked_mul(page_size)?;
        if offset + page_size > output.len() {
            output.resize(offset + page_size, 0);
        }
        output[offset..offset + page_size].copy_from_slice(page);
        cursor += record_bytes;
    }

    if original_pages > 0 {
        output.resize(original_pages * page_size, 0);
    }
    Some(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two-page image with a real format-3 page-size field and distinct bytes.
    fn image() -> Vec<u8> {
        let mut image = vec![0u8; 2 * 4096];
        image[16..18].copy_from_slice(&4096u16.to_be_bytes());
        image[4096..4100].copy_from_slice(b"old!");
        image
    }

    fn temp_database(label: &str) -> PathBuf {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock before epoch").as_nanos();
        let directory = std::env::temp_dir().join(format!("sqlite-journal-{label}-{}-{stamp}", std::process::id()));
        fs::create_dir_all(&directory).expect("temp directory");
        directory.join("db.sqlite")
    }

    #[test]
    fn a_written_journal_restores_the_image_and_truncates_to_its_page_count() {
        let database = temp_database("restore");
        let original = image();
        write(&database, &original, true).expect("write journal");

        let journal = fs::read(sidecar(&database)).expect("journal exists");
        assert_eq!(&journal[..8], &JOURNAL_MAGIC);
        assert_eq!(u32::from_be_bytes([journal[8], journal[9], journal[10], journal[11]]), 2);
        assert_eq!(u32::from_be_bytes([journal[16], journal[17], journal[18], journal[19]]), 2);
        assert_eq!(u32::from_be_bytes([journal[20], journal[21], journal[22], journal[23]]), 512);
        assert_eq!(u32::from_be_bytes([journal[24], journal[25], journal[26], journal[27]]), 4096);

        // The published image is one page longer and carries new bytes.
        let mut published = original.clone();
        published.extend_from_slice(&vec![0x55; 4096]);
        published[4096..4100].copy_from_slice(b"new!");
        assert_eq!(apply(&published, &journal), Some(original.clone()));

        finish(&database, false).expect("delete journal");
        assert!(!sidecar(&database).exists());
        fs::remove_dir_all(database.parent().expect("parent")).expect("cleanup");
    }

    #[test]
    fn a_truncated_journal_is_empty_and_not_hot() {
        let database = temp_database("truncate");
        let original = image();
        write(&database, &original, false).expect("write journal");
        finish(&database, true).expect("truncate journal");
        let journal = fs::read(sidecar(&database)).expect("zero-byte sidecar survives");
        assert!(journal.is_empty());
        assert_eq!(apply(&original, &journal), None);
        fs::remove_dir_all(database.parent().expect("parent")).expect("cleanup");
    }

    #[test]
    fn a_torn_record_stops_the_replay() {
        let database = temp_database("torn");
        let original = image();
        write(&database, &original, false).expect("write journal");
        let mut journal = fs::read(sidecar(&database)).expect("journal exists");
        journal.truncate(journal.len() - 100);
        let recovered = apply(&original, &journal).expect("header still valid");
        assert_eq!(recovered, original);
        fs::remove_dir_all(database.parent().expect("parent")).expect("cleanup");
    }
}
