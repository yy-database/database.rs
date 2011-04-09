//! Read-only rollback-journal recovery for offline snapshots.
//!
//! A process that dies mid-transaction leaves a hot journal that still holds
//! the original page images. Reading only the main file would expose partially
//! written pages, so this module rolls those pages back in memory.

const JOURNAL_MAGIC: [u8; 8] = [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];
const JOURNAL_HEADER_BYTES: usize = 28;

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
pub(crate) fn apply(main: &[u8], journal: &[u8]) -> Option<Vec<u8>> {
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
