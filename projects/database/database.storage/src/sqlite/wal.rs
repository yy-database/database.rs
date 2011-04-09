//! Read-only WAL replay for offline snapshots.
//!
//! SQLite keeps committed pages in `<db>-wal` until a checkpoint writes them
//! back. A snapshot reader that only reads the main file silently returns stale
//! data, so this module replays committed frames into an in-memory page image.
//! It never mutates the files on disk and ignores frames that fail the WAL
//! checksum or salt checks.

/// Magic 数值的低位决定校验和字节序：`0x377f0682` 为小端，`0x377f0683` 为大端。
const WAL_MAGIC_LITTLE_ENDIAN: u32 = 0x377f_0682;
const WAL_MAGIC_BIG_ENDIAN: u32 = 0x377f_0683;
const WAL_HEADER_BYTES: usize = 32;
const WAL_FRAME_HEADER_BYTES: usize = 24;

enum ByteOrder {
    Big,
    Little,
}

impl ByteOrder {
    fn words<'a>(&self, bytes: &'a [u8]) -> impl Iterator<Item = u32> + 'a {
        let order = matches!(self, Self::Big);
        bytes.chunks_exact(4).map(move |chunk| {
            let word = [chunk[0], chunk[1], chunk[2], chunk[3]];
            if order { u32::from_be_bytes(word) } else { u32::from_le_bytes(word) }
        })
    }
}

/// SQLite WAL 累计校验：每两个 32 位字一组，`s0 += x[i] + s1` 后 `s1 += x[i+1] + s0`。
fn checksum(order: &ByteOrder, bytes: &[u8], initial: (u32, u32)) -> (u32, u32) {
    let (mut s0, mut s1) = initial;
    let mut words = order.words(bytes);
    while let (Some(left), Some(right)) = (words.next(), words.next()) {
        s0 = s0.wrapping_add(left).wrapping_add(s1);
        s1 = s1.wrapping_add(right).wrapping_add(s0);
    }
    (s0, s1)
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

/// WAL 头与帧头的整数字段固定为大端，只有校验和按 magic 选择字序。
fn be(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3]])
}

/// Replays committed WAL frames over a main-file snapshot.
///
/// Returns `None` when the WAL is absent, empty, malformed, or belongs to a
/// different database image. A WAL with no committed frame also returns the
/// main image unchanged.
pub(crate) fn apply(main: &[u8], wal: &[u8]) -> Option<Vec<u8>> {
    if wal.len() < WAL_HEADER_BYTES {
        return None;
    }
    let magic = be(wal, 0);
    let order = match magic {
        WAL_MAGIC_LITTLE_ENDIAN => ByteOrder::Little,
        WAL_MAGIC_BIG_ENDIAN => ByteOrder::Big,
        _ => return None,
    };
    let page_size = be(wal, 8) as usize;
    let expected = page_size_of(main)?;
    if page_size != expected {
        return None;
    }
    let (s0, s1) = checksum(&order, &wal[..24], (0, 0));
    if s0 != be(wal, 24) || s1 != be(wal, 28) {
        return None;
    }
    let salt1 = be(wal, 16);
    let salt2 = be(wal, 20);

    let frame_bytes = WAL_FRAME_HEADER_BYTES + page_size;
    let mut cursor = WAL_HEADER_BYTES;
    let mut committed: Option<(usize, u32)> = None;
    let mut frames: Vec<(u32, &[u8])> = Vec::new();
    let mut running = (s0, s1);

    while cursor + frame_bytes <= wal.len() {
        let frame = &wal[cursor..cursor + frame_bytes];
        let page_number = be(frame, 0);
        if page_number == 0 || be(frame, 8) != salt1 || be(frame, 12) != salt2 {
            break;
        }
        let state = checksum(&order, &frame[..8], running);
        let page = &frame[WAL_FRAME_HEADER_BYTES..];
        let state = checksum(&order, page, state);
        if state.0 != be(frame, 16) || state.1 != be(frame, 20) {
            break;
        }
        running = state;
        frames.push((page_number, page));
        let image_pages = be(frame, 4);
        if image_pages != 0 {
            committed = Some((frames.len(), image_pages));
        }
        cursor += frame_bytes;
    }

    let (keep, size) = committed?;
    let size = size as usize;
    let needed = size.checked_mul(page_size)?;
    let mut output = main.to_vec();
    output.resize(needed, 0);
    for (page_number, page) in frames.iter().take(keep) {
        let offset = (*page_number as usize - 1).checked_mul(page_size)?;
        if offset + page_size > output.len() {
            output.resize(offset + page_size, 0);
        }
        output[offset..offset + page_size].copy_from_slice(*page);
    }
    output.truncate(needed);
    Some(output)
}
