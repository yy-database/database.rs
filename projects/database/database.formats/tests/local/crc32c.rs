//! CRC32C conformance against the published vectors and a bitwise reference.

use database_formats::yy::yydb::crc32c::crc32c;

/// Castagnoli polynomial, restated here so the reference decoder does not
/// borrow the table implementation it is checking.
const POLY: u32 = 0x82F6_3B78;

fn bitwise(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ POLY } else { crc >> 1 };
        }
    }
    crc ^ 0xFFFF_FFFF
}

#[test]
fn matches_standard_vectors() {
    assert_eq!(crc32c(b""), 0);
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
}

#[test]
fn matches_bitwise_reference_for_every_remainder() {
    let data: Vec<u8> = (0..=255).chain(0..=255).collect();
    for len in 0..=64 {
        let prefix = &data[..len];
        assert_eq!(crc32c(prefix), bitwise(prefix), "length {len}");
    }
    assert_eq!(crc32c(&data), bitwise(&data));
}
