//! SQLite record payload varint and storage-class codec round trips.

use database_formats::sqlite::{RecordLimits, RecordValue, TextEncoding, WriteValue, decode_record, decode_varint, encode_record};
use database_types::{Error, Result};

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

fn decode(payload: &[u8], encoding: TextEncoding) -> Result<Vec<RecordValue<'_>>> {
    decode_record(payload, encoding, RecordLimits::default())
}

fn error(payload: &[u8], encoding: TextEncoding) -> Error {
    decode(payload, encoding).expect_err("malformed record must not decode")
}

#[test]
fn varint_covers_every_byte_length() {
    assert_eq!(decode_varint(&[0x00]).expect("one byte"), (0, 1));
    assert_eq!(decode_varint(&[0x7f]).expect("one byte max"), (127, 1));
    assert_eq!(decode_varint(&[0x81, 0x00]).expect("two bytes"), (128, 2));
    assert_eq!(decode_varint(&[0x82, 0x01]).expect("two bytes offset"), (257, 2));
    assert_eq!(decode_varint(&[0xff, 0x7f]).expect("two bytes max"), (16_383, 2));
    assert_eq!(decode_varint(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]).expect("eight bytes"), (0x00ff_ffff_ffff_ffff, 8));
    assert_eq!(decode_varint(&[0xff; 9]).expect("nine bytes"), (u64::MAX, 9));

    let truncated = decode_varint(&[0x80]).expect_err("continuation without payload");
    assert_eq!(truncated.code(), "database.corrupt");
    assert_eq!(decode_varint(&[]).expect_err("empty varint").code(), "database.corrupt");
}

#[test]
fn integer_serial_types_cover_the_signed_range() {
    let payload = record(
        &[1, 2, 3, 4, 5, 6, 6],
        &[
            0x80, // -128
            0x80, 0x00, // -32768
            0x80, 0x00, 0x00, // -8388608
            0x80, 0x00, 0x00, 0x00, // -2147483648
            0x80, 0x00, 0x00, 0x00, 0x00, 0x00, // -2^47
            0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // i64::MIN
            0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // i64::MAX
        ],
    );
    let values = decode(&payload, TextEncoding::Utf8).expect("decode integers");
    assert_eq!(
        values,
        [
            RecordValue::Integer(-128),
            RecordValue::Integer(-32_768),
            RecordValue::Integer(-8_388_608),
            RecordValue::Integer(-2_147_483_648),
            RecordValue::Integer(-140_737_488_355_328),
            RecordValue::Integer(i64::MIN),
            RecordValue::Integer(i64::MAX),
        ]
    );
}

#[test]
fn constants_null_and_zero_length_classes_decode() {
    let payload = record(&[0, 8, 9, 12, 13], &[]);
    assert_eq!(
        decode(&payload, TextEncoding::Utf8).expect("decode constants"),
        [RecordValue::Null, RecordValue::Integer(0), RecordValue::Integer(1), RecordValue::Blob(&[]), RecordValue::Text(String::new()),]
    );
}

#[test]
fn real_and_blob_classes_preserve_exact_bits() {
    let negative_zero = (-0.0f64).to_bits().to_be_bytes();
    let payload = record(
        &[7, 14],
        &[
            negative_zero[0],
            negative_zero[1],
            negative_zero[2],
            negative_zero[3],
            negative_zero[4],
            negative_zero[5],
            negative_zero[6],
            negative_zero[7],
            0x00,
        ],
    );
    let values = decode(&payload, TextEncoding::Utf8).expect("decode real and blob");
    match &values[0] {
        RecordValue::Real(value) => assert_eq!(value.to_bits(), (-0.0f64).to_bits()),
        other => panic!("expected REAL, got {other:?}"),
    }
    assert_eq!(values[1], RecordValue::Blob(&[0x00]));
}

#[test]
fn utf16_text_round_trips_through_both_byte_orders() {
    // "中文" plus U+1F600, which needs a surrogate pair.
    let text: [u8; 8] = [0x2d, 0x4e, 0x87, 0x65, 0x3d, 0xd8, 0x00, 0xde];
    let payload = record(&[13 + 2 * 8], &text);
    assert_eq!(decode(&payload, TextEncoding::Utf16Le).expect("le text"), [RecordValue::Text("中文😀".into())]);

    let big_endian: Vec<u8> = [[0x4e, 0x2d], [0x65, 0x87], [0xd8, 0x3d], [0xde, 0x00]].concat();
    let payload = record(&[13 + 2 * 8], &big_endian);
    assert_eq!(decode(&payload, TextEncoding::Utf16Be).expect("be text"), [RecordValue::Text("中文😀".into())]);
}

#[test]
fn malformed_text_is_a_structured_error() {
    let invalid_utf8 = record(&[15], &[0xff]);
    assert_eq!(error(&invalid_utf8, TextEncoding::Utf8).code(), "database.unsupported");

    let odd_utf16 = record(&[15], &[0x41]);
    let odd = error(&odd_utf16, TextEncoding::Utf16Le);
    assert_eq!(odd.code(), "database.corrupt");
    assert!(odd.to_string().contains("odd UTF-16"), "{odd}");

    let lone_surrogate = record(&[17], &[0x00, 0xd8]);
    assert_eq!(error(&lone_surrogate, TextEncoding::Utf16Le).code(), "database.unsupported");
}

#[test]
fn malformed_record_layout_is_a_structured_error() {
    let reserved = record(&[10], &[]);
    let reserved_error = error(&reserved, TextEncoding::Utf8);
    assert_eq!(reserved_error.code(), "database.corrupt");
    assert!(reserved_error.to_string().contains("reserved serial type"), "{reserved_error}");

    let zero_header = vec![0x00];
    assert!(error(&zero_header, TextEncoding::Utf8).to_string().contains("header size invalid"));

    let oversized_header = vec![0x05, 0x00];
    assert!(error(&oversized_header, TextEncoding::Utf8).to_string().contains("header size invalid"));

    let truncated_body = record(&[6], &[0x00, 0x01]);
    assert!(error(&truncated_body, TextEncoding::Utf8).to_string().contains("truncated"));

    let trailing_body = vec![0x02, 0x00, 0x00];
    assert!(error(&trailing_body, TextEncoding::Utf8).to_string().contains("trailing"));

    let reserved_error = error(&record(&[11], &[]), TextEncoding::Utf8);
    assert_eq!(reserved_error.code(), "database.corrupt");
}

#[test]
fn encode_record_round_trips_boundary_integers() {
    // Width boundaries of the signed serial classes: values whose top
    // magnitude bit is set need an extra sign byte.
    for number in [
        0i64,
        1,
        -1,
        127,
        128,
        255,
        256,
        -128,
        -129,
        32_767,
        32_768,
        -32_768,
        -32_769,
        8_388_607,
        8_388_608,
        -8_388_609,
        2_147_483_647,
        2_147_483_648,
        -2_147_483_649,
        140_737_488_355_327,
        140_737_488_355_328,
        -140_737_488_355_329,
        i64::MAX,
        i64::MIN,
    ] {
        let payload = encode_record(&[WriteValue::Integer(number)], TextEncoding::Utf8);
        let values = decode_record(&payload, TextEncoding::Utf8, RecordLimits::default()).expect("decode");
        assert_eq!(values, [RecordValue::Integer(number)], "round trip for {number}");
    }
}

#[test]
fn encode_record_round_trips_every_storage_class() {
    let values = [
        WriteValue::Null,
        WriteValue::Integer(-42),
        WriteValue::Real(-0.5),
        WriteValue::Text("中文😀".to_string()),
        WriteValue::Blob(vec![0, 255, 128]),
        WriteValue::Text(String::new()),
        WriteValue::Blob(Vec::new()),
    ];
    let payload = encode_record(&values, TextEncoding::Utf8);
    let decoded = decode_record(&payload, TextEncoding::Utf8, RecordLimits::default()).expect("decode");
    assert_eq!(decoded[0], RecordValue::Null);
    assert_eq!(decoded[1], RecordValue::Integer(-42));
    assert_eq!(decoded[2], RecordValue::Real(-0.5));
    assert_eq!(decoded[3], RecordValue::Text("中文😀".to_string()));
    assert_eq!(decoded[4], RecordValue::Blob(&[0, 255, 128]));
    assert_eq!(decoded[5], RecordValue::Text(String::new()));
    assert_eq!(decoded[6], RecordValue::Blob(&[]));
}

#[test]
fn limits_are_enforced_before_materializing() {
    let payload = record(&[0, 0], &[]);
    let payload_limit =
        decode_record(&payload, TextEncoding::Utf8, RecordLimits { max_payload_bytes: 1, max_columns: 8 }).expect_err("payload limit");
    assert_eq!(payload_limit.code(), "database.unsupported");

    let column_limit =
        decode_record(&payload, TextEncoding::Utf8, RecordLimits { max_payload_bytes: 64, max_columns: 1 }).expect_err("column limit");
    assert_eq!(column_limit.code(), "database.unsupported");
}
