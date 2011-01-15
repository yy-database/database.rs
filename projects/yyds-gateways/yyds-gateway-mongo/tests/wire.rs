//! MongoDB wire protocol frame tests.
//!
//! Round-trips synthetic OP_MSG frames through `read_frame` / `write_frame`
//! to lock down header layout, length calculation and opcode validation.

use std::io::{Cursor, Write};
use yyds_gateway_mongo::wire::{read_frame, write_frame, FrameLimits, MsgHeader};

fn hello_body() -> Vec<u8> {
    // OP_MSG body: flags(4) + kind(1) + BSON doc
    let mut bson = Vec::new();
    bson.extend_from_slice(&0_u32.to_le_bytes());
    bson.push(0x10);
    bson.extend_from_slice(b"hello\x00");
    bson.extend_from_slice(&1_i32.to_le_bytes());
    bson.push(0);
    let len = bson.len() as u32;
    bson[0..4].copy_from_slice(&len.to_le_bytes());

    let mut body = Vec::new();
    body.extend_from_slice(&0_u32.to_le_bytes()); // flags
    body.push(0x00); // kind
    body.extend_from_slice(&bson);
    body
}

#[test]
fn command_name_extracts_hello_from_op_msg_frame() {
    let body = hello_body();
    let header = MsgHeader { total_length: 16 + body.len() as u32, request_id: 1, response_to: 0, opcode: 2013 };
    let frame = yyds_gateway_mongo::wire::Frame { header, body };
    let name = yyds_gateway_mongo::command_name_from_frame(&frame);
    assert_eq!(name, Some("hello"));
}

#[test]
fn op_msg_frame_roundtrip_preserves_header_and_body() {
    let body = hello_body();
    let mut cursor = Cursor::new(Vec::<u8>::new());
    let request_id = 42_i32;
    let response_to = 7_i32;
    write_frame(&mut cursor, request_id, response_to, &body).unwrap();
    let bytes = cursor.into_inner();

    // Header + body
    assert!(bytes.len() >= 16 + body.len());

    let mut reader = Cursor::new(bytes);
    let frame = read_frame(&mut reader, FrameLimits::default()).unwrap().unwrap();
    assert_eq!(frame.header, MsgHeader { total_length: (16 + body.len()) as u32, request_id, response_to, opcode: 2013 });
    assert_eq!(frame.body, body);
}

#[test]
fn frame_too_large_is_rejected() {
    // total_length = 64MB + 1，超过默认 48MB 上限
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&64_000_000_u32.to_le_bytes());
    bytes.extend_from_slice(&0_i32.to_le_bytes());
    bytes.extend_from_slice(&0_i32.to_le_bytes());
    bytes.extend_from_slice(&2013_i32.to_le_bytes());
    bytes.push(0); // 占位 body

    let mut reader = Cursor::new(bytes);
    let result = read_frame(&mut reader, FrameLimits::default());
    assert!(matches!(result, Err(yyds_gateway_mongo::wire::FrameError::FrameTooLarge { .. })));
}

#[test]
fn unsupported_opcode_is_rejected() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&16_u32.to_le_bytes()); // 最小 header
    bytes.extend_from_slice(&1_i32.to_le_bytes());
    bytes.extend_from_slice(&0_i32.to_le_bytes());
    bytes.extend_from_slice(&5000_i32.to_le_bytes()); // 非 OP_MSG opcode

    let mut reader = Cursor::new(bytes);
    let result = read_frame(&mut reader, FrameLimits::default());
    assert!(matches!(result, Err(yyds_gateway_mongo::wire::FrameError::UnsupportedOpcode { opcode: 5000 })));
}

#[test]
fn truncated_header_is_reported_as_connection_closed() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&[0u8; 8]); // 只来半拉 header

    let mut reader = Cursor::new(bytes);
    let result = read_frame(&mut reader, FrameLimits::default());
    // 全空流 → ConnectionClosed；半拉 → UnexpectedEof (Io)。两者都是有效 IO 终止。
    assert!(
        matches!(result, Err(yyds_gateway_mongo::wire::FrameError::ConnectionClosed))
            || matches!(result, Err(yyds_gateway_mongo::wire::FrameError::Io(_)))
    );
}

#[test]
fn write_opens_a_valid_header_when_no_body() {
    // 空 body 也应该能 read back。
    let mut cursor = Cursor::new(Vec::<u8>::new());
    write_frame(&mut cursor, 99, 0, &[]).unwrap();
    let bytes = cursor.into_inner();
    assert_eq!(bytes.len(), 16); // 只有 header
    let mut reader = Cursor::new(bytes);
    let frame = read_frame(&mut reader, FrameLimits::default()).unwrap().unwrap();
    assert_eq!(frame.header.total_length, 16);
    assert!(frame.body.is_empty());
}
