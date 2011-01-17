//! End-to-end MongoDB handshake tests.
//!
//! Start a loopback MongoService, send an OP_MSG `hello` command over TCP,
//! and verify the disguise reply BSON contains expected fields.

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::atomic::AtomicBool,
    thread,
    time::Duration,
};

use yyds_gateway_mongo::{
    connection::serve_cancellable,
    wire::{self, FrameLimits},
};

/// Build a hello command OP_MSG frame — `{ hello: 1, $db: "admin" }`.
/// OP_MSG body layout: [flags u32][kind u8=0x00][BSON doc].
fn build_hello_frame(request_id: i32) -> Vec<u8> {
    let mut bson = Vec::new();
    bson.extend_from_slice(&0_u32.to_le_bytes());
    bson.push(0x10);
    bson.extend_from_slice(b"hello\x00");
    bson.extend_from_slice(&1_i32.to_le_bytes());
    bson.push(0x02);
    bson.extend_from_slice(b"$db\x00");
    let s = b"admin";
    bson.extend_from_slice(&(s.len() as i32 + 1).to_le_bytes());
    bson.extend_from_slice(s);
    bson.push(0);
    bson.push(0);
    let len = bson.len() as u32;
    bson[0..4].copy_from_slice(&len.to_le_bytes());

    // OP_MSG body: flags + kind(0x00) + BSON doc
    let mut op_msg_body = Vec::new();
    op_msg_body.extend_from_slice(&0_u32.to_le_bytes()); // flags
    op_msg_body.push(0x00); // kind = single document
    op_msg_body.extend_from_slice(&bson);

    let mut frame = Vec::new();
    let total = 16 + op_msg_body.len() as u32;
    frame.extend_from_slice(&total.to_le_bytes());
    frame.extend_from_slice(&request_id.to_le_bytes());
    frame.extend_from_slice(&0_i32.to_le_bytes());
    frame.extend_from_slice(&wire::OP_MSG.to_le_bytes());
    frame.extend_from_slice(&op_msg_body);
    frame
}

fn spawn_handshake_server() -> (TcpStream, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let stopping = std::sync::Arc::new(AtomicBool::new(false));
    let stopping_clone = std::sync::Arc::clone(&stopping);

    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_cancellable(stream, FrameLimits::default(), stopping_clone).unwrap();
    });

    let client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    client.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
    (client, server)
}

#[test]
fn hello_command_returns_is_writable_primary() {
    let (mut client, server) = spawn_handshake_server();

    let frame = build_hello_frame(1);
    client.write_all(&frame).unwrap();
    client.shutdown(Shutdown::Write).unwrap();

    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    let _ = server.join();

    // 应该至少有 16-byte header
    assert!(response.len() >= 16, "response too short: {} bytes", response.len());

    // 读 frame 验证
    let mut reader = std::io::Cursor::new(response.clone());
    let reply = wire::read_frame(&mut reader, FrameLimits::default()).unwrap().unwrap();

    // request_id 回显
    assert_eq!(reply.header.request_id, 0);
    assert_eq!(reply.header.response_to, 1);
    assert_eq!(reply.header.opcode, wire::OP_MSG);

    // OP_MSG replies start with flag bits and a section kind before BSON.
    assert_eq!(&reply.body[..4], &0_u32.to_le_bytes());
    assert_eq!(reply.body[4], 0x00);

    // BSON body 里必须有 ok >= 1 和 isWritablePrimary: true
    let body = &reply.body;
    let mut pos = 5 + 4; // skip flag bits, section kind and doc length
    let mut found_ok = false;
    let mut found_writable = false;
    while pos < body.len() - 1 {
        let elem_type = body[pos];
        pos += 1;
        let key_start = pos;
        while pos < body.len() && body[pos] != 0 {
            pos += 1;
        }
        let key = &body[key_start..pos];
        pos += 1; // skip null terminator

        match elem_type {
            0x10 => {
                // int32
                let val = i32::from_le_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
                pos += 4;
                if key == b"ok" {
                    found_ok = val >= 1;
                }
            }
            0x01 => {
                // double
                let val = f64::from_le_bytes(body[pos..pos + 8].try_into().unwrap());
                pos += 8;
                if key == b"ok" {
                    found_ok = val >= 1.0;
                }
            }
            0x08 => {
                // bool
                let val = body[pos] != 0;
                pos += 1;
                if key == b"isWritablePrimary" {
                    found_writable = val;
                }
            }
            _ => break,
        }
    }
    let body_text = String::from_utf8_lossy(body);
    assert!(found_ok, "hello reply missing ok: body={body_text:?}");
    assert!(found_writable, "hello reply missing isWritablePrimary: body={body_text:?}");
}

#[test]
fn ping_command_returns_ok_document() {
    let (mut client, server) = spawn_handshake_server();

    // ping 命令: { ping: 1 }
    let mut bson = Vec::new();
    bson.extend_from_slice(&0_u32.to_le_bytes());
    bson.push(0x10);
    bson.extend_from_slice(b"ping\x00");
    bson.extend_from_slice(&1_i32.to_le_bytes());
    bson.push(0);
    let len = bson.len() as u32;
    bson[0..4].copy_from_slice(&len.to_le_bytes());

    let mut op_msg_body = Vec::new();
    op_msg_body.extend_from_slice(&0_u32.to_le_bytes());
    op_msg_body.push(0x00);
    op_msg_body.extend_from_slice(&bson);

    let mut frame = Vec::new();
    let total = 16 + op_msg_body.len() as u32;
    frame.extend_from_slice(&total.to_le_bytes());
    frame.extend_from_slice(&7_i32.to_le_bytes()); // request_id
    frame.extend_from_slice(&0_i32.to_le_bytes());
    frame.extend_from_slice(&wire::OP_MSG.to_le_bytes());
    frame.extend_from_slice(&op_msg_body);

    client.write_all(&frame).unwrap();
    client.shutdown(Shutdown::Write).unwrap();

    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    let _ = server.join();

    let mut reader = std::io::Cursor::new(response);
    let reply = wire::read_frame(&mut reader, FrameLimits::default()).unwrap().unwrap();
    assert_eq!(reply.header.response_to, 7);

    // body 应该是 { ok: 1 }，前面带 OP_MSG 的 flag bits 与 section kind
    let body = &reply.body;
    let mut pos = 5 + 4;
    let mut found_ok = false;
    while pos < body.len() - 1 {
        let elem_type = body[pos];
        pos += 1;
        let key_start = pos;
        while pos < body.len() && body[pos] != 0 {
            pos += 1;
        }
        let key = &body[key_start..pos];
        pos += 1;
        match elem_type {
            0x10 => {
                let val = i32::from_le_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
                pos += 4;
                if key == b"ok" {
                    found_ok = val >= 1;
                    break;
                }
            }
            0x01 => {
                let val = f64::from_le_bytes(body[pos..pos + 8].try_into().unwrap());
                pos += 8;
                if key == b"ok" {
                    found_ok = val >= 1.0;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(found_ok);
}

#[test]
fn unknown_command_returns_error_document() {
    let (mut client, server) = spawn_handshake_server();

    let mut bson = Vec::new();
    bson.extend_from_slice(&0_u32.to_le_bytes());
    bson.push(0x10);
    bson.extend_from_slice(b"doSomething\x00");
    bson.extend_from_slice(&1_i32.to_le_bytes());
    bson.push(0);
    let len = bson.len() as u32;
    bson[0..4].copy_from_slice(&len.to_le_bytes());

    let mut op_msg_body = Vec::new();
    op_msg_body.extend_from_slice(&0_u32.to_le_bytes());
    op_msg_body.push(0x00);
    op_msg_body.extend_from_slice(&bson);

    let mut frame = Vec::new();
    let total = 16 + op_msg_body.len() as u32;
    frame.extend_from_slice(&total.to_le_bytes());
    frame.extend_from_slice(&99_i32.to_le_bytes());
    frame.extend_from_slice(&0_i32.to_le_bytes());
    frame.extend_from_slice(&wire::OP_MSG.to_le_bytes());
    frame.extend_from_slice(&op_msg_body);

    client.write_all(&frame).unwrap();
    client.shutdown(Shutdown::Write).unwrap();

    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    let _ = server.join();

    let mut reader = std::io::Cursor::new(response);
    let reply = wire::read_frame(&mut reader, FrameLimits::default()).unwrap().unwrap();
    assert_eq!(reply.header.response_to, 99);

    // body 应该是 generic_error_body: { ok: 0, errmsg: "unsupported" }，前面带 OP_MSG 的 flag bits 与 section kind
    let body = &reply.body;
    let mut pos = 5 + 4;
    let mut ok_val: Option<i32> = None;
    let mut errmsg: Option<String> = None;
    while pos < body.len() - 1 {
        let elem_type = body[pos];
        pos += 1;
        let key_start = pos;
        while pos < body.len() && body[pos] != 0 {
            pos += 1;
        }
        let key = &body[key_start..pos];
        pos += 1;
        match elem_type {
            0x10 => {
                // int32
                let val = i32::from_le_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
                pos += 4;
                if key == b"ok" {
                    ok_val = Some(val);
                }
            }
            0x02 => {
                // string
                let str_len = i32::from_le_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]) as usize;
                pos += 4;
                let s = String::from_utf8_lossy(&body[pos..pos + str_len - 1]).into_owned();
                pos += str_len;
                if key == b"errmsg" {
                    errmsg = Some(s);
                }
            }
            _ => break,
        }
    }
    assert_eq!(ok_val, Some(0), "expected ok: 0 for unknown command");
    assert_eq!(errmsg.as_deref(), Some("unsupported"));
}
