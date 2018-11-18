//! String and key family smoke tests — value forms, expiry options, key moves,
//! enumeration and the public family entry, over a real `LocalExecutor`.

use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::Duration,
};

use database_cluster::topology::ShardMap;
use database_execution::cluster::LocalExecutor;
use database_server::yyds::redis::{
    connection::serve_cancellable_with_executor, execute_string_command, resp::RequestLimits,
};
use database_storage::cluster_kv::MemoryShard;
use database_types::cluster::{Namespace, ShardEpoch, ShardId};

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

fn memory_server(name: &str) -> (TcpStream, thread::JoinHandle<()>) {
    let shard = ShardId(name.into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, MemoryShard::new())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let stopping = Arc::new(AtomicBool::new(false));
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_cancellable_with_executor(stream, RequestLimits::default(), stopping, executor).unwrap();
    });
    let client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    client.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
    (client, server)
}

fn send(client: &mut TcpStream, arguments: &[&str]) {
    let mut frame = format!("*{}\r\n", arguments.len());
    for argument in arguments {
        frame.push_str(&format!("${}\r\n{}\r\n", argument.len(), argument));
    }
    client.write_all(frame.as_bytes()).unwrap();
}

#[derive(Debug)]
enum Reply {
    Status(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

fn read_line(client: &mut TcpStream) -> Vec<u8> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        client.read_exact(&mut byte).unwrap();
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            return line;
        }
    }
}

fn read_reply(client: &mut TcpStream) -> Reply {
    let line = read_line(client);
    let body = &line[1..line.len() - 2];
    match line[0] {
        b'+' => Reply::Status(String::from_utf8(body.to_vec()).unwrap()),
        b'-' => Reply::Error(String::from_utf8(body.to_vec()).unwrap()),
        b':' => Reply::Integer(std::str::from_utf8(body).unwrap().parse().unwrap()),
        b'$' => {
            let length: i64 = std::str::from_utf8(body).unwrap().parse().unwrap();
            if length < 0 {
                return Reply::Bulk(None);
            }
            let mut data = vec![0u8; length as usize];
            client.read_exact(&mut data).unwrap();
            let mut terminator = [0u8; 2];
            client.read_exact(&mut terminator).unwrap();
            assert_eq!(terminator, *b"\r\n", "bulk string not terminated by CRLF");
            Reply::Bulk(Some(data))
        }
        b'*' => {
            let length: i64 = std::str::from_utf8(body).unwrap().parse().unwrap();
            if length < 0 {
                return Reply::Array(None);
            }
            let mut items = Vec::with_capacity(length as usize);
            for _ in 0..length {
                items.push(read_reply(client));
            }
            Reply::Array(Some(items))
        }
        prefix => panic!("unexpected reply prefix {prefix}"),
    }
}

fn describe(reply: &Reply) -> String {
    match reply {
        Reply::Status(value) => format!("status {value:?}"),
        Reply::Error(value) => format!("error {value:?}"),
        Reply::Integer(value) => format!("integer {value}"),
        Reply::Bulk(value) => format!("bulk {value:?}"),
        Reply::Array(value) => format!("array {value:?}"),
    }
}

fn expect_status(client: &mut TcpStream, expected: &str) {
    match read_reply(client) {
        Reply::Status(status) => assert_eq!(status, expected),
        other => panic!("expected status {expected}, got {}", describe(&other)),
    }
}

fn expect_error(client: &mut TcpStream, expected: &str) {
    match read_reply(client) {
        Reply::Error(error) => assert_eq!(error, expected),
        other => panic!("expected error {expected}, got {}", describe(&other)),
    }
}

fn expect_integer(client: &mut TcpStream) -> i64 {
    match read_reply(client) {
        Reply::Integer(value) => value,
        other => panic!("expected integer, got {}", describe(&other)),
    }
}

fn expect_bulk(client: &mut TcpStream, expected: Option<&str>) {
    match read_reply(client) {
        Reply::Bulk(value) => {
            let value = value.map(|bytes| String::from_utf8(bytes).unwrap());
            assert_eq!(value.as_deref(), expected);
        }
        other => panic!("expected bulk {expected:?}, got {}", describe(&other)),
    }
}

fn expect_array(client: &mut TcpStream, expected: &[&str]) {
    match read_reply(client) {
        Reply::Array(Some(items)) => {
            let items: Vec<String> = items
                .into_iter()
                .map(|item| match item {
                    Reply::Bulk(Some(bytes)) => String::from_utf8(bytes).unwrap(),
                    other => panic!("expected bulk array element, got {}", describe(&other)),
                })
                .collect();
            assert_eq!(items, expected);
        }
        other => panic!("expected array {expected:?}, got {}", describe(&other)),
    }
}

/// `SET ... GET` replies the old value and still honours `NX` / `XX`.
#[test]
fn set_get_option_reports_the_old_value() {
    let (mut client, _server) = memory_server("redis-string-set-get");
    send(&mut client, &["SET", "k", "first", "GET"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["SET", "k", "second", "GET"]);
    expect_bulk(&mut client, Some("first"));
    send(&mut client, &["GET", "k"]);
    expect_bulk(&mut client, Some("second"));

    // NX fails but the GET reply is still the old value, not nil.
    send(&mut client, &["SET", "k", "third", "NX", "GET"]);
    expect_bulk(&mut client, Some("second"));
    send(&mut client, &["GET", "k"]);
    expect_bulk(&mut client, Some("second"));

    // XX fails on a missing key and leaves it missing.
    send(&mut client, &["SET", "nope", "value", "XX", "GET"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["EXISTS", "nope"]);
    assert_eq!(expect_integer(&mut client), 0);

    // A wrong-typed key fails before the write and before the condition check.
    send(&mut client, &["HSET", "h", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SET", "h", "value", "GET"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["TYPE", "h"]);
    expect_status(&mut client, "hash");
}

/// Repeated options keep the last value, matching a real server.
#[test]
fn set_accepts_repeated_options() {
    let (mut client, _server) = memory_server("redis-string-set-repeat");
    send(&mut client, &["SET", "k", "one", "NX", "NX"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SET", "k", "two", "NX", "NX"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["SET", "k", "three", "EX", "10", "EX", "20"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["PTTL", "k"]);
    let ttl = expect_integer(&mut client);
    assert!((19_000..=20_000).contains(&ttl), "unexpected PTTL after repeated EX: {ttl}");
    send(&mut client, &["SET", "k", "four", "KEEPTTL", "KEEPTTL"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["PTTL", "k"]);
    assert!(expect_integer(&mut client) > 0);
    // Different expiry kinds stay a syntax error.
    send(&mut client, &["SET", "k", "five", "EX", "10", "PX", "20000"]);
    expect_error(&mut client, "ERR syntax error");
}

/// `SETNX`, `PSETEX` and `MSET` share the string namespace and replace collections.
#[test]
fn setnx_psetex_and_mset() {
    let (mut client, _server) = memory_server("redis-string-set-forms");
    send(&mut client, &["SETNX", "k", "one"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SETNX", "k", "two"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["GET", "k"]);
    expect_bulk(&mut client, Some("one"));

    send(&mut client, &["PSETEX", "p", "10000", "value"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["PTTL", "p"]);
    let ttl = expect_integer(&mut client);
    assert!((9_000..=10_000).contains(&ttl), "unexpected PSETEX PTTL: {ttl}");
    send(&mut client, &["PSETEX", "p", "0", "value"]);
    expect_error(&mut client, "ERR invalid expire time in 'psetex' command");
    send(&mut client, &["PSETEX", "p", "abc", "value"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");

    send(&mut client, &["HSET", "h", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["MSET", "a", "1", "h", "2"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["TYPE", "h"]);
    expect_status(&mut client, "string");
    send(&mut client, &["MGET", "a", "h"]);
    expect_array(&mut client, &["1", "2"]);
    send(&mut client, &["MSET", "a"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'mset' command");
}

/// `FLUSHDB` clears only the selected database, `FLUSHALL` clears every database.
#[test]
fn flushdb_and_flushall_scope() {
    let (mut client, _server) = memory_server("redis-string-flush");
    send(&mut client, &["SET", "k0", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SELECT", "1"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SET", "k1", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["HSET", "h1", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["FLUSHDB"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["DBSIZE"]);
    assert_eq!(expect_integer(&mut client), 0);

    send(&mut client, &["SELECT", "0"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["GET", "k0"]);
    expect_bulk(&mut client, Some("v"));
    send(&mut client, &["FLUSHDB", "LATER"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["FLUSHALL"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["DBSIZE"]);
    assert_eq!(expect_integer(&mut client), 0);
}

/// `OBJECT ENCODING` reports the same layout names a real server uses.
#[test]
fn object_encoding_reports_real_values() {
    let (mut client, _server) = memory_server("redis-string-encoding");
    let cases: &[(&str, &str)] = &[
        ("SET int 12345", "int"),
        ("SET int-negative -42", "int"),
        ("SET int-leading 007", "embstr"),
        ("SET plus +1", "embstr"),
        ("SET short hello", "embstr"),
    ];
    for (command, expected) in cases {
        let parts: Vec<&str> = command.split(' ').collect();
        send(&mut client, &parts);
        expect_status(&mut client, "OK");
        send(&mut client, &["OBJECT", "ENCODING", parts[1]]);
        expect_bulk(&mut client, Some(expected));
    }
    // 真机 Redis 8.10.3：38 字节仍是 embstr，39 字节起才是 raw。
    let embstr = "a".repeat(38);
    send(&mut client, &["SET", "embstr", &embstr]);
    expect_status(&mut client, "OK");
    send(&mut client, &["OBJECT", "ENCODING", "embstr"]);
    expect_bulk(&mut client, Some("embstr"));
    let raw = "a".repeat(39);
    send(&mut client, &["SET", "raw", &raw]);
    expect_status(&mut client, "OK");
    send(&mut client, &["OBJECT", "ENCODING", "raw"]);
    expect_bulk(&mut client, Some("raw"));
    send(&mut client, &["OBJECT", "BOGUS", "x"]);
    expect_error(&mut client, "ERR unknown subcommand 'BOGUS'. Try OBJECT HELP.");
    send(&mut client, &["OBJECT", "ENCODING"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'object|encoding' command");

    send(&mut client, &["HSET", "h", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["OBJECT", "ENCODING", "h"]);
    expect_bulk(&mut client, Some("listpack"));
    send(&mut client, &["SADD", "si", "1", "2", "3"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["OBJECT", "ENCODING", "si"]);
    expect_bulk(&mut client, Some("intset"));
    send(&mut client, &["SADD", "ss", "a", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["OBJECT", "ENCODING", "ss"]);
    expect_bulk(&mut client, Some("listpack"));
    send(&mut client, &["RPUSH", "l", "a", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["OBJECT", "ENCODING", "l"]);
    expect_bulk(&mut client, Some("listpack"));
    send(&mut client, &["ZADD", "z", "1", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["OBJECT", "ENCODING", "z"]);
    expect_bulk(&mut client, Some("listpack"));
    send(&mut client, &["OBJECT", "ENCODING", "missing"]);
    expect_bulk(&mut client, None);
}

/// The public family entry runs the same code path as the server connection.
#[test]
fn string_family_entry_matches_the_server() {
    let shard = ShardId("redis-string-entry".into());
    let executor = LocalExecutor::new(
        ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
        HashMap::from([(shard, MemoryShard::new())]),
        Vec::new(),
    )
    .unwrap();
    let namespace = Namespace("redis:0".into());
    let run = |arguments: &[&[u8]]| {
        let mut output = Vec::new();
        execute_string_command(&mut output, arguments, &executor, &namespace, 2).unwrap();
        String::from_utf8(output).unwrap()
    };
    assert_eq!(run(&[b"SET", b"k", b"v"]), "+OK\r\n");
    assert_eq!(run(&[b"GET", b"k"]), "$1\r\nv\r\n");
    assert_eq!(run(&[b"INCR", b"n"]), ":1\r\n");
    assert_eq!(run(&[b"OBJECT", b"ENCODING", b"k"]), "$6\r\nembstr\r\n");
    assert_eq!(run(&[b"SET", b"k", b"v2", b"GET"]), "$1\r\nv\r\n");
    assert_eq!(run(&[b"FLUSHDB"]), "+OK\r\n");
    assert_eq!(run(&[b"GET", b"k"]), "$-1\r\n");

    // An embedded binding mounts the catalog under a prefix of its own, so the
    // database helpers must preserve it instead of rebuilding `redis:<index>`.
    let prefixed = Namespace("redis:db0".into());
    let run_prefixed = |arguments: &[&[u8]]| {
        let mut output = Vec::new();
        execute_string_command(&mut output, arguments, &executor, &prefixed, 2).unwrap();
        String::from_utf8(output).unwrap()
    };
    assert_eq!(run_prefixed(&[b"SET", b"k", b"v"]), "+OK\r\n");
    assert_eq!(run_prefixed(&[b"SET", b"k", b"v", b"BOGUS"]), "-ERR syntax error\r\n");
    assert_eq!(run_prefixed(&[b"DBSIZE"]), ":1\r\n");
    assert_eq!(run_prefixed(&[b"FLUSHDB"]), "+OK\r\n");
    assert_eq!(run_prefixed(&[b"DBSIZE"]), ":0\r\n");
}
