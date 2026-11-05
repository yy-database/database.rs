//! `GETEX` option-combination smoke tests — TTL mutation and rejection over a real
//! `LocalExecutor`.

use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use database_cluster::topology::ShardMap;
use database_execution::cluster::LocalExecutor;
use database_server::yyds::redis::{connection::serve_cancellable_with_executor, resp::RequestLimits};
use database_storage::cluster_kv::MemoryShard;
use database_types::cluster::{ShardEpoch, ShardId};

fn executor_server() -> (TcpStream, thread::JoinHandle<()>) {
    let shard = ShardId("redis-getex".into());
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

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

enum Reply {
    Status(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
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
        prefix => panic!("unexpected reply prefix {prefix}"),
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

fn expect_bulk(client: &mut TcpStream, expected: Option<&str>) {
    match read_reply(client) {
        Reply::Bulk(value) => {
            let value = value.map(|bytes| String::from_utf8(bytes).unwrap());
            assert_eq!(value.as_deref(), expected);
        }
        other => panic!("expected bulk {expected:?}, got {}", describe(&other)),
    }
}

fn expect_integer(client: &mut TcpStream) -> i64 {
    match read_reply(client) {
        Reply::Integer(value) => value,
        other => panic!("expected integer, got {}", describe(&other)),
    }
}

fn describe(reply: &Reply) -> String {
    match reply {
        Reply::Status(value) => format!("status {value:?}"),
        Reply::Error(value) => format!("error {value:?}"),
        Reply::Integer(value) => format!("integer {value}"),
        Reply::Bulk(value) => format!("bulk {value:?}"),
    }
}

/// Without an option the command is a plain `GET`: an absent TTL stays absent, and an
/// existing TTL keeps counting down from its original deadline instead of being reset.
#[test]
fn getex_without_option_keeps_ttl() {
    let (mut client, _server) = executor_server();
    send(&mut client, &["SET", "k", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["PTTL", "k"]);
    assert_eq!(expect_integer(&mut client), -1);
    send(&mut client, &["GETEX", "k"]);
    expect_bulk(&mut client, Some("v"));
    send(&mut client, &["PTTL", "k"]);
    assert_eq!(expect_integer(&mut client), -1);

    send(&mut client, &["SETEX", "k", "5", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["PTTL", "k"]);
    let before = expect_integer(&mut client);
    assert!((4_500..=5_000).contains(&before), "unexpected TTL before GETEX: {before}");
    thread::sleep(Duration::from_secs(1));
    send(&mut client, &["GETEX", "k"]);
    expect_bulk(&mut client, Some("v"));
    send(&mut client, &["PTTL", "k"]);
    let after = expect_integer(&mut client);
    assert!((3_500..=4_500).contains(&after), "GETEX reset the TTL to {after} ms");
}

/// `EX` takes seconds and `PX` takes milliseconds, both counted from the current time.
#[test]
fn getex_ex_and_px_set_relative_deadlines() {
    let (mut client, _server) = executor_server();
    send(&mut client, &["SET", "k", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["GETEX", "k", "EX", "100"]);
    expect_bulk(&mut client, Some("v"));
    send(&mut client, &["PTTL", "k"]);
    let remaining = expect_integer(&mut client);
    assert!((98_000..=100_000).contains(&remaining), "EX 100 left {remaining} ms");

    send(&mut client, &["SET", "k", "v2"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["GETEX", "k", "PX", "5000"]);
    expect_bulk(&mut client, Some("v2"));
    send(&mut client, &["PTTL", "k"]);
    let remaining = expect_integer(&mut client);
    assert!((4_500..=5_000).contains(&remaining), "PX 5000 left {remaining} ms");
}

/// `EXAT` and `PXAT` take absolute Unix deadlines, and a deadline already in the past
/// makes the key disappear after the value is returned.
#[test]
fn getex_exat_and_pxat_set_absolute_deadlines() {
    let (mut client, _server) = executor_server();
    let now = now_ms();
    send(&mut client, &["SET", "k", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["GETEX", "k", "EXAT", &(now / 1000 + 100).to_string()]);
    expect_bulk(&mut client, Some("v"));
    send(&mut client, &["PTTL", "k"]);
    let remaining = expect_integer(&mut client);
    assert!((98_000..=101_000).contains(&remaining), "EXAT left {remaining} ms");

    send(&mut client, &["SET", "k", "v2"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["GETEX", "k", "PXAT", &(now + 5_000).to_string()]);
    expect_bulk(&mut client, Some("v2"));
    send(&mut client, &["PTTL", "k"]);
    let remaining = expect_integer(&mut client);
    assert!((4_500..=5_000).contains(&remaining), "PXAT left {remaining} ms");

    send(&mut client, &["SET", "k", "v3"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["GETEX", "k", "EXAT", "1"]);
    expect_bulk(&mut client, Some("v3"));
    send(&mut client, &["GET", "k"]);
    expect_bulk(&mut client, None);
}

/// `PERSIST` removes the deadline and leaves the value readable without a TTL.
#[test]
fn getex_persist_clears_ttl() {
    let (mut client, _server) = executor_server();
    send(&mut client, &["SETEX", "k", "120", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["PTTL", "k"]);
    let remaining = expect_integer(&mut client);
    assert!((118_000..=120_000).contains(&remaining), "unexpected TTL before PERSIST: {remaining}");
    send(&mut client, &["GETEX", "k", "PERSIST"]);
    expect_bulk(&mut client, Some("v"));
    send(&mut client, &["PTTL", "k"]);
    assert_eq!(expect_integer(&mut client), -1);
    send(&mut client, &["TTL", "k"]);
    assert_eq!(expect_integer(&mut client), -1);
}

/// Two expiry options in one command are a syntax error and leave the key untouched.
#[test]
fn getex_rejects_combined_options() {
    let (mut client, _server) = executor_server();
    send(&mut client, &["SET", "k", "v"]);
    expect_status(&mut client, "OK");
    for options in [
        ["EX", "10", "PERSIST"].as_slice(),
        ["PERSIST", "EX", "10"].as_slice(),
        ["EX", "10", "PX", "3000"].as_slice(),
        ["EX", "10", "EX", "20"].as_slice(),
    ] {
        let mut arguments = vec!["GETEX", "k"];
        arguments.extend_from_slice(options);
        send(&mut client, &arguments);
        expect_error(&mut client, "ERR syntax error");
    }
    send(&mut client, &["PTTL", "k"]);
    assert_eq!(expect_integer(&mut client), -1);
    send(&mut client, &["GET", "k"]);
    expect_bulk(&mut client, Some("v"));
}

/// Expiry arguments are validated with the Redis messages for arity, syntax, integers
/// and out-of-range deadlines.
#[test]
fn getex_validates_expiry_arguments() {
    let (mut client, _server) = executor_server();
    send(&mut client, &["SET", "k", "v"]);
    expect_status(&mut client, "OK");
    for (arguments, expected) in [
        (["GETEX", "k", "EX", "abc"].as_slice(), "ERR value is not an integer or out of range"),
        (["GETEX", "k", "PX", "-5"].as_slice(), "ERR invalid expire time in 'getex' command"),
        (["GETEX", "k", "EX", "0"].as_slice(), "ERR invalid expire time in 'getex' command"),
        (["GETEX", "k", "EXAT", "9223372036854775807"].as_slice(), "ERR invalid expire time in 'getex' command"),
        (["GETEX", "k", "EX"].as_slice(), "ERR syntax error"),
        (["GETEX", "k", "BOGUS"].as_slice(), "ERR syntax error"),
        (["GETEX"].as_slice(), "ERR wrong number of arguments for 'getex' command"),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, expected);
    }
    send(&mut client, &["PTTL", "k"]);
    assert_eq!(expect_integer(&mut client), -1);
}

/// A missing key returns nil and creates nothing. The numeric expiry argument is only
/// checked after the lookup, so a bad number on a missing key is nil rather than an
/// error, while option syntax is still validated first.
#[test]
fn getex_missing_key_returns_nil() {
    let (mut client, _server) = executor_server();
    send(&mut client, &["GETEX", "missing"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["GETEX", "missing", "PERSIST"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["GETEX", "missing", "EX", "abc"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["GETEX", "missing", "BOGUS"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["EXISTS", "missing"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["PTTL", "missing"]);
    assert_eq!(expect_integer(&mut client), -2);
}
