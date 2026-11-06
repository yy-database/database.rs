//! `OBJECT` subcommand smoke tests — idle time, reference count, frequency policy and
//! missing-key replies over a real `LocalExecutor`.

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
use database_server::yyds::redis::{connection::serve_cancellable_with_executor, resp::RequestLimits};
use database_storage::cluster_kv::MemoryShard;
use database_types::cluster::{ShardEpoch, ShardId};

const LFU_POLICY_ERROR: &str = "ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.";

fn executor_server() -> (TcpStream, thread::JoinHandle<()>) {
    let shard = ShardId("redis-object".into());
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

fn idle_seconds(client: &mut TcpStream, key: &str) -> i64 {
    send(client, &["OBJECT", "IDLETIME", key]);
    expect_integer(client)
}

#[test]
fn object_idletime_advances_and_resets_on_read() {
    let (mut client, _server) = executor_server();
    send(&mut client, &["SET", "k", "v"]);
    expect_status(&mut client, "OK");

    // Written a moment ago, so the object has not been idle for a whole second yet.
    assert_eq!(idle_seconds(&mut client, "k"), 0);

    thread::sleep(Duration::from_millis(1100));
    let first = idle_seconds(&mut client, "k");
    assert!(first >= 1, "idle time did not advance after a write: {first}");

    thread::sleep(Duration::from_millis(1100));
    let second = idle_seconds(&mut client, "k");
    // `OBJECT IDLETIME` must not touch the object: two probes a second apart keep counting.
    assert!(second > first, "OBJECT IDLETIME reset the idle clock: {first} then {second}");

    send(&mut client, &["GET", "k"]);
    expect_bulk(&mut client, Some("v"));
    assert_eq!(idle_seconds(&mut client, "k"), 0, "a read hit did not refresh the idle clock");
}

#[test]
fn object_missing_key_returns_null() {
    let (mut client, _server) = executor_server();
    for subcommand in ["IDLETIME", "REFCOUNT", "FREQ"] {
        send(&mut client, &["OBJECT", subcommand, "missing"]);
        expect_bulk(&mut client, None);
    }
}

#[test]
fn object_refcount_reports_one_for_existing_key() {
    let (mut client, _server) = executor_server();
    send(&mut client, &["SET", "k", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["OBJECT", "REFCOUNT", "k"]);
    assert_eq!(expect_integer(&mut client), 1);
}

#[test]
fn object_freq_reports_missing_lfu_policy() {
    let (mut client, _server) = executor_server();
    send(&mut client, &["SET", "k", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["OBJECT", "FREQ", "k"]);
    expect_error(&mut client, LFU_POLICY_ERROR);
}

#[test]
fn object_rejects_unknown_subcommands_and_wrong_arity() {
    let (mut client, _server) = executor_server();
    send(&mut client, &["OBJECT"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'object' command");

    send(&mut client, &["OBJECT", "IDLETIME"]);
    expect_error(&mut client, "ERR Unknown subcommand or wrong number of arguments for 'IDLETIME'. Try OBJECT HELP.");

    send(&mut client, &["OBJECT", "BOGUS", "k"]);
    expect_error(&mut client, "ERR Unknown subcommand or wrong number of arguments for 'BOGUS'. Try OBJECT HELP.");

    send(&mut client, &["OBJECT", "REFCOUNT", "k", "extra"]);
    expect_error(&mut client, "ERR Unknown subcommand or wrong number of arguments for 'REFCOUNT'. Try OBJECT HELP.");
}
