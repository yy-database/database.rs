//! `hash` / `list` value type smoke tests — semantics, cross-type rejection and
//! consistency with the key-expiry family, over a real `LocalExecutor`.

use std::{
    collections::HashMap,
    fmt::Debug,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use database_cluster::topology::ShardMap;
use database_execution::cluster::LocalExecutor;
use database_server::yyds::redis::{connection::serve_cancellable_with_executor, resp::RequestLimits};
use database_storage::cluster_kv::{FileShard, KvStore, MemoryShard};
use database_types::cluster::{ShardEpoch, ShardId};

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

fn server<S>(shard_name: &str, store: S) -> (TcpStream, thread::JoinHandle<()>)
where
    S: KvStore + Send + Debug + 'static,
{
    let shard = ShardId(shard_name.into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, store)]),
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

fn memory_server(name: &str) -> (TcpStream, thread::JoinHandle<()>) {
    server(name, MemoryShard::new())
}

fn temp_path(label: &str) -> PathBuf {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("yyds-redis-{label}-{}-{unique}.yykv", std::process::id()))
}

fn file_server(path: &PathBuf) -> (TcpStream, thread::JoinHandle<()>) {
    server("redis-collections-file", FileShard::open(path).unwrap())
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

fn expect_empty_array(client: &mut TcpStream) {
    match read_reply(client) {
        Reply::Array(Some(items)) => assert!(items.is_empty(), "expected empty array, got {} elements", items.len()),
        other => panic!("expected empty array, got {}", describe(&other)),
    }
}

fn expect_null_array(client: &mut TcpStream) {
    match read_reply(client) {
        Reply::Array(None) => {}
        other => panic!("expected null array, got {}", describe(&other)),
    }
}

/// `HSET` counts only newly added fields, updates keep the original position, and
/// `HGETALL` answers in insertion order.
#[test]
fn hash_set_get_and_field_order() {
    let (mut client, _server) = memory_server("redis-hash-order");
    send(&mut client, &["HSET", "h", "f1", "v1", "f2", "v2"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["HSET", "h", "f1", "v1b", "f3", "v3"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["HGET", "h", "f1"]);
    expect_bulk(&mut client, Some("v1b"));
    send(&mut client, &["HGET", "h", "missing"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["HGET", "no-such-key", "f"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["HGETALL", "h"]);
    expect_array(&mut client, &["f1", "v1b", "f2", "v2", "f3", "v3"]);
    send(&mut client, &["HGETALL", "no-such-key"]);
    expect_empty_array(&mut client);
    send(&mut client, &["HLEN", "h"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["HLEN", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);
}

/// Deleting the last field deletes the key itself, and deleting from a missing key
/// answers 0 instead of creating one.
#[test]
fn hash_delete_removes_empty_key() {
    let (mut client, _server) = memory_server("redis-hash-del");
    send(&mut client, &["HSET", "h", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["HDEL", "h", "f", "missing"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["EXISTS", "h"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["TYPE", "h"]);
    expect_status(&mut client, "none");
    send(&mut client, &["HDEL", "no-such-key", "f"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["EXISTS", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);
}

/// A logical key carries exactly one type: string writes reject hash/list commands
/// and collection writes reject string commands, in both directions.
#[test]
fn wrong_type_in_both_directions() {
    let (mut client, _server) = memory_server("redis-wrongtype");
    send(&mut client, &["SET", "s", "v"]);
    expect_status(&mut client, "OK");
    for arguments in [
        ["HSET", "s", "f", "v"].as_slice(),
        ["HGET", "s", "f"].as_slice(),
        ["HGETALL", "s"].as_slice(),
        ["HDEL", "s", "f"].as_slice(),
        ["HLEN", "s"].as_slice(),
        ["LPUSH", "s", "a"].as_slice(),
        ["RPUSH", "s", "a"].as_slice(),
        ["LRANGE", "s", "0", "-1"].as_slice(),
        ["LPOP", "s"].as_slice(),
        ["RPOP", "s"].as_slice(),
        ["LLEN", "s"].as_slice(),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, WRONGTYPE);
    }
    send(&mut client, &["GET", "s"]);
    expect_bulk(&mut client, Some("v"));

    send(&mut client, &["HSET", "h", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    for arguments in [
        ["GET", "h"].as_slice(),
        ["GETDEL", "h"].as_slice(),
        ["GETSET", "h", "x"].as_slice(),
        ["GETEX", "h"].as_slice(),
        ["STRLEN", "h"].as_slice(),
        ["APPEND", "h", "x"].as_slice(),
        ["INCR", "h"].as_slice(),
        ["DECR", "h"].as_slice(),
        ["MGET", "h", "other"].as_slice(),
        ["LPUSH", "h", "a"].as_slice(),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, WRONGTYPE);
    }
    send(&mut client, &["TYPE", "h"]);
    expect_status(&mut client, "hash");
    send(&mut client, &["HGET", "h", "f"]);
    expect_bulk(&mut client, Some("v"));

    send(&mut client, &["LPUSH", "l", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    for arguments in [
        ["GET", "l"].as_slice(),
        ["STRLEN", "l"].as_slice(),
        ["INCR", "l"].as_slice(),
        ["APPEND", "l", "x"].as_slice(),
        ["HSET", "l", "f", "v"].as_slice(),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, WRONGTYPE);
    }
    send(&mut client, &["TYPE", "l"]);
    expect_status(&mut client, "list");
    send(&mut client, &["LRANGE", "l", "0", "-1"]);
    expect_array(&mut client, &["a"]);
}

/// `LPUSH` prepends each value in order, `LRANGE` handles negative indexes and
/// out-of-range slices, and pops trim the list down to an absent key.
#[test]
fn list_push_range_and_pop() {
    let (mut client, _server) = memory_server("redis-list");
    send(&mut client, &["LPUSH", "l", "a", "b", "c"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["LRANGE", "l", "0", "-1"]);
    expect_array(&mut client, &["c", "b", "a"]);
    send(&mut client, &["RPUSH", "l", "d"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["LRANGE", "l", "0", "-1"]);
    expect_array(&mut client, &["c", "b", "a", "d"]);
    send(&mut client, &["LRANGE", "l", "1", "2"]);
    expect_array(&mut client, &["b", "a"]);
    send(&mut client, &["LRANGE", "l", "-2", "-1"]);
    expect_array(&mut client, &["a", "d"]);
    send(&mut client, &["LRANGE", "l", "-100", "1"]);
    expect_array(&mut client, &["c", "b"]);
    send(&mut client, &["LRANGE", "l", "0", "100"]);
    expect_array(&mut client, &["c", "b", "a", "d"]);
    send(&mut client, &["LRANGE", "l", "9", "10"]);
    expect_empty_array(&mut client);
    send(&mut client, &["LRANGE", "no-such-key", "0", "-1"]);
    expect_empty_array(&mut client);

    send(&mut client, &["LPOP", "l"]);
    expect_bulk(&mut client, Some("c"));
    send(&mut client, &["RPOP", "l"]);
    expect_bulk(&mut client, Some("d"));
    send(&mut client, &["LLEN", "l"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["LPOP", "l", "5"]);
    expect_array(&mut client, &["b", "a"]);
    send(&mut client, &["LLEN", "l"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["EXISTS", "l"]);
    assert_eq!(expect_integer(&mut client), 0);

    send(&mut client, &["LPOP", "no-such-key"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["RPOP", "no-such-key"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["LPOP", "no-such-key", "2"]);
    expect_null_array(&mut client);
    send(&mut client, &["LLEN", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);
}

/// Key-level commands work uniformly on the new types: `TYPE`, `EXISTS`, `DEL`,
/// `EXPIRE`/`TTL`/`PERSIST` and the `SET` write conditions.
#[test]
fn key_operations_cover_new_types() {
    let (mut client, _server) = memory_server("redis-key-ops");
    send(&mut client, &["HSET", "h", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TYPE", "h"]);
    expect_status(&mut client, "hash");
    send(&mut client, &["EXISTS", "h", "h"]); // duplicates count twice, like upstream
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["EXPIRE", "h", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TTL", "h"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "unexpected hash TTL: {ttl}");
    send(&mut client, &["PERSIST", "h"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TTL", "h"]);
    assert_eq!(expect_integer(&mut client), -1);
    send(&mut client, &["HGET", "h", "f"]);
    expect_bulk(&mut client, Some("v"));

    send(&mut client, &["LPUSH", "l", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TYPE", "l"]);
    expect_status(&mut client, "list");
    send(&mut client, &["EXPIRE", "l", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["PERSIST", "l"]);
    assert_eq!(expect_integer(&mut client), 1);

    // `DEL` removes any type and counts each logical key once, ignoring duplicates.
    send(&mut client, &["DEL", "h", "l", "h", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["EXISTS", "h", "l"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["DEL", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);

    // Unconditional `SET` replaces a collection, `SET NX` sees it as present and
    // `SET XX` replaces it as upstream does.
    send(&mut client, &["HSET", "k", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SET", "k", "nx", "NX"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["SET", "k", "xx", "XX"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["GET", "k"]);
    expect_bulk(&mut client, Some("xx"));
    send(&mut client, &["TYPE", "k"]);
    expect_status(&mut client, "string");
    send(&mut client, &["HSET", "k", "f", "v"]);
    expect_error(&mut client, WRONGTYPE);

    // `SETEX` also replaces a list, leaving no stale list representation.
    send(&mut client, &["LPUSH", "lx", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SETEX", "lx", "5", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["TYPE", "lx"]);
    expect_status(&mut client, "string");
    send(&mut client, &["LRANGE", "lx", "0", "-1"]);
    expect_error(&mut client, WRONGTYPE);
}

/// Reopening the same `.yykv` snapshot restores both the type marker (encoded in the
/// record namespace) and the full contents.
#[test]
fn restart_preserves_types_and_contents() {
    let path = temp_path("collections");
    let (mut first, _first_server) = file_server(&path);
    send(&mut first, &["SET", "s", "v"]);
    expect_status(&mut first, "OK");
    send(&mut first, &["HSET", "h", "f1", "v1", "f2", "v2"]);
    assert_eq!(expect_integer(&mut first), 2);
    send(&mut first, &["LPUSH", "l", "a", "b"]);
    assert_eq!(expect_integer(&mut first), 2);

    let (mut second, _second_server) = file_server(&path);
    send(&mut second, &["TYPE", "s"]);
    expect_status(&mut second, "string");
    send(&mut second, &["TYPE", "h"]);
    expect_status(&mut second, "hash");
    send(&mut second, &["TYPE", "l"]);
    expect_status(&mut second, "list");
    send(&mut second, &["GET", "s"]);
    expect_bulk(&mut second, Some("v"));
    send(&mut second, &["HGETALL", "h"]);
    expect_array(&mut second, &["f1", "v1", "f2", "v2"]);
    send(&mut second, &["LRANGE", "l", "0", "-1"]);
    expect_array(&mut second, &["b", "a"]);
    send(&mut second, &["GET", "h"]);
    expect_error(&mut second, WRONGTYPE);
}
