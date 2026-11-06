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

fn expect_scan(client: &mut TcpStream) -> (String, Vec<String>) {
    match read_reply(client) {
        Reply::Array(Some(items)) => {
            assert_eq!(items.len(), 2, "SCAN replies with a cursor and a key list");
            let cursor = match &items[0] {
                Reply::Bulk(Some(bytes)) => String::from_utf8(bytes.clone()).unwrap(),
                other => panic!("expected bulk SCAN cursor, got {}", describe(other)),
            };
            let keys = match &items[1] {
                Reply::Array(Some(entries)) => entries
                    .iter()
                    .map(|entry| match entry {
                        Reply::Bulk(Some(bytes)) => String::from_utf8(bytes.clone()).unwrap(),
                        other => panic!("expected bulk SCAN key, got {}", describe(other)),
                    })
                    .collect(),
                other => panic!("expected SCAN key array, got {}", describe(other)),
            };
            (cursor, keys)
        }
        other => panic!("expected SCAN reply, got {}", describe(&other)),
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

/// `RPOP key count` pops from the tail and replies tail-first, while `LPOP key count`
/// replies head-first.
#[test]
fn list_pop_count_order() {
    let (mut client, _server) = memory_server("redis-list-pop-count");
    send(&mut client, &["RPUSH", "r", "a", "b", "c", "d"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["RPOP", "r", "2"]);
    expect_array(&mut client, &["d", "c"]);
    send(&mut client, &["LRANGE", "r", "0", "-1"]);
    expect_array(&mut client, &["a", "b"]);
    send(&mut client, &["LPOP", "r", "2"]);
    expect_array(&mut client, &["a", "b"]);
    send(&mut client, &["EXISTS", "r"]);
    assert_eq!(expect_integer(&mut client), 0);
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

/// `HINCRBY` starts missing fields at zero and rejects non-hash keys and non-integer
/// field values, while `LINDEX` supports negative indexes and out-of-range nils.
#[test]
fn hash_incrby_and_list_index() {
    let (mut client, _server) = memory_server("redis-hincrby-lindex");
    send(&mut client, &["HINCRBY", "h", "f", "5"]);
    assert_eq!(expect_integer(&mut client), 5);
    send(&mut client, &["HINCRBY", "h", "f", "-2"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["HINCRBY", "h", "missing", "7"]);
    assert_eq!(expect_integer(&mut client), 7);
    send(&mut client, &["TYPE", "h"]);
    expect_status(&mut client, "hash");
    send(&mut client, &["HSET", "h", "text", "abc"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["HINCRBY", "h", "text", "1"]);
    expect_error(&mut client, "ERR hash value is not an integer");
    send(&mut client, &["HINCRBY", "h", "f", "1.5"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");
    send(&mut client, &["SET", "s", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["HINCRBY", "s", "f", "1"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["EXISTS", "h"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["EXPIRE", "h", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TTL", "h"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "unexpected hash TTL: {ttl}");
    send(&mut client, &["PERSIST", "h"]);
    assert_eq!(expect_integer(&mut client), 1);

    send(&mut client, &["RPUSH", "l", "a", "b", "c"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["LINDEX", "l", "0"]);
    expect_bulk(&mut client, Some("a"));
    send(&mut client, &["LINDEX", "l", "-1"]);
    expect_bulk(&mut client, Some("c"));
    send(&mut client, &["LINDEX", "l", "-3"]);
    expect_bulk(&mut client, Some("a"));
    send(&mut client, &["LINDEX", "l", "3"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["LINDEX", "l", "-4"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["LINDEX", "no-such-key", "0"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["LINDEX", "l", "not-a-number"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");
    send(&mut client, &["HINCRBY", "l", "f", "1"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["TYPE", "l"]);
    expect_status(&mut client, "list");
    send(&mut client, &["DEL", "h", "l"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["EXISTS", "h", "l"]);
    assert_eq!(expect_integer(&mut client), 0);
}

/// Set members, cross-type rejection, key-level commands and restart persistence.
#[test]
fn set_members_and_key_ops() {
    let (mut client, _server) = memory_server("redis-set");
    send(&mut client, &["SADD", "s", "a", "b", "c"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["SADD", "s", "a", "d"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SADD", "s", "a", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SCARD", "s"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["SISMEMBER", "s", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SISMEMBER", "s", "missing"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SMEMBERS", "s"]);
    expect_array(&mut client, &["a", "b", "c", "d"]);
    send(&mut client, &["SMEMBERS", "no-such-key"]);
    expect_empty_array(&mut client);
    send(&mut client, &["SCARD", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SISMEMBER", "no-such-key", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SREM", "no-such-key", "a"]);
    assert_eq!(expect_integer(&mut client), 0);

    // Duplicate names count once, and deleting every member removes the key.
    send(&mut client, &["SREM", "s", "a", "a", "c"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["SCARD", "s"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["SREM", "s", "b", "d"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["EXISTS", "s"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["TYPE", "s"]);
    expect_status(&mut client, "none");

    // Cross-type reads and writes reject each other.
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SADD", "str", "m"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["HSET", "h", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SADD", "h", "m"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["SADD", "s2", "m"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["HGET", "s2", "f"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["LPUSH", "s2", "x"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["GET", "s2"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["TYPE", "s2"]);
    expect_status(&mut client, "set");

    send(&mut client, &["EXPIRE", "s2", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TTL", "s2"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "unexpected set TTL: {ttl}");
    send(&mut client, &["SADD", "s2", "n"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TTL", "s2"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "SADD dropped the set TTL: {ttl}");
    send(&mut client, &["PERSIST", "s2"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["DEL", "s2"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["EXISTS", "s2"]);
    assert_eq!(expect_integer(&mut client), 0);
}

/// Sorted-set ordering, score updates, `WITHSCORES`, tie-breaking and key operations.
#[test]
fn zset_order_and_key_ops() {
    let (mut client, _server) = memory_server("redis-zset");
    send(&mut client, &["ZADD", "z", "1", "a", "2", "b", "2", "c"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["ZADD", "z", "5", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZSCORE", "z", "a"]);
    expect_bulk(&mut client, Some("5"));
    send(&mut client, &["ZSCORE", "z", "missing"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["ZSCORE", "no-such-key", "a"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["ZCARD", "z"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["ZCARD", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZRANGE", "z", "0", "-1"]);
    expect_array(&mut client, &["b", "c", "a"]);
    send(&mut client, &["ZRANGE", "z", "0", "-1", "WITHSCORES"]);
    expect_array(&mut client, &["b", "2", "c", "2", "a", "5"]);
    send(&mut client, &["ZRANGE", "z", "-2", "-1"]);
    expect_array(&mut client, &["c", "a"]);
    send(&mut client, &["ZRANGE", "z", "9", "10"]);
    expect_empty_array(&mut client);
    send(&mut client, &["ZRANGE", "no-such-key", "0", "-1"]);
    expect_empty_array(&mut client);

    // A bad score rejects the whole call without touching the stored set.
    send(&mut client, &["ZADD", "z", "not-a-float", "m"]);
    expect_error(&mut client, "ERR value is not a valid float");
    send(&mut client, &["ZCARD", "z"]);
    assert_eq!(expect_integer(&mut client), 3);

    send(&mut client, &["ZREM", "z", "b", "b"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZREM", "z", "c", "a"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["EXISTS", "z"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["TYPE", "z"]);
    expect_status(&mut client, "none");
    send(&mut client, &["ZREM", "no-such-key", "a"]);
    assert_eq!(expect_integer(&mut client), 0);

    // Cross-type rejection and key-level commands.
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["ZADD", "str", "1", "m"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["ZADD", "z2", "1", "m"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["GET", "z2"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["SADD", "z2", "x"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["ZSCORE", "z2", "m"]);
    expect_bulk(&mut client, Some("1"));
    send(&mut client, &["TYPE", "z2"]);
    expect_status(&mut client, "zset");
    send(&mut client, &["EXPIRE", "z2", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TTL", "z2"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "unexpected zset TTL: {ttl}");
    send(&mut client, &["ZADD", "z2", "2", "n"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TTL", "z2"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "ZADD dropped the zset TTL: {ttl}");
    send(&mut client, &["PERSIST", "z2"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["DEL", "z2"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["EXISTS", "z2"]);
    assert_eq!(expect_integer(&mut client), 0);
}

/// `SCAN` walks every value type in batches, filters with `MATCH` and ends with cursor
/// `0`; the cursor is opaque but must round-trip as a decimal string.
#[test]
fn scan_cursor_match_and_count() {
    let (mut client, _server) = memory_server("redis-scan");
    send(&mut client, &["SET", "s1", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SET", "s2", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["HSET", "h1", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["LPUSH", "l1", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SADD", "set1", "m"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZADD", "z1", "1", "m"]);
    assert_eq!(expect_integer(&mut client), 1);

    let mut cursor = "0".to_string();
    let mut seen = Vec::new();
    let mut rounds = 0;
    loop {
        send(&mut client, &["SCAN", &cursor, "COUNT", "2"]);
        let (next, keys) = expect_scan(&mut client);
        seen.extend(keys);
        rounds += 1;
        assert!(rounds <= 10, "SCAN did not terminate");
        if next == "0" {
            break;
        }
        cursor = next;
    }
    seen.sort();
    assert_eq!(seen, ["h1", "l1", "s1", "s2", "set1", "z1"]);

    send(&mut client, &["SCAN", "0", "MATCH", "s*", "COUNT", "100"]);
    let (cursor, keys) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert_eq!(keys, ["s1", "s2", "set1"]);
    send(&mut client, &["SCAN", "0", "MATCH", "h?", "COUNT", "100"]);
    let (cursor, keys) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert_eq!(keys, ["h1"]);
    send(&mut client, &["SCAN", "0", "MATCH", "nomatch*", "COUNT", "100"]);
    let (cursor, keys) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert!(keys.is_empty(), "unexpected MATCH hits: {keys:?}");

    send(&mut client, &["SCAN", "not-a-cursor"]);
    expect_error(&mut client, "ERR invalid cursor");
    send(&mut client, &["SCAN", "0", "COUNT", "0"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["SCAN", "0", "BOGUS", "x"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["SCAN", "0", "MATCH"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["SCAN"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'scan' command");
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
    send(&mut first, &["HINCRBY", "h", "f3", "9"]);
    assert_eq!(expect_integer(&mut first), 9);
    send(&mut first, &["SADD", "ps", "x", "y"]);
    assert_eq!(expect_integer(&mut first), 2);
    send(&mut first, &["ZADD", "pz", "2", "b", "1", "a"]);
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
    expect_array(&mut second, &["f1", "v1", "f2", "v2", "f3", "9"]);
    send(&mut second, &["LRANGE", "l", "0", "-1"]);
    expect_array(&mut second, &["b", "a"]);
    send(&mut second, &["LINDEX", "l", "-1"]);
    expect_bulk(&mut second, Some("a"));
    send(&mut second, &["GET", "h"]);
    expect_error(&mut second, WRONGTYPE);
    send(&mut second, &["TYPE", "ps"]);
    expect_status(&mut second, "set");
    send(&mut second, &["SMEMBERS", "ps"]);
    expect_array(&mut second, &["x", "y"]);
    send(&mut second, &["SCARD", "ps"]);
    assert_eq!(expect_integer(&mut second), 2);
    send(&mut second, &["TYPE", "pz"]);
    expect_status(&mut second, "zset");
    send(&mut second, &["ZRANGE", "pz", "0", "-1", "WITHSCORES"]);
    expect_array(&mut second, &["a", "1", "b", "2"]);
    send(&mut second, &["ZCARD", "pz"]);
    assert_eq!(expect_integer(&mut second), 2);
    send(&mut second, &["SCAN", "0", "COUNT", "100"]);
    let (cursor, keys) = expect_scan(&mut second);
    assert_eq!(cursor, "0");
    assert_eq!(keys, ["h", "l", "ps", "pz", "s"]);
}
