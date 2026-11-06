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

/// `RENAME` / `RENAMENX` move any value type together with its TTL and overwrite a
/// destination of a different type without leaving a stale representation behind.
#[test]
fn rename_any_type_and_overwrite() {
    // `RENAME` 依赖分片的持久原子提交边界，因此跑在 `FileShard` 上。
    let path = temp_path("rename");
    let (mut client, _server) = file_server(&path);
    send(&mut client, &["SET", "src", "v", "EX", "100"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["RENAME", "src", "dst"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["GET", "dst"]);
    expect_bulk(&mut client, Some("v"));
    send(&mut client, &["TTL", "dst"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "RENAME dropped the TTL: {ttl}");
    send(&mut client, &["EXISTS", "src"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["TYPE", "dst"]);
    expect_status(&mut client, "string");

    send(&mut client, &["HSET", "h", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["RENAME", "h", "h2"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["TYPE", "h2"]);
    expect_status(&mut client, "hash");
    send(&mut client, &["HGETALL", "h2"]);
    expect_array(&mut client, &["f", "v"]);
    send(&mut client, &["TYPE", "h"]);
    expect_status(&mut client, "none");

    send(&mut client, &["LPUSH", "l", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["RENAME", "l", "l2"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["TYPE", "l2"]);
    expect_status(&mut client, "list");
    send(&mut client, &["LRANGE", "l2", "0", "-1"]);
    expect_array(&mut client, &["a"]);

    send(&mut client, &["SADD", "s", "m", "n"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["RENAME", "s", "s2"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["TYPE", "s2"]);
    expect_status(&mut client, "set");
    send(&mut client, &["SMEMBERS", "s2"]);
    expect_array(&mut client, &["m", "n"]);

    send(&mut client, &["ZADD", "z", "1", "a", "2", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["RENAME", "z", "z2"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["TYPE", "z2"]);
    expect_status(&mut client, "zset");
    send(&mut client, &["ZRANGE", "z2", "0", "-1", "WITHSCORES"]);
    expect_array(&mut client, &["a", "1", "b", "2"]);

    // Overwriting a hash destination with a list leaves exactly one representation.
    send(&mut client, &["HSET", "hdst", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["LPUSH", "lsrc", "a", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["RENAME", "lsrc", "hdst"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["TYPE", "hdst"]);
    expect_status(&mut client, "list");
    send(&mut client, &["LRANGE", "hdst", "0", "-1"]);
    expect_array(&mut client, &["b", "a"]);
    send(&mut client, &["HGETALL", "hdst"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["EXISTS", "lsrc"]);
    assert_eq!(expect_integer(&mut client), 0);

    send(&mut client, &["SET", "a", "1"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SET", "b", "2"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["RENAMENX", "a", "b"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["GET", "b"]);
    expect_bulk(&mut client, Some("2"));
    send(&mut client, &["GET", "a"]);
    expect_bulk(&mut client, Some("1"));
    send(&mut client, &["RENAMENX", "a", "c"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["GET", "c"]);
    expect_bulk(&mut client, Some("1"));
    send(&mut client, &["EXISTS", "a"]);
    assert_eq!(expect_integer(&mut client), 0);

    send(&mut client, &["RENAME", "no-such-key", "target"]);
    expect_error(&mut client, "ERR no such key");
    send(&mut client, &["RENAMENX", "no-such-key", "target"]);
    expect_error(&mut client, "ERR no such key");

    // Renaming a key onto itself is a no-op for RENAME and a failed NX.
    send(&mut client, &["RENAME", "c", "c"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["RENAMENX", "c", "c"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["GET", "c"]);
    expect_bulk(&mut client, Some("1"));

    send(&mut client, &["DEL", "z2"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["EXISTS", "z2"]);
    assert_eq!(expect_integer(&mut client), 0);
}

/// `SETRANGE` / `GETRANGE` / `APPEND` / `STRLEN` cover negative indexes, silent
/// zero-padding, empty payloads and boundary offsets.
#[test]
fn string_range_append_and_strlen() {
    let (mut client, _server) = memory_server("redis-string-range");
    send(&mut client, &["SET", "k", "Hello World"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["STRLEN", "k"]);
    assert_eq!(expect_integer(&mut client), 11);
    send(&mut client, &["GETRANGE", "k", "0", "4"]);
    expect_bulk(&mut client, Some("Hello"));
    send(&mut client, &["GETRANGE", "k", "-5", "-1"]);
    expect_bulk(&mut client, Some("World"));
    send(&mut client, &["GETRANGE", "k", "0", "-1"]);
    expect_bulk(&mut client, Some("Hello World"));
    send(&mut client, &["GETRANGE", "k", "-100", "4"]);
    expect_bulk(&mut client, Some("Hello"));
    send(&mut client, &["GETRANGE", "k", "0", "100"]);
    expect_bulk(&mut client, Some("Hello World"));
    send(&mut client, &["GETRANGE", "k", "5", "3"]);
    expect_bulk(&mut client, Some(""));
    send(&mut client, &["GETRANGE", "k", "100", "200"]);
    expect_bulk(&mut client, Some(""));
    send(&mut client, &["GETRANGE", "no-such-key", "0", "-1"]);
    expect_bulk(&mut client, Some(""));
    send(&mut client, &["GETRANGE", "k", "not-a-number", "1"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");

    // Overwriting in place keeps the original length.
    send(&mut client, &["SETRANGE", "k", "6", "Redis"]);
    assert_eq!(expect_integer(&mut client), 11);
    send(&mut client, &["GET", "k"]);
    expect_bulk(&mut client, Some("Hello Redis"));
    // Offsets past the end are zero-padded, and the reply is the new length.
    send(&mut client, &["SET", "pad", "abc"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SETRANGE", "pad", "5", "xy"]);
    assert_eq!(expect_integer(&mut client), 7);
    send(&mut client, &["STRLEN", "pad"]);
    assert_eq!(expect_integer(&mut client), 7);
    send(&mut client, &["GETRANGE", "pad", "3", "4"]);
    expect_bulk(&mut client, Some("\0\0"));
    send(&mut client, &["GETRANGE", "pad", "5", "-1"]);
    expect_bulk(&mut client, Some("xy"));
    // An empty payload never creates a key and never changes an existing value.
    send(&mut client, &["SETRANGE", "no-such-key", "0", ""]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["EXISTS", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SETRANGE", "k", "0", ""]);
    assert_eq!(expect_integer(&mut client), 11);
    send(&mut client, &["GET", "k"]);
    expect_bulk(&mut client, Some("Hello Redis"));
    send(&mut client, &["SETRANGE", "k", "-1", "x"]);
    expect_error(&mut client, "ERR offset is out of range");
    send(&mut client, &["SETRANGE", "k", "abc", "x"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");

    // `APPEND` creates missing keys and replies the new length.
    send(&mut client, &["APPEND", "app", "abc"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["APPEND", "app", "def"]);
    assert_eq!(expect_integer(&mut client), 6);
    send(&mut client, &["GET", "app"]);
    expect_bulk(&mut client, Some("abcdef"));
    send(&mut client, &["APPEND", "app", ""]);
    assert_eq!(expect_integer(&mut client), 6);
    send(&mut client, &["STRLEN", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);

    // TTL survives in-place string edits.
    send(&mut client, &["EXPIRE", "k", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SETRANGE", "k", "0", "J"]);
    assert_eq!(expect_integer(&mut client), 11);
    send(&mut client, &["TTL", "k"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "SETRANGE dropped the TTL: {ttl}");
    send(&mut client, &["APPEND", "k", "!"]);
    assert_eq!(expect_integer(&mut client), 12);
    send(&mut client, &["TTL", "k"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "APPEND dropped the TTL: {ttl}");
    send(&mut client, &["PERSIST", "k"]);
    assert_eq!(expect_integer(&mut client), 1);

    // Key-level commands agree with the other value types.
    send(&mut client, &["TYPE", "k"]);
    expect_status(&mut client, "string");
    send(&mut client, &["EXISTS", "k", "app"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["DEL", "k"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["EXISTS", "k"]);
    assert_eq!(expect_integer(&mut client), 0);

    send(&mut client, &["HSET", "h", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    for arguments in [
        ["STRLEN", "h"].as_slice(),
        ["APPEND", "h", "x"].as_slice(),
        ["GETRANGE", "h", "0", "-1"].as_slice(),
        ["SETRANGE", "h", "0", "x"].as_slice(),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, WRONGTYPE);
    }
}

/// `INCRBYFLOAT` keeps Redis float formatting and rejects non-finite inputs, while
/// `INCR` / `DECR` reuse the overflow message of the KV layer.
#[test]
fn string_incrbyfloat_and_integer_overflow() {
    let (mut client, _server) = memory_server("redis-string-incr");
    send(&mut client, &["INCRBYFLOAT", "f", "10.5"]);
    expect_bulk(&mut client, Some("10.5"));
    send(&mut client, &["INCRBYFLOAT", "f", "0.1"]);
    expect_bulk(&mut client, Some("10.6"));
    send(&mut client, &["INCRBYFLOAT", "f", "-0.1"]);
    expect_bulk(&mut client, Some("10.5"));
    send(&mut client, &["INCRBYFLOAT", "missing", "5.0e3"]);
    expect_bulk(&mut client, Some("5000"));
    send(&mut client, &["TYPE", "missing"]);
    expect_status(&mut client, "string");
    send(&mut client, &["INCRBYFLOAT", "f", "0"]);
    expect_bulk(&mut client, Some("10.5"));

    send(&mut client, &["INCRBYFLOAT", "f", "+inf"]);
    expect_error(&mut client, "ERR increment would produce NaN or Infinity");
    send(&mut client, &["INCRBYFLOAT", "f", "-inf"]);
    expect_error(&mut client, "ERR increment would produce NaN or Infinity");
    send(&mut client, &["INCRBYFLOAT", "f", "nan"]);
    expect_error(&mut client, "ERR value is not a valid float");
    send(&mut client, &["INCRBYFLOAT", "f", "abc"]);
    expect_error(&mut client, "ERR value is not a valid float");
    send(&mut client, &["INCRBYFLOAT", "f", "1"]);
    expect_bulk(&mut client, Some("11.5"));

    // A non-numeric stored value is rejected without being overwritten.
    send(&mut client, &["SET", "bad", "abc"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["INCRBYFLOAT", "bad", "1"]);
    expect_error(&mut client, "ERR value is not a valid float");
    send(&mut client, &["GET", "bad"]);
    expect_bulk(&mut client, Some("abc"));

    // TTL survives `INCRBYFLOAT` just like the integer counters.
    send(&mut client, &["EXPIRE", "f", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["INCRBYFLOAT", "f", "1"]);
    expect_bulk(&mut client, Some("12.5"));
    send(&mut client, &["TTL", "f"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "INCRBYFLOAT dropped the TTL: {ttl}");

    // Integer reply types and overflow messages.
    send(&mut client, &["INCR", "n"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["DECR", "n"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SET", "n", "9223372036854775807"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["INCR", "n"]);
    expect_error(&mut client, "ERR increment or decrement would overflow");
    send(&mut client, &["SET", "n", "-9223372036854775808"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["DECR", "n"]);
    expect_error(&mut client, "ERR increment or decrement would overflow");

    send(&mut client, &["HSET", "h", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["INCRBYFLOAT", "h", "1"]);
    expect_error(&mut client, WRONGTYPE);
}

/// `SETBIT` / `GETBIT` / `BITCOUNT` / `BITPOS` follow Redis bit ordering, zero padding,
/// empty keys and the byte/bit range units.
#[test]
fn bit_operations() {
    let (mut client, _server) = memory_server("redis-bits");
    send(&mut client, &["SETBIT", "bits", "7", "1"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["GETBIT", "bits", "7"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["GETBIT", "bits", "0"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["GETBIT", "no-such-key", "0"]);
    assert_eq!(expect_integer(&mut client), 0);
    // The first byte is 0x01, so the value survives as a string of length 1.
    send(&mut client, &["STRLEN", "bits"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SETBIT", "bits", "7", "0"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["GETBIT", "bits", "7"]);
    assert_eq!(expect_integer(&mut client), 0);
    // Writing a high offset zero-fills every byte before it.
    send(&mut client, &["SETBIT", "bits", "100", "1"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["GETBIT", "bits", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["STRLEN", "bits"]);
    assert_eq!(expect_integer(&mut client), 13);
    send(&mut client, &["SETBIT", "bits", "abc", "1"]);
    expect_error(&mut client, "ERR bit offset is not an integer or out of range");
    send(&mut client, &["SETBIT", "bits", "-1", "1"]);
    expect_error(&mut client, "ERR bit offset is not an integer or out of range");
    send(&mut client, &["SETBIT", "bits", "0", "2"]);
    expect_error(&mut client, "ERR bit is not an integer or out of range");
    send(&mut client, &["GETBIT", "bits", "not-a-number"]);
    expect_error(&mut client, "ERR bit offset is not an integer or out of range");

    // `BITCOUNT` on a known payload, in both byte and bit units.
    send(&mut client, &["SET", "be", "foobar"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["BITCOUNT", "be"]);
    assert_eq!(expect_integer(&mut client), 26);
    send(&mut client, &["BITCOUNT", "be", "0", "0"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["BITCOUNT", "be", "1", "1"]);
    assert_eq!(expect_integer(&mut client), 6);
    send(&mut client, &["BITCOUNT", "be", "0", "-5"]);
    assert_eq!(expect_integer(&mut client), 10);
    send(&mut client, &["BITCOUNT", "be", "-2", "-1"]);
    assert_eq!(expect_integer(&mut client), 7);
    send(&mut client, &["BITCOUNT", "be", "0", "5", "BYTE"]);
    assert_eq!(expect_integer(&mut client), 26);
    send(&mut client, &["BITCOUNT", "be", "0", "7", "BIT"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["BITCOUNT", "be", "8", "15", "BIT"]);
    assert_eq!(expect_integer(&mut client), 6);
    send(&mut client, &["BITCOUNT", "be", "100", "200", "BYTE"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["BITCOUNT", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["BITCOUNT", "be", "0", "0", "BOGUS"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["BITCOUNT", "be", "not-a-number", "1"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");

    // Three bytes: 0xff, 0x00, 0xf0.
    for bit in 0..8 {
        let offset = bit.to_string();
        send(&mut client, &["SETBIT", "bp", &offset, "1"]);
        assert_eq!(expect_integer(&mut client), 0);
    }
    for bit in 16..20 {
        let offset = bit.to_string();
        send(&mut client, &["SETBIT", "bp", &offset, "1"]);
        assert_eq!(expect_integer(&mut client), 0);
    }
    send(&mut client, &["BITPOS", "bp", "1"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["BITPOS", "bp", "0"]);
    assert_eq!(expect_integer(&mut client), 8);
    send(&mut client, &["BITPOS", "bp", "0", "1"]);
    assert_eq!(expect_integer(&mut client), 8);
    send(&mut client, &["BITPOS", "bp", "1", "2"]);
    assert_eq!(expect_integer(&mut client), 16);
    send(&mut client, &["BITPOS", "bp", "0", "2"]);
    assert_eq!(expect_integer(&mut client), 20);
    send(&mut client, &["BITPOS", "bp", "1", "2", "2"]);
    assert_eq!(expect_integer(&mut client), 16);
    send(&mut client, &["BITPOS", "bp", "0", "8", "23", "BIT"]);
    assert_eq!(expect_integer(&mut client), 8);
    send(&mut client, &["BITPOS", "bp", "1", "0", "-1"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["BITPOS", "bp", "1", "2", "-1"]);
    assert_eq!(expect_integer(&mut client), 16);
    // Out-of-range starts clamp to the last unit, negative ends clamp to the first.
    send(&mut client, &["BITPOS", "bp", "0", "100"]);
    assert_eq!(expect_integer(&mut client), 20);
    send(&mut client, &["BITPOS", "bp", "0", "0", "-100"]);
    assert_eq!(expect_integer(&mut client), -1);
    // Missing keys are treated as infinite zero strings, ignoring any range.
    send(&mut client, &["BITPOS", "no-such-key", "0"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["BITPOS", "no-such-key", "0", "0", "-1"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["BITPOS", "no-such-key", "1"]);
    assert_eq!(expect_integer(&mut client), -1);
    // An existing empty string is a range with no bits at all.
    send(&mut client, &["SET", "empty", ""]);
    expect_status(&mut client, "OK");
    send(&mut client, &["BITPOS", "empty", "0"]);
    assert_eq!(expect_integer(&mut client), -1);
    // All-ones without an explicit end reports the first bit past the string.
    for bit in 0..8 {
        let offset = bit.to_string();
        send(&mut client, &["SETBIT", "ones", &offset, "1"]);
        assert_eq!(expect_integer(&mut client), 0);
    }
    send(&mut client, &["BITPOS", "ones", "0"]);
    assert_eq!(expect_integer(&mut client), 8);
    send(&mut client, &["BITPOS", "ones", "0", "0", "-1"]);
    assert_eq!(expect_integer(&mut client), -1);
    send(&mut client, &["BITPOS", "bp", "2"]);
    expect_error(&mut client, "ERR The bit argument must be 1 or 0");
    send(&mut client, &["BITPOS", "bp", "0", "0", "5", "BOGUS"]);
    expect_error(&mut client, "ERR syntax error");

    send(&mut client, &["HSET", "h", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    for arguments in [
        ["SETBIT", "h", "0", "1"].as_slice(),
        ["GETBIT", "h", "0"].as_slice(),
        ["BITCOUNT", "h"].as_slice(),
        ["BITPOS", "h", "0"].as_slice(),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, WRONGTYPE);
    }
    send(&mut client, &["TYPE", "bp"]);
    expect_status(&mut client, "string");
    send(&mut client, &["EXISTS", "bp", "bits", "be"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["DEL", "bp", "bits"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["EXPIRE", "be", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TTL", "be"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "unexpected string TTL: {ttl}");
}

fn read_bulk_text(client: &mut TcpStream) -> Option<String> {
    match read_reply(client) {
        Reply::Bulk(value) => value.map(|bytes| String::from_utf8(bytes).unwrap()),
        other => panic!("expected bulk, got {}", describe(&other)),
    }
}

fn read_array_text(client: &mut TcpStream) -> Vec<String> {
    match read_reply(client) {
        Reply::Array(Some(items)) => items
            .into_iter()
            .map(|item| match item {
                Reply::Bulk(Some(bytes)) => String::from_utf8(bytes).unwrap(),
                other => panic!("expected bulk array element, got {}", describe(&other)),
            })
            .collect(),
        other => panic!("expected array, got {}", describe(&other)),
    }
}

/// `SINTER` / `SUNION` / `SDIFF` cover missing keys, empty results and wrong types,
/// while the `*STORE` variants overwrite the destination and drop its old TTL.
#[test]
fn set_algebra_and_store_variants() {
    let (mut client, _server) = memory_server("redis-set-algebra");
    send(&mut client, &["SADD", "a", "1", "2", "3"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["SADD", "b", "2", "3", "4"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["SADD", "c", "3", "5"]);
    assert_eq!(expect_integer(&mut client), 2);

    send(&mut client, &["SINTER", "a", "b"]);
    expect_array(&mut client, &["2", "3"]);
    send(&mut client, &["SUNION", "a", "b", "c"]);
    expect_array(&mut client, &["1", "2", "3", "4", "5"]);
    send(&mut client, &["SDIFF", "a", "b"]);
    expect_array(&mut client, &["1"]);
    send(&mut client, &["SDIFF", "a", "b", "c"]);
    expect_array(&mut client, &["1"]);

    // Missing keys behave as empty sets.
    send(&mut client, &["SINTER", "a", "no-such-key"]);
    expect_empty_array(&mut client);
    send(&mut client, &["SUNION", "a", "no-such-key"]);
    expect_array(&mut client, &["1", "2", "3"]);
    send(&mut client, &["SDIFF", "a", "no-such-key"]);
    expect_array(&mut client, &["1", "2", "3"]);
    send(&mut client, &["SDIFF", "no-such-key", "a"]);
    expect_empty_array(&mut client);

    // Any wrong-typed source rejects the whole command.
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    for arguments in [
        ["SINTER", "a", "str"].as_slice(),
        ["SUNION", "a", "str"].as_slice(),
        ["SDIFF", "a", "str"].as_slice(),
        ["SINTERSTORE", "dst", "a", "str"].as_slice(),
        ["SUNIONSTORE", "dst", "a", "str"].as_slice(),
        ["SDIFFSTORE", "dst", "a", "str"].as_slice(),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, WRONGTYPE);
    }

    // Store variants count the result and write it as a set, replacing a string
    // destination and dropping its TTL.
    send(&mut client, &["SET", "dst", "old", "EX", "100"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SINTERSTORE", "dst", "a", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["TYPE", "dst"]);
    expect_status(&mut client, "set");
    send(&mut client, &["SMEMBERS", "dst"]);
    expect_array(&mut client, &["2", "3"]);
    send(&mut client, &["TTL", "dst"]);
    assert_eq!(expect_integer(&mut client), -1, "STORE must not inherit the destination TTL");
    send(&mut client, &["SUNIONSTORE", "dst", "a", "c"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["SDIFFSTORE", "dst", "a", "b"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SMEMBERS", "dst"]);
    expect_array(&mut client, &["1"]);

    // An empty result deletes the destination key entirely.
    send(&mut client, &["SADD", "dst", "keep"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SADD", "d", "7"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SINTERSTORE", "dst", "a", "d"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["EXISTS", "dst"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["TYPE", "dst"]);
    expect_status(&mut client, "none");

    send(&mut client, &["SINTERSTORE", "dst", "no-such-key", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["EXISTS", "dst"]);
    assert_eq!(expect_integer(&mut client), 0);
}

/// `SMOVE` moves one member, deletes an emptied source, ignores a missing member and
/// rejects a wrong-typed destination; `SRANDMEMBER` honours the count sign.
#[test]
fn set_move_and_random_member() {
    let (mut client, _server) = memory_server("redis-set-move-random");
    send(&mut client, &["SADD", "s1", "a", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["SADD", "s2", "c"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SMOVE", "s1", "s2", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SMEMBERS", "s1"]);
    expect_array(&mut client, &["b"]);
    send(&mut client, &["SMEMBERS", "s2"]);
    expect_array(&mut client, &["c", "a"]);
    send(&mut client, &["SMOVE", "s1", "s2", "missing"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SMOVE", "no-such-key", "s2", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SMOVE", "s1", "s1", "b"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SMOVE", "s1", "s1", "gone"]);
    assert_eq!(expect_integer(&mut client), 0);
    // Moving the last member deletes the source.
    send(&mut client, &["SMOVE", "s1", "s2", "b"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["EXISTS", "s1"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SMOVE", "s2", "str", "a"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["SMEMBERS", "s2"]);
    expect_array(&mut client, &["c", "a", "b"]);

    send(&mut client, &["SADD", "r", "x", "y", "z"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["SRANDMEMBER", "r"]);
    let picked = read_bulk_text(&mut client).expect("SRANDMEMBER without count must return a member");
    assert!(["x", "y", "z"].contains(&picked.as_str()));
    send(&mut client, &["SRANDMEMBER", "r", "2"]);
    let distinct = read_array_text(&mut client);
    assert_eq!(distinct.len(), 2);
    let mut unique = distinct.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 2, "positive count must not repeat members");
    send(&mut client, &["SRANDMEMBER", "r", "10"]);
    let capped = read_array_text(&mut client);
    let mut sorted = capped.clone();
    sorted.sort();
    assert_eq!(sorted, ["x", "y", "z"]);
    send(&mut client, &["SRANDMEMBER", "r", "0"]);
    expect_empty_array(&mut client);
    send(&mut client, &["SRANDMEMBER", "r", "-5"]);
    let repeated = read_array_text(&mut client);
    assert_eq!(repeated.len(), 5);
    assert!(repeated.iter().all(|member| ["x", "y", "z"].contains(&member.as_str())));
    send(&mut client, &["SRANDMEMBER", "no-such-key"]);
    assert_eq!(read_bulk_text(&mut client), None);
    send(&mut client, &["SRANDMEMBER", "no-such-key", "3"]);
    expect_empty_array(&mut client);
    send(&mut client, &["SRANDMEMBER", "r", "not-a-number"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");
    send(&mut client, &["SRANDMEMBER", "str"]);
    expect_error(&mut client, WRONGTYPE);
}

/// `SSCAN` walks a set in pages, honours `MATCH` and ends with cursor `0`.
#[test]
fn set_scan_pages_and_match() {
    let (mut client, _server) = memory_server("redis-sscan");
    send(&mut client, &["SADD", "s", "a1", "a2", "b1", "b2"]);
    assert_eq!(expect_integer(&mut client), 4);
    let mut cursor = "0".to_string();
    let mut seen = Vec::new();
    loop {
        send(&mut client, &["SSCAN", "s", &cursor, "COUNT", "2"]);
        let (next, members) = expect_scan(&mut client);
        seen.extend(members);
        if next == "0" {
            break;
        }
        cursor = next;
    }
    seen.sort();
    assert_eq!(seen, ["a1", "a2", "b1", "b2"]);
    send(&mut client, &["SSCAN", "s", "0", "MATCH", "a*", "COUNT", "100"]);
    let (cursor, members) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert_eq!(members, ["a1", "a2"]);
    send(&mut client, &["SSCAN", "no-such-key", "0"]);
    let (cursor, members) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert!(members.is_empty());
    send(&mut client, &["SSCAN", "s", "not-a-cursor"]);
    expect_error(&mut client, "ERR invalid cursor");
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SSCAN", "str", "0"]);
    expect_error(&mut client, WRONGTYPE);
}

/// Set algebra results and moves survive a `FileShard` reopen with their TTLs.
#[test]
fn set_algebra_survives_reopen() {
    let path = temp_path("set-algebra");
    let (mut first, _first_server) = file_server(&path);
    send(&mut first, &["SADD", "a", "1", "2", "3"]);
    assert_eq!(expect_integer(&mut first), 3);
    send(&mut first, &["SADD", "b", "2", "3", "4"]);
    assert_eq!(expect_integer(&mut first), 3);
    send(&mut first, &["SINTERSTORE", "dst", "a", "b"]);
    assert_eq!(expect_integer(&mut first), 2);
    send(&mut first, &["EXPIRE", "dst", "100"]);
    assert_eq!(expect_integer(&mut first), 1);
    send(&mut first, &["SADD", "src", "m", "n"]);
    assert_eq!(expect_integer(&mut first), 2);
    send(&mut first, &["SMOVE", "src", "dst", "m"]);
    assert_eq!(expect_integer(&mut first), 1);

    let (mut second, _second_server) = file_server(&path);
    send(&mut second, &["TYPE", "dst"]);
    expect_status(&mut second, "set");
    send(&mut second, &["SMEMBERS", "dst"]);
    expect_array(&mut second, &["2", "3", "m"]);
    send(&mut second, &["TTL", "dst"]);
    let ttl = expect_integer(&mut second);
    assert!((1..=100).contains(&ttl), "set TTL missing after reopen: {ttl}");
    send(&mut second, &["SMEMBERS", "src"]);
    expect_array(&mut second, &["n"]);
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
    send(&mut first, &["SET", "rn", "v"]);
    expect_status(&mut first, "OK");
    send(&mut first, &["RENAME", "rn", "rn2"]);
    expect_status(&mut first, "OK");

    // 字符串族的编辑结果同样落在同一份快照里。
    send(&mut first, &["SETRANGE", "range", "0", "abc"]);
    assert_eq!(expect_integer(&mut first), 3);
    send(&mut first, &["SETRANGE", "range", "4", "de"]);
    assert_eq!(expect_integer(&mut first), 6);
    send(&mut first, &["APPEND", "app", "hello"]);
    assert_eq!(expect_integer(&mut first), 5);
    send(&mut first, &["INCRBYFLOAT", "flt", "2.5"]);
    expect_bulk(&mut first, Some("2.5"));
    send(&mut first, &["SETBIT", "bitk", "3", "1"]);
    assert_eq!(expect_integer(&mut first), 0);
    // `LT` applies because a key without a deadline counts as infinite.
    send(&mut first, &["EXPIRE", "app", "100", "LT"]);
    assert_eq!(expect_integer(&mut first), 1);

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
    assert_eq!(keys, ["app", "bitk", "flt", "h", "l", "ps", "pz", "range", "rn2", "s"]);

    send(&mut second, &["TYPE", "range"]);
    expect_status(&mut second, "string");
    send(&mut second, &["STRLEN", "range"]);
    assert_eq!(expect_integer(&mut second), 6);
    send(&mut second, &["GETRANGE", "range", "0", "2"]);
    expect_bulk(&mut second, Some("abc"));
    send(&mut second, &["GETRANGE", "range", "4", "5"]);
    expect_bulk(&mut second, Some("de"));
    send(&mut second, &["STRLEN", "app"]);
    assert_eq!(expect_integer(&mut second), 5);
    send(&mut second, &["GET", "app"]);
    expect_bulk(&mut second, Some("hello"));
    send(&mut second, &["TTL", "app"]);
    let ttl = expect_integer(&mut second);
    assert!((1..=100).contains(&ttl), "string TTL missing after reopen: {ttl}");
    send(&mut second, &["INCRBYFLOAT", "flt", "0"]);
    expect_bulk(&mut second, Some("2.5"));
    send(&mut second, &["GETBIT", "bitk", "3"]);
    assert_eq!(expect_integer(&mut second), 1);
    send(&mut second, &["BITCOUNT", "bitk"]);
    assert_eq!(expect_integer(&mut second), 1);
    send(&mut second, &["GET", "rn2"]);
    expect_bulk(&mut second, Some("v"));
    send(&mut second, &["TYPE", "rn2"]);
    expect_status(&mut second, "string");
    send(&mut second, &["EXISTS", "rn"]);
    assert_eq!(expect_integer(&mut second), 0);
}
