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

fn expect_integer_array(client: &mut TcpStream, expected: &[i64]) {
    match read_reply(client) {
        Reply::Array(Some(items)) => {
            let values = items
                .into_iter()
                .map(|item| match item {
                    Reply::Integer(value) => value,
                    other => panic!("expected integer array element, got {}", describe(&other)),
                })
                .collect::<Vec<i64>>();
            assert_eq!(values, expected);
        }
        other => panic!("expected integer array {expected:?}, got {}", describe(&other)),
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

/// `LMPOP` scans keys in order, honours `LEFT`/`RIGHT` and `COUNT`, removes the
/// key once it empties, and reports every argument error with the server wording.
#[test]
fn list_mpop_scans_and_counts() {
    let (mut client, _server) = memory_server("redis-list-mpop");
    let read_mpop = |client: &mut TcpStream| -> Option<(String, Vec<String>)> {
        match read_reply(client) {
            Reply::Array(None) => None,
            Reply::Array(Some(mut items)) if items.len() == 2 => {
                let elements = match items.pop().unwrap() {
                    Reply::Array(Some(elements)) => elements
                        .into_iter()
                        .map(|item| match item {
                            Reply::Bulk(Some(bytes)) => String::from_utf8(bytes).unwrap(),
                            other => panic!("expected bulk element, got {}", describe(&other)),
                        })
                        .collect(),
                    other => panic!("expected element array, got {}", describe(&other)),
                };
                let key = match items.pop().unwrap() {
                    Reply::Bulk(Some(bytes)) => String::from_utf8(bytes).unwrap(),
                    other => panic!("expected key bulk, got {}", describe(&other)),
                };
                Some((key, elements))
            }
            other => panic!("expected LMPOP reply, got {}", describe(&other)),
        }
    };

    send(&mut client, &["LMPOP", "2", "a", "b", "LEFT"]);
    assert_eq!(read_mpop(&mut client), None);
    send(&mut client, &["RPUSH", "a", "a1", "a2"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["RPUSH", "b", "b1", "b2", "b3"]);
    assert_eq!(expect_integer(&mut client), 3);

    send(&mut client, &["LMPOP", "2", "a", "b", "LEFT"]);
    assert_eq!(read_mpop(&mut client), Some(("a".into(), vec!["a1".into()])));
    send(&mut client, &["LMPOP", "2", "a", "b", "RIGHT", "COUNT", "5"]);
    assert_eq!(read_mpop(&mut client), Some(("a".into(), vec!["a2".into()])));
    send(&mut client, &["EXISTS", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["LMPOP", "2", "a", "b", "RIGHT", "COUNT", "2"]);
    assert_eq!(read_mpop(&mut client), Some(("b".into(), vec!["b3".into(), "b2".into()])));
    send(&mut client, &["LRANGE", "b", "0", "-1"]);
    expect_array(&mut client, &["b1"]);

    send(&mut client, &["LMPOP", "2", "a", "b", "LEFT"]);
    assert_eq!(read_mpop(&mut client), Some(("b".into(), vec!["b1".into()])));
    send(&mut client, &["EXISTS", "b"]);
    assert_eq!(expect_integer(&mut client), 0);

    send(&mut client, &["LMPOP", "0", "a", "LEFT"]);
    expect_error(&mut client, "ERR numkeys should be greater than 0");
    send(&mut client, &["LMPOP", "abc", "a", "LEFT"]);
    expect_error(&mut client, "ERR numkeys should be greater than 0");
    send(&mut client, &["LMPOP", "3", "a", "b", "LEFT"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["LMPOP", "1", "a", "MIDDLE"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["LMPOP", "1", "a", "LEFT", "COUNT", "0"]);
    expect_error(&mut client, "ERR count should be greater than 0");
    send(&mut client, &["LMPOP", "1", "a", "LEFT", "COUNT", "abc"]);
    expect_error(&mut client, "ERR count should be greater than 0");
    send(&mut client, &["SET", "s", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["LMPOP", "2", "a", "s", "LEFT"]);
    expect_error(&mut client, WRONGTYPE);
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
    expect_error(&mut client, "ERR The bit argument must be 1 or 0.");
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

/// `SPOP` removes random members, `SMISMEMBER` reports per-member presence and
/// `SINTERCARD` counts an intersection with an optional `LIMIT`.
#[test]
fn set_pop_mismember_and_intercard() {
    let (mut client, _server) = memory_server("redis-set-pop-mismember-intercard");
    send(&mut client, &["SADD", "s", "a", "b", "c"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["SADD", "t", "b", "c", "d"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");

    send(&mut client, &["SMISMEMBER", "s", "a", "ghost", "c"]);
    expect_integer_array(&mut client, &[1, 0, 1]);
    send(&mut client, &["SMISMEMBER", "no-such-key", "a", "b"]);
    expect_integer_array(&mut client, &[0, 0]);
    send(&mut client, &["SMISMEMBER", "str", "a"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["SMISMEMBER", "s"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'smismember' command");

    send(&mut client, &["SINTERCARD", "2", "s", "t"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["SINTERCARD", "2", "s", "t", "LIMIT", "1"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SINTERCARD", "2", "s", "t", "LIMIT", "0"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["SINTERCARD", "1", "s"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["SINTERCARD", "2", "s", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SINTERCARD", "1", "str"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["SINTERCARD", "0", "s"]);
    expect_error(&mut client, "ERR numkeys should be greater than 0");
    send(&mut client, &["SINTERCARD", "two", "s"]);
    expect_error(&mut client, "ERR numkeys should be greater than 0");
    send(&mut client, &["SINTERCARD", "3", "s", "t"]);
    expect_error(&mut client, "ERR Number of keys can't be greater than number of args");
    send(&mut client, &["SINTERCARD", "2", "s", "t", "LIMIT", "-1"]);
    expect_error(&mut client, "ERR LIMIT can't be negative");
    send(&mut client, &["SINTERCARD", "2", "s", "t", "LIMIT", "x"]);
    expect_error(&mut client, "ERR LIMIT can't be negative");
    send(&mut client, &["SINTERCARD", "2", "s", "t", "COUNT", "1"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["SINTERCARD", "2", "s", "t", "LIMIT", "1", "extra"]);
    expect_error(&mut client, "ERR syntax error");

    send(&mut client, &["SADD", "p", "x", "y", "z"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["EXPIRE", "p", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SPOP", "p"]);
    let popped = read_bulk_text(&mut client).expect("SPOP without count must reply one member");
    assert!(["x", "y", "z"].contains(&popped.as_str()));
    send(&mut client, &["SCARD", "p"]);
    assert_eq!(expect_integer(&mut client), 2);
    let remaining_ttl = {
        send(&mut client, &["TTL", "p"]);
        expect_integer(&mut client)
    };
    assert!(remaining_ttl > 0, "SPOP must not drop the key deadline");

    send(&mut client, &["SPOP", "p", "0"]);
    expect_empty_array(&mut client);
    send(&mut client, &["SCARD", "p"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["SPOP", "p", "5"]);
    let drained = read_array_text(&mut client);
    let mut sorted = drained.clone();
    sorted.sort();
    assert_eq!(sorted.len(), 2, "SPOP with a count above the cardinality replies every member");
    send(&mut client, &["EXISTS", "p"]);
    assert_eq!(expect_integer(&mut client), 0);

    send(&mut client, &["SPOP", "no-such-key"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["SPOP", "no-such-key", "3"]);
    expect_empty_array(&mut client);
    send(&mut client, &["SPOP"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'spop' command");
    send(&mut client, &["SPOP", "s", "1", "extra"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'spop' command");
    send(&mut client, &["SPOP", "s", "-1"]);
    expect_error(&mut client, "ERR value is out of range, must be positive");
    send(&mut client, &["SPOP", "s", "not-a-number"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");
    send(&mut client, &["SPOP", "str"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["SPOP", "str", "2"]);
    expect_error(&mut client, WRONGTYPE);

    // Popping from a single-member set removes the key.
    send(&mut client, &["SADD", "one", "only"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SPOP", "one"]);
    assert_eq!(read_bulk_text(&mut client).as_deref(), Some("only"));
    send(&mut client, &["EXISTS", "one"]);
    assert_eq!(expect_integer(&mut client), 0);
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

fn read_rank_with_score(client: &mut TcpStream) -> Option<(i64, String)> {
    match read_reply(client) {
        Reply::Array(Some(items)) => {
            assert_eq!(items.len(), 2, "WITHSCORE reply must hold rank and score");
            match (&items[0], &items[1]) {
                (Reply::Integer(rank), Reply::Bulk(Some(score))) => {
                    Some((*rank, String::from_utf8(score.clone()).unwrap()))
                }
                other => panic!("unexpected rank reply {:?}", other),
            }
        }
        // 真机的 `WITHSCORE` 缺失成员回 null array（RESP2 `*-1`）。
        Reply::Array(None) => None,
        other => panic!("expected rank array, got {}", describe(&other)),
    }
}

/// Score ranges honour open intervals, infinities, `LIMIT` and `WITHSCORES`; `ZCOUNT`
/// uses the same bounds.
#[test]
fn zset_score_ranges_and_limits() {
    let (mut client, _server) = memory_server("redis-zset-score-range");
    send(&mut client, &["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["ZRANGEBYSCORE", "z", "-inf", "+inf"]);
    expect_array(&mut client, &["a", "b", "c", "d"]);
    send(&mut client, &["ZRANGEBYSCORE", "z", "(1", "+inf"]);
    expect_array(&mut client, &["b", "c", "d"]);
    send(&mut client, &["ZRANGEBYSCORE", "z", "-inf", "(3", "WITHSCORES"]);
    expect_array(&mut client, &["a", "1", "b", "2"]);
    send(&mut client, &["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "2"]);
    expect_array(&mut client, &["b", "c"]);
    send(&mut client, &["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "2", "-1"]);
    expect_array(&mut client, &["c", "d"]);
    send(&mut client, &["ZREVRANGEBYSCORE", "z", "+inf", "-inf"]);
    expect_array(&mut client, &["d", "c", "b", "a"]);
    send(&mut client, &["ZREVRANGEBYSCORE", "z", "3", "(1"]);
    expect_array(&mut client, &["c", "b"]);
    send(&mut client, &["ZREVRANGEBYSCORE", "z", "+inf", "-inf", "LIMIT", "0", "2", "WITHSCORES"]);
    expect_array(&mut client, &["d", "4", "c", "3"]);
    send(&mut client, &["ZRANGEBYSCORE", "no-such-key", "-inf", "+inf"]);
    expect_empty_array(&mut client);
    send(&mut client, &["ZREVRANGEBYSCORE", "no-such-key", "+inf", "-inf"]);
    expect_empty_array(&mut client);
    send(&mut client, &["ZRANGEBYSCORE", "z", "bad", "+inf"]);
    expect_error(&mut client, "ERR min or max is not a float");
    send(&mut client, &["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "x", "1"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");
    send(&mut client, &["ZRANGEBYSCORE", "z", "-inf", "+inf", "BOGUS"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["ZCOUNT", "z", "1", "3"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["ZCOUNT", "z", "(1", "(3"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZCOUNT", "z", "4", "1"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZCOUNT", "no-such-key", "-inf", "+inf"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    for arguments in [
        ["ZRANGEBYSCORE", "str", "-inf", "+inf"].as_slice(),
        ["ZREVRANGEBYSCORE", "str", "+inf", "-inf"].as_slice(),
        ["ZCOUNT", "str", "-inf", "+inf"].as_slice(),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, WRONGTYPE);
    }
}

/// Lex ranges compare member bytes with `[` / `(` / `-` / `+`, and ranks report the
/// optional score.
#[test]
fn zset_lex_ranges_and_rank() {
    let (mut client, _server) = memory_server("redis-zset-lex-rank");
    send(&mut client, &["ZADD", "lex", "0", "a", "0", "b", "0", "c", "0", "d"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["ZRANGEBYLEX", "lex", "-", "+"]);
    expect_array(&mut client, &["a", "b", "c", "d"]);
    send(&mut client, &["ZRANGEBYLEX", "lex", "[b", "(d"]);
    expect_array(&mut client, &["b", "c"]);
    send(&mut client, &["ZRANGEBYLEX", "lex", "(a", "+"]);
    expect_array(&mut client, &["b", "c", "d"]);
    send(&mut client, &["ZRANGEBYLEX", "lex", "-", "[b"]);
    expect_array(&mut client, &["a", "b"]);
    send(&mut client, &["ZRANGEBYLEX", "lex", "-", "+", "LIMIT", "1", "2"]);
    expect_array(&mut client, &["b", "c"]);
    send(&mut client, &["ZREVRANGEBYLEX", "lex", "+", "-"]);
    expect_array(&mut client, &["d", "c", "b", "a"]);
    send(&mut client, &["ZREVRANGEBYLEX", "lex", "(d", "[a"]);
    expect_array(&mut client, &["c", "b", "a"]);
    send(&mut client, &["ZREVRANGEBYLEX", "lex", "+", "-", "LIMIT", "1", "2"]);
    expect_array(&mut client, &["c", "b"]);
    // 分数不一致时真机按 (score, member) 遍历后按成员边界过滤，不重排成字典序。
    send(&mut client, &["ZADD", "mixed", "2", "a", "1", "z", "3", "m"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["ZRANGEBYLEX", "mixed", "-", "+"]);
    expect_array(&mut client, &["z", "a", "m"]);
    send(&mut client, &["ZREVRANGEBYLEX", "mixed", "+", "-"]);
    expect_array(&mut client, &["m", "a", "z"]);
    // 首元素已越过上界时，真机整段判空而不是按成员逐条筛。
    send(&mut client, &["ZRANGEBYLEX", "mixed", "-", "[a"]);
    expect_empty_array(&mut client);
    send(&mut client, &["ZREMRANGEBYLEX", "mixed", "-", "[a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZRANGEBYLEX", "lex", "-", "+", "WITHSCORES"]);
    expect_error(&mut client, "ERR syntax error, WITHSCORES not supported in combination with BYLEX");
    send(&mut client, &["ZREVRANGEBYLEX", "lex", "+", "-", "WITHSCORES"]);
    expect_error(&mut client, "ERR syntax error, WITHSCORES not supported in combination with BYLEX");
    send(&mut client, &["ZRANGEBYLEX", "lex", "a", "+"]);
    expect_error(&mut client, "ERR min or max not valid string range item");
    send(&mut client, &["ZRANGEBYLEX", "no-such-key", "-", "+"]);
    expect_empty_array(&mut client);
    send(&mut client, &["ZREVRANGEBYLEX", "no-such-key", "+", "-"]);
    expect_empty_array(&mut client);

    send(&mut client, &["ZRANK", "lex", "b"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZREVRANK", "lex", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZRANK", "lex", "b", "WITHSCORE"]);
    assert_eq!(read_rank_with_score(&mut client), Some((1, "0".to_string())));
    send(&mut client, &["ZREVRANK", "lex", "b", "WITHSCORE"]);
    assert_eq!(read_rank_with_score(&mut client), Some((2, "0".to_string())));
    send(&mut client, &["ZRANK", "lex", "missing"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["ZRANK", "lex", "missing", "WITHSCORE"]);
    assert_eq!(read_rank_with_score(&mut client), None);
    send(&mut client, &["ZRANK", "no-such-key", "a"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["ZREVRANK", "no-such-key", "a", "WITHSCORE"]);
    assert_eq!(read_rank_with_score(&mut client), None);
    send(&mut client, &["ZRANK", "lex", "b", "BOGUS"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    for arguments in [
        ["ZRANGEBYLEX", "str", "-", "+"].as_slice(),
        ["ZREVRANGEBYLEX", "str", "+", "-"].as_slice(),
        ["ZRANK", "str", "a"].as_slice(),
        ["ZREVRANK", "str", "a"].as_slice(),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, WRONGTYPE);
    }
}

/// `ZINCRBY` creates missing keys, formats scores like `ZSCORE` and rejects NaN results.
#[test]
fn zset_incrby_and_nan_rejection() {
    let (mut client, _server) = memory_server("redis-zset-incrby");
    send(&mut client, &["ZINCRBY", "inc", "2", "m"]);
    expect_bulk(&mut client, Some("2"));
    send(&mut client, &["ZINCRBY", "inc", "0.5", "m"]);
    expect_bulk(&mut client, Some("2.5"));
    send(&mut client, &["ZINCRBY", "inc", "1", "other"]);
    expect_bulk(&mut client, Some("1"));
    send(&mut client, &["ZSCORE", "inc", "m"]);
    expect_bulk(&mut client, Some("2.5"));
    send(&mut client, &["ZINCRBY", "inc", "not-a-float", "m"]);
    expect_error(&mut client, "ERR value is not a valid float");
    send(&mut client, &["ZADD", "inf", "+inf", "x"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZINCRBY", "inf", "-inf", "x"]);
    expect_error(&mut client, "ERR resulting score is not a number (NaN)");
    send(&mut client, &["ZSCORE", "inf", "x"]);
    expect_bulk(&mut client, Some("inf"));
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["ZINCRBY", "str", "1", "m"]);
    expect_error(&mut client, WRONGTYPE);
    // Incrementing keeps the existing TTL.
    send(&mut client, &["EXPIRE", "inc", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZINCRBY", "inc", "1", "m"]);
    expect_bulk(&mut client, Some("3.5"));
    send(&mut client, &["TTL", "inc"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "ZINCRBY dropped the TTL: {ttl}");
}

/// Range removals return the removed count, delete emptied keys and survive `ZSCAN`.
#[test]
fn zset_remove_ranges_and_scan() {
    let (mut client, _server) = memory_server("redis-zset-remove-scan");
    send(&mut client, &["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"]);
    assert_eq!(expect_integer(&mut client), 5);
    send(&mut client, &["ZREMRANGEBYRANK", "z", "0", "1"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZRANGE", "z", "0", "-1"]);
    expect_array(&mut client, &["c", "d", "e"]);
    send(&mut client, &["ZREMRANGEBYRANK", "z", "-1", "-1"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZRANGE", "z", "0", "-1"]);
    expect_array(&mut client, &["c", "d"]);
    send(&mut client, &["ZREMRANGEBYSCORE", "z", "(3", "4"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZRANGE", "z", "0", "-1"]);
    expect_array(&mut client, &["c"]);
    send(&mut client, &["ZREMRANGEBYSCORE", "z", "-inf", "+inf"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["EXISTS", "z"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZREMRANGEBYRANK", "no-such-key", "0", "-1"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZREMRANGEBYSCORE", "no-such-key", "-inf", "+inf"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZADD", "z", "1", "a", "2", "b", "3", "c"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["ZREMRANGEBYSCORE", "z", "bad", "+inf"]);
    expect_error(&mut client, "ERR min or max is not a float");
    send(&mut client, &["ZREMRANGEBYRANK", "z", "not-an-index", "0"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");

    send(&mut client, &["ZADD", "scan", "1", "a", "2", "b", "3", "c", "4", "d"]);
    assert_eq!(expect_integer(&mut client), 4);
    let mut cursor = "0".to_string();
    let mut seen = Vec::new();
    loop {
        send(&mut client, &["ZSCAN", "scan", &cursor, "COUNT", "2"]);
        let (next, items) = expect_scan(&mut client);
        assert_eq!(items.len() % 2, 0, "ZSCAN must reply member/score pairs");
        seen.extend(items.into_iter().step_by(2));
        if next == "0" {
            break;
        }
        cursor = next;
    }
    seen.sort();
    assert_eq!(seen, ["a", "b", "c", "d"]);
    send(&mut client, &["ZSCAN", "scan", "0", "MATCH", "b*", "COUNT", "100"]);
    let (cursor, items) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert_eq!(items, ["b", "2"]);
    send(&mut client, &["ZSCAN", "no-such-key", "0"]);
    let (cursor, items) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert!(items.is_empty());
    send(&mut client, &["ZSCAN", "scan", "not-a-cursor"]);
    expect_error(&mut client, "ERR invalid cursor");
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["ZSCAN", "str", "0"]);
    expect_error(&mut client, WRONGTYPE);
}

/// zset mutations keep the key TTL, and range removals plus increments survive reopen.
#[test]
fn zset_mutations_survive_reopen() {
    let path = temp_path("zset-reopen");
    let (mut first, _first_server) = file_server(&path);
    send(&mut first, &["ZADD", "z", "1", "a", "2", "b", "3", "c"]);
    assert_eq!(expect_integer(&mut first), 3);
    send(&mut first, &["EXPIRE", "z", "100"]);
    assert_eq!(expect_integer(&mut first), 1);
    send(&mut first, &["ZINCRBY", "z", "1", "a"]);
    expect_bulk(&mut first, Some("2"));
    send(&mut first, &["ZREMRANGEBYRANK", "z", "0", "0"]);
    assert_eq!(expect_integer(&mut first), 1);

    let (mut second, _second_server) = file_server(&path);
    send(&mut second, &["TYPE", "z"]);
    expect_status(&mut second, "zset");
    send(&mut second, &["ZRANGE", "z", "0", "-1", "WITHSCORES"]);
    expect_array(&mut second, &["b", "2", "c", "3"]);
    send(&mut second, &["TTL", "z"]);
    let ttl = expect_integer(&mut second);
    assert!((1..=100).contains(&ttl), "zset TTL missing after reopen: {ttl}");
}

/// `HKEYS` / `HVALS` / `HEXISTS` and `HINCRBYFLOAT` cover missing keys, non-numeric
/// fields and infinite results.
#[test]
fn hash_keys_values_exists_and_float_increment() {
    let (mut client, _server) = memory_server("redis-hash-extras");
    send(&mut client, &["HSET", "h", "a", "1", "b", "2", "c", "3"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["HKEYS", "h"]);
    expect_array(&mut client, &["a", "b", "c"]);
    send(&mut client, &["HVALS", "h"]);
    expect_array(&mut client, &["1", "2", "3"]);
    send(&mut client, &["HEXISTS", "h", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["HEXISTS", "h", "missing"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["HEXISTS", "no-such-key", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["HKEYS", "no-such-key"]);
    expect_empty_array(&mut client);
    send(&mut client, &["HVALS", "no-such-key"]);
    expect_empty_array(&mut client);

    send(&mut client, &["HINCRBYFLOAT", "h", "f", "1.5"]);
    expect_bulk(&mut client, Some("1.5"));
    send(&mut client, &["HINCRBYFLOAT", "h", "f", "0.25"]);
    expect_bulk(&mut client, Some("1.75"));
    send(&mut client, &["HINCRBYFLOAT", "fresh", "n", "2"]);
    expect_bulk(&mut client, Some("2"));
    send(&mut client, &["TYPE", "fresh"]);
    expect_status(&mut client, "hash");
    send(&mut client, &["HSET", "h", "bad", "not-a-number"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["HINCRBYFLOAT", "h", "bad", "1"]);
    expect_error(&mut client, "ERR hash value is not a float");
    send(&mut client, &["HINCRBYFLOAT", "h", "f", "not-a-float"]);
    expect_error(&mut client, "ERR value is not a valid float");
    send(&mut client, &["HSET", "h", "big", "1e308"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["HINCRBYFLOAT", "h", "big", "1e308"]);
    expect_error(&mut client, "ERR increment would produce NaN or Infinity");
    send(&mut client, &["HGET", "h", "big"]);
    expect_bulk(&mut client, Some("1e308"));

    // Float increments keep the existing TTL.
    send(&mut client, &["EXPIRE", "h", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["HINCRBYFLOAT", "h", "f", "1"]);
    expect_bulk(&mut client, Some("2.75"));
    send(&mut client, &["TTL", "h"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "HINCRBYFLOAT dropped the TTL: {ttl}");

    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    for arguments in [
        ["HKEYS", "str"].as_slice(),
        ["HVALS", "str"].as_slice(),
        ["HEXISTS", "str", "a"].as_slice(),
        ["HINCRBYFLOAT", "str", "f", "1"].as_slice(),
        ["HSCAN", "str", "0"].as_slice(),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, WRONGTYPE);
    }
}

/// `HSCAN` walks field/value pairs in pages and honours `MATCH`.
#[test]
fn hash_scan_pages_and_match() {
    let (mut client, _server) = memory_server("redis-hscan");
    send(&mut client, &["HSET", "h", "a1", "v1", "a2", "v2", "b1", "v3", "b2", "v4"]);
    assert_eq!(expect_integer(&mut client), 4);
    let mut cursor = "0".to_string();
    let mut seen = Vec::new();
    loop {
        send(&mut client, &["HSCAN", "h", &cursor, "COUNT", "2"]);
        let (next, items) = expect_scan(&mut client);
        assert_eq!(items.len() % 2, 0, "HSCAN must reply field/value pairs");
        seen.extend(items.into_iter().step_by(2));
        if next == "0" {
            break;
        }
        cursor = next;
    }
    seen.sort();
    assert_eq!(seen, ["a1", "a2", "b1", "b2"]);
    send(&mut client, &["HSCAN", "h", "0", "MATCH", "a*", "COUNT", "100"]);
    let (cursor, items) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert_eq!(items, ["a1", "v1", "a2", "v2"]);
    send(&mut client, &["HSCAN", "no-such-key", "0"]);
    let (cursor, items) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert!(items.is_empty());
    send(&mut client, &["HSCAN", "h", "not-a-cursor"]);
    expect_error(&mut client, "ERR invalid cursor");
}

/// Float increments and field visibility survive a `FileShard` reopen with their TTL.
#[test]
fn hash_extras_survive_reopen() {
    let path = temp_path("hash-extras");
    let (mut first, _first_server) = file_server(&path);
    send(&mut first, &["HSET", "h", "a", "1"]);
    assert_eq!(expect_integer(&mut first), 1);
    send(&mut first, &["HINCRBYFLOAT", "h", "a", "0.5"]);
    expect_bulk(&mut first, Some("1.5"));
    send(&mut first, &["EXPIRE", "h", "100"]);
    assert_eq!(expect_integer(&mut first), 1);

    let (mut second, _second_server) = file_server(&path);
    send(&mut second, &["HGET", "h", "a"]);
    expect_bulk(&mut second, Some("1.5"));
    send(&mut second, &["HKEYS", "h"]);
    expect_array(&mut second, &["a"]);
    send(&mut second, &["TTL", "h"]);
    let ttl = expect_integer(&mut second);
    assert!((1..=100).contains(&ttl), "hash TTL missing after reopen: {ttl}");
}

/// `LREM` honours the count sign, `LSET` rejects missing keys and bad indexes, `LTRIM`
/// deletes emptied ranges and `LINSERT` reports the pivot miss.
#[test]
fn list_remove_set_trim_and_insert() {
    let (mut client, _server) = memory_server("redis-list-extras");
    send(&mut client, &["RPUSH", "l", "a", "b", "a", "c", "a"]);
    assert_eq!(expect_integer(&mut client), 5);
    send(&mut client, &["LREM", "l", "2", "a"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["LRANGE", "l", "0", "-1"]);
    expect_array(&mut client, &["b", "c", "a"]);
    send(&mut client, &["LREM", "l", "-1", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["LRANGE", "l", "0", "-1"]);
    expect_array(&mut client, &["b", "c"]);
    send(&mut client, &["LREM", "l", "0", "missing"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["LREM", "no-such-key", "0", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["LREM", "l", "not-a-count", "a"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");

    send(&mut client, &["LSET", "l", "0", "B"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["LSET", "l", "-1", "C"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["LRANGE", "l", "0", "-1"]);
    expect_array(&mut client, &["B", "C"]);
    send(&mut client, &["LSET", "l", "2", "x"]);
    expect_error(&mut client, "ERR index out of range");
    send(&mut client, &["LSET", "l", "-3", "x"]);
    expect_error(&mut client, "ERR index out of range");
    send(&mut client, &["LSET", "no-such-key", "0", "x"]);
    expect_error(&mut client, "ERR no such key");

    send(&mut client, &["LTRIM", "l", "0", "0"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["LRANGE", "l", "0", "-1"]);
    expect_array(&mut client, &["B"]);
    send(&mut client, &["LTRIM", "l", "5", "10"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["EXISTS", "l"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["LTRIM", "no-such-key", "0", "1"]);
    expect_status(&mut client, "OK");

    send(&mut client, &["RPUSH", "l", "a", "b", "c"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["LINSERT", "l", "BEFORE", "b", "B"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["LINSERT", "l", "AFTER", "c", "D"]);
    assert_eq!(expect_integer(&mut client), 5);
    send(&mut client, &["LRANGE", "l", "0", "-1"]);
    expect_array(&mut client, &["a", "B", "b", "c", "D"]);
    send(&mut client, &["LINSERT", "l", "BEFORE", "missing", "x"]);
    assert_eq!(expect_integer(&mut client), -1);
    send(&mut client, &["LINSERT", "no-such-key", "BEFORE", "a", "x"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["LINSERT", "l", "SIDEWAYS", "b", "x"]);
    expect_error(&mut client, "ERR syntax error");

    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    for arguments in [
        ["LREM", "str", "0", "a"].as_slice(),
        ["LSET", "str", "0", "a"].as_slice(),
        ["LTRIM", "str", "0", "1"].as_slice(),
        ["LINSERT", "str", "BEFORE", "a", "b"].as_slice(),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, WRONGTYPE);
    }

    // Edits keep the list TTL.
    send(&mut client, &["EXPIRE", "l", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["LSET", "l", "0", "Z"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["TTL", "l"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "LSET dropped the TTL: {ttl}");
}

/// `RPOPLPUSH` / `LMOVE` move across keys, rotate in place and preserve TTLs.
#[test]
fn list_move_between_keys() {
    let (mut client, _server) = memory_server("redis-list-move");
    send(&mut client, &["RPUSH", "src", "a", "b", "c"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["RPOPLPUSH", "src", "dst"]);
    expect_bulk(&mut client, Some("c"));
    send(&mut client, &["LRANGE", "dst", "0", "-1"]);
    expect_array(&mut client, &["c"]);
    send(&mut client, &["RPOPLPUSH", "src", "dst"]);
    expect_bulk(&mut client, Some("b"));
    send(&mut client, &["LRANGE", "dst", "0", "-1"]);
    expect_array(&mut client, &["b", "c"]);
    send(&mut client, &["LMOVE", "src", "dst", "LEFT", "LEFT"]);
    expect_bulk(&mut client, Some("a"));
    send(&mut client, &["EXISTS", "src"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["LRANGE", "dst", "0", "-1"]);
    expect_array(&mut client, &["a", "b", "c"]);
    send(&mut client, &["LMOVE", "no-such-key", "dst", "LEFT", "LEFT"]);
    expect_bulk(&mut client, None);

    // Same key rotates instead of deleting.
    send(&mut client, &["RPUSH", "one", "x"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["RPOPLPUSH", "one", "one"]);
    expect_bulk(&mut client, Some("x"));
    send(&mut client, &["LRANGE", "one", "0", "-1"]);
    expect_array(&mut client, &["x"]);
    send(&mut client, &["RPUSH", "multi", "1", "2", "3"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["LMOVE", "multi", "multi", "RIGHT", "LEFT"]);
    expect_bulk(&mut client, Some("3"));
    send(&mut client, &["LRANGE", "multi", "0", "-1"]);
    expect_array(&mut client, &["3", "1", "2"]);

    // A missing destination is created on the fly.
    send(&mut client, &["RPUSH", "s2", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["RPOPLPUSH", "s2", "fresh"]);
    expect_bulk(&mut client, Some("a"));
    send(&mut client, &["LRANGE", "fresh", "0", "-1"]);
    expect_array(&mut client, &["a"]);

    // Destination TTL is reused, not reset.
    send(&mut client, &["EXPIRE", "dst", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["RPUSH", "donor", "z"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["RPOPLPUSH", "donor", "dst"]);
    expect_bulk(&mut client, Some("z"));
    send(&mut client, &["TTL", "dst"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "move reset the destination TTL: {ttl}");

    send(&mut client, &["LMOVE", "multi", "dst", "UP", "LEFT"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["RPOPLPUSH", "str", "dst"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["RPOPLPUSH", "dst", "str"]);
    expect_error(&mut client, WRONGTYPE);
}

/// List edits and cross-key moves survive a `FileShard` reopen with their TTL.
#[test]
fn list_extras_survive_reopen() {
    let path = temp_path("list-extras");
    let (mut first, _first_server) = file_server(&path);
    send(&mut first, &["RPUSH", "l", "a", "b", "a", "c"]);
    assert_eq!(expect_integer(&mut first), 4);
    send(&mut first, &["LREM", "l", "1", "a"]);
    assert_eq!(expect_integer(&mut first), 1);
    send(&mut first, &["LSET", "l", "1", "B"]);
    expect_status(&mut first, "OK");
    send(&mut first, &["RPOPLPUSH", "l", "l2"]);
    expect_bulk(&mut first, Some("c"));
    send(&mut first, &["EXPIRE", "l", "100"]);
    assert_eq!(expect_integer(&mut first), 1);

    let (mut second, _second_server) = file_server(&path);
    send(&mut second, &["TYPE", "l"]);
    expect_status(&mut second, "list");
    send(&mut second, &["LRANGE", "l", "0", "-1"]);
    expect_array(&mut second, &["b", "B"]);
    send(&mut second, &["LRANGE", "l2", "0", "-1"]);
    expect_array(&mut second, &["c"]);
    send(&mut second, &["TTL", "l"]);
    let ttl = expect_integer(&mut second);
    assert!((1..=100).contains(&ttl), "list TTL missing after reopen: {ttl}");
}

/// `DBSIZE` counts keys across every type namespace, `RANDOMKEY` returns an existing key
/// and `FLUSHALL` clears every logical database.
#[test]
fn keyspace_size_random_and_flush() {
    let (mut client, _server) = memory_server("redis-keyspace");
    send(&mut client, &["DBSIZE"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["RANDOMKEY"]);
    assert_eq!(read_bulk_text(&mut client), None);
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SADD", "set", "x"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["HSET", "hash", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["RPUSH", "list", "x"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZADD", "zset", "1", "m"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["DBSIZE"]);
    assert_eq!(expect_integer(&mut client), 5);
    send(&mut client, &["RANDOMKEY"]);
    let key = read_bulk_text(&mut client).expect("RANDOMKEY must return one of the existing keys");
    assert!(["str", "set", "hash", "list", "zset"].contains(&key.as_str()));

    // FLUSHALL also clears other logical databases.
    send(&mut client, &["SELECT", "1"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SET", "other", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SELECT", "0"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["FLUSHALL"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["DBSIZE"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["EXISTS", "str", "set", "hash", "list", "zset"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["RANDOMKEY"]);
    assert_eq!(read_bulk_text(&mut client), None);
    send(&mut client, &["SELECT", "1"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["GET", "other"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["FLUSHALL", "ASYNC"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["FLUSHALL", "BOGUS"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["RANDOMKEY", "extra"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'randomkey' command");
}

/// `COPY` duplicates every value type, honours `REPLACE` and copies TTLs across `DB`.
#[test]
fn copy_across_keys_and_databases() {
    let (mut client, _server) = memory_server("redis-copy");
    send(&mut client, &["SET", "src", "value", "EX", "100"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["COPY", "src", "dst"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["GET", "dst"]);
    expect_bulk(&mut client, Some("value"));
    send(&mut client, &["TTL", "dst"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "COPY must copy the source TTL: {ttl}");
    send(&mut client, &["COPY", "src", "dst"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SET", "dst", "other"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["COPY", "src", "dst", "REPLACE"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["GET", "dst"]);
    expect_bulk(&mut client, Some("value"));
    send(&mut client, &["COPY", "missing", "elsewhere"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["COPY", "src", "src"]);
    expect_error(&mut client, "ERR source and destination objects are the same");
    send(&mut client, &["COPY", "src", "other", "DB", "99"]);
    expect_error(&mut client, "ERR DB index is out of range");
    send(&mut client, &["COPY", "src", "other", "BOGUS"]);
    expect_error(&mut client, "ERR syntax error");

    // Different DB copies keep the value and the TTL.
    send(&mut client, &["COPY", "src", "remote", "DB", "2"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SELECT", "2"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["GET", "remote"]);
    expect_bulk(&mut client, Some("value"));
    send(&mut client, &["TTL", "remote"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "cross-DB COPY must copy the TTL: {ttl}");
    send(&mut client, &["COPY", "remote", "remote2"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["GET", "remote2"]);
    expect_bulk(&mut client, Some("value"));
    send(&mut client, &["SELECT", "0"]);
    expect_status(&mut client, "OK");

    // Non-string values are copied with their exact representation.
    send(&mut client, &["SADD", "set", "a", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["COPY", "set", "setcopy"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TYPE", "setcopy"]);
    expect_status(&mut client, "set");
    send(&mut client, &["SMEMBERS", "setcopy"]);
    expect_array(&mut client, &["a", "b"]);
    send(&mut client, &["ZADD", "zset", "1", "m"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["COPY", "zset", "zsetcopy", "REPLACE"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZSCORE", "zsetcopy", "m"]);
    expect_bulk(&mut client, Some("1"));
}

/// `EXPIRETIME` / `PEXPIRETIME` report missing keys, keys without a TTL and absolute
/// deadlines for every value type.
#[test]
fn expiretime_reports_absolute_deadlines() {
    let (mut client, _server) = memory_server("redis-expiretime");
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    send(&mut client, &["EXPIRETIME", "missing"]);
    assert_eq!(expect_integer(&mut client), -2);
    send(&mut client, &["PEXPIRETIME", "missing"]);
    assert_eq!(expect_integer(&mut client), -2);
    send(&mut client, &["SET", "k", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["EXPIRETIME", "k"]);
    assert_eq!(expect_integer(&mut client), -1);
    send(&mut client, &["PEXPIRETIME", "k"]);
    assert_eq!(expect_integer(&mut client), -1);
    send(&mut client, &["SET", "k", "v", "EX", "100"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["EXPIRETIME", "k"]);
    let seconds = expect_integer(&mut client);
    assert!(
        (now_ms / 1000 + 90..=now_ms / 1000 + 110).contains(&seconds),
        "EXPIRETIME not near the expected deadline: {seconds}"
    );
    send(&mut client, &["PEXPIRETIME", "k"]);
    let milliseconds = expect_integer(&mut client);
    assert!(
        (now_ms + 90_000..=now_ms + 110_000).contains(&milliseconds),
        "PEXPIRETIME not near the expected deadline: {milliseconds}"
    );
    send(&mut client, &["SADD", "s", "x"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["EXPIRE", "s", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["EXPIRETIME", "s"]);
    let seconds = expect_integer(&mut client);
    assert!(seconds > now_ms / 1000, "EXPIRETIME must work on non-string keys: {seconds}");
}

/// Copy results, key counts and absolute deadlines survive a `FileShard` reopen.
#[test]
fn keyspace_extensions_survive_reopen() {
    let path = temp_path("keyspace-reopen");
    let (mut first, _first_server) = file_server(&path);
    send(&mut first, &["SET", "src", "v", "EX", "100"]);
    expect_status(&mut first, "OK");
    send(&mut first, &["COPY", "src", "copy"]);
    assert_eq!(expect_integer(&mut first), 1);
    send(&mut first, &["SADD", "set", "x"]);
    assert_eq!(expect_integer(&mut first), 1);

    let (mut second, _second_server) = file_server(&path);
    send(&mut second, &["DBSIZE"]);
    assert_eq!(expect_integer(&mut second), 3);
    send(&mut second, &["GET", "copy"]);
    expect_bulk(&mut second, Some("v"));
    send(&mut second, &["TTL", "copy"]);
    let ttl = expect_integer(&mut second);
    assert!((1..=100).contains(&ttl), "copied TTL missing after reopen: {ttl}");
    send(&mut second, &["EXPIRETIME", "copy"]);
    assert!(expect_integer(&mut second) > 0);
    send(&mut second, &["RANDOMKEY"]);
    let key = read_bulk_text(&mut second).expect("RANDOMKEY after reopen");
    assert!(["src", "copy", "set"].contains(&key.as_str()));
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

/// `ZUNIONSTORE` / `ZINTERSTORE` / `ZDIFF` / `ZDIFFSTORE` / `ZINTERCARD` cover
/// weights, aggregation, overwrite semantics and cross-type rejection.
#[test]
fn zset_set_operations() {
    let (mut client, _server) = memory_server("redis-zset-algebra");
    send(&mut client, &["ZADD", "z1", "1", "a", "2", "b", "3", "c"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["ZADD", "z2", "3", "b", "4", "c", "5", "d"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["ZADD", "z3", "5", "c", "6", "e"]);
    assert_eq!(expect_integer(&mut client), 2);

    send(&mut client, &["ZUNIONSTORE", "dst", "2", "z1", "z2"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["ZRANGE", "dst", "0", "-1", "WITHSCORES"]);
    expect_array(&mut client, &["a", "1", "b", "5", "d", "5", "c", "7"]);
    send(&mut client, &["TYPE", "dst"]);
    expect_status(&mut client, "zset");

    // WEIGHTS 先逐键乘，之后才聚合。
    send(&mut client, &["ZUNIONSTORE", "dst", "2", "z1", "z2", "WEIGHTS", "2", "1"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["ZRANGE", "dst", "0", "-1", "WITHSCORES"]);
    expect_array(&mut client, &["a", "2", "d", "5", "b", "7", "c", "10"]);

    send(&mut client, &["ZUNIONSTORE", "dst", "2", "z1", "z2", "AGGREGATE", "MAX"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["ZRANGE", "dst", "0", "-1", "WITHSCORES"]);
    expect_array(&mut client, &["a", "1", "b", "3", "c", "4", "d", "5"]);

    send(&mut client, &["ZINTERSTORE", "dst", "2", "z1", "z2"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZRANGE", "dst", "0", "-1", "WITHSCORES"]);
    expect_array(&mut client, &["b", "5", "c", "7"]);

    send(&mut client, &["ZINTERSTORE", "dst", "2", "z1", "z2", "WEIGHTS", "3", "1", "AGGREGATE", "MIN"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZRANGE", "dst", "0", "-1", "WITHSCORES"]);
    expect_array(&mut client, &["b", "3", "c", "4"]);

    // 差集保留第一个键的分数，其余键只参与去重。
    send(&mut client, &["ZDIFF", "2", "z1", "z2"]);
    expect_array(&mut client, &["a"]);
    send(&mut client, &["ZDIFF", "2", "z1", "z2", "WITHSCORES"]);
    expect_array(&mut client, &["a", "1"]);
    send(&mut client, &["ZDIFF", "2", "z1", "z3"]);
    expect_array(&mut client, &["a", "b"]);
    send(&mut client, &["ZDIFF", "2", "z1", "no-such-key"]);
    expect_array(&mut client, &["a", "b", "c"]);
    send(&mut client, &["ZDIFFSTORE", "dst", "2", "z1", "z3"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZRANGE", "dst", "0", "-1", "WITHSCORES"]);
    expect_array(&mut client, &["a", "1", "b", "2"]);

    send(&mut client, &["ZINTERCARD", "2", "z1", "z2"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZINTERCARD", "2", "z1", "z2", "LIMIT", "1"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZINTERCARD", "2", "z1", "z2", "LIMIT", "0"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZINTERCARD", "1", "z1"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["ZINTERCARD", "2", "z1", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);

    // 空交集删除目标，覆盖写入清掉目标原有的 TTL 与类型。
    send(&mut client, &["SET", "dst", "old", "EX", "100"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["ZINTERSTORE", "dst", "2", "z1", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["EXISTS", "dst"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SET", "dst", "old", "EX", "100"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["ZUNIONSTORE", "dst", "1", "z1"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["TTL", "dst"]);
    assert_eq!(expect_integer(&mut client), -1);

    // 目标同时作为源时，先按源读取再覆盖。
    send(&mut client, &["ZUNIONSTORE", "z1", "2", "z1", "z2"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["ZRANGE", "z1", "0", "-1", "WITHSCORES"]);
    expect_array(&mut client, &["a", "1", "b", "5", "d", "5", "c", "7"]);

    // 任一源键类型非法即整体 WRONGTYPE。
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    for arguments in [
        ["ZUNIONSTORE", "dst", "2", "z2", "str"].as_slice(),
        ["ZINTERSTORE", "dst", "2", "z2", "str"].as_slice(),
        ["ZDIFF", "2", "z2", "str"].as_slice(),
        ["ZDIFFSTORE", "dst", "2", "z2", "str"].as_slice(),
        ["ZINTERCARD", "2", "z2", "str"].as_slice(),
    ] {
        send(&mut client, arguments);
        expect_error(&mut client, WRONGTYPE);
    }

    // numkeys / WEIGHTS / AGGREGATE / LIMIT 的参数校验。
    send(&mut client, &["ZUNIONSTORE", "dst", "0", "z2"]);
    expect_error(&mut client, "ERR at least 1 input key is needed for 'zunionstore' command");
    send(&mut client, &["ZINTERSTORE", "dst", "0", "z2"]);
    expect_error(&mut client, "ERR at least 1 input key is needed for 'zinterstore' command");
    send(&mut client, &["ZUNIONSTORE", "dst", "2", "z2"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["ZUNIONSTORE", "dst", "2", "z2", "z3", "WEIGHTS", "1"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["ZUNIONSTORE", "dst", "2", "z2", "z3", "WEIGHTS", "1", "x"]);
    expect_error(&mut client, "ERR weight value is not a float");
    send(&mut client, &["ZUNIONSTORE", "dst", "2", "z2", "z3", "AGGREGATE", "AVG"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["ZUNIONSTORE", "dst", "1", "z2", "z3"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["ZUNIONSTORE", "dst"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'zunionstore' command");
    send(&mut client, &["ZDIFF", "0", "z2"]);
    expect_error(&mut client, "ERR at least 1 input key is needed for 'zdiff' command");
    send(&mut client, &["ZDIFFSTORE", "dst", "0", "z2"]);
    expect_error(&mut client, "ERR at least 1 input key is needed for 'zdiffstore' command");
    send(&mut client, &["ZDIFF", "2", "z2", "z3", "SCORES"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["ZDIFFSTORE", "dst", "2", "z2", "z3", "WITHSCORES"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["ZINTERCARD", "0", "z2"]);
    expect_error(&mut client, "ERR at least 1 input key is needed for 'zintercard' command");
    send(&mut client, &["ZINTERCARD", "2", "z2"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["ZINTERCARD", "2", "z2", "z3", "LIMIT", "-1"]);
    expect_error(&mut client, "ERR LIMIT can't be negative");
    send(&mut client, &["ZINTERCARD", "2", "z2", "z3", "LIMIT", "x"]);
    expect_error(&mut client, "ERR LIMIT can't be negative");
    send(&mut client, &["ZINTERCARD", "2", "z2", "z3", "LIMIT"]);
    expect_error(&mut client, "ERR syntax error");
}

/// `SORT` / `SORT_RO` cover numeric and `ALPHA` ordering, `BY` / `GET` patterns,
/// `LIMIT`, `DESC` and the `STORE` destination type.
#[test]
fn sort_commands() {
    let (mut client, _server) = memory_server("redis-sort");
    send(&mut client, &["RPUSH", "nums", "3", "1", "2", "10"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["SORT", "nums"]);
    expect_array(&mut client, &["1", "2", "3", "10"]);
    send(&mut client, &["SORT", "nums", "DESC"]);
    expect_array(&mut client, &["10", "3", "2", "1"]);
    send(&mut client, &["SORT", "nums", "LIMIT", "1", "2"]);
    expect_array(&mut client, &["2", "3"]);
    send(&mut client, &["SORT", "nums", "LIMIT", "5", "2"]);
    expect_empty_array(&mut client);
    send(&mut client, &["SORT_RO", "nums"]);
    expect_array(&mut client, &["1", "2", "3", "10"]);

    send(&mut client, &["RPUSH", "words", "banana", "apple", "cherry"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["SORT", "words", "ALPHA"]);
    expect_array(&mut client, &["apple", "banana", "cherry"]);
    send(&mut client, &["SORT", "words", "ALPHA", "DESC"]);
    expect_array(&mut client, &["cherry", "banana", "apple"]);

    send(&mut client, &["RPUSH", "items", "a", "b", "c"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["SET", "weight_a", "10"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SET", "weight_b", "1"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SET", "weight_c", "5"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SORT", "items", "BY", "weight_*"]);
    expect_array(&mut client, &["b", "c", "a"]);
    send(&mut client, &["SORT", "items", "BY", "weight_*", "DESC"]);
    expect_array(&mut client, &["a", "c", "b"]);
    send(&mut client, &["SORT", "items", "BY", "weight_*", "GET", "#"]);
    expect_array(&mut client, &["b", "c", "a"]);

    // 哈希模式：`BY key_*->field` 与 `GET` 的哈希字段取值。
    send(&mut client, &["HSET", "h_a", "v", "7"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["HSET", "h_b", "v", "9"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["HSET", "h_c", "v", "1"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SORT", "items", "BY", "h_*->v"]);
    expect_array(&mut client, &["c", "a", "b"]);

    // 不带 `*` 的 BY 关闭排序；zset 仍按自身分数顺序输出。
    send(&mut client, &["ZADD", "zs", "5", "3", "9", "1", "1", "2"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["SORT", "zs", "BY", "nosort"]);
    expect_array(&mut client, &["2", "3", "1"]);
    send(&mut client, &["SORT", "zs"]);
    expect_array(&mut client, &["1", "2", "3"]);
    send(&mut client, &["SORT", "zs", "DESC"]);
    expect_array(&mut client, &["3", "2", "1"]);

    send(&mut client, &["SADD", "opts", "3", "1", "2"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["SORT", "opts"]);
    expect_array(&mut client, &["1", "2", "3"]);

    // STORE 覆盖目标为 list，并清掉目标原有的类型与 TTL。
    send(&mut client, &["SET", "out", "old", "EX", "100"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SORT", "nums", "STORE", "out"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["LRANGE", "out", "0", "-1"]);
    expect_array(&mut client, &["1", "2", "3", "10"]);
    send(&mut client, &["TYPE", "out"]);
    expect_status(&mut client, "list");
    send(&mut client, &["TTL", "out"]);
    assert_eq!(expect_integer(&mut client), -1);

    send(&mut client, &["SORT", "no-such-key", "STORE", "out"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["EXISTS", "out"]);
    assert_eq!(expect_integer(&mut client), 0);

    send(&mut client, &["SET", "plain", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["SORT", "plain"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["RPUSH", "bad", "x", "1"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["SORT", "bad"]);
    expect_error(&mut client, "ERR One or more scores can't be converted into double");
    send(&mut client, &["SORT_RO", "nums", "STORE", "out"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["SORT", "nums", "BOGUS"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["SORT", "nums", "LIMIT", "1", "x"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");
    send(&mut client, &["SORT"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'sort' command");
}

/// `BITFIELD` / `BITFIELD_RO` cover signed and unsigned widths, `#` offsets,
/// all three overflow policies and the create-on-write rules.
#[test]
fn bitfield_commands() {
    fn expect_values(client: &mut TcpStream, expected: &[Option<i64>]) {
        match read_reply(client) {
            Reply::Array(Some(items)) => {
                assert_eq!(items.len(), expected.len());
                for (item, wanted) in items.iter().zip(expected) {
                    match (item, wanted) {
                        (Reply::Integer(value), Some(wanted)) => assert_eq!(*value, *wanted),
                        (Reply::Bulk(None), None) => {}
                        other => panic!("unexpected bitfield reply {other:?}"),
                    }
                }
            }
            other => panic!("unexpected bitfield reply {other:?}"),
        }
    }

    let (mut client, _server) = memory_server("redis-bitfield");
    send(&mut client, &["BITFIELD", "bf", "SET", "u8", "0", "255", "GET", "u8", "0"]);
    expect_values(&mut client, &[Some(0), Some(255)]);
    send(&mut client, &["TYPE", "bf"]);
    expect_status(&mut client, "string");
    send(&mut client, &["GET", "bf"]);
    match read_reply(&mut client) {
        Reply::Bulk(Some(bytes)) => assert_eq!(bytes, vec![0xff]),
        other => panic!("expected bulk 0xff, got {}", describe(&other)),
    }

    // 无符号回绕：200 + 100 在 u8 上落到 44。
    send(&mut client, &["BITFIELD", "cnt", "SET", "u8", "0", "200"]);
    expect_values(&mut client, &[Some(0)]);
    send(&mut client, &["BITFIELD", "cnt", "INCRBY", "u8", "0", "100"]);
    expect_values(&mut client, &[Some(44)]);
    send(&mut client, &["BITFIELD", "cnt", "GET", "u8", "0"]);
    expect_values(&mut client, &[Some(44)]);

    send(&mut client, &["BITFIELD", "cnt", "SET", "u8", "0", "200"]);
    expect_values(&mut client, &[Some(44)]);
    send(&mut client, &["BITFIELD", "cnt", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "100"]);
    expect_values(&mut client, &[Some(255)]);
    send(&mut client, &["BITFIELD", "cnt", "SET", "u8", "0", "200"]);
    expect_values(&mut client, &[Some(255)]);
    send(&mut client, &["BITFIELD", "cnt", "OVERFLOW", "FAIL", "INCRBY", "u8", "0", "100"]);
    expect_values(&mut client, &[None]);
    send(&mut client, &["BITFIELD", "cnt", "GET", "u8", "0"]);
    expect_values(&mut client, &[Some(200)]);

    // 有符号回绕与饱和，下限 -128 与上限 127。
    send(&mut client, &["BITFIELD", "s", "SET", "i8", "0", "-128", "INCRBY", "i8", "0", "-1"]);
    expect_values(&mut client, &[Some(0), Some(127)]);
    send(&mut client, &["BITFIELD", "s", "SET", "i8", "0", "127"]);
    expect_values(&mut client, &[Some(127)]);
    send(&mut client, &["BITFIELD", "s", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "1"]);
    expect_values(&mut client, &[Some(127)]);
    send(&mut client, &["BITFIELD", "s", "OVERFLOW", "FAIL", "INCRBY", "i8", "0", "1"]);
    expect_values(&mut client, &[None]);

    // u1 只有一位，1 + 1 回绕到 0。
    send(&mut client, &["BITFIELD", "one", "SET", "u1", "0", "1", "INCRBY", "u1", "0", "1"]);
    expect_values(&mut client, &[Some(0), Some(0)]);
    send(&mut client, &["BITFIELD", "one", "GET", "u1", "0"]);
    expect_values(&mut client, &[Some(0)]);

    // i64 边界：加一从最大值回绕到最小值。
    send(&mut client, &["BITFIELD", "wide", "INCRBY", "i64", "0", "9223372036854775807"]);
    expect_values(&mut client, &[Some(9223372036854775807)]);
    send(&mut client, &["BITFIELD", "wide", "INCRBY", "i64", "0", "1"]);
    expect_values(&mut client, &[Some(-9223372036854775808)]);

    // `#` 偏移按编码宽度倍乘，#1 与位偏移 8 等价。
    send(&mut client, &["BITFIELD", "hashy", "SET", "u8", "#1", "7", "GET", "u8", "8"]);
    expect_values(&mut client, &[Some(0), Some(7)]);
    send(&mut client, &["BITFIELD", "hashy", "GET", "u8", "#1"]);
    expect_values(&mut client, &[Some(7)]);

    // 只读 GET 不创建键，写入路径保留原有 TTL。
    send(&mut client, &["BITFIELD", "absent", "GET", "u8", "0"]);
    expect_values(&mut client, &[Some(0)]);
    send(&mut client, &["EXISTS", "absent"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["BITFIELD_RO", "absent", "GET", "u8", "0"]);
    expect_values(&mut client, &[Some(0)]);
    send(&mut client, &["SET", "keep", "abc", "EX", "100"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["BITFIELD", "keep", "INCRBY", "u8", "0", "1"]);
    expect_values(&mut client, &[Some(98)]);
    send(&mut client, &["TTL", "keep"]);
    assert_eq!(expect_integer(&mut client), 100);

    // 越界写入按需增长并补零，SET 的返回值是旧值。
    send(&mut client, &["BITFIELD", "grow", "SET", "u8", "0", "256"]);
    expect_values(&mut client, &[Some(0)]);
    send(&mut client, &["BITFIELD", "grow", "GET", "u8", "0"]);
    expect_values(&mut client, &[Some(0)]);

    // 真机把 OVERFLOW 同样作用于 SET：SAT 饱和到上界，FAIL 返回空并建键补零。
    send(&mut client, &["BITFIELD", "setov", "OVERFLOW", "SAT", "SET", "u8", "0", "300", "GET", "u8", "0"]);
    expect_values(&mut client, &[Some(0), Some(255)]);
    send(&mut client, &["BITFIELD", "setfail", "OVERFLOW", "FAIL", "SET", "u8", "0", "300"]);
    expect_values(&mut client, &[None]);
    send(&mut client, &["EXISTS", "setfail"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["STRLEN", "setfail"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["GET", "setfail"]);
    match read_reply(&mut client) {
        Reply::Bulk(Some(bytes)) => assert_eq!(bytes, vec![0x00]),
        other => panic!("expected zero byte, got {}", describe(&other)),
    }
    // 无符号 SAT 把负数按 u64 视角处理，落到上界而不是下限。
    send(&mut client, &["BITFIELD", "setneg", "OVERFLOW", "SAT", "SET", "u8", "0", "-1"]);
    expect_values(&mut client, &[Some(0)]);
    send(&mut client, &["BITFIELD", "setneg", "GET", "u8", "0"]);
    expect_values(&mut client, &[Some(255)]);
    // FAIL 的 INCRBY 也要按键宽补零，键照样存在。
    send(&mut client, &["BITFIELD", "failgrow", "OVERFLOW", "FAIL", "INCRBY", "u8", "16", "256"]);
    expect_values(&mut client, &[None]);
    send(&mut client, &["EXISTS", "failgrow"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["STRLEN", "failgrow"]);
    assert_eq!(expect_integer(&mut client), 3);

    // u63 用满 63 位：最大值写入后最高字节是 0xfe，加一回绕到 0。
    send(&mut client, &["BITFIELD", "u63", "SET", "u63", "0", "9223372036854775807", "GET", "u63", "0"]);
    expect_values(&mut client, &[Some(0), Some(9223372036854775807)]);
    send(&mut client, &["GET", "u63"]);
    match read_reply(&mut client) {
        Reply::Bulk(Some(bytes)) => {
            assert_eq!(bytes, vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe])
        }
        other => panic!("expected u63 bytes, got {}", describe(&other)),
    }
    send(&mut client, &["BITFIELD", "u63", "INCRBY", "u63", "0", "1"]);
    expect_values(&mut client, &[Some(0)]);
    send(&mut client, &["BITFIELD", "u63", "SET", "u63", "0", "9223372036854775807"]);
    expect_values(&mut client, &[Some(0)]);
    send(&mut client, &["BITFIELD", "u63", "OVERFLOW", "SAT", "INCRBY", "u63", "0", "1"]);
    expect_values(&mut client, &[Some(9223372036854775807)]);
    send(&mut client, &["BITFIELD", "u63", "SET", "u63", "0", "9223372036854775807"]);
    expect_values(&mut client, &[Some(9223372036854775807)]);
    send(&mut client, &["BITFIELD", "u63", "OVERFLOW", "FAIL", "INCRBY", "u63", "0", "1"]);
    expect_values(&mut client, &[None]);

    send(&mut client, &["HSET", "h", "f", "1"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["BITFIELD", "h", "GET", "u8", "0"]);
    expect_error(&mut client, WRONGTYPE);
    send(&mut client, &["BITFIELD_RO", "keep", "SET", "u8", "0", "1"]);
    expect_error(&mut client, "ERR BITFIELD_RO only supports the GET subcommand");
    send(&mut client, &["BITFIELD", "bf", "GET", "u64", "0"]);
    expect_error(&mut client, "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.");
    send(&mut client, &["BITFIELD", "bf", "GET", "i65", "0"]);
    expect_error(&mut client, "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.");
    send(&mut client, &["BITFIELD", "bf", "GET", "U8", "0"]);
    expect_error(&mut client, "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.");
    send(&mut client, &["BITFIELD", "bf", "GET", "i08", "0"]);
    expect_error(&mut client, "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.");
    send(&mut client, &["BITFIELD", "bf", "GET", "u8", "-1"]);
    expect_error(&mut client, "ERR bit offset is not an integer or out of range");
    send(&mut client, &["BITFIELD", "bf", "GET", "u8", "+8"]);
    expect_error(&mut client, "ERR bit offset is not an integer or out of range");
    send(&mut client, &["BITFIELD", "bf", "GET", "u8", "08"]);
    expect_error(&mut client, "ERR bit offset is not an integer or out of range");
    send(&mut client, &["BITFIELD", "bf", "SET", "u8", "0", "x"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");
    send(&mut client, &["BITFIELD", "bf", "OVERFLOW", "PREV", "INCRBY", "u8", "0", "1"]);
    expect_error(&mut client, "ERR Invalid OVERFLOW type specified");
    send(&mut client, &["BITFIELD", "bf", "BOGUS", "u8", "0"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["BITFIELD"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'bitfield' command");
}

#[test]
fn list_position_commands() {
    fn expect_positions(client: &mut TcpStream, expected: &[i64]) {
        match read_reply(client) {
            Reply::Array(Some(items)) => {
                assert_eq!(items.len(), expected.len());
                for (item, wanted) in items.iter().zip(expected) {
                    match item {
                        Reply::Integer(value) => assert_eq!(*value, *wanted),
                        other => panic!("unexpected LPOS reply {other:?}"),
                    }
                }
            }
            other => panic!("unexpected LPOS reply {other:?}"),
        }
    }

    let (mut client, _server) = memory_server("redis-lpos");
    send(&mut client, &["RPUSH", "l", "a", "b", "c", "a", "b", "c"]);
    expect_integer(&mut client);

    // 默认从头部找第一次命中。
    send(&mut client, &["LPOS", "l", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["LPOS", "l", "d"]);
    expect_bulk(&mut client, None);

    // 正 rank 从头部跳过前 n-1 次命中，负 rank 从尾部计次。
    send(&mut client, &["LPOS", "l", "a", "RANK", "2"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["LPOS", "l", "a", "RANK", "-1"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["LPOS", "l", "a", "RANK", "-2"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["LPOS", "l", "a", "RANK", "-3"]);
    expect_bulk(&mut client, None);

    // COUNT 0 取全部命中，正数只取前 n 次，负 rank 时按尾部到头部排序。
    send(&mut client, &["LPOS", "l", "a", "COUNT", "0"]);
    expect_positions(&mut client, &[0, 3]);
    send(&mut client, &["LPOS", "l", "a", "COUNT", "1"]);
    expect_positions(&mut client, &[0]);
    send(&mut client, &["LPOS", "l", "a", "RANK", "-1", "COUNT", "0"]);
    expect_positions(&mut client, &[3, 0]);
    send(&mut client, &["LPOS", "l", "d", "COUNT", "0"]);
    expect_positions(&mut client, &[]);

    // MAXLEN 只比较指定数量的元素，0 表示不限。
    send(&mut client, &["LPOS", "l", "a", "MAXLEN", "3"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["LPOS", "l", "a", "RANK", "2", "MAXLEN", "4"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["LPOS", "l", "a", "RANK", "2", "MAXLEN", "2"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["LPOS", "l", "a", "MAXLEN", "0"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["LPOS", "l", "a", "RANK", "-1", "MAXLEN", "1"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["LPOS", "l", "a", "RANK", "-1", "MAXLEN", "3"]);
    assert_eq!(expect_integer(&mut client), 3);

    // 空键：不带 COUNT 回复 null，带 COUNT 回复空数组。
    send(&mut client, &["LPOS", "missing", "a"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["LPOS", "missing", "a", "COUNT", "0"]);
    expect_empty_array(&mut client);

    // 负参数与非法参数。
    send(&mut client, &["LPOS", "l", "a", "RANK", "0"]);
    expect_error(&mut client, "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list");
    send(&mut client, &["LPOS", "l", "a", "COUNT", "-1"]);
    expect_error(&mut client, "ERR COUNT can't be negative");
    send(&mut client, &["LPOS", "l", "a", "MAXLEN", "-1"]);
    expect_error(&mut client, "ERR MAXLEN can't be negative");
    send(&mut client, &["LPOS", "l", "a", "RANK"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["LPOS", "l", "a", "BOGUS", "1"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["LPOS", "l"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'lpos' command");

    // 类型错误与非列表键。
    send(&mut client, &["SET", "str", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["LPOS", "str", "v"]);
    expect_error(&mut client, "WRONGTYPE Operation against a key holding the wrong kind of value");
}

#[test]
fn hyperloglog_commands() {
    let (mut client, _server) = memory_server("redis-hll");

    // 不带元素创建键：真机同样回复 1，并留下一个空的 dense HLL 字符串。
    send(&mut client, &["PFADD", "hll"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["EXISTS", "hll"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["PFCOUNT", "hll"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["TYPE", "hll"]);
    expect_status(&mut client, "string");
    send(&mut client, &["STRLEN", "hll"]);
    assert_eq!(expect_integer(&mut client), 12304);

    // 首次加入元素改动寄存器，重复加入不再改动。
    send(&mut client, &["PFADD", "hll", "a", "b", "c"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["PFCOUNT", "hll"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["PFADD", "hll", "a", "b", "c"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["PFCOUNT", "hll"]);
    assert_eq!(expect_integer(&mut client), 3);

    // 空字符串也是合法元素。
    send(&mut client, &["PFADD", "hll", ""]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["PFCOUNT", "hll"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["PFADD", "hll", "a", "b", "c", ""]);
    assert_eq!(expect_integer(&mut client), 0);

    // 第二个键与前四个元素无交集。
    send(&mut client, &["PFADD", "hll2", "d", "e", "f"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["PFCOUNT", "hll2"]);
    assert_eq!(expect_integer(&mut client), 3);

    // 多键按寄存器取最大值合并后估计，重复键不重复计数。
    send(&mut client, &["PFCOUNT", "hll", "hll2"]);
    assert_eq!(expect_integer(&mut client), 7);
    send(&mut client, &["PFCOUNT", "hll", "hll"]);
    assert_eq!(expect_integer(&mut client), 4);

    // 目标键也是来源之一，合并结果与多键估计一致。
    send(&mut client, &["PFMERGE", "dest", "hll", "hll2"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["PFCOUNT", "dest"]);
    assert_eq!(expect_integer(&mut client), 7);
    send(&mut client, &["TYPE", "dest"]);
    expect_status(&mut client, "string");
    send(&mut client, &["STRLEN", "dest"]);
    assert_eq!(expect_integer(&mut client), 12304);

    // 全部来源缺失时创建一个空的 HLL。
    send(&mut client, &["PFMERGE", "empty", "no-such-key", "also-missing"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["PFCOUNT", "empty"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["PFCOUNT", "no-such-key"]);
    assert_eq!(expect_integer(&mut client), 0);

    // 修改 HLL 内容不影响既有 TTL，合并写入目标键也保留目标的 TTL。
    send(&mut client, &["EXPIRE", "hll", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["PFADD", "hll", "g"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TTL", "hll"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "unexpected HLL TTL after PFADD: {ttl}");
    send(&mut client, &["EXPIRE", "dest", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["PFMERGE", "dest", "hll"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["TTL", "dest"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "unexpected HLL TTL after PFMERGE: {ttl}");

    // 大批量元素走 dense 寄存器，估计值与真机公式一致。
    let items: Vec<String> = (0..1000).map(|index| format!("item-{index}")).collect();
    let mut arguments: Vec<&str> = vec!["PFADD", "many"];
    arguments.extend(items.iter().map(String::as_str));
    send(&mut client, &arguments);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["PFCOUNT", "many"]);
    assert_eq!(expect_integer(&mut client), 996);

    // 普通字符串与非 dense 内容走 HLL 专用的类型错误文案。
    send(&mut client, &["SET", "plain", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["PFADD", "plain", "x"]);
    expect_error(&mut client, "WRONGTYPE Key is not a valid HyperLogLog string value.");
    send(&mut client, &["PFCOUNT", "plain"]);
    expect_error(&mut client, "WRONGTYPE Key is not a valid HyperLogLog string value.");
    send(&mut client, &["PFCOUNT", "hll", "plain"]);
    expect_error(&mut client, "WRONGTYPE Key is not a valid HyperLogLog string value.");
    send(&mut client, &["PFMERGE", "dest", "plain"]);
    expect_error(&mut client, "WRONGTYPE Key is not a valid HyperLogLog string value.");

    // 非字符串类型走常规 WRONGTYPE。
    send(&mut client, &["LPUSH", "list", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["PFADD", "list", "v"]);
    expect_error(&mut client, "WRONGTYPE Operation against a key holding the wrong kind of value");
    send(&mut client, &["PFCOUNT", "list"]);
    expect_error(&mut client, "WRONGTYPE Operation against a key holding the wrong kind of value");
    send(&mut client, &["PFMERGE", "dest", "list"]);
    expect_error(&mut client, "WRONGTYPE Operation against a key holding the wrong kind of value");

    // arity。
    send(&mut client, &["PFADD"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'pfadd' command");
    send(&mut client, &["PFCOUNT"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'pfcount' command");
    send(&mut client, &["PFMERGE", "dest"]);
    expect_status(&mut client, "OK");
}

/// `KEYS` / `TYPE` / `SCAN` 共用同一个逻辑键空间：五种集合类型与 HLL 都参与列举，
/// `MATCH` 只过滤返回项，读数类命令不改动既有 TTL。
#[test]
fn key_space_commands() {
    let (mut client, _server) = memory_server("redis-keyspace");

    send(&mut client, &["KEYS", "*"]);
    expect_empty_array(&mut client);
    send(&mut client, &["KEYS"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'keys' command");

    send(&mut client, &["SET", "user:1", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["HSET", "user:2", "f", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["RPUSH", "user:3", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["SADD", "order:1", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZADD", "order:2", "1", "v"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["PFADD", "order:3", "v"]);
    assert_eq!(expect_integer(&mut client), 1);

    send(&mut client, &["KEYS", "user:*"]);
    expect_array(&mut client, &["user:1", "user:2", "user:3"]);
    send(&mut client, &["KEYS", "order:*"]);
    expect_array(&mut client, &["order:1", "order:2", "order:3"]);
    send(&mut client, &["KEYS", "user:?"]);
    expect_array(&mut client, &["user:1", "user:2", "user:3"]);
    send(&mut client, &["KEYS", "*"]);
    expect_array(&mut client, &["order:1", "order:2", "order:3", "user:1", "user:2", "user:3"]);
    send(&mut client, &["KEYS", "user:1"]);
    expect_array(&mut client, &["user:1"]);
    send(&mut client, &["KEYS", "nomatch*"]);
    expect_empty_array(&mut client);
    send(&mut client, &["KEYS", "user:1", "extra"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'keys' command");

    for (key, expected) in [
        ("user:1", "string"),
        ("user:2", "hash"),
        ("user:3", "list"),
        ("order:1", "set"),
        ("order:2", "zset"),
        ("order:3", "string"),
        ("ghost", "none"),
    ] {
        send(&mut client, &["TYPE", key]);
        expect_status(&mut client, expected);
    }

    // 读数类命令不改动 TTL：过期时间仍保留，SCAN 也能看到该键。
    send(&mut client, &["EXPIRE", "user:2", "100"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TYPE", "user:2"]);
    expect_status(&mut client, "hash");
    send(&mut client, &["TTL", "user:2"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "TYPE changed the TTL: {ttl}");

    send(&mut client, &["SCAN", "0", "MATCH", "user:*", "COUNT", "100"]);
    let (cursor, keys) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert_eq!(keys, ["user:1", "user:2", "user:3"]);
    send(&mut client, &["TTL", "user:2"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "SCAN changed the TTL: {ttl}");

    send(&mut client, &["KEYS", "user:*"]);
    expect_array(&mut client, &["user:1", "user:2", "user:3"]);
    send(&mut client, &["TTL", "user:2"]);
    let ttl = expect_integer(&mut client);
    assert!((1..=100).contains(&ttl), "KEYS changed the TTL: {ttl}");

    // 游标进位：每页 3 个键连续推进，直到 0。
    let mut cursor = "0".to_string();
    let mut seen = Vec::new();
    let mut rounds = 0;
    loop {
        send(&mut client, &["SCAN", &cursor, "COUNT", "3"]);
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
    assert_eq!(seen, ["order:1", "order:2", "order:3", "user:1", "user:2", "user:3"]);

    send(&mut client, &["SCAN", "0", "COUNT", "-1"]);
    expect_error(&mut client, "ERR syntax error");

    // 字符类：`[abc]`、`[^...]`、`[a-z]`、类内 `\` 转义与未闭合类。
    send(&mut client, &["SET", "star-*", "v"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["KEYS", "user:[12]"]);
    expect_array(&mut client, &["user:1", "user:2"]);
    send(&mut client, &["KEYS", "user:[^1]"]);
    expect_array(&mut client, &["user:2", "user:3"]);
    send(&mut client, &["KEYS", "user:[1-2]"]);
    expect_array(&mut client, &["user:1", "user:2"]);
    send(&mut client, &["KEYS", "user:[1"]);
    expect_array(&mut client, &["user:1"]);
    send(&mut client, &["KEYS", "star-\\*"]);
    expect_array(&mut client, &["star-*"]);
    send(&mut client, &["KEYS", "star-[*]"]);
    expect_array(&mut client, &["star-*"]);
    send(&mut client, &["SCAN", "0", "MATCH", "user:[12]", "COUNT", "100"]);
    let (cursor, keys) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert_eq!(keys, ["user:1", "user:2"]);
    send(&mut client, &["SCAN", "0", "MATCH", "order:[^12]", "COUNT", "100"]);
    let (cursor, keys) = expect_scan(&mut client);
    assert_eq!(cursor, "0");
    assert_eq!(keys, ["order:3"]);
}

/// 读取一段原始回复字节，用于 RESP3 下的精确比对。
fn expect_raw(client: &mut TcpStream, expected: &[u8]) {
    let mut buffer = vec![0u8; expected.len()];
    client.read_exact(&mut buffer).unwrap();
    assert_eq!(buffer, expected, "unexpected reply bytes");
}

/// 把 `[member, score]` 子数组解成字符串对。
fn pair_items(reply: &Reply) -> Vec<(String, String)> {
    match reply {
        Reply::Array(Some(items)) => items
            .iter()
            .map(|item| match item {
                Reply::Array(Some(pair)) if pair.len() == 2 => {
                    let member = match &pair[0] {
                        Reply::Bulk(Some(bytes)) => String::from_utf8(bytes.clone()).unwrap(),
                        other => panic!("expected member bulk, got {}", describe(other)),
                    };
                    let score = match &pair[1] {
                        Reply::Bulk(Some(bytes)) => String::from_utf8(bytes.clone()).unwrap(),
                        other => panic!("expected score bulk, got {}", describe(other)),
                    };
                    (member, score)
                }
                other => panic!("expected member/score pair, got {}", describe(other)),
            })
            .collect(),
        other => panic!("expected pair array, got {}", describe(other)),
    }
}

/// `ZMPOP` 的 `[key, [[member, score]...]]` 回复。
fn expect_mpop(client: &mut TcpStream, key: &str, expected: &[(&str, &str)]) {
    match read_reply(client) {
        Reply::Array(Some(items)) if items.len() == 2 => {
            match &items[0] {
                Reply::Bulk(Some(bytes)) => assert_eq!(bytes, key.as_bytes()),
                other => panic!("expected key bulk, got {}", describe(other)),
            }
            let pairs = pair_items(&items[1]);
            let expected: Vec<(String, String)> =
                expected.iter().map(|(member, score)| (member.to_string(), score.to_string())).collect();
            assert_eq!(pairs, expected);
        }
        other => panic!("expected mpop array, got {}", describe(&other)),
    }
}

/// zset 族的选项、范围、弹出、随机与集合运算。
#[test]
fn zset_family_options_ranges_and_pops() {
    let (mut client, _server) = memory_server("redis-zset-family");

    // ZADD 的 NX / XX / GT / LT / CH / INCR 组合。
    send(&mut client, &["ZADD", "z", "1", "a", "2", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZADD", "z", "NX", "9", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZSCORE", "z", "a"]);
    expect_bulk(&mut client, Some("1"));
    send(&mut client, &["ZADD", "z", "XX", "9", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZSCORE", "z", "a"]);
    expect_bulk(&mut client, Some("9"));
    send(&mut client, &["ZADD", "z", "CH", "9", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZADD", "z", "CH", "5", "a"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZADD", "z", "GT", "3", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZADD", "z", "LT", "3", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZADD", "z", "GT", "7", "a"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZSCORE", "z", "a"]);
    expect_bulk(&mut client, Some("7"));
    send(&mut client, &["ZADD", "z", "INCR", "2.5", "a"]);
    expect_bulk(&mut client, Some("9.5"));
    send(&mut client, &["ZADD", "z", "NX", "INCR", "1", "a"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["ZADD", "z", "XX", "INCR", "1", "missing"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["ZADD", "z", "NX", "XX", "1", "a"]);
    expect_error(&mut client, "ERR XX and NX options at the same time are not compatible");
    send(&mut client, &["ZADD", "z", "GT", "NX", "1", "a"]);
    expect_error(&mut client, "ERR GT, LT, and/or NX options at the same time are not compatible");
    send(&mut client, &["ZADD", "z", "GT", "LT", "1", "a"]);
    expect_error(&mut client, "ERR GT, LT, and/or NX options at the same time are not compatible");
    send(&mut client, &["ZADD", "z", "INCR", "1", "a", "2", "b"]);
    expect_error(&mut client, "ERR INCR option supports a single increment-element pair");
    send(&mut client, &["ZADD", "z", "NX", "1"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["ZADD", "z"]);
    expect_error(&mut client, "ERR wrong number of arguments for 'zadd' command");

    // ZRANGE 的四种范围形态与选项互斥。
    send(&mut client, &["ZADD", "r", "1", "a", "2", "b", "2", "c", "3", "d"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["ZRANGE", "r", "0", "-1"]);
    expect_array(&mut client, &["a", "b", "c", "d"]);
    send(&mut client, &["ZRANGE", "r", "0", "-1", "REV"]);
    expect_array(&mut client, &["d", "c", "b", "a"]);
    send(&mut client, &["ZRANGE", "r", "1", "2", "REV"]);
    expect_array(&mut client, &["c", "b"]);
    send(&mut client, &["ZRANGE", "r", "(1", "3", "BYSCORE", "WITHSCORES"]);
    expect_array(&mut client, &["b", "2", "c", "2", "d", "3"]);
    send(&mut client, &["ZRANGE", "r", "3", "(1", "BYSCORE", "REV"]);
    expect_array(&mut client, &["d", "c", "b"]);
    send(&mut client, &["ZRANGE", "r", "3", "2", "BYSCORE", "LIMIT", "0", "1", "REV"]);
    expect_array(&mut client, &["d"]);
    send(&mut client, &["ZRANGE", "r", "-", "+", "BYLEX"]);
    expect_array(&mut client, &["a", "b", "c", "d"]);
    send(&mut client, &["ZRANGE", "r", "+", "-", "BYLEX", "REV"]);
    expect_array(&mut client, &["d", "c", "b", "a"]);
    send(&mut client, &["ZRANGE", "r", "[b", "(d", "BYLEX"]);
    expect_array(&mut client, &["b", "c"]);
    send(&mut client, &["ZRANGE", "r", "0", "-1", "LIMIT", "0", "1"]);
    expect_error(
        &mut client,
        "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
    );
    send(&mut client, &["ZRANGE", "r", "-", "+", "BYLEX", "WITHSCORES"]);
    expect_error(&mut client, "ERR syntax error, WITHSCORES not supported in combination with BYLEX");
    send(&mut client, &["ZRANGE", "r", "0", "-1", "BOGUS"]);
    expect_error(&mut client, "ERR syntax error");

    send(&mut client, &["ZREVRANGE", "r", "0", "-1", "WITHSCORES"]);
    expect_array(&mut client, &["d", "3", "c", "2", "b", "2", "a", "1"]);
    send(&mut client, &["ZREVRANGE", "r", "0", "0"]);
    expect_array(&mut client, &["d"]);

    // ZRANGESTORE 覆盖任意类型的目标，空结果删除目标。
    send(&mut client, &["ZRANGESTORE", "copy", "r", "3", "1", "BYSCORE", "REV", "LIMIT", "0", "2"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZRANGE", "copy", "0", "-1", "WITHSCORES"]);
    expect_array(&mut client, &["c", "2", "d", "3"]);
    send(&mut client, &["ZRANGESTORE", "copy", "r", "5", "6", "BYSCORE"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["EXISTS", "copy"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["SET", "text", "x"]);
    expect_status(&mut client, "OK");
    send(&mut client, &["ZRANGESTORE", "text", "r", "0", "0"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["TYPE", "text"]);
    expect_status(&mut client, "zset");
    send(&mut client, &["ZRANGESTORE", "copy", "text", "-", "+", "BYLEX", "WITHSCORES"]);
    expect_error(&mut client, "ERR syntax error");

    // 排名、批量分数与字典序计数。
    send(&mut client, &["ZRANK", "r", "b"]);
    assert_eq!(expect_integer(&mut client), 1);
    send(&mut client, &["ZREVRANK", "r", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZRANK", "r", "b", "WITHSCORE"]);
    match read_reply(&mut client) {
        Reply::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                Reply::Integer(rank) => assert_eq!(*rank, 1),
                other => panic!("expected rank integer, got {}", describe(other)),
            }
            match &items[1] {
                Reply::Bulk(Some(bytes)) => assert_eq!(bytes, b"2"),
                other => panic!("expected score bulk, got {}", describe(other)),
            }
        }
        other => panic!("expected WITHSCORE array, got {}", describe(&other)),
    }
    send(&mut client, &["ZRANK", "r", "ghost", "WITHSCORE"]);
    expect_null_array(&mut client);
    send(&mut client, &["ZRANK", "r", "b", "BOGUS"]);
    expect_error(&mut client, "ERR syntax error");

    send(&mut client, &["ZMSCORE", "r", "a", "b", "ghost"]);
    match read_reply(&mut client) {
        Reply::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            match &items[0] {
                Reply::Bulk(Some(bytes)) => assert_eq!(bytes, b"1"),
                other => panic!("expected score bulk, got {}", describe(other)),
            }
            assert!(matches!(items[2], Reply::Bulk(None)), "missing member should be null");
        }
        other => panic!("expected ZMSCORE array, got {}", describe(&other)),
    }
    send(&mut client, &["ZMSCORE", "ghost", "a"]);
    match read_reply(&mut client) {
        Reply::Array(Some(items)) => assert!(matches!(items[0], Reply::Bulk(None))),
        other => panic!("expected ZMSCORE array, got {}", describe(&other)),
    }

    send(&mut client, &["ZLEXCOUNT", "r", "-", "+"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["ZLEXCOUNT", "r", "[b", "[c"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZLEXCOUNT", "r", "b", "c"]);
    expect_error(&mut client, "ERR min or max not valid string range item");

    // 弹出族与键清空即删除。
    send(&mut client, &["ZADD", "p", "1", "a", "2", "b", "3", "c", "4", "d"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["ZPOPMIN", "p"]);
    expect_array(&mut client, &["a", "1"]);
    send(&mut client, &["ZPOPMAX", "p", "2"]);
    expect_array(&mut client, &["d", "4", "c", "3"]);
    send(&mut client, &["ZPOPMIN", "p", "0"]);
    expect_empty_array(&mut client);
    send(&mut client, &["ZPOPMIN", "p", "-1"]);
    expect_error(&mut client, "ERR value is out of range, must be positive");
    send(&mut client, &["ZPOPMIN", "p", "x"]);
    expect_error(&mut client, "ERR value is out of range, must be positive");
    send(&mut client, &["ZPOPMIN", "p", "5"]);
    expect_array(&mut client, &["b", "2"]);
    send(&mut client, &["EXISTS", "p"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZPOPMIN", "p"]);
    expect_empty_array(&mut client);

    // ZRANDMEMBER 的成员域、数量与去重契约。
    send(&mut client, &["ZADD", "rand", "1", "a", "2", "b", "3", "c"]);
    assert_eq!(expect_integer(&mut client), 3);
    send(&mut client, &["ZRANDMEMBER", "rand"]);
    let single = match read_reply(&mut client) {
        Reply::Bulk(Some(bytes)) => String::from_utf8(bytes).unwrap(),
        other => panic!("expected bulk member, got {}", describe(&other)),
    };
    assert!(["a", "b", "c"].contains(&single.as_str()), "unexpected member {single}");
    send(&mut client, &["ZRANDMEMBER", "rand", "10"]);
    let mut unique = match read_reply(&mut client) {
        Reply::Array(Some(items)) => items
            .into_iter()
            .map(|item| match item {
                Reply::Bulk(Some(bytes)) => String::from_utf8(bytes).unwrap(),
                other => panic!("expected bulk member, got {}", describe(&other)),
            })
            .collect::<Vec<_>>(),
        other => panic!("expected member array, got {}", describe(&other)),
    };
    assert_eq!(unique.len(), 3);
    unique.sort();
    assert_eq!(unique, ["a", "b", "c"]);
    send(&mut client, &["ZRANDMEMBER", "rand", "-5"]);
    let repeated = match read_reply(&mut client) {
        Reply::Array(Some(items)) => items
            .into_iter()
            .map(|item| match item {
                Reply::Bulk(Some(bytes)) => String::from_utf8(bytes).unwrap(),
                other => panic!("expected bulk member, got {}", describe(&other)),
            })
            .collect::<Vec<_>>(),
        other => panic!("expected member array, got {}", describe(&other)),
    };
    assert_eq!(repeated.len(), 5);
    assert!(repeated.iter().all(|member| ["a", "b", "c"].contains(&member.as_str())));
    send(&mut client, &["ZRANDMEMBER", "rand", "0"]);
    expect_empty_array(&mut client);
    send(&mut client, &["ZRANDMEMBER", "rand", "2", "WITHSCORES"]);
    match read_reply(&mut client) {
        Reply::Array(Some(items)) => {
            // RESP2 把分数平铺在成员之后。
            assert_eq!(items.len(), 4);
            for pair in items.chunks(2) {
                let (Reply::Bulk(Some(member)), Reply::Bulk(Some(score))) = (&pair[0], &pair[1])
                else {
                    panic!("expected member/score bulk pair");
                };
                let member = String::from_utf8(member.clone()).unwrap();
                let score = String::from_utf8(score.clone()).unwrap();
                assert!(
                    matches!((member.as_str(), score.as_str()), ("a", "1") | ("b", "2") | ("c", "3")),
                    "unexpected member {member} with score {score}"
                );
            }
        }
        other => panic!("expected WITHSCORES array, got {}", describe(&other)),
    }
    send(&mut client, &["ZRANDMEMBER", "rand", "-1", "WITHSCORES"]);
    match read_reply(&mut client) {
        Reply::Array(Some(items)) => assert_eq!(items.len(), 2),
        other => panic!("expected WITHSCORES array, got {}", describe(&other)),
    }
    send(&mut client, &["ZRANDMEMBER", "ghost"]);
    expect_bulk(&mut client, None);
    send(&mut client, &["ZRANDMEMBER", "rand", "WITHSCORES"]);
    expect_error(&mut client, "ERR value is not an integer or out of range");

    // ZMPOP / BZMPOP 的非阻塞形态。
    send(&mut client, &["ZADD", "m2", "1", "a", "2", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZMPOP", "1", "ghost", "MIN"]);
    expect_null_array(&mut client);
    send(&mut client, &["ZMPOP", "2", "ghost", "m2", "MAX", "COUNT", "1"]);
    expect_mpop(&mut client, "m2", &[("b", "2")]);
    send(&mut client, &["ZMPOP", "1", "m2", "MIN"]);
    expect_mpop(&mut client, "m2", &[("a", "1")]);
    send(&mut client, &["EXISTS", "m2"]);
    assert_eq!(expect_integer(&mut client), 0);
    send(&mut client, &["ZMPOP", "0", "m2", "MIN"]);
    expect_error(&mut client, "ERR numkeys should be greater than 0");
    send(&mut client, &["ZMPOP", "1", "m2", "SIDE"]);
    expect_error(&mut client, "ERR syntax error");
    send(&mut client, &["ZMPOP", "1", "m2", "MIN", "COUNT", "0"]);
    expect_error(&mut client, "ERR count should be greater than 0");

    send(&mut client, &["ZADD", "bz", "1", "a", "2", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["BZPOPMIN", "ghost", "bz", "0"]);
    expect_array(&mut client, &["bz", "a", "1"]);
    send(&mut client, &["BZPOPMAX", "bz", "0.01"]);
    expect_array(&mut client, &["bz", "b", "2"]);
    send(&mut client, &["BZPOPMIN", "bz", "0"]);
    expect_null_array(&mut client);
    send(&mut client, &["BZPOPMIN", "bz", "-1"]);
    expect_error(&mut client, "ERR timeout is negative");
    send(&mut client, &["BZPOPMIN", "bz", "abc"]);
    expect_error(&mut client, "ERR timeout is not a float or out of range");
    send(&mut client, &["BZMPOP", "0", "1", "bz", "MIN"]);
    expect_null_array(&mut client);

    // 非存储型的并集与交集。
    send(&mut client, &["ZADD", "u1", "1", "a", "2", "b"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZADD", "u2", "3", "b", "4", "c"]);
    assert_eq!(expect_integer(&mut client), 2);
    send(&mut client, &["ZUNION", "2", "u1", "u2", "WITHSCORES"]);
    expect_array(&mut client, &["a", "1", "c", "4", "b", "5"]);
    send(&mut client, &["ZINTER", "2", "u1", "u2", "WITHSCORES"]);
    expect_array(&mut client, &["b", "5"]);
    send(&mut client, &["ZINTER", "2", "u1", "u2", "WEIGHTS", "2", "3", "AGGREGATE", "MAX"]);
    expect_array(&mut client, &["b"]);
    send(&mut client, &["ZUNION", "0", "u1"]);
    expect_error(&mut client, "ERR at least 1 input key is needed for 'zunion' command");
    send(&mut client, &["ZUNION", "2", "u1"]);
    expect_error(&mut client, "ERR syntax error");

    // 分数格式逐字节对齐真机。
    send(&mut client, &["ZADD", "score", "1e-20", "tiny", "inf", "up", "-inf", "down", "-3", "neg"]);
    assert_eq!(expect_integer(&mut client), 4);
    send(&mut client, &["ZADD", "score", "1.5", "half", "0.1", "tenth", "1", "one"]);
    assert_eq!(expect_integer(&mut client), 3);
    // `2^62` 仍在整数路径上，超过后改走最短往返数字。
    send(&mut client, &["ZADD", "score", "4611686018427387904", "edge", "4611686018427388928", "past"]);
    assert_eq!(expect_integer(&mut client), 2);
    for (member, expected) in [
        ("tiny", "1e-20"),
        ("up", "inf"),
        ("down", "-inf"),
        ("neg", "-3"),
        ("half", "1.5"),
        ("tenth", "0.1"),
        ("one", "1"),
        ("edge", "4611686018427387904"),
        ("past", "4611686018427389000"),
    ] {
        send(&mut client, &["ZSCORE", "score", member]);
        expect_bulk(&mut client, Some(expected));
    }
}

/// RESP3 下分数使用原生 double，`WITHSCORES` 使用成员/分数子数组。
#[test]
fn zset_resp3_double_replies() {
    let (mut client, _server) = memory_server("redis-zset-resp3");
    send(&mut client, &["HELLO", "3"]);
    let mut header = [0u8; 4];
    client.read_exact(&mut header).unwrap();
    assert_eq!(&header, b"%7\r\n", "HELLO 3 did not switch protocol");
    let mut rest = Vec::new();
    loop {
        let mut chunk = [0u8; 1];
        client.read_exact(&mut chunk).unwrap();
        rest.push(chunk[0]);
        if rest.ends_with(b"*0\r\n") {
            break;
        }
    }

    send(&mut client, &["ZADD", "z", "1", "a", "2.5", "b"]);
    expect_raw(&mut client, b":2\r\n");
    send(&mut client, &["ZSCORE", "z", "a"]);
    expect_raw(&mut client, b",1\r\n");
    send(&mut client, &["ZRANGE", "z", "0", "-1", "WITHSCORES"]);
    expect_raw(&mut client, b"*2\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2.5\r\n");
    send(&mut client, &["ZINCRBY", "z", "0.5", "a"]);
    expect_raw(&mut client, b",1.5\r\n");
    send(&mut client, &["ZMSCORE", "z", "a", "ghost"]);
    expect_raw(&mut client, b"*2\r\n,1.5\r\n_\r\n");
    send(&mut client, &["ZRANK", "z", "ghost", "WITHSCORE"]);
    expect_raw(&mut client, b"_\r\n");
    send(&mut client, &["ZPOPMIN", "z"]);
    expect_raw(&mut client, b"*2\r\n$1\r\na\r\n,1.5\r\n");
    send(&mut client, &["ZRANDMEMBER", "ghost"]);
    expect_raw(&mut client, b"_\r\n");
    send(&mut client, &["ZMPOP", "1", "ghost", "MIN"]);
    expect_raw(&mut client, b"_\r\n");
    send(&mut client, &["BZPOPMIN", "ghost", "0"]);
    expect_raw(&mut client, b"_\r\n");
    send(&mut client, &["ZRANGE", "z", "0", "-1", "WITHSCORES"]);
    expect_raw(&mut client, b"*1\r\n*2\r\n$1\r\nb\r\n,2.5\r\n");
}
