//! `WATCH` / `UNWATCH` smoke test — optimistic locking over a real LocalExecutor.

use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::Duration,
};

use database_cluster::topology::ShardMap;
use database_execution::cluster::{KeyValueExecutor, LocalExecutor};
use database_query::cluster_kv::{KeyValueAction, KeyValueCommand};
use database_server::yyds::redis::{connection::serve_cancellable_with_executor, resp::RequestLimits};
use database_storage::cluster_kv::MemoryShard;
use database_types::cluster::{Namespace, ShardEpoch, ShardId};

const NAMESPACE: &str = "redis:0";

fn executor_server() -> (TcpStream, Arc<dyn KeyValueExecutor>, thread::JoinHandle<()>) {
    let shard = ShardId("redis-watch".into());
    let executor: Arc<dyn KeyValueExecutor> = Arc::new(
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
    let executor_handle = Arc::clone(&executor);
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_cancellable_with_executor(stream, RequestLimits::default(), stopping, executor).unwrap();
    });
    let client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    client.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
    (client, executor_handle, server)
}

fn send(stream: &mut TcpStream, arguments: &[&str]) {
    let mut frame = format!("*{}\r\n", arguments.len());
    for argument in arguments {
        frame.push_str(&format!("${}\r\n{}\r\n", argument.len(), argument));
    }
    stream.write_all(frame.as_bytes()).unwrap();
}

fn expect(stream: &mut TcpStream, expected: &[u8]) {
    let mut buffer = vec![0u8; expected.len()];
    stream.read_exact(&mut buffer).unwrap();
    assert_eq!(buffer, expected, "unexpected RESP reply");
}

/// Writes a watched key from outside the connection, through the same executor the
/// server uses, so the `EXEC` comparison sees a genuine post-`WATCH` write.
fn put(executor: &dyn KeyValueExecutor, key: &str, value: &str) {
    executor
        .execute(
            None,
            KeyValueCommand {
                namespace: Namespace(NAMESPACE.into()),
                key: key.as_bytes().to_vec(),
                action: KeyValueAction::Put(value.as_bytes().to_vec()),
            },
        )
        .unwrap();
}

/// A write between `WATCH` and `EXEC` turns `EXEC` into the nil array and drops the queue.
#[test]
fn exec_aborts_when_watched_key_changed() {
    let (mut client, executor, _server) = executor_server();
    send(&mut client, &["SET", "k", "v1"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["WATCH", "k"]);
    expect(&mut client, b"+OK\r\n");
    put(executor.as_ref(), "k", "v2");
    send(&mut client, &["MULTI"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["SET", "k", "v3"]);
    expect(&mut client, b"+QUEUED\r\n");
    send(&mut client, &["EXEC"]);
    expect(&mut client, b"*-1\r\n");
    // The aborted queue never ran, so the external write is still the visible value.
    send(&mut client, &["GET", "k"]);
    expect(&mut client, b"$2\r\nv2\r\n");
    // EXEC released the watch, so an unchanged transaction runs next.
    send(&mut client, &["WATCH", "k"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["MULTI"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["SET", "k", "v4"]);
    expect(&mut client, b"+QUEUED\r\n");
    send(&mut client, &["EXEC"]);
    expect(&mut client, b"*1\r\n+OK\r\n");
}

/// `UNWATCH` and `DISCARD` both release the pins, so later writes no longer abort `EXEC`.
#[test]
fn unwatch_and_discard_release_watches() {
    let (mut client, executor, _server) = executor_server();
    send(&mut client, &["SET", "a", "1"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["WATCH", "a"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["UNWATCH"]);
    expect(&mut client, b"+OK\r\n");
    put(executor.as_ref(), "a", "2");
    send(&mut client, &["MULTI"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["SET", "b", "1"]);
    expect(&mut client, b"+QUEUED\r\n");
    send(&mut client, &["EXEC"]);
    expect(&mut client, b"*1\r\n+OK\r\n");

    send(&mut client, &["WATCH", "a"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["MULTI"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["DISCARD"]);
    expect(&mut client, b"+OK\r\n");
    put(executor.as_ref(), "a", "3");
    send(&mut client, &["MULTI"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["SET", "c", "1"]);
    expect(&mut client, b"+QUEUED\r\n");
    send(&mut client, &["EXEC"]);
    expect(&mut client, b"*1\r\n+OK\r\n");
}

/// `WATCH` inside `MULTI` is rejected, and a `EXEC` after it runs an empty queue.
#[test]
fn watch_inside_multi_is_rejected() {
    let (mut client, _executor, _server) = executor_server();
    send(&mut client, &["SET", "k", "v1"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["MULTI"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["WATCH", "k"]);
    expect(&mut client, b"-ERR WATCH inside MULTI is not allowed\r\n");
    send(&mut client, &["EXEC"]);
    expect(&mut client, b"*0\r\n");
}

/// RESP3 reports the aborted transaction as the dedicated null type.
#[test]
fn exec_abort_uses_resp3_null() {
    let (mut client, executor, _server) = executor_server();
    send(&mut client, &["SET", "k", "v1"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["HELLO", "3"]);
    // HELLO 3 replies with a RESP3 map; consume its body up to the trailing empty
    // `modules` array so the next reply is read from a clean boundary.
    let mut header = [0u8; 4];
    client.read_exact(&mut header).unwrap();
    assert_eq!(&header, b"%7\r\n", "HELLO 3 did not switch protocol");
    let mut rest = Vec::new();
    // The remaining map body ends with the `modules` empty array.
    loop {
        let mut chunk = [0u8; 1];
        client.read_exact(&mut chunk).unwrap();
        rest.push(chunk[0]);
        if rest.ends_with(b"*0\r\n") {
            break;
        }
    }
    send(&mut client, &["WATCH", "k"]);
    expect(&mut client, b"+OK\r\n");
    put(executor.as_ref(), "k", "v2");
    send(&mut client, &["MULTI"]);
    expect(&mut client, b"+OK\r\n");
    send(&mut client, &["SET", "k", "v3"]);
    expect(&mut client, b"+QUEUED\r\n");
    send(&mut client, &["EXEC"]);
    expect(&mut client, b"_\r\n");
}
