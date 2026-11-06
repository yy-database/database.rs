//! 阻塞列表命令的多连接集成测试。
//!
//! 覆盖超时精度、`0` 无限等待、同键多客户端的 FIFO 唤醒、RESP2 / RESP3 的 null 形态，
//! 以及 `BLMOVE` / `BRPOPLPUSH` 的原子搬运与 `WRONGTYPE`。所有连接共享同一个
//! executor、访问计时器与阻塞等待表，和真实监听器的布局一致。

use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, atomic::AtomicBool},
    thread,
    time::{Duration, Instant},
};

use database_cluster::topology::ShardMap;
use database_execution::cluster::{KeyValueExecutor, LocalExecutor};
use database_server::yyds::redis::{
    blocking::BlockingRegistry,
    connection::{KeyAccessTracker, serve_cancellable_with_executor_and_tracker},
    pubsub::PubSubRegistry,
    resp::RequestLimits,
};
use database_storage::cluster_kv::MemoryShard;
use database_types::cluster::{ShardEpoch, ShardId};

/// 启动一个监听器，接受 `clients` 条连接并为每条连接开线程。
fn start_server(clients: usize) -> (SocketAddr, Arc<dyn KeyValueExecutor>) {
    let shard = ShardId("redis-blocking".into());
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
    let tracker = Arc::new(KeyAccessTracker::new());
    let registry = Arc::new(BlockingRegistry::new());
    let pubsub = Arc::new(PubSubRegistry::new());
    let server_executor = Arc::clone(&executor);
    thread::spawn(move || {
        let mut workers = Vec::new();
        for _ in 0..clients {
            let (stream, _) = listener.accept().unwrap();
            let executor = Arc::clone(&server_executor);
            let tracker = Arc::clone(&tracker);
            let registry = Arc::clone(&registry);
            let pubsub = Arc::clone(&pubsub);
            let cancellation = Arc::clone(&stopping);
            workers.push(thread::spawn(move || {
                let _ = serve_cancellable_with_executor_and_tracker(
                    stream,
                    RequestLimits::default(),
                    cancellation,
                    executor,
                    tracker,
                    registry,
                    pubsub,
                );
            }));
        }
        for worker in workers {
            let _ = worker.join();
        }
    });
    (address, executor)
}

fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    stream
}

fn send(stream: &mut TcpStream, arguments: &[&str]) {
    let mut frame = format!("*{}\r\n", arguments.len());
    for argument in arguments {
        frame.push_str(&format!("${}\r\n{}\r\n", argument.len(), argument));
    }
    stream.write_all(frame.as_bytes()).unwrap();
}

fn read_line(stream: &mut TcpStream) -> Vec<u8> {
    let mut line = Vec::new();
    let mut previous = 0_u8;
    loop {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).unwrap();
        line.push(byte[0]);
        if previous == b'\r' && byte[0] == b'\n' {
            return line;
        }
        previous = byte[0];
    }
}

/// 读回一个完整 RESP 值，按原始字节重建，便于直接和期望文本比较。
fn read_value(stream: &mut TcpStream) -> Vec<u8> {
    let mut head = [0_u8; 1];
    stream.read_exact(&mut head).unwrap();
    read_after_head(stream, head[0])
}

fn read_after_head(stream: &mut TcpStream, head: u8) -> Vec<u8> {
    let mut out = vec![head];
    match head {
        b'+' | b'-' | b':' | b',' | b'(' | b'#' | b'_' => {
            out.extend(read_line(stream));
        }
        b'$' | b'!' => {
            let line = read_line(stream);
            let length = String::from_utf8(line.clone()).unwrap();
            let length: i64 = length.trim_end_matches("\r\n").parse().unwrap();
            out.extend(line);
            if length >= 0 {
                let mut body = vec![0_u8; length as usize + 2];
                stream.read_exact(&mut body).unwrap();
                out.extend(body);
            }
        }
        b'*' | b'~' | b'>' => {
            let line = read_line(stream);
            let count = String::from_utf8(line.clone()).unwrap();
            let count: i64 = count.trim_end_matches("\r\n").parse().unwrap();
            out.extend(line);
            for _ in 0..count.max(0) {
                out.extend(read_value(stream));
            }
        }
        b'%' => {
            let line = read_line(stream);
            let count = String::from_utf8(line.clone()).unwrap();
            let count: i64 = count.trim_end_matches("\r\n").parse().unwrap();
            out.extend(line);
            for _ in 0..count.max(0) * 2 {
                out.extend(read_value(stream));
            }
        }
        other => panic!("unexpected RESP head byte {other}"),
    }
    out
}

fn expect(stream: &mut TcpStream, expected: &str) -> Vec<u8> {
    let reply = read_value(stream);
    assert_eq!(String::from_utf8(reply.clone()).unwrap(), expected, "unexpected RESP reply");
    reply
}

fn set(stream: &mut TcpStream, key: &str, value: &str) {
    send(stream, &["SET", key, value]);
    expect(stream, "+OK\r\n");
}

fn push(stream: &mut TcpStream, command: &str, key: &str, values: &[&str]) {
    let mut arguments = vec![command, key];
    arguments.extend_from_slice(values);
    send(stream, &arguments);
}

/// 等待阻塞方真正登记到等待表；测试用固定间隔代替内部状态探针。
fn settle() {
    thread::sleep(Duration::from_millis(150));
}

#[test]
fn blpop_returns_immediately_when_data_exists() {
    let (address, _executor) = start_server(2);
    let mut producer = connect(address);
    let mut consumer = connect(address);
    push(&mut producer, "RPUSH", "k", &["a", "b"]);
    expect(&mut producer, ":2\r\n");
    send(&mut consumer, &["BLPOP", "k", "0"]);
    expect(&mut consumer, "*2\r\n$1\r\nk\r\n$1\r\na\r\n");
    send(&mut producer, &["LRANGE", "k", "0", "-1"]);
    expect(&mut producer, "*1\r\n$1\r\nb\r\n");
}

#[test]
fn blpop_times_out_with_null_array_in_resp2() {
    let (address, _executor) = start_server(1);
    let mut client = connect(address);
    let started = Instant::now();
    send(&mut client, &["BLPOP", "missing", "0.15"]);
    expect(&mut client, "*-1\r\n");
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(150), "timeout fired early: {elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "timeout fired late: {elapsed:?}");
}

#[test]
fn blpop_with_zero_timeout_waits_until_push() {
    let (address, _executor) = start_server(2);
    let mut producer = connect(address);
    let mut consumer = connect(address);
    send(&mut consumer, &["BLPOP", "k", "0"]);
    settle();
    push(&mut producer, "LPUSH", "k", &["late"]);
    expect(&mut producer, ":1\r\n");
    expect(&mut consumer, "*2\r\n$1\r\nk\r\n$4\r\nlate\r\n");
}

#[test]
fn brpop_pops_the_tail() {
    let (address, _executor) = start_server(1);
    let mut client = connect(address);
    push(&mut client, "RPUSH", "k", &["a", "b", "c"]);
    expect(&mut client, ":3\r\n");
    send(&mut client, &["BRPOP", "k", "0"]);
    expect(&mut client, "*2\r\n$1\r\nk\r\n$1\r\nc\r\n");
}

#[test]
fn waiting_clients_are_woken_in_registration_order() {
    let (address, _executor) = start_server(3);
    let mut producer = connect(address);
    let mut first = connect(address);
    let mut second = connect(address);
    send(&mut first, &["BLPOP", "q", "0"]);
    settle();
    send(&mut second, &["BLPOP", "q", "0"]);
    settle();
    push(&mut producer, "LPUSH", "q", &["one"]);
    expect(&mut producer, ":1\r\n");
    expect(&mut first, "*2\r\n$1\r\nq\r\n$3\r\none\r\n");
    push(&mut producer, "LPUSH", "q", &["two"]);
    expect(&mut producer, ":1\r\n");
    expect(&mut second, "*2\r\n$1\r\nq\r\n$3\r\ntwo\r\n");
}

#[test]
fn blmove_moves_and_wakes_the_destination_waiter() {
    let (address, _executor) = start_server(3);
    let mut producer = connect(address);
    let mut mover = connect(address);
    let mut destination = connect(address);
    send(&mut destination, &["BLPOP", "dst", "0"]);
    settle();
    send(&mut mover, &["BLMOVE", "src", "dst", "LEFT", "RIGHT", "0"]);
    settle();
    push(&mut producer, "LPUSH", "src", &["moved"]);
    expect(&mut producer, ":1\r\n");
    expect(&mut mover, "$5\r\nmoved\r\n");
    expect(&mut destination, "*2\r\n$3\r\ndst\r\n$5\r\nmoved\r\n");
}

#[test]
fn brpoplpush_moves_immediately_and_times_out_with_null_array() {
    let (address, _executor) = start_server(1);
    let mut client = connect(address);
    push(&mut client, "RPUSH", "src", &["a", "b"]);
    expect(&mut client, ":2\r\n");
    send(&mut client, &["BRPOPLPUSH", "src", "dst", "0"]);
    expect(&mut client, "$1\r\nb\r\n");
    send(&mut client, &["LRANGE", "dst", "0", "-1"]);
    expect(&mut client, "*1\r\n$1\r\nb\r\n");
    send(&mut client, &["LRANGE", "src", "0", "-1"]);
    expect(&mut client, "*1\r\n$1\r\na\r\n");
    send(&mut client, &["BRPOPLPUSH", "src", "dst", "0"]);
    expect(&mut client, "$1\r\na\r\n");
    send(&mut client, &["BRPOPLPUSH", "src", "dst", "0.1"]);
    expect(&mut client, "*-1\r\n");
}

#[test]
fn blmpop_pops_count_from_the_first_non_empty_key() {
    let (address, _executor) = start_server(1);
    let mut client = connect(address);
    push(&mut client, "RPUSH", "k1", &["a", "b"]);
    expect(&mut client, ":2\r\n");
    send(&mut client, &["BLMPOP", "0", "2", "missing", "k1", "LEFT", "COUNT", "2"]);
    expect(&mut client, "*2\r\n$2\r\nk1\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n");
    send(&mut client, &["BLMPOP", "0.1", "1", "missing", "LEFT"]);
    expect(&mut client, "*-1\r\n");
}

#[test]
fn blmpop_right_returns_tail_first() {
    let (address, _executor) = start_server(1);
    let mut client = connect(address);
    push(&mut client, "RPUSH", "k", &["a", "b", "c"]);
    expect(&mut client, ":3\r\n");
    send(&mut client, &["BLMPOP", "0", "1", "k", "RIGHT", "COUNT", "2"]);
    expect(&mut client, "*2\r\n$1\r\nk\r\n*2\r\n$1\r\nc\r\n$1\r\nb\r\n");
}

#[test]
fn blpop_rejects_wrong_type_and_bad_timeouts() {
    let (address, _executor) = start_server(1);
    let mut client = connect(address);
    set(&mut client, "s", "v");
    send(&mut client, &["BLPOP", "s", "0"]);
    expect(&mut client, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    send(&mut client, &["BLPOP", "s", "-1"]);
    expect(&mut client, "-ERR timeout is negative\r\n");
    send(&mut client, &["BLPOP", "s", "abc"]);
    expect(&mut client, "-ERR timeout is not a float or out of range\r\n");
    send(&mut client, &["BLPOP", "s"]);
    expect(&mut client, "-ERR wrong number of arguments for 'blpop' command\r\n");
}

#[test]
fn blmove_rejects_wrong_type_source_and_destination() {
    let (address, _executor) = start_server(1);
    let mut client = connect(address);
    set(&mut client, "s", "v");
    send(&mut client, &["BLMOVE", "s", "dst", "LEFT", "LEFT", "0"]);
    expect(&mut client, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    push(&mut client, "RPUSH", "src", &["a"]);
    expect(&mut client, ":1\r\n");
    send(&mut client, &["BLMOVE", "src", "s", "LEFT", "LEFT", "0"]);
    expect(&mut client, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    send(&mut client, &["LRANGE", "src", "0", "-1"]);
    expect(&mut client, "*1\r\n$1\r\na\r\n");
}

#[test]
fn resp3_uses_the_null_type_for_timeouts() {
    let (address, _executor) = start_server(1);
    let mut client = connect(address);
    send(&mut client, &["HELLO", "3"]);
    let hello = read_value(&mut client);
    assert_eq!(hello[0], b'%', "HELLO 3 should answer with a map");
    send(&mut client, &["BLPOP", "missing", "0.1"]);
    expect(&mut client, "_\r\n");
    send(&mut client, &["BLMOVE", "missing", "dst", "LEFT", "LEFT", "0.1"]);
    expect(&mut client, "_\r\n");
    send(&mut client, &["BLMPOP", "0.1", "1", "missing", "LEFT"]);
    expect(&mut client, "_\r\n");
}

#[test]
fn exec_replays_blocking_commands_without_waiting() {
    let (address, _executor) = start_server(1);
    let mut client = connect(address);
    send(&mut client, &["MULTI"]);
    expect(&mut client, "+OK\r\n");
    send(&mut client, &["BLPOP", "missing", "0"]);
    expect(&mut client, "+QUEUED\r\n");
    send(&mut client, &["EXEC"]);
    expect(&mut client, "*1\r\n*-1\r\n");
}
