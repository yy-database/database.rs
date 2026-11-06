//! 真实监听器上的 pub/sub 对照：多连接订阅、推送帧形状与查询子命令。

use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::Duration,
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
fn start_server(clients: usize) -> SocketAddr {
    let shard = ShardId("redis-pubsub".into());
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
    thread::spawn(move || {
        let mut workers = Vec::new();
        for _ in 0..clients {
            let (stream, _) = listener.accept().unwrap();
            let executor = Arc::clone(&executor);
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
    address
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
        b'+' | b'-' | b':' | b',' | b'(' | b'#' | b'_' => out.extend(read_line(stream)),
        b'$' | b'!' => {
            let line = read_line(stream);
            let length: i64 = String::from_utf8(line.clone()).unwrap().trim_end_matches("\r\n").parse().unwrap();
            out.extend(line);
            if length >= 0 {
                let mut body = vec![0_u8; length as usize + 2];
                stream.read_exact(&mut body).unwrap();
                out.extend(body);
            }
        }
        b'*' | b'~' | b'>' => {
            let line = read_line(stream);
            let count: i64 = String::from_utf8(line.clone()).unwrap().trim_end_matches("\r\n").parse().unwrap();
            out.extend(line);
            for _ in 0..count.max(0) {
                out.extend(read_value(stream));
            }
        }
        b'%' => {
            let line = read_line(stream);
            let count: i64 = String::from_utf8(line.clone()).unwrap().trim_end_matches("\r\n").parse().unwrap();
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

fn hello(stream: &mut TcpStream, protocol: u8) {
    send(stream, &["HELLO", &protocol.to_string()]);
    let _ = read_value(stream);
}

#[test]
fn subscribe_then_publish_delivers_message() {
    let address = start_server(2);
    let mut subscriber = connect(address);
    let mut publisher = connect(address);

    send(&mut subscriber, &["SUBSCRIBE", "news"]);
    expect(&mut subscriber, "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
    send(&mut publisher, &["PUBLISH", "news", "hello"]);
    expect(&mut publisher, ":1\r\n");
    expect(&mut subscriber, "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n");
}

#[test]
fn resp3_subscribe_and_message_use_push_frames() {
    let address = start_server(2);
    let mut subscriber = connect(address);
    let mut publisher = connect(address);
    hello(&mut subscriber, 3);

    send(&mut subscriber, &["SUBSCRIBE", "news"]);
    expect(&mut subscriber, ">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
    send(&mut publisher, &["PUBLISH", "news", "hello"]);
    expect(&mut publisher, ":1\r\n");
    expect(&mut subscriber, ">3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n");
}

#[test]
fn unsubscribe_stops_delivery() {
    let address = start_server(2);
    let mut subscriber = connect(address);
    let mut publisher = connect(address);

    send(&mut subscriber, &["SUBSCRIBE", "news"]);
    expect(&mut subscriber, "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
    send(&mut subscriber, &["UNSUBSCRIBE", "news"]);
    expect(&mut subscriber, "*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n");
    send(&mut publisher, &["PUBLISH", "news", "hello"]);
    expect(&mut publisher, ":0\r\n");
}

#[test]
fn unsubscribe_without_subscriptions_answers_null_name() {
    let address = start_server(1);
    let mut client = connect(address);
    send(&mut client, &["UNSUBSCRIBE"]);
    expect(&mut client, "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n");
    send(&mut client, &["PUNSUBSCRIBE"]);
    expect(&mut client, "*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n");
}

#[test]
fn pattern_subscription_receives_pmessage() {
    let address = start_server(2);
    let mut subscriber = connect(address);
    let mut publisher = connect(address);

    send(&mut subscriber, &["PSUBSCRIBE", "news.*"]);
    expect(&mut subscriber, "*3\r\n$10\r\npsubscribe\r\n$6\r\nnews.*\r\n:1\r\n");
    send(&mut publisher, &["PUBLISH", "news.tech", "tick"]);
    expect(&mut publisher, ":1\r\n");
    expect(
        &mut subscriber,
        "*4\r\n$8\r\npmessage\r\n$6\r\nnews.*\r\n$9\r\nnews.tech\r\n$4\r\ntick\r\n",
    );
}

#[test]
fn direct_and_matching_pattern_count_twice() {
    let address = start_server(2);
    let mut subscriber = connect(address);
    let mut publisher = connect(address);

    send(&mut subscriber, &["PSUBSCRIBE", "news.*"]);
    expect(&mut subscriber, "*3\r\n$10\r\npsubscribe\r\n$6\r\nnews.*\r\n:1\r\n");
    send(&mut subscriber, &["SUBSCRIBE", "news.tech"]);
    expect(&mut subscriber, "*3\r\n$9\r\nsubscribe\r\n$9\r\nnews.tech\r\n:2\r\n");
    send(&mut publisher, &["PUBLISH", "news.tech", "both"]);
    expect(&mut publisher, ":2\r\n");
    expect(&mut subscriber, "*3\r\n$7\r\nmessage\r\n$9\r\nnews.tech\r\n$4\r\nboth\r\n");
    expect(
        &mut subscriber,
        "*4\r\n$8\r\npmessage\r\n$6\r\nnews.*\r\n$9\r\nnews.tech\r\n$4\r\nboth\r\n",
    );
}

#[test]
fn pubsub_queries_report_channels_counts_and_patterns() {
    let address = start_server(2);
    let mut subscriber = connect(address);
    let mut inspector = connect(address);

    send(&mut subscriber, &["SUBSCRIBE", "news"]);
    expect(&mut subscriber, "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
    send(&mut subscriber, &["PSUBSCRIBE", "news.*"]);
    expect(&mut subscriber, "*3\r\n$10\r\npsubscribe\r\n$6\r\nnews.*\r\n:2\r\n");

    send(&mut inspector, &["PUBSUB", "CHANNELS"]);
    expect(&mut inspector, "*1\r\n$4\r\nnews\r\n");
    send(&mut inspector, &["PUBSUB", "NUMSUB", "news", "missing"]);
    expect(&mut inspector, "*4\r\n$4\r\nnews\r\n:1\r\n$7\r\nmissing\r\n:0\r\n");
    send(&mut inspector, &["PUBSUB", "NUMPAT"]);
    expect(&mut inspector, ":1\r\n");
}

#[test]
fn resp2_subscriber_rejects_ordinary_commands() {
    let address = start_server(1);
    let mut client = connect(address);

    send(&mut client, &["SUBSCRIBE", "news"]);
    expect(&mut client, "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
    send(&mut client, &["GET", "plain"]);
    expect(
        &mut client,
        "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n",
    );
    send(&mut client, &["PING"]);
    expect(&mut client, "*2\r\n$4\r\npong\r\n$0\r\n\r\n");
    send(&mut client, &["RESET"]);
    expect(&mut client, "+RESET\r\n");
    send(&mut client, &["GET", "plain"]);
    expect(&mut client, "$-1\r\n");
}

#[test]
fn resp3_subscriber_still_runs_ordinary_commands() {
    let address = start_server(1);
    let mut client = connect(address);
    hello(&mut client, 3);

    send(&mut client, &["SUBSCRIBE", "news"]);
    expect(&mut client, ">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
    send(&mut client, &["GET", "plain"]);
    expect(&mut client, "_\r\n");
    send(&mut client, &["PING"]);
    expect(&mut client, "+PONG\r\n");
}

#[test]
fn publish_without_subscribers_answers_zero() {
    let address = start_server(1);
    let mut publisher = connect(address);
    send(&mut publisher, &["PUBLISH", "solo", "nobody"]);
    expect(&mut publisher, ":0\r\n");
}

#[test]
fn arity_and_unknown_subcommand_errors_match_upstream() {
    let address = start_server(1);
    let mut client = connect(address);
    send(&mut client, &["SUBSCRIBE"]);
    expect(&mut client, "-ERR wrong number of arguments for 'subscribe' command\r\n");
    send(&mut client, &["PUBSUB"]);
    expect(&mut client, "-ERR wrong number of arguments for 'pubsub' command\r\n");
    send(&mut client, &["PUBSUB", "BOGUS"]);
    expect(&mut client, "-ERR unknown subcommand 'BOGUS'. Try PUBSUB HELP.\r\n");
    send(&mut client, &["PUBSUB", "CHANNELS", "a", "b"]);
    expect(&mut client, "-ERR unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.\r\n");
}
