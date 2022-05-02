//! 可取消会话（`serve_cancellable`）的 I/O 地板与取消语义回归。
//!
//! 修复前 `CancellableSocket` 把套接字设为 nonblocking 后对每次 `WouldBlock`
//! 固定 `sleep(5ms)` 再重试，每语句往返至少命中两次地板（实测中位 6.2ms，真库 0.13ms）。
//! 修复后读路径保持阻塞、由内核在数据到达时即时唤醒，50ms 读超时只是停机标志的
//! 检查节拍。本文件钉死两件事：
//! - `SELECT 1` 往返不再带 5ms 级地板（计时断言）；
//! - 阻塞读中被置停机标志时，会话及时以 `ConnectionAborted` 返回（取消延迟断言）。

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use database_cluster::topology::ShardMap;
use database_execution::cluster::LocalExecutor;
use database_query::SqlSchemaRegistry;
use database_server::yyds::mysql::connection::{MysqlSqlRuntime, serve_cancellable};
use database_storage::cluster_kv::MemoryShard;
use database_types::cluster::{ShardEpoch, ShardId};

const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const COM_QUERY: u8 = 0x03;

/// 起一个可取消的 MySQL 网关并完成 TCP 接入（不做握手），返回客户端 socket、
/// 停机标志与服务线程句柄。`SELECT 1` 不落盘，执行面用 `MemoryShard` 即可。
fn connect_gateway() -> (TcpStream, Arc<AtomicBool>, thread::JoinHandle<io::Result<()>>) {
    let shard = ShardId("local-0".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, MemoryShard::new())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let runtime = Arc::new(MysqlSqlRuntime { schema: Arc::new(SqlSchemaRegistry::new_in_memory()), executor });
    let stopping = Arc::new(AtomicBool::new(false));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let cancellation = Arc::clone(&stopping);
    let worker = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_cancellable(stream, cancellation, Some(runtime))
    });
    let client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    client.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
    (client, stopping, worker)
}

/// 完成 protocol 41 握手：读服务端握手包（sequence 0），回认证响应（sequence 1），
/// 收 OK（sequence 2）。与 `affected_rows.rs` 的接线一致。
fn handshake(client: &mut TcpStream) {
    let (sequence, _) = read_packet(client);
    assert_eq!(sequence, 0, "handshake must carry sequence 0");
    let mut response = Vec::new();
    response.extend_from_slice(&(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION).to_le_bytes());
    response.extend_from_slice(&(16 * 1024 * 1024_u32).to_le_bytes());
    response.push(45);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(b"yyds\0");
    response.push(0); // SECURE_CONNECTION 形态：auth response 长度 0（空密码）
    write_packet(client, 1, &response);
    let (sequence, payload) = read_packet(client);
    assert_eq!(sequence, 2);
    assert_eq!(payload.first(), Some(&0x00), "handshake must complete with OK, got {payload:?}");
}

fn read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 4];
    stream.read_exact(&mut header).unwrap();
    let length = usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).unwrap();
    (header[3], payload)
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) {
    let length = payload.len();
    let mut frame = Vec::with_capacity(4 + length);
    frame.extend_from_slice(&[length as u8, (length >> 8) as u8, (length >> 16) as u8, sequence]);
    frame.extend_from_slice(payload);
    stream.write_all(&frame).unwrap();
    stream.flush().unwrap();
}

fn send_query(client: &mut TcpStream, sql: &str) {
    let mut payload = vec![COM_QUERY];
    payload.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &payload);
}

/// 简化 EOF 包判定：5 字节、首字节 0xFE（网关 `write_eof` 的固定形态）。
fn is_eof(payload: &[u8]) -> bool {
    payload.len() == 5 && payload[0] == 0xFE
}

/// 读一个文本协议结果集并返回行数；ERR 包直接 panic 带出错误码与文案。
fn read_result_set(client: &mut TcpStream) -> usize {
    let (sequence, first) = read_packet(client);
    assert_eq!(sequence, 1, "COM_QUERY response must start at sequence 1");
    match first.first() {
        Some(0xff) => {
            let code = u16::from_le_bytes([first[1], first[2]]);
            panic!("query failed with MySQL error {code}: {}", String::from_utf8_lossy(&first[3..]));
        }
        Some(0x00) => return 0,
        Some(byte) if *byte <= 250 => {}
        other => panic!("unexpected result-set prefix {other:?}"),
    }
    let column_count = usize::from(first[0]);
    for _ in 0..column_count {
        read_packet(client);
    }
    let (_sequence, eof) = read_packet(client);
    assert!(is_eof(&eof), "expected column-definition EOF, got {eof:?}");
    let mut rows = 0;
    loop {
        let (_sequence, payload) = read_packet(client);
        if is_eof(&payload) {
            break;
        }
        assert_ne!(payload.first(), Some(&0xff), "row stream turned into an ERR packet");
        rows += 1;
    }
    rows
}

#[test]
fn select_one_round_trips_lose_the_sleep_polling_floor() {
    let (mut client, stopping, worker) = connect_gateway();
    handshake(&mut client);
    // 预热，把列定义编码与进程首跑的惰性开销排除在计时外。
    for _ in 0..3 {
        send_query(&mut client, "SELECT 1");
        assert_eq!(read_result_set(&mut client), 1);
    }

    const ROUNDS: usize = 200;
    let mut samples: Vec<u64> = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let started = Instant::now();
        send_query(&mut client, "SELECT 1");
        assert_eq!(read_result_set(&mut client), 1);
        samples.push(started.elapsed().as_micros() as u64);
    }
    samples.sort_unstable();
    let median = samples[samples.len() / 2];
    eprintln!("SELECT 1 round-trip: median {median}µs over {ROUNDS} rounds");
    // 修复前每语句中位 6.2ms（地板 5ms×2 次命中）；修复后数据到达即唤醒，
    // debug 实测中位 ~0.1ms。中位数口径抗调度与首跑抖动，阈值 2ms 仍远低于旧地板。
    assert!(median < 2_000, "SELECT 1 round-trip median is {median}µs — the polling floor is still there");
    drop(client);
    stopping.store(true, Ordering::Release);
    let _ = worker.join();
}

#[test]
fn stopping_flag_interrupts_a_blocked_read() {
    let (mut client, stopping, worker) = connect_gateway();
    // 读走服务端握手包但不回响应：服务线程此刻阻塞在握手响应的读上。
    let (sequence, _) = read_packet(&mut client);
    assert_eq!(sequence, 0, "handshake must carry sequence 0");
    thread::sleep(Duration::from_millis(100));
    let started = Instant::now();
    stopping.store(true, Ordering::Release);
    let result = worker.join().expect("serve worker panicked");
    let latency = started.elapsed();
    assert!(
        matches!(&result, Err(error) if error.kind() == io::ErrorKind::ConnectionAborted),
        "blocked read must abort with ConnectionAborted, got {result:?}"
    );
    // 取消延迟上界 = 读超时节拍 50ms；阈值取 2 倍节拍再加 Windows 计时粒度与线程调度余量。
    assert!(latency <= Duration::from_millis(200), "cancellation latency {latency:?} exceeded twice the 50ms check cadence");
    drop(client);
}
