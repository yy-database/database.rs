//! 两面一致性对照：同一批命令分别打 TS 服务端与 L1 网关，逐条比较回复。
//!
//! 门禁入口是仓库根的 `scripts/face-parity.mjs`：它启动 TS 服务端，再把地址经
//! `FACE_PARITY_TS_ADDR` 传给本用例。环境变量缺失时用例只打印跳过原因并返回——
//! 门禁脚本会断言实际比较条数，跳过不能伪装成通过。
//!
//! `INFO` 的进程号、运行时长、内存占用与 `CLIENT LIST` 的连接标识天然不同，比较前按
//! 字段名归一化；其余字段逐字比对，注释类的漂移也会被抓到。

use std::{
    collections::HashMap,
    io::{self, Read, Write},
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
    execute_server_command,
    pubsub::PubSubRegistry,
    resp::RequestLimits,
    server_face::ClientRegistry,
    EmbeddedServerFace,
};
use database_storage::cluster_kv::MemoryShard;
use database_types::cluster::{ShardEpoch, ShardId};

/// 一条命令的原始参数。
type Command<'a> = &'a [&'a str];

/// 两面共享的命令批次。顺序有意义：后面的命令依赖前面的状态。
const BATCH: &[Command<'static>] = &[
    &["PING"],
    &["ECHO", "hello"],
    &["CLIENT", "GETNAME"],
    &["CLIENT", "SETNAME", "parity"],
    &["CLIENT", "GETNAME"],
    &["CLIENT", "SETNAME", ""],
    &["CLIENT", "GETNAME"],
    &["CLIENT", "SETINFO", "LIB-NAME", "parity"],
    &["CLIENT", "SETINFO", "LIB-VER", "9.9"],
    &["CLIENT", "SETINFO", "BOGUS", "x"],
    &["CLIENT", "SETINFO", "LIB-NAME"],
    &["CLIENT", "ID"],
    &["CLIENT", "NO-EVICT", "on"],
    &["CLIENT", "NO-EVICT", "bogus"],
    &["CLIENT", "NO-TOUCH", "on"],
    &["CLIENT", "UNPAUSE"],
    &["CLIENT", "LIST"],
    &["CLIENT", "INFO"],
    &["CLIENT", "KILL", "ADDR", "127.0.0.1:1"],
    &["CLIENT", "BOGUS"],
    &["COMMAND", "COUNT"],
    &["COMMAND", "INFO", "get", "missing"],
    &["COMMAND", "DOCS"],
    &["COMMAND", "BOGUS"],
    &["INFO"],
    &["INFO", "server"],
    &["INFO", "bogus"],
    &["CONFIG", "GET", "maxmemory"],
    &["CONFIG", "GET", "appendonly"],
    &["CONFIG", "GET", "*"],
    &["CONFIG", "SET", "maxmemory", "100mb"],
    &["CONFIG", "GET", "maxmemory"],
    &["CONFIG", "SET", "databases", "32"],
    &["CONFIG", "SET", "bogus", "1"],
    &["CONFIG", "BOGUS"],
    &["ACL", "WHOAMI"],
    &["ACL", "LIST"],
    &["ACL", "CAT", "string"],
    &["ACL", "GETUSER", "default"],
    &["ACL", "BOGUS"],
    &["SELECT", "5"],
    &["CLIENT", "LIST"],
    &["SET", "k", "v"],
    &["GET", "k"],
    &["TYPE", "k"],
    &["DEL", "k"],
    &["MULTI"],
    &["SET", "onlykey"],
    &["EXEC"],
    &["SET", "q", "r"],
    &["GET", "q"],
    &["DEL", "q"],
    &["COMMAND"],
];

/// 启动一个进程内 L1 监听器，只接受一条连接。
fn start_l1() -> SocketAddr {
    let shard = ShardId("redis-face-parity".into());
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
    let clients = Arc::new(ClientRegistry::new(address.port(), address.ip().to_string()));
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let _ = serve_cancellable_with_executor_and_tracker(
            stream,
            RequestLimits::default(),
            stopping,
            executor,
            tracker,
            registry,
            pubsub,
            Some(clients),
        );
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

/// 一个解析后的 RESP 值，只保留比较需要的形状。
#[derive(Debug, Clone, PartialEq, Eq)]
enum Frame {
    Simple(String),
    Error(String),
    Int(String),
    Bulk(Option<String>),
    Array(Option<Vec<Frame>>),
}

fn read_line(stream: &mut TcpStream) -> io::Result<String> {
    let mut line = Vec::new();
    let mut previous = 0_u8;
    loop {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte)?;
        line.push(byte[0]);
        if previous == b'\r' && byte[0] == b'\n' {
            line.truncate(line.len() - 2);
            return Ok(String::from_utf8_lossy(&line).into_owned());
        }
        previous = byte[0];
    }
}

fn read_frame(stream: &mut TcpStream) -> io::Result<Frame> {
    let mut head = [0_u8; 1];
    stream.read_exact(&mut head)?;
    let line = read_line(stream)?;
    match head[0] {
        b'+' => Ok(Frame::Simple(line)),
        b'-' => Ok(Frame::Error(line)),
        b':' | b'(' | b',' | b'#' => Ok(Frame::Int(line)),
        b'_' => Ok(Frame::Bulk(None)),
        b'$' => {
            let length: i64 = line.parse().unwrap_or(-1);
            if length < 0 {
                return Ok(Frame::Bulk(None));
            }
            let mut body = vec![0_u8; length as usize + 2];
            stream.read_exact(&mut body)?;
            body.truncate(length as usize);
            Ok(Frame::Bulk(Some(String::from_utf8_lossy(&body).into_owned())))
        }
        b'*' => {
            let count: i64 = line.parse().unwrap_or(-1);
            if count < 0 {
                return Ok(Frame::Array(None));
            }
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                items.push(read_frame(stream)?);
            }
            Ok(Frame::Array(Some(items)))
        }
        other => Err(io::Error::other(format!("unsupported RESP prefix {other}"))),
    }
}

/// `INFO` 里随进程、时间、内存变化的字段，比较前替换成 `<n>`。
const VOLATILE_INFO: &[&str] = &[
    "process_id",
    "run_id",
    "tcp_port",
    "server_time_usec",
    "uptime_in_seconds",
    "uptime_in_days",
    "lru_clock",
    "executable",
    "used_memory",
    "used_memory_human",
    "used_memory_rss",
    "used_memory_rss_human",
    "used_memory_peak",
    "used_memory_peak_human",
    "used_memory_overhead",
    "used_memory_dataset",
    "total_connections_received",
    "total_commands_processed",
    "total_net_input_bytes",
    "total_net_output_bytes",
    "expired_keys",
    "evicted_keys",
    "keyspace_hits",
    "keyspace_misses",
    "pubsub_channels",
    "pubsub_patterns",
    "total_reads_processed",
    "total_writes_processed",
    "total_error_replies",
];

/// `CLIENT LIST` 里随连接变化的字段，比较前替换成 `<n>`。
const VOLATILE_CLIENT: &[&str] = &["id", "addr", "laddr", "fd", "age", "idle", "tot-cmds"];

fn mask_rows(text: &str, volatile: &[&str], separator: char) -> String {
    text.split(separator)
        .map(|row| {
            row.split(' ')
                .map(|field| match field.split_once('=') {
                    Some((name, _)) if volatile.contains(&name) => format!("{name}=<n>"),
                    _ => field.to_string(),
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join(&separator.to_string())
}

fn mask_info(text: &str) -> String {
    text.split("\r\n")
        .map(|line| match line.split_once(':') {
            Some((name, _)) if VOLATILE_INFO.contains(&name) => format!("{name}:<n>"),
            // 监听行带端口与绑定地址，两个服务端天然不同。
            Some((name, _)) if name.starts_with("listener") => format!("{name}:<n>"),
            _ => line.to_string(),
        })
        .collect::<Vec<_>>()
        .join("\r\n")
}

/// 命名字段的批量文本按行归一化；`CLIENT LIST` 用空格分行，`INFO` 用 CRLF。
fn mask_bulk(command: &[&str], body: &str) -> String {
    let Some(first) = command.first() else {
        return body.to_string();
    };
    let name = first.to_ascii_uppercase();
    let sub = command.get(1).map(|value| value.to_ascii_uppercase());
    if name == "INFO" {
        return mask_info(body);
    }
    if name == "CLIENT" && matches!(sub.as_deref(), Some("LIST") | Some("INFO")) {
        return mask_rows(body, VOLATILE_CLIENT, '\n');
    }
    body.to_string()
}

/// 把解析后的值折叠成可直接比较的文本，同时套用该命令的归一化规则。
fn canonical(command: &[&str], frame: &Frame) -> String {
    match frame {
        Frame::Bulk(Some(body)) => format!("${}", mask_bulk(command, body)),
        Frame::Bulk(None) => "$null".into(),
        Frame::Int(value) => format!(":{value}"),
        Frame::Simple(value) => format!("+{value}"),
        Frame::Error(value) => format!("-{value}"),
        Frame::Array(None) => "*null".into(),
        Frame::Array(Some(items)) => {
            let parts: Vec<String> = items.iter().map(|item| canonical(&[], item)).collect();
            format!("*[{}]", parts.join(","))
        }
    }
}

/// `CONFIG GET *` 的端口值两面天然不同，除了它就是逐字比较。
fn canonical_config(command: &[&str], frame: &Frame) -> String {
    let Frame::Array(Some(items)) = frame else {
        return canonical(command, frame);
    };
    let mut masked = Vec::with_capacity(items.len());
    for pair in items.chunks(2) {
        match pair {
            [Frame::Bulk(Some(name)), Frame::Bulk(Some(_))] if name == "port" => {
                masked.push(Frame::Bulk(Some(name.clone())));
                masked.push(Frame::Bulk(Some("<n>".into())));
            }
            _ => masked.extend(pair.iter().cloned()),
        }
    }
    canonical(command, &Frame::Array(Some(masked)))
}

#[test]
fn two_face_parity() {
    let Ok(ts_address) = std::env::var("FACE_PARITY_TS_ADDR") else {
        eprintln!("face parity skipped: FACE_PARITY_TS_ADDR is not set");
        return;
    };
    let ts_address: SocketAddr = ts_address.parse().expect("FACE_PARITY_TS_ADDR must be host:port");
    let l1_address = start_l1();
    let mut l1 = connect(l1_address);
    let mut ts = connect(ts_address);

    let mut problems = Vec::new();
    let mut compared = 0_usize;
    for command in BATCH {
        let (left, right) = match read_pair(&mut l1, &mut ts, command) {
            Ok(pair) => pair,
            Err(error) => {
                problems.push(format!("{}: transport error {error}", command.join(" ")));
                break;
            }
        };
        compared += 1;
        let mask = if command.len() >= 2 && command[0].eq_ignore_ascii_case("CONFIG") && command[1].eq_ignore_ascii_case("GET")
        {
            canonical_config(command, &left)
        }
        else if command[0].eq_ignore_ascii_case("CLIENT") && command.get(1).is_some_and(|sub| sub.eq_ignore_ascii_case("ID")) {
            "nil".into()
        }
        else {
            canonical(command, &left)
        };
        let other = if command.len() >= 2 && command[0].eq_ignore_ascii_case("CONFIG") && command[1].eq_ignore_ascii_case("GET")
        {
            canonical_config(command, &right)
        }
        else if command[0].eq_ignore_ascii_case("CLIENT") && command.get(1).is_some_and(|sub| sub.eq_ignore_ascii_case("ID")) {
            "nil".into()
        }
        else {
            canonical(command, &right)
        };
        if mask != other {
            problems.push(format!("{}: l1 {mask} != ts {other}", command.join(" ")));
        }
    }

    if !problems.is_empty() {
        for problem in &problems {
            eprintln!("{problem}");
        }
        panic!("face parity disagreed on {} of {compared} command(s)", problems.len());
    }
    println!("face parity compared {compared} commands, 0 mismatches");
}

/// 语言绑定没有 socket，嵌入式入口必须自己保留 `CLIENT` / `CONFIG` 的会话状态。
#[test]
fn embedded_server_face_keeps_session_state() {
    let shard = ShardId("redis-server-entry".into());
    let executor = LocalExecutor::new(
        ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
        HashMap::from([(shard, MemoryShard::new())]),
        Vec::new(),
    )
    .unwrap();
    let mut session = EmbeddedServerFace::new();
    let run = |session: &mut EmbeddedServerFace, protocol: u8, arguments: &[&[u8]]| {
        let mut output = Vec::new();
        execute_server_command(session, &mut output, arguments, &executor, 0, protocol).unwrap();
        String::from_utf8(output).unwrap()
    };
    assert_eq!(run(&mut session, 2, &[b"CLIENT", b"SETNAME", b"entry"]), "+OK\r\n");
    assert_eq!(run(&mut session, 2, &[b"CLIENT", b"GETNAME"]), "$5\r\nentry\r\n");
    assert_eq!(run(&mut session, 2, &[b"CONFIG", b"SET", b"maxmemory", b"100mb"]), "+OK\r\n");
    assert_eq!(
        run(&mut session, 2, &[b"CONFIG", b"GET", b"maxmemory"]),
        "*2\r\n$9\r\nmaxmemory\r\n$9\r\n104857600\r\n"
    );
    assert!(run(&mut session, 2, &[b"COMMAND", b"COUNT"]).starts_with(':'));
    assert_eq!(run(&mut session, 2, &[b"INFO", b"bogus"]), "$0\r\n\r\n");
    assert_eq!(run(&mut session, 2, &[b"ACL", b"GETUSER", b"nosuch"]), "$-1\r\n");
    assert_eq!(run(&mut session, 3, &[b"ACL", b"GETUSER", b"nosuch"]), "_\r\n");
}

#[test]
fn embedded_server_face_kills_a_peer_session() {
    let shard = ShardId("redis-server-entry".into());
    let executor = LocalExecutor::new(
        ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
        HashMap::from([(shard, MemoryShard::new())]),
        Vec::new(),
    )
    .unwrap();
    let registry = Arc::new(ClientRegistry::standalone());
    let mut client = EmbeddedServerFace::with_id(Arc::clone(&registry), 1);
    let _victim = EmbeddedServerFace::with_id(Arc::clone(&registry), 2);
    let run = |session: &mut EmbeddedServerFace, arguments: &[&[u8]]| {
        let mut output = Vec::new();
        execute_server_command(session, &mut output, arguments, &executor, 0, 2).unwrap();
        String::from_utf8(output).unwrap()
    };
    assert!(run(&mut client, &[b"CLIENT", b"LIST"]).contains("id=2 "));
    assert_eq!(run(&mut client, &[b"CLIENT", b"KILL", b"ID", b"2"]), ":1\r\n");
    assert_eq!(registry.take_killed(), vec![2]);
    let listing = run(&mut client, &[b"CLIENT", b"LIST"]);
    assert!(!listing.contains("id=2 "), "killed session stayed in CLIENT LIST: {listing}");
}

/// 把同一条命令同时发给两面并按顺序读回，避免一方阻塞时另一方的回复串位。
fn read_pair(l1: &mut TcpStream, ts: &mut TcpStream, command: &[&str]) -> io::Result<(Frame, Frame)> {
    send(l1, command);
    send(ts, command);
    Ok((read_frame(l1)?, read_frame(ts)?))
}
