//! DML `affectedRows` 回归：握手后走 `COM_QUERY`，从 OK 包解出 length-encoded
//! 行数，钉死单行、多行、零匹配与全表 DELETE 四种形态。
//!
//! 期望值 oracle（unverified-oracle：本测试不连真库，逐条引用 MySQL 8.4 官方文档条款，
//! 不拿本仓行为当期望值）：
//! - MySQL 8.4 Reference Manual §14.15 / C API §5.4.1 `mysql_affected_rows()`：
//!   UPDATE 默认返回「实际改变」的行数，仅当连接带 `CLIENT_FOUND_ROWS` 才返回「匹配」行数。
//!   网关握手 `SERVER_CAPABILITIES` 不含 `CLIENT_FOUND_ROWS`（0x40），客户端无法协商，
//!   因此本测试只在「匹配行确实被改成不同值」的语句上断言——此时 matched == changed，
//!   两种口径同值。
//! - §15.2.17 UPDATE：`UPDATE returns the number of rows that were actually changed.`
//! - INSERT 返回插入行数（多行 `VALUES` 计 N），DELETE 返回删除行数（同上条款）。
//!
//! 已知语义分叉（不在本测试断言，登记给执行器线）：`UPDATE ... SET col = <当前相同值>`
//! 真库默认回 0（未 changed），网关回匹配数。落点在 `database.execution` 的
//! `execute_update` / `execute_update_scan`——写回前比较 pre-image 与重编码行即可跳过
//! 未变行；该 crate 属他人线，本批不改。

use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

use database_cluster::topology::ShardMap;
use database_execution::cluster::LocalExecutor;
use database_query::SqlSchemaRegistry;
use database_server::yyds::mysql::connection::{MysqlSqlRuntime, serve_connection};
use database_storage::cluster_kv::FileShard;
use database_types::cluster::{ShardEpoch, ShardId};

const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const COM_QUERY: u8 = 0x03;

/// 起一个 MySQL 网关并完成握手，返回客户端 socket。
///
/// 执行面必须用 `FileShard`：多行 `INSERT` 的原子提交路径要求 store 提供 durable 原子提交
/// 边界，`MemoryShard` 对该边界返回 `Unsupported`，与生产 `instance.rs` 的接线一致。
/// 每个用例传独立 `name`，数据目录落在系统临时目录，用例结束即清理。
fn connect_gateway(name: &str) -> (TcpStream, std::path::PathBuf) {
    let directory = std::env::temp_dir().join(format!("yyds-mysql-affected-rows-{name}"));
    let _ = fs::remove_file(directory.join("local-0.yykv"));
    fs::create_dir_all(&directory).unwrap();
    let shard = ShardId("local-0".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, FileShard::open(directory.join("local-0.yykv")).unwrap())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let runtime = Arc::new(MysqlSqlRuntime { schema: Arc::new(SqlSchemaRegistry::new_in_memory()), executor });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let _ = serve_connection(stream, Some(runtime));
    });
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    client.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
    // 服务端握手包（sequence 0）。
    let (sequence, _) = read_packet(&mut client);
    assert_eq!(sequence, 0, "handshake must carry sequence 0");
    // protocol 41 握手响应（sequence 1）：非空用户名 + 空密码，不带库名与 plugin auth。
    let mut response = Vec::new();
    response.extend_from_slice(&(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION).to_le_bytes());
    response.extend_from_slice(&(16 * 1024 * 1024_u32).to_le_bytes());
    response.push(45);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(b"yyds\0");
    response.push(0); // SECURE_CONNECTION 形态：auth response 长度 0（空密码）
    write_packet(&mut client, 1, &response);
    let (sequence, payload) = read_packet(&mut client);
    assert_eq!(sequence, 2);
    assert_eq!(payload.first(), Some(&0x00), "handshake must complete with OK, got {payload:?}");
    (client, directory)
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

/// 解 OK 包里的 length-encoded `affected_rows`；ERR 包直接 panic 带出错误码与文案。
fn query_affected_rows(client: &mut TcpStream, sql: &str) -> u64 {
    let mut payload = vec![COM_QUERY];
    payload.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &payload);
    let (sequence, response) = read_packet(client);
    assert_eq!(sequence, 1, "COM_QUERY response must start at sequence 1");
    match response.first() {
        Some(0x00) => decode_lenenc(&response, 1),
        Some(0xff) => {
            let code = u16::from_le_bytes([response[1], response[2]]);
            panic!("query `{sql}` failed with MySQL error {code}: {}", String::from_utf8_lossy(&response[3..]));
        }
        other => panic!("query `{sql}` expected an OK packet, got prefix {other:?}"),
    }
}

fn decode_lenenc(payload: &[u8], offset: usize) -> u64 {
    match payload[offset] {
        0..=250 => u64::from(payload[offset]),
        252 => {
            u64::from(u16::from_le_bytes([payload[offset + 1], payload[offset + 2]]))
        }
        253 => {
            u64::from(u32::from_le_bytes([payload[offset + 1], payload[offset + 2], payload[offset + 3], 0]))
        }
        254 => u64::from_le_bytes(payload[offset + 1..offset + 9].try_into().unwrap()),
        other => panic!("invalid length-encoded prefix {other} in OK packet"),
    }
}

#[test]
fn dml_affected_rows_follows_mysql_84_semantics() {
    let (mut client, directory) = connect_gateway("multi");
    query_affected_rows(&mut client, "CREATE TABLE aff (id INT NOT NULL PRIMARY KEY, v INT)");

    // 单行 INSERT：真库回 1（插入行数）。
    assert_eq!(query_affected_rows(&mut client, "INSERT INTO aff (id, v) VALUES (1, 10)"), 1);
    // 多行 INSERT：真库回 N，这里 3 行。
    assert_eq!(query_affected_rows(&mut client, "INSERT INTO aff (id, v) VALUES (2, 20), (3, 30), (4, 40)"), 3);

    // 单行 UPDATE 且值确实改变：matched == changed == 1。
    assert_eq!(query_affected_rows(&mut client, "UPDATE aff SET v = 11 WHERE id = 1"), 1);
    // 多行 UPDATE（扫描路径）：id > 1 命中 2/3/4 三行，值全部由 20/30/40 改为 7。
    assert_eq!(query_affected_rows(&mut client, "UPDATE aff SET v = 7 WHERE id > 1"), 3);
    // 零匹配 UPDATE：真库回 0。
    assert_eq!(query_affected_rows(&mut client, "UPDATE aff SET v = 5 WHERE id = 999"), 0);

    // 零匹配 DELETE：真库回 0。
    assert_eq!(query_affected_rows(&mut client, "DELETE FROM aff WHERE id = 999"), 0);
    // 全表 DELETE：表内共 4 行（1 + 3），真库回删除行数 4。
    assert_eq!(query_affected_rows(&mut client, "DELETE FROM aff"), 4);
    drop(client);
    let _ = fs::remove_file(directory.join("local-0.yykv"));
    let _ = fs::remove_dir(&directory);
}

#[test]
fn affected_rows_stay_real_for_single_row_dml_and_remaining_table() {
    // 单行 DELETE 走 `Delete`（主键定位）路径，全表 DELETE 走 `DeleteScan` 路径，
    // 两者都必须回真实行数。
    let (mut client, directory) = connect_gateway("single");
    query_affected_rows(&mut client, "CREATE TABLE affp (id INT NOT NULL PRIMARY KEY, v INT)");
    assert_eq!(query_affected_rows(&mut client, "INSERT INTO affp (id, v) VALUES (1, 1), (2, 2)"), 2);
    assert_eq!(query_affected_rows(&mut client, "UPDATE affp SET v = 9 WHERE id > 0"), 2);
    assert_eq!(query_affected_rows(&mut client, "DELETE FROM affp WHERE id = 1"), 1);
    assert_eq!(query_affected_rows(&mut client, "DELETE FROM affp"), 1);
    drop(client);
    let _ = fs::remove_file(directory.join("local-0.yykv"));
    let _ = fs::remove_dir(&directory);
}

/// 解 SELECT 文本协议结果集：列数包 → 列定义包若干 → EOF → 行包若干 → EOF。
/// 只支持单列结果，行值按 length-encoded string 解出，`0xFB` 记为 `None`（NULL）。
fn query_text_rows(client: &mut TcpStream, sql: &str) -> Vec<Option<String>> {
    let mut payload = vec![COM_QUERY];
    payload.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &payload);
    let (_, column_count) = read_packet(client);
    assert_eq!(column_count.first(), Some(&0x01), "`{sql}` must return one column, got {column_count:?}");
    loop {
        let (_, packet) = read_packet(client);
        if is_eof_packet(&packet) {
            break;
        }
    }
    let mut rows = Vec::new();
    loop {
        let (_, packet) = read_packet(client);
        if is_eof_packet(&packet) {
            break;
        }
        let (length, offset) = read_lenenc(&packet, 0);
        if length == usize::MAX {
            rows.push(None);
        }
        else {
            rows.push(Some(String::from_utf8_lossy(&packet[offset..offset + length]).into_owned()));
        }
    }
    rows
}

fn is_eof_packet(packet: &[u8]) -> bool {
    packet.first() == Some(&0xFE) && packet.len() < 9
}

fn read_lenenc(payload: &[u8], offset: usize) -> (usize, usize) {
    match payload[offset] {
        0x00..=0xFA => (usize::from(payload[offset]), offset + 1),
        0xFB => (usize::MAX, offset + 1),
        0xFC => (usize::from(u16::from_le_bytes([payload[offset + 1], payload[offset + 2]])), offset + 3),
        0xFD => (usize::try_from(u32::from_le_bytes([payload[offset + 1], payload[offset + 2], payload[offset + 3], 0])).expect("3-byte length fits usize"), offset + 4),
        other => panic!("invalid length-encoded prefix {other} in row packet"),
    }
}

/// `UPDATE ... SET` 右值算术经文本协议端到端生效：点读路径与扫描路径都按当前行求值，
/// affectedRows 与读回的行值一起钉死，防止「计数对但值错」的回归。
#[test]
fn update_set_arithmetic_round_trips_over_the_wire() {
    let (mut client, directory) = connect_gateway("update-expr");
    query_affected_rows(&mut client, "CREATE TABLE inv (id INT NOT NULL PRIMARY KEY, stock INT)");
    assert_eq!(query_affected_rows(&mut client, "INSERT INTO inv (id, stock) VALUES (1, 10), (2, 20)"), 2);

    // 点读路径：`SET stock = stock - 1 WHERE id = 1`。
    assert_eq!(query_affected_rows(&mut client, "UPDATE inv SET stock = stock - 1 WHERE id = 1"), 1);
    assert_eq!(query_text_rows(&mut client, "SELECT stock FROM inv WHERE id = 1"), vec![Some("9".to_string())]);

    // 扫描路径：范围条件按数值序命中两行（旧字节边界会漏掉 id=1..9 里的一部分）。
    assert_eq!(query_affected_rows(&mut client, "UPDATE inv SET stock = stock + 5 WHERE id > 0"), 2);
    assert_eq!(query_text_rows(&mut client, "SELECT stock FROM inv WHERE id = 2"), vec![Some("25".to_string())]);
    assert_eq!(query_text_rows(&mut client, "SELECT stock FROM inv WHERE id = 1"), vec![Some("14".to_string())]);

    drop(client);
    let _ = fs::remove_file(directory.join("local-0.yykv"));
    let _ = fs::remove_dir(&directory);
}

/// 整数主键的范围条件经文本协议按数值序过滤：`id >= 50` 命中 51 行而不是字节序的 55 行。
#[test]
fn integer_primary_key_range_scan_counts_over_the_wire() {
    let (mut client, directory) = connect_gateway("pk-range");
    query_affected_rows(&mut client, "CREATE TABLE nums (id INT NOT NULL PRIMARY KEY, v INT)");
    let values: Vec<String> = (1..=100).map(|id| format!("({id}, {id})")).collect();
    assert_eq!(query_affected_rows(&mut client, &format!("INSERT INTO nums (id, v) VALUES {}", values.join(", "))), 100);
    assert_eq!(query_text_rows(&mut client, "SELECT COUNT(*) FROM nums WHERE id >= 50"), vec![Some("51".to_string())]);
    assert_eq!(query_text_rows(&mut client, "SELECT COUNT(*) FROM nums WHERE id BETWEEN 6 AND 46"), vec![Some("41".to_string())]);
    assert_eq!(query_text_rows(&mut client, "SELECT COUNT(*) FROM nums WHERE id < 10"), vec![Some("9".to_string())]);
    drop(client);
    let _ = fs::remove_file(directory.join("local-0.yykv"));
    let _ = fs::remove_dir(&directory);
}
